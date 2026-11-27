"""Safely check and deploy the official Gateway Hub stable GitHub Release."""
import ctypes
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath

OWNER = "Lyn-Linyanhe"
REPOSITORY = "gateway-hub"
REPO_URL = f"https://github.com/{OWNER}/{REPOSITORY}.git"
API_ROOT = f"https://api.github.com/repos/{OWNER}/{REPOSITORY}"
API_RELEASE = API_ROOT + "/releases/latest"
API_TAG = API_ROOT + "/git/ref/tags/"
MAX_JSON_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_FILES = 2000
MAX_ARCHIVE_EXPANDED = 64 * 1024 * 1024


def version_tuple(value):
    match = re.fullmatch(r"v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", str(value or ""))
    return tuple(map(int, match.groups())) if match else None


def is_newer(current, latest):
    old, new = version_tuple(current), version_tuple(latest)
    return bool(old and new and new > old)


def validate_release(release, commit):
    if not isinstance(release, dict) or release.get("draft") or release.get("prerelease"):
        raise ValueError("latest stable Release metadata is invalid")
    tag = str(release.get("tag_name") or "")
    version = version_tuple(tag)
    if version is None:
        raise ValueError("Release tag is not a stable semantic version")
    commit = str(commit or "").lower()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Release target commit is invalid")
    return tag if tag.startswith("v") else "v" + tag, ".".join(map(str, version)), commit


def repository_status(root):
    root = Path(root).resolve()
    git = subprocess.run(["git", "-C", str(root), "rev-parse", "--show-toplevel"],
                         capture_output=True, text=True, timeout=10)
    if git.returncode:
        return {"is_repo": False, "clean": None, "branch": None, "origin_ok": False}
    top = Path(git.stdout.strip()).resolve()
    branch = subprocess.run(["git", "-C", str(root), "branch", "--show-current"],
                            capture_output=True, text=True, timeout=10)
    remote = subprocess.run(["git", "-C", str(root), "remote", "get-url", "origin"],
                            capture_output=True, text=True, timeout=10)
    status = subprocess.run(["git", "-C", str(root), "status", "--porcelain=v1",
                             "--untracked-files=normal"], capture_output=True, text=True, timeout=10)
    url = remote.stdout.strip().lower().rstrip("/") if remote.returncode == 0 else ""
    origin_ok = url in {
        REPO_URL.lower(),
        f"git@github.com:{OWNER}/{REPOSITORY}.git".lower(),
        f"ssh://git@github.com/{OWNER}/{REPOSITORY}.git".lower(),
    }
    return {"is_repo": top == root, "clean": status.returncode == 0 and not status.stdout,
            "branch": branch.stdout.strip() if branch.returncode == 0 else None,
            "origin_ok": origin_ok}


def get_current_version(root):
    text = (Path(root) / "router.py").read_text(encoding="utf-8-sig")
    match = re.search(r'^VERSION\s*=\s*["\']([^"\']+)["\']', text, re.M)
    if not match or version_tuple(match.group(1)) is None:
        raise ValueError("cannot read valid installed Gateway Hub version")
    return match.group(1)


def _open_url(url, limit=MAX_JSON_BYTES, timeout=12):
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json", "User-Agent": "gateway-hub-updater"})
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        response = opener.open(request, timeout=timeout)
    except (OSError, urllib.error.URLError):
        response = urllib.request.urlopen(request, timeout=timeout)
    with response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("GitHub response exceeds the size limit")
    return data


def _tag_commit(tag):
    if not re.fullmatch(r"v?(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", tag):
        raise ValueError("release tag format is invalid")
    url = API_TAG + urllib.parse.quote(tag, safe="")
    ref = json.loads(_open_url(url))
    obj = ref.get("object") if isinstance(ref, dict) else None
    for _ in range(5):
        if not isinstance(obj, dict) or obj.get("type") not in ("tag", "commit"):
            raise ValueError("GitHub tag ref is malformed")
        sha = str(obj.get("sha") or "").lower()
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("GitHub tag SHA is invalid")
        if obj["type"] == "commit":
            return sha
        annotated = json.loads(_open_url(API_ROOT + "/git/tags/" + sha))
        obj = annotated.get("object") if isinstance(annotated, dict) else None
    raise ValueError("GitHub annotated tag is nested too deeply")


def fetch_latest_release():
    try:
        release = json.loads(_open_url(API_RELEASE))
        tag, version, _ = validate_release(release, "0" * 40)
        commit = _tag_commit(tag)
        tag, version, commit = validate_release(release, commit)
        return {"tag": tag, "version": version, "commit": commit,
                "url": str(release.get("html_url") or
                           f"https://github.com/{OWNER}/{REPOSITORY}/releases/tag/{tag}"),
                "published_at": str(release.get("published_at") or "")}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {str(exc)[:240]}"}


def _git(root, *args, timeout=30):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                          text=True, encoding="utf-8", errors="replace", timeout=timeout)


def _fetch_release_commit(root, latest):
    tag, version, commit = validate_release({"tag_name": latest.get("tag")}, latest.get("commit"))
    if tag != latest.get("tag") or version != latest.get("version"):
        raise ValueError("release version and tag do not match")
    fetched = _git(root, "fetch", "--no-tags", "origin", f"refs/tags/{tag}", timeout=60)
    if fetched.returncode:
        raise ValueError("could not fetch the selected official Release tag")
    actual = _git(root, "rev-parse", "FETCH_HEAD")
    if actual.returncode or actual.stdout.strip().lower() != commit:
        raise ValueError("fetched Release tag does not resolve to the verified commit")
    head = _git(root, "rev-parse", "HEAD")
    if head.returncode:
        raise ValueError("could not resolve installed Gateway Hub commit")
    ancestor = _git(root, "merge-base", "--is-ancestor", head.stdout.strip(), commit)
    if ancestor.returncode:
        raise ValueError("the official Release is not a fast-forward from this installation")
    return tag, version, commit, head.stdout.strip()


def _safe_archive_name(name):
    if "\\" in name or name.startswith("/") or re.match(r"^[A-Za-z]:", name):
        raise ValueError("release archive contains an unsafe path")
    path = PurePosixPath(name)
    if not path.parts or any(part in ("", ".", "..") for part in path.parts):
        raise ValueError("release archive contains path traversal")
    if any(part.lower() in {"accounts", "auths", ".git"} for part in path.parts):
        raise ValueError("release archive contains runtime state")
    if path.name.lower() in {"config.json", "router.log", "api_keys.json",
                             "api_key_usage.json"} \
            or path.name.lower().endswith(".jsonl"):
        raise ValueError("release archive contains runtime state")
    return path


def _extract_git_archive(root, commit, destination):
    archive = subprocess.run(["git", "-C", str(root), "archive", "--format=tar", commit],
                             capture_output=True, timeout=60)
    if archive.returncode or len(archive.stdout) > MAX_ARCHIVE_BYTES:
        raise ValueError("could not prepare a bounded source archive for the selected Release")
    dest = Path(destination).resolve()
    total = 0
    count = 0
    import io
    with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as tf:
        for member in tf:
            count += 1
            if count > MAX_ARCHIVE_FILES:
                raise ValueError("release archive contains too many entries")
            rel = _safe_archive_name(member.name)
            target = (dest / Path(*rel.parts)).resolve()
            if not target.is_relative_to(dest):
                raise ValueError("release archive path escapes staging directory")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ValueError("release archive contains a non-regular entry")
            total += member.size
            if member.size < 0 or total > MAX_ARCHIVE_EXPANDED:
                raise ValueError("expanded release archive exceeds size limit")
            target.parent.mkdir(parents=True, exist_ok=True)
            source = tf.extractfile(member)
            if source is None:
                raise ValueError("release archive entry is unreadable")
            with source, target.open("wb") as output:
                shutil.copyfileobj(source, output)


def _candidate_test_command(script):
    """候选目录里跑离线回归的命令。

    -I 隔离模式下脚本目录不在 sys.path：带本地 import 的测试脚本必须走
    unittest discover（与 CI/README 相同口径），否则 import gateway_update
    必然 ModuleNotFoundError，一键自更新在预检阶段 100% 失败。
    """
    name = Path(script).name
    if name == "test_gateway_update.py":
        return [sys.executable, "-I", "-B", "-m", "unittest", "discover",
                "-p", name]
    return [sys.executable, "-I", "-B", name]


def _run_candidate_tests(stage):
    for script in ("test_regressions.py", "test_gateway_update.py"):
        path = Path(stage) / script
        if path.is_file():
            test = subprocess.run(_candidate_test_command(path), cwd=str(stage),
                                  capture_output=True, text=True, timeout=600)
            if test.returncode:
                tail = "\n".join((test.stderr or test.stdout or "").splitlines()[-6:])
                raise ValueError(f"candidate {script} did not pass" + (f": {tail}" if tail else ""))


def _check_candidate(stage, version):
    stage = Path(stage)
    router = stage / "router.py"
    if not router.is_file():
        raise ValueError("release does not contain router.py")
    syntax = subprocess.run([sys.executable, "-I", "-B", "-m", "py_compile",
                             str(router)], cwd=str(stage), capture_output=True,
                            text=True, timeout=30)
    if syntax.returncode:
        raise ValueError("candidate Python syntax check failed")
    if get_current_version(stage) != version:
        raise ValueError("candidate router version does not match release tag")
    example = json.loads((stage / "config.example.json").read_text(encoding="utf-8-sig"))
    if example.get("api_key") or any(row.get("panel_password") == "admin"
                                      for row in example.get("upstreams", [])):
        raise ValueError("candidate example config contains an unsafe default credential")
    _run_candidate_tests(stage)


def prepare_release(root, latest):
    root = Path(root).resolve()
    status = repository_status(root)
    if not status["is_repo"] or not status["origin_ok"] or status["branch"] != "main" or not status["clean"]:
        raise ValueError("deployment requires a clean main checkout of the official repository")
    current = get_current_version(root)
    if not is_newer(current, latest.get("version")):
        raise ValueError("the latest official Release is not newer than this installation")
    tag, version, commit, old_head = _fetch_release_commit(root, latest)
    temp = Path(tempfile.mkdtemp(prefix="gateway-hub-candidate-", dir=str(root.parent)))
    try:
        _extract_git_archive(root, commit, temp)
        _check_candidate(temp, version)
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    return {"tag": tag, "version": version, "commit": commit, "previous_commit": old_head}


_STILL_ACTIVE = 0x00000103


def _pid_alive(pid):
    pid = int(pid)
    if os.name == "nt":
        # os.kill(pid, 0) 在 Windows 走 GenerateConsoleCtrlEvent(CTRL_C_EVENT)，
        # 只反映"能否发控制台事件"，不反映进程是否存在——实测对已退出进程
        # 仍返回成功。探活必须走 OpenProcess + GetExitCodeProcess。
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == _STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def deploy_release(root, latest, package_loader=None):
    """Verified local deployment transaction; no implicit service restart in tests."""
    root = Path(root).resolve()
    state = repository_status(root)
    if not state["is_repo"] or not state["origin_ok"] or state["branch"] != "main" or not state["clean"]:
        raise ValueError("deployment requires a clean main checkout of the official repository")
    current = get_current_version(root)
    if not is_newer(current, latest.get("version")):
        raise ValueError("the official Release is not newer than this installation")
    if package_loader is None:
        _tag, version, commit, _old_head = _fetch_release_commit(root, latest)
    else:
        _tag, version, commit = validate_release({"tag_name": "v" + str(latest["version"])},
                                                  latest["commit"])
    temp = Path(tempfile.mkdtemp(prefix="gateway-hub-update-", dir=str(root.parent)))
    backup = Path(tempfile.mkdtemp(prefix="gateway-hub-backup-", dir=str(root.parent)))
    replaced = []
    try:
        if package_loader is None:
            _extract_git_archive(root, commit, temp)
            _check_candidate(temp, version)
            files = [path.relative_to(temp) for path in temp.rglob("*") if path.is_file()]
        else:
            manifest = package_loader({"version": version, "commit": commit}, temp)
            _check_candidate(temp, version)
            files = [Path(name) for name in manifest["files"]]
        for relative in files:
            safe = _safe_archive_name(relative.as_posix())
            source = (temp / safe).resolve()
            destination = (root / safe).resolve()
            if not source.is_file() or not destination.is_relative_to(root):
                raise ValueError("candidate file is missing or escapes the installation")
        for relative in files:
            safe = _safe_archive_name(relative.as_posix())
            source, destination, saved = temp / safe, root / safe, backup / safe
            existed = destination.exists()
            if existed:
                if destination.is_dir():
                    raise ValueError("updater only replaces tracked files")
                saved.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, saved)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, destination)
            replaced.append((destination, saved if existed else None))
        _check_candidate(root, version)
        return {"ok": True, "version": version, "commit": commit}
    except Exception:
        for destination, saved in reversed(replaced):
            if saved is not None and saved.exists():
                os.replace(saved, destination)
            elif destination.exists():
                destination.unlink()
        raise
    finally:
        shutil.rmtree(temp, ignore_errors=True)
        shutil.rmtree(backup, ignore_errors=True)


def _launch_gateway(root):
    root = Path(root)
    if os.name == "nt":
        flags = (getattr(subprocess, "DETACHED_PROCESS", 0)
                 | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        return subprocess.Popen(["cmd.exe", "/c", str(root / "run-router.bat")],
                                cwd=str(root), stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                creationflags=flags, close_fds=True)
    return subprocess.Popen([sys.executable, str(root / "router.py")], cwd=str(root),
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)


def _wait_process_exit(pid, timeout=30):
    deadline = time.monotonic() + timeout
    while _pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.25)
    return not _pid_alive(pid)


def schedule_update(root, snapshot, parent_pid, launcher=None, wait_exit=None):
    """Validate and stage first, then spawn a child that waits for gateway exit."""
    root = Path(root).resolve()
    if not isinstance(snapshot, dict) or not snapshot.get("ok"):
        raise ValueError("check the latest official Release before deployment")
    latest = snapshot.get("latest")
    if not isinstance(latest, dict) or snapshot.get("has_update") is not True:
        raise ValueError("there is no newer stable official Release to deploy")
    state = repository_status(root)
    if not state["is_repo"] or not state["origin_ok"] or state["branch"] != "main" or not state["clean"]:
        raise ValueError("deployment requires a clean main checkout of the official repository")
    current_head = _git(root, "rev-parse", "HEAD")
    if current_head.returncode:
        raise ValueError("cannot resolve installed Git commit")
    staged = prepare_release(root, latest)
    if staged["previous_commit"] != current_head.stdout.strip():
        raise ValueError("local HEAD changed during update preflight")
    launcher = launcher or _launch_update_worker
    launcher(root, staged, int(parent_pid), current_head.stdout.strip())
    return {"ok": True, "status": "scheduled", "current": snapshot.get("current"),
            "version": staged["version"], "commit": staged["commit"]}


def _launch_update_worker(root, staged, parent_pid, expected_head):
    updater = Path(root) / "gateway_update.py"
    if os.name == "nt":
        flags = (getattr(subprocess, "DETACHED_PROCESS", 0)
                 | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        command = [sys.executable, "-I", str(updater), "--apply-commit", staged["commit"],
                   "--version", staged["version"], "--root", str(root),
                   "--parent-pid", str(parent_pid), "--expected-head", expected_head]
        kwargs = {"creationflags": flags, "close_fds": True}
    else:
        command = [sys.executable, "-I", str(updater), "--apply-commit", staged["commit"],
                   "--version", staged["version"], "--root", str(root),
                   "--parent-pid", str(parent_pid), "--expected-head", expected_head]
        kwargs = {"start_new_session": True, "close_fds": True}
    subprocess.Popen(command, cwd=str(root), stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kwargs)


def _health_port(root):
    try:
        config = json.loads((Path(root) / "config.json").read_text(encoding="utf-8-sig"))
        port = int(config.get("listen_port") or 8787)
    except Exception:
        port = 8787
    return port if 1 <= port <= 65535 else 8787


def _wait_gateway_health(root, timeout=30):
    deadline = time.monotonic() + timeout
    url = f"http://127.0.0.1:{_health_port(root)}/health"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                data = json.loads(response.read(64 * 1024).decode("utf-8"))
            if data.get("ok") and data.get("version") == get_current_version(root):
                return True
        except Exception:
            time.sleep(0.4)
    return False


def _stop_listeners(root):
    """强杀占用健康端口的监听进程（回滚时清掉占着端口的新版网关）。"""
    port = _health_port(root)
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-NetTCPConnection -State Listen -LocalPort "
                 f"{port} | Select-Object -ExpandProperty OwningProcess -Unique"],
                capture_output=True, text=True, timeout=20)
        except Exception:
            return
        for line in (out.stdout or "").split():
            if line.strip().isdigit():
                try:
                    subprocess.run(["taskkill", "/F", "/PID", line.strip()],
                                   capture_output=True, timeout=20)
                except Exception:
                    pass
        return
    try:
        subprocess.run(["fuser", "-k", f"{port}/tcp"], capture_output=True, timeout=20)
    except Exception:
        pass


def apply_release_after_exit(root, commit, version, parent_pid, expected_head=None):
    """Child updater: wait for gateway exit, fast-forward one verified Release, relaunch."""
    root = Path(root).resolve()
    deadline = time.monotonic() + 90
    while _pid_alive(parent_pid) and time.monotonic() < deadline:
        time.sleep(0.25)
    if _pid_alive(parent_pid):
        raise TimeoutError("running Gateway Hub did not exit in time")
    status = repository_status(root)
    if not status["is_repo"] or not status["origin_ok"] or status["branch"] != "main" or not status["clean"]:
        raise ValueError("local checkout changed before deployment; no files were updated")
    actual_head = _git(root, "rev-parse", "HEAD")
    if actual_head.returncode or (expected_head and actual_head.stdout.strip() != expected_head):
        raise ValueError("local HEAD changed after update preflight; no files were updated")
    latest = {"tag": "v" + version, "version": version, "commit": commit}
    _tag, verified_version, verified_commit, previous = _fetch_release_commit(root, latest)
    if verified_version != version or verified_commit != commit:
        raise ValueError("selected Release tag changed after preflight")
    merged = _git(root, "merge", "--ff-only", commit)
    if merged.returncode:
        raise ValueError("Git fast-forward deployment failed; checkout was not forcibly reset")
    if get_current_version(root) != version:
        _git(root, "reset", "--keep", previous)
        _launch_gateway(root)
        raise ValueError("deployed router version did not match the selected Release; rollback attempted")
    proc = _launch_gateway(root)
    if not _wait_gateway_health(root, timeout=45):
        # 新版仍占着健康端口时，旧版绑不上；先清掉监听者再回滚，
        # 否则磁盘是旧版、内存是坏的新版，报错里的"已恢复"是假的。
        _stop_listeners(root)
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
        time.sleep(1.0)  # 留给端口释放
        _git(root, "reset", "--keep", previous)
        _launch_gateway(root)
        raise RuntimeError("updated gateway failed health check; previous version was restored")
    return {"ok": True, "version": version, "commit": commit}


def cli(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Gateway Hub stable Release updater")
    parser.add_argument("--apply-commit", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--root", default=str(Path(__file__).resolve().parent))
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args(argv)
    try:
        result = apply_release_after_exit(args.root, args.apply_commit, args.version,
                                         args.parent_pid, expected_head=args.expected_head)
        print(json.dumps(result))
        return 0
    except Exception as exc:
        print(f"Gateway Hub update failed: {type(exc).__name__}: {str(exc)[:240]}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
