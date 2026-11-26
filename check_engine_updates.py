#!/usr/bin/env python
"""检查各上游引擎的版本、最新发布与本地补丁状态。

给「每周更新」任务和人工排查共用。只读，不改任何文件。

各引擎的版本来源与更新机制都不统一（有的读常量、有的读 CHANGELOG、
有的只能读二进制），这里把差异固化在一张表里，避免每次重新摸索。

用法：
    python check_engine_updates.py            # 人类可读表格
    python check_engine_updates.py --json     # 机器可读，供定时任务处理
    python check_engine_updates.py --no-net   # 只读本地版本与补丁，不查 GitHub
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = Path("E:/fascinating")

#: 每个引擎的探测方式。kind 决定版本从哪里读：
#:   const     —— 源码里的 VERSION 常量（regex 抓）
#:   changelog —— CHANGELOG.md 顶部的 ## vX.Y.Z
#:   http      —— 跑起来时从 HTTP 端点读（需要引擎在线）
#:   health    —— 从 /health 的 Server 响应头读
SPECS = [
    {
        "name": "gateway-hub",
        "dir": BASE / "gateway-hub",
        "repo": "Lyn-Linyanhe/gateway-hub",
        "kind": "const",
        "file": "router.py",
        "pattern": r'^VERSION\s*=\s*"([^"]+)"',
        "update": "内置自更新（gateway_update.py + update.ps1），要求 clean main",
        # 网关自己的本地改动由 git status 判定，不算「引擎补丁」
        "self": True,
    },
    {
        "name": "workbuddy2api-hub",
        "dir": BASE / "workbuddy2api-hub",
        "repo": "niyouqianma/workbuddy2api-hub",
        "kind": "const",
        "file": "wb_proxy.py",
        "pattern": r'server_version\s*=\s*"wb-proxy/([^"]+)"',
        "update": "release zip 覆盖（便携更新器在本机不可用：无 release-manifest.json）",
        "patch_note": "wb_proxy.py note_stream_limit 流内限额记账 + reopen_switch 换号重试；"
                      "wb_accounts.py reserve_activated 门槛语义",
    },
    {
        "name": "xiaohuanxiong2api",
        "dir": BASE / "xiaohuanxiong2api",
        "src_dir": BASE / "_build_xhx2api" / "src",
        "repo": "rsfree/xhx2api",
        "kind": "const",
        "file": "internal/version/version.go",
        "file_root": "src_dir",   # 版本文件在源码树里，不在部署目录
        "pattern": r'var Version\s*=\s*"([^"]+)"',
        "update": "替换 exe（部署目录非 git；源码树在 _build_xhx2api/src，需 go build）",
        "patch_note": "internal/upstream/account.go dailyBatchRemain（FEFO 剩余额度）+ "
                      "points_audit.go expire_points 透出",
    },
    {
        "name": "trae2api-hub",
        "dir": BASE / "trae2api-hub",
        "repo": "shuishuipingan/trae2api-hub",
        "kind": "const",
        "file": "trae_proxy.py",
        "pattern": r'^VERSION\s*=\s*"([^"]+)"',
        "update": "引擎内 /update/apply（整树覆盖式复制，不查 git status）",
        "patch_note": "SOLO 通道透传缓存/积分字段 + SOLO 分组/风控/配额（2 个未推送提交）",
    },
    {
        "name": "qoder2api-hub",
        "dir": BASE / "qoder2api-hub",
        "repo": "shuishuipingan/qoder2api-hub",
        "kind": "const",
        "file": "qoder_proxy.py",
        "pattern": r'^VERSION\s*=\s*"([^"]+)"',
        "update": "引擎内 /update/apply（不看 git 状态，直接覆盖 .py）",
        "patch_note": "签到窗口 10:00 边界 + 账号级并发 inflight/concurrencyLimit + "
                      "scheduler status() 重算 next_run_time",
    },
    {
        "name": "Buddy2api",
        "dir": BASE / "Buddy2api",
        "repo": "wicm84266964/Buddy2api",
        "kind": "const",
        "file": "version.py",
        "pattern": r'^VERSION\s*=\s*"([^"]+)"',
        "update": "引擎内 self_update（工作区脏时会拒绝，这是保护）",
        "patch_note": "qwenwork provider 工具调用支持（chat.py +253/-115）+ store/token/database",
    },
    {
        "name": "codearts2api",
        "dir": BASE / "codearts2api",
        "repo": "fkfbnv/codearts2api-hardened",
        "kind": "changelog",
        "file": "CHANGELOG.md",
        "pattern": r"^##\s*(v[\d.]+)",
        "update": "下载 codearts2api-windows.zip → 覆盖 bin/ + scripts/（保留 config/auths/data）",
        "patch_note": "",
    },
    {
        "name": "qwenwork2api",
        "dir": BASE / "qwenwork2api",
        # 原仓库 Alphaxiaoteng/qwenwork2api 已 301 迁移到 AlphaFDE/qwenwork2api
        "repo": "AlphaFDE/qwenwork2api",
        "kind": "const",
        "file": "main.py",
        "pattern": r'"version":\s*"([\d.]+)"',
        "update": "无更新机制（且未接入网关，qwenwork 平台指向 Buddy2api）",
        "patch_note": "",
    },
]


def _run(cmd, cwd=None, timeout=30):
    try:
        out = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                             timeout=timeout, shell=isinstance(cmd, str))
        return out.returncode, (out.stdout or ""), (out.stderr or "")
    except Exception:
        return -1, "", ""


def read_version(spec):
    """从源码/CHANGELOG 读当前版本。读不到返回 None。

    file_root="src_dir" 表示版本文件在源码树里（xhx2api 的部署目录只有 exe）。
    """
    root = spec.get("src_dir") if spec.get("file_root") == "src_dir" else spec["dir"]
    path = (root or spec["dir"]) / spec["file"]
    if not path.is_file():
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    m = re.search(spec["pattern"], text, re.M)
    return m.group(1) if m else None


def git_state(directory):
    """返回 {is_repo, branch, head, dirty_files, ahead}；非 git 返回 is_repo=False。"""
    code, out, _ = _run(["git", "rev-parse", "--show-toplevel"], cwd=str(directory))
    if code != 0:
        return {"is_repo": False}
    code, branch, _ = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(directory))
    code2, head, _ = _run(["git", "rev-parse", "--short", "HEAD"], cwd=str(directory))
    code3, status, _ = _run(["git", "status", "--porcelain"], cwd=str(directory))
    dirty = [ln for ln in (status or "").splitlines() if ln.strip()]
    # 与上游的差距（shallow 仓库可能没有 upstream，失败就当未知）
    code4, ahead_out, _ = _run(["git", "rev-list", "--count", "@{upstream}..HEAD"],
                               cwd=str(directory))
    ahead = int(ahead_out.strip()) if code4 == 0 and ahead_out.strip().isdigit() else None
    return {"is_repo": True, "branch": (branch or "").strip(),
            "head": (head or "").strip(), "dirty_files": dirty, "ahead": ahead}


def local_patches(spec):
    """本地补丁判定：git 脏文件 + *.bak* 备份文件。"""
    found = {"dirty_files": [], "backup_files": [], "note": spec.get("patch_note", "")}
    directory = spec["dir"]
    # xhx2api 的补丁在源码树里，不在部署目录
    probe = spec.get("src_dir") or directory
    state = git_state(probe)
    if state.get("is_repo"):
        found["dirty_files"] = state.get("dirty_files") or []
        found["ahead"] = state.get("ahead")
    try:
        found["backup_files"] = sorted(
            p.name for p in directory.glob("*.bak*") if p.is_file())
    except OSError:
        pass
    found["has_patch"] = bool(found["dirty_files"] or found["backup_files"]
                              or found.get("ahead"))
    return found


def github_latest(repo, timeout=15):
    """GitHub 最新 release tag。先 API，失败回退 release 页 302。

    本机 7890 代理常常是死的，所以显式清代理直连（GitHub 可直连）。
    API 匿名配额只有 60 次/小时，一周查一次够用；但撞上 403 时回退到
    release 页的 302 Location（那里带 tag），不吃 API 配额。
    """
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    api_err = None
    url = "https://api.github.com/repos/%s/releases/latest" % repo
    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "gateway-hub-update-check"})
        with opener.open(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
        tag = str(data.get("tag_name") or "").strip()
        if tag:
            return tag, None
        api_err = "release 里没有 tag_name"
    except urllib.error.HTTPError as exc:
        api_err = ("上游没有 release（404）" if exc.code == 404
                   else "GitHub API HTTP %s" % exc.code)
    except Exception as exc:
        api_err = "%s: %s" % (type(exc).__name__, str(exc)[:80])

    # 回退：release 页的 302 会跳到 /releases/tag/<tag>
    page = "https://github.com/%s/releases/latest" % repo
    try:
        req = urllib.request.Request(page, headers={"User-Agent": "gateway-hub-update-check"})
        with opener.open(req, timeout=timeout) as resp:  # 跟随跳转后看最终 URL
            final = resp.geturl() or ""
        m = re.search(r"/releases/tag/([^/?#]+)", final)
        if m:
            return m.group(1), None
        return None, api_err or "release 页未跳转到 tag"
    except Exception as exc:
        return None, "%s（回退也失败: %s）" % (api_err, type(exc).__name__)


def normalize(tag):
    """v1.2.3 / 1.2.3 统一成可比较的元组。"""
    if not tag:
        return None
    nums = re.findall(r"\d+", str(tag))
    return tuple(int(n) for n in nums[:4]) if nums else None


def collect(with_net=True):
    rows = []
    for spec in SPECS:
        row = {
            "name": spec["name"],
            "dir": str(spec["dir"]),
            "repo": spec["repo"],
            "exists": spec["dir"].is_dir(),
            "current": None,
            "latest": None,
            "latest_error": None,
            "update_method": spec["update"],
            "patch": None,
            "has_update": None,
            "safe_auto": False,
        }
        if not row["exists"]:
            rows.append(row)
            continue
        row["current"] = read_version(spec)
        row["patch"] = local_patches(spec)
        if with_net:
            latest, err = github_latest(spec["repo"])
            row["latest"] = latest
            row["latest_error"] = err
            cur, lat = normalize(row["current"]), normalize(latest)
            row["has_update"] = bool(cur and lat and lat > cur)
        # 无补丁 + 有更新机制 + 不是网关自己 → 可安全自动更新
        row["safe_auto"] = (row["exists"]
                            and not spec.get("self")
                            and not (row["patch"] or {}).get("has_patch"))
        rows.append(row)
    return rows


def print_table(rows):
    print("%-20s %-10s %-10s %-6s %-8s %s" % (
        "引擎", "当前", "最新", "有更新", "本地补丁", "更新方式"))
    print("-" * 118)
    for r in rows:
        if not r["exists"]:
            print("%-20s %s" % (r["name"], "（目录不存在）"))
            continue
        patch = r["patch"] or {}
        patch_desc = "有" if patch.get("has_patch") else "无"
        if patch.get("dirty_files"):
            patch_desc += "(%d 文件改动)" % len(patch["dirty_files"])
        if patch.get("backup_files"):
            patch_desc += "(%d 个 .bak)" % len(patch["backup_files"])
        upd = {True: "是", False: "否", None: "?"}[r["has_update"]]
        latest = r["latest"] or (r["latest_error"] or "?")
        print("%-20s %-10s %-10s %-6s %-8s %s" % (
            r["name"], r["current"] or "?", latest, upd, patch_desc,
            r["update_method"][:44]))
        if patch.get("note"):
            print("%22s 补丁内容: %s" % ("", patch["note"]))
    print()
    auto = [r["name"] for r in rows if r["safe_auto"] and r["has_update"]]
    manual = [r["name"] for r in rows if r["has_update"] and not r["safe_auto"]]
    print("可自动更新（无本地补丁）: %s" % (", ".join(auto) or "无"))
    print("需人工处理（有本地补丁，更新会覆盖）: %s" % (", ".join(manual) or "无"))


def main():
    # Windows 控制台默认 GBK，管道给别的进程时会乱码/解码失败；强制 UTF-8。
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    with_net = "--no-net" not in sys.argv
    rows = collect(with_net=with_net)
    if "--json" in sys.argv:
        print(json.dumps({"rows": rows}, ensure_ascii=False, indent=2))
    else:
        print_table(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
