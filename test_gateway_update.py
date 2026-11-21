"""Offline tests for the self-update release decision boundary."""
import json
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import gateway_update


class GatewayUpdateTests(unittest.TestCase):
    def git(self, root, *args):
        return subprocess.run(["git", "-C", str(root), *args], check=True,
                              capture_output=True, text=True)

    def make_repo(self, branch="main"):
        root = Path(tempfile.mkdtemp(prefix="gateway-update-test-"))
        self.git(root, "init", "-b", branch)
        self.git(root, "config", "user.email", "offline@example.invalid")
        self.git(root, "config", "user.name", "Offline Test")
        (root / "router.py").write_text('VERSION = "1.10.0"\n', encoding="utf-8")
        self.git(root, "add", "router.py")
        self.git(root, "commit", "-m", "initial")
        self.git(root, "remote", "add", "origin", "https://github.com/Lyn-Linyanhe/gateway-hub.git")
        return root

    def test_release_tag_and_commit_are_strictly_validated(self):
        self.assertEqual(gateway_update.version_tuple("v1.11.2"), (1, 11, 2))
        self.assertIsNone(gateway_update.version_tuple("v1.11.2-rc1"))
        self.assertIsNone(gateway_update.version_tuple("main"))
        self.assertTrue(gateway_update.is_newer("1.10.0", "v1.11.0"))
        self.assertFalse(gateway_update.is_newer("1.10.0", "v1.10.0"))

    def test_clean_main_checkout_is_deployable_but_local_change_is_not(self):
        root = self.make_repo()
        status = gateway_update.repository_status(root)
        self.assertTrue(status["is_repo"])
        self.assertTrue(status["origin_ok"])
        self.assertEqual(status["branch"], "main")
        self.assertTrue(status["clean"])
        (root / "router.py").write_text("# local edit\n", encoding="utf-8")
        self.assertFalse(gateway_update.repository_status(root)["clean"])

    def test_non_main_or_wrong_origin_checkout_is_not_deployable(self):
        feature = self.make_repo(branch="feature")
        self.assertEqual(gateway_update.repository_status(feature)["branch"], "feature")
        wrong = self.make_repo()
        self.git(wrong, "remote", "set-url", "origin", "https://github.com/other/repo.git")
        self.assertFalse(gateway_update.repository_status(wrong)["origin_ok"])

    def test_release_response_requires_a_stable_tag_and_commit(self):
        validate = gateway_update.validate_release
        digest = "a" * 40
        self.assertEqual(validate({"tag_name": "v1.11.0"}, digest),
                         ("v1.11.0", "1.11.0", digest))
        with self.assertRaises(ValueError):
            validate({"tag_name": "v1.11.0-rc1"}, digest)
        with self.assertRaises(ValueError):
            validate({"tag_name": "v1.11.0"}, "not-a-commit")

    def test_manifest_paths_cannot_replace_local_runtime_state(self):
        for name in ("config.json", "accounts/x.json", "auths/token.json",
                     "router.log", "cache.jsonl", "../escape.py"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                gateway_update._safe_archive_name(name)
        self.assertEqual(gateway_update._safe_archive_name("docs/readme.md").as_posix(),
                         "docs/readme.md")

    def test_prepare_release_checks_candidate_without_changing_installation(self):
        root = self.make_repo()
        old_head = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        files = {
            "router.py": b'VERSION = "1.11.0"\n',
            "config.example.json": b'{"api_key":""}\n',
            "test_regressions.py": b"assert True\n",
            "README.md": b"# release\n",
        }
        original_extract = gateway_update._extract_git_archive
        original_check = gateway_update._check_candidate
        try:
            def extract(_root, _commit, destination):
                for name, data in files.items():
                    target = Path(destination) / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
            gateway_update._extract_git_archive = extract
            gateway_update._check_candidate = lambda _candidate, _version: None
            gateway_update._fetch_release_commit = lambda _root, _latest: (
                "v1.11.0", "1.11.0", "b" * 40, old_head)
            info = gateway_update.prepare_release(root, {
                "tag": "v1.11.0", "version": "1.11.0", "commit": "b" * 40})
        finally:
            gateway_update._extract_git_archive = original_extract
            gateway_update._check_candidate = original_check
        self.assertEqual(info["previous_commit"], old_head)
        self.assertEqual(subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip(), old_head)
        self.assertTrue(gateway_update.repository_status(root)["clean"])


    def test_candidate_test_commands_match_ci(self):
        direct = gateway_update._candidate_test_command(Path("test_regressions.py"))
        discover = gateway_update._candidate_test_command(Path("test_gateway_update.py"))
        self.assertEqual(direct[:3], [sys.executable, "-I", "-B"])
        self.assertEqual(direct[-1], "test_regressions.py")
        # -I 直跑带本地 import 的脚本必然 ModuleNotFoundError；必须走 discover。
        self.assertIn("-m", discover)
        self.assertIn("unittest", discover)
        self.assertEqual(discover[-1], "test_gateway_update.py")

    def test_run_candidate_tests_passes_in_minimal_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            stage = Path(tmp)
            (stage / "test_gateway_update.py").write_text(
                "import unittest\n"
                "class T(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        self.assertTrue(True)\n"
                "\n"
                "if __name__ == '__main__':\n"
                "    unittest.main()\n", encoding="utf-8")
            gateway_update._run_candidate_tests(stage)

    def test_run_candidate_tests_reports_failure_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            stage = Path(tmp)
            (stage / "test_gateway_update.py").write_text(
                "import unittest\n"
                "class T(unittest.TestCase):\n"
                "    def test_bad(self):\n"
                "        self.fail('synthetic candidate failure')\n"
                "\n"
                "if __name__ == '__main__':\n"
                "    unittest.main()\n", encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                gateway_update._run_candidate_tests(stage)
            self.assertIn("test_gateway_update.py did not pass", str(ctx.exception))
            self.assertIn("synthetic candidate failure", str(ctx.exception))

    def test_pid_alive_distinguishes_living_and_dead_processes(self):
        if sys.platform != "win32":
            self.skipTest("os.kill(pid, 0) is reliable liveness on POSIX")
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
        try:
            time.sleep(0.4)
            self.assertTrue(gateway_update._pid_alive(proc.pid))
        finally:
            proc.kill()
            proc.wait()
        deadline = time.time() + 3
        while gateway_update._pid_alive(proc.pid) and time.time() < deadline:
            time.sleep(0.05)
        self.assertFalse(gateway_update._pid_alive(proc.pid),
                         "os.kill(pid, 0) reports dead Windows processes as alive")

    def test_stop_listeners_kills_port_owner(self):
        if sys.platform != "win32":
            self.skipTest("Windows-only listener kill path")
        script = ("import socket,time\n"
                  "s=socket.socket()\ns.bind(('127.0.0.1',18801))\ns.listen(1)\n"
                  "print('LISTENING', flush=True)\ntime.sleep(30)\n")
        flags = getattr(subprocess, "DETACHED_PROCESS", 0)
        proc = subprocess.Popen(
            [sys.executable.replace("python.exe", "pythonw.exe"), "-c", script],
            creationflags=flags)
        try:
            deadline = time.time() + 5
            while time.time() < deadline and proc.poll() is None:
                time.sleep(0.1)
            # 临时把健康端口指向实验端口，复用同一条强杀路径。
            root = Path(tempfile.mkdtemp(prefix="gw-stop-listener-"))
            try:
                (root / "config.json").write_text(
                    json.dumps({"listen_port": 18801}), encoding="utf-8")
                gateway_update._stop_listeners(root)
                self.assertFalse(
                    gateway_update._pid_alive(proc.pid),
                    "listener on the health port must be killed for rollback")
            finally:
                shutil.rmtree(root, ignore_errors=True)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    unittest.main()
