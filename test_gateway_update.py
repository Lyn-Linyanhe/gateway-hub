"""Offline tests for the self-update release decision boundary."""
import subprocess
import tempfile
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


if __name__ == "__main__":
    unittest.main()
