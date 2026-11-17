"""只读在线巡检；不改配置、不调用模型、不签到或导入账号。"""
import json
import os
import sys
import urllib.error
import urllib.request


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def valid_response(path, code, expected, data):
    """Verify each gateway contract, including legitimate empty catalogs."""
    if code != expected or not isinstance(data, dict):
        return False
    if expected == 401:
        return isinstance(data.get("error"), dict) and bool(data["error"].get("message"))
    if data.get("error"):
        return False
    if path == "/health":
        return (data.get("ok") is True and isinstance(data.get("version"), str)
                and isinstance(data.get("platforms"), dict)
                and all(value is True for value in data["platforms"].values()))
    if path == "/api/ping":
        return data.get("ok") is True
    if path == "/api/catalog":
        return data.get("ok") is True and isinstance(data.get("items"), list)
    if path == "/v1/models":
        return data.get("object") == "list" and isinstance(data.get("data"), list)
    if path == "/api/gwconfig":
        return (data.get("ok") is True and isinstance(data.get("hide_pro_locked"), bool)
                and data.get("wb_dual_prefer") in ("auto", "cn", "intl")
                and isinstance(data.get("wb_realms"), dict))
    return False


def local_opener():
    """Loopback only: never use environment proxies or follow redirects."""
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def run_checks(base, key, opener):
    failed = 0
    checks = [("/health", False, 200), ("/api/ping", False, 401),
              ("/api/ping", True, 200), ("/api/catalog", True, 200),
              ("/v1/models", True, 200), ("/api/gwconfig", True, 200)]
    for path, auth, expected in checks:
        headers = {"Authorization": "Bearer " + key} if auth else {}
        try:
            request = urllib.request.Request(base + path, headers=headers)
            try:
                response = opener.open(request, timeout=30)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                ok = valid_response(path, response.code, expected, json.load(response))
        except Exception as exc:
            ok = False
            print(type(exc).__name__, path)
        print("%s %s auth=%s" % ("PASS" if ok else "FAIL", path, auth))
        failed += not ok
    return 1 if failed else 0


def main():
    with open(os.path.join(os.path.dirname(__file__), "config.json"), encoding="utf-8") as f:
        config = json.load(f)
    base = "http://127.0.0.1:%d" % (config.get("listen_port") or 8787)
    opener = local_opener()
    return run_checks(base, config.get("api_key") or "", opener)


if __name__ == "__main__":
    sys.exit(main())
