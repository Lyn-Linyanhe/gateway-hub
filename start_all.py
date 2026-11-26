#!/usr/bin/env python
"""一键启动网关与引擎：机器重启后跑一次。

网关自身**不会**拉起引擎进程，恢复后 `/health` 里 platforms 全 false 就是引擎没起来。
本脚本：

1. 网关监听端口不在 → 用 run-router.bat 脱离进程树启动，等端口就绪；
2. 对 config.json 里启用（不在 `disabled_platforms`）且配了 `launch` 的平台，
   按其 `base_url` 的端口判断引擎是否在跑，没跑的发
   `POST /api/platforms/process {action:start}`，再等端口就绪。

**判活一律用 TCP connect，不用 /health**：本机 loopback 的 HTTP 响应偶发丢失
（服务端 router.log 记了 200、客户端收不到任何字节），HTTP 探测的假阴性会把
「网关在跑」误判成「网关没跑」，于是白等一轮、还跳过引擎那一步。端口 connect
没有这个问题；HTTP 只在真正要下发动作和读平台开关时才用，失败也不影响判断。

用法：`start-all.bat` 或 `python start_all.py`（在 gateway-hub 目录下）。
"""

import json
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config.json"
LAUNCHER = ROOT / "run-router.bat"
LOADED = json.loads(CONFIG.read_text(encoding="utf-8"))


def port_open(port, timeout=1.5):
    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        return sock.connect_ex(("127.0.0.1", int(port))) == 0
    finally:
        sock.close()


def wait_port(port, seconds):
    """等到端口就绪；返回实际等待秒数，超时返回 None。"""
    started = time.time()
    while time.time() - started < seconds:
        if port_open(port):
            return round(time.time() - started, 1)
        time.sleep(1.5)
    return None


def http(path, key, payload=None, timeout=20):
    """本机 HTTP 调用（清代理）。失败返回 None——调用方按「结果未知」处理。"""
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        "http://127.0.0.1:%d%s" % (gateway_port(), path), data=data,
        headers={"x-api-key": key, "Content-Type": "application/json"},
        method="POST" if data else "GET")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except Exception:
        return None


def gateway_port():
    return int(LOADED.get("listen_port") or 8787)


def upstream_port(up):
    try:
        return urlsplit(str(up.get("base_url") or "")).port
    except ValueError:
        return None


def ensure_gateway():
    port = gateway_port()
    if port_open(port):
        print("网关已在运行（端口 %d）" % port)
        return True
    if not LAUNCHER.is_file():
        print("找不到 %s" % LAUNCHER)
        return False
    print("网关端口 %d 未监听，启动 run-router.bat ..." % port)
    subprocess.Popen(["cmd", "/c", str(LAUNCHER)], cwd=str(ROOT),
                     creationflags=getattr(subprocess, "DETACHED_PROCESS", 0))
    waited = wait_port(port, 90)
    if waited is None:
        print("等待 90s 后端口仍未监听，放弃")
        return False
    print("网关已就绪（%.1fs）" % waited)
    return True


def main():
    key = str(LOADED.get("api_key") or "").strip()
    disabled = {str(n) for n in (LOADED.get("disabled_platforms") or [])}

    if not ensure_gateway():
        return 1

    health = http("/health", key)
    if health:
        print("网关自述: version=%s platforms=%s" % (
            health.get("version"), json.dumps(health.get("platforms"), ensure_ascii=False)))
    else:
        print("网关自述 /health 读不到（HTTP 丢响应），继续按端口判断")

    targets = []
    for up in LOADED.get("upstreams") or []:
        name = str(up.get("name") or "")
        port = upstream_port(up)
        if not port:
            continue
        if name in disabled:
            print("skip  %-10s -> 已在 config.json 停用" % name)
            continue
        if not isinstance(up.get("launch"), dict):
            print("skip  %-10s -> 未配 launch（需手工/外部启动）" % name)
            continue
        if port_open(port):
            print("start %-10s -> 端口 %d 已在监听，跳过" % (name, port))
            continue
        targets.append((name, port))

    for name, port in targets:
        res = http("/api/platforms/process", key, {"name": name, "action": "start"}, timeout=150)
        brief = "（HTTP 无响应，按端口复查）" if res is None else json.dumps(
            {k: res.get(k) for k in ("ok", "pid", "error") if k in res}, ensure_ascii=False)
        waited = wait_port(port, 90)
        state = "端口 %d 就绪（%.1fs）" % (port, waited) if waited is not None else "端口 %d 起不来" % port
        print("start %-10s -> %s %s" % (name, brief, state))

    print("---- 结果 ----")
    print("网关      : %s (port %d)" % ("在线" if port_open(gateway_port()) else "离线", gateway_port()))
    for up in LOADED.get("upstreams") or []:
        name, port = str(up.get("name") or ""), upstream_port(up)
        if not port:
            continue
        tag = "停用" if name in disabled else ("在线" if port_open(port) else "离线")
        print("%-10s: %s (port %d)" % (name, tag, port))

    models = http("/v1/models", key, timeout=30)
    if models:
        counts = {}
        for m in models.get("data", []):
            head = m["id"].split("/")[0]
            counts[head] = counts.get(head, 0) + 1
        print("模型目录  : %d %s" % (len(models.get("data", [])), json.dumps(counts, ensure_ascii=False)))
    else:
        print("模型目录  : /v1/models 读不到（HTTP 丢响应）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
