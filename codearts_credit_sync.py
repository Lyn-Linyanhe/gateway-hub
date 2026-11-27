#!/usr/bin/env python
"""刷新 CodeArts 积分快照（codearts_credits.json）。

为什么需要这一步：网关自己拿不到 CodeArts 余额。引擎 codearts2api 的
credits 恒为 0（只记「领取动作」），而余额接口
`snap-manager/v1/statistics/plugin` 需要华为云 AK/SK 签名——**只有
workbuddy2api-hub 的 wb_codearts 客户端已经实现了这套签名**。

所以这里复用 WB 引擎的客户端（不启动引擎进程）：读 codearts2api 的
auths/*.json，签名请求上游，把结果按网关既有格式写回快照。

用法：
    python codearts_credit_sync.py          # 刷新并打印摘要
    python codearts_credit_sync.py --check  # 只打印，不写文件

退出码：0 成功；1 失败（网络/认证/上游异常）。
"""

import glob
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
WB_DIR = os.path.join(os.path.dirname(HERE), "workbuddy2api-hub")
CREDITS_FILE = os.path.join(HERE, "codearts_credits.json")
#: 余额历史：每次同步追加一条（值不变则跳过）。未来用来「余额差 ÷ 同期 token」
#: 反推 CodeArts 的 分/Mtok（它不返回逐请求扣费）。
WALLET_HISTORY = os.path.join(HERE, "codearts_wallet_history.jsonl")
AUTHS_DIR = os.path.join(os.path.dirname(HERE), "codearts2api", "auths")


class _Account:
    """wb_codearts.fetch_account_info 需要的最小账号表面。"""

    proxy = ""

    def __init__(self, auth):
        self.auth = dict(auth)
        self.uid = str(auth.get("user_id") or "")

    def credential(self):
        return {"access_key_id": self.auth.get("access_key_id") or "",
                "secret_access_key": self.auth.get("secret_access_key") or "",
                "security_token": self.auth.get("cloud_dragon_token") or ""}


def _load_auth():
    """取最新的一份 codearts2api 凭证（按 mtime）。"""
    paths = glob.glob(os.path.join(AUTHS_DIR, "codearts-*.json"))
    if not paths:
        raise SystemExit("没有找到 CodeArts 凭证：%s" % AUTHS_DIR)
    path = max(paths, key=os.path.getmtime)
    with open(path, encoding="utf-8") as handle:
        auth = json.load(handle)
    if not auth.get("cloud_dragon_token") or not auth.get("access_key_id"):
        raise SystemExit("凭证缺少 cloud_dragon_token / access_key_id：%s" % path)
    return path, auth


def _bucket(credits, key):
    data = credits.get(key) if isinstance(credits, dict) else None
    data = data if isinstance(data, dict) else {}

    def num(name):
        try:
            return round(float(data.get(name) or 0), 2)
        except (TypeError, ValueError):
            return 0.0

    return {"amount": num("amount"), "remain": num("remain"),
            "used": num("used"), "expiring": num("expiring")}


def build_snapshot(info, uid):
    """上游 info → 网关快照格式（保持与浏览器快照同一 schema）。"""
    credits = info.get("credits") if isinstance(info.get("credits"), dict) else {}
    total = _bucket(credits, "total")
    basic = _bucket(credits, "basic")
    bonus = _bucket(credits, "bonus")
    on_demand = _bucket(credits, "on_demand")
    return {
        "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "wb-codearts-client",
        "uid": uid,
        "package": str(info.get("package_name") or ""),
        "end_date": str(info.get("end_date") or ""),
        "basic": {"amount": basic["amount"], "remain": basic["remain"],
                  "used": basic["used"]},
        "bonus": {"amount": bonus["amount"], "remain": bonus["remain"],
                  "used": bonus["used"]},
        "on_demand": {"amount": on_demand["amount"], "remain": on_demand["remain"],
                      "used": on_demand["used"]},
        "total": {
            "amount": total["amount"],
            "remain": total["remain"],
            "used": total["used"],
            "expiring": total["expiring"],
            # 与浏览器快照保持兼容：available 就是可用余额，pool 是基础包
            "available": total["remain"],
            "pool": basic["remain"],
        },
    }


def _append_wallet(remain):
    """余额快照追加（只在值变化时写，避免每 30 分钟一条重复行）。"""
    last = None
    try:
        with open(WALLET_HISTORY, encoding="utf-8") as handle:
            lines = handle.readlines()[-1:]
        if lines:
            last = json.loads(lines[0]).get("balance")
    except (OSError, ValueError):
        last = None
    if last is not None and abs(float(last) - float(remain)) < 0.005:
        return
    row = {"ts": round(time.time(), 1), "account": "codearts", "metric": "balance",
           "balance": round(float(remain), 2)}
    try:
        with open(WALLET_HISTORY, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + chr(10))
    except OSError:
        pass


def _write_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main():
    check_only = "--check" in sys.argv
    if not os.path.isdir(WB_DIR):
        print("找不到 workbuddy2api-hub：%s" % WB_DIR)
        return 1
    # 清代理：WB 客户端与 7890 无关，代理由凭证/环境决定；死代理会让请求直接失败。
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(key, None)
    if WB_DIR not in sys.path:
        sys.path.insert(0, WB_DIR)
    try:
        import wb_codearts
    except Exception as exc:
        print("导入 wb_codearts 失败：%s: %s" % (type(exc).__name__, exc))
        return 1

    auth_path, auth = _load_auth()
    account = _Account(auth)
    try:
        info = wb_codearts.fetch_account_info(account, timeout=30.0)
    except Exception as exc:
        print("查询 CodeArts 余额失败：%s: %s" % (type(exc).__name__, exc))
        return 1

    snapshot = build_snapshot(info, account.uid)
    total = snapshot["total"]
    print("CodeArts %s | %s → %s/%s（已用 %s，临期 %s）到期 %s  凭证 %s" % (
        snapshot["package"] or "-", account.uid[:8], total["remain"], total["amount"],
        total["used"], total["expiring"], snapshot["end_date"] or "?",
        os.path.basename(auth_path)))
    if not check_only:
        _write_atomic(CREDITS_FILE, snapshot)
        _append_wallet(total["remain"])
        print("已写入 %s" % CREDITS_FILE)
    return 0


if __name__ == "__main__":
    sys.exit(main())