"""gateway-hub 全平台功能审计：逐接口断言，输出 PASS/FAIL 汇总。"""
import json
import time
import urllib.request
import urllib.error

BASE = "http://127.0.0.1:8787"
import os
KEY = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"), encoding="utf-8"))["api_key"]

results = []


def call(path, method="GET", body=None, auth=True, expect=200, base=BASE, timeout=90):
    headers = {}
    if auth:
        headers["Authorization"] = "Bearer " + KEY
    data = json.dumps(body).encode() if body is not None else None
    if data:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.code
            payload = json.loads(resp.read().decode("utf-8", "ignore") or "{}")
    except urllib.error.HTTPError as exc:
        code = exc.code
        try:
            payload = json.loads(exc.read().decode("utf-8", "ignore") or "{}")
        except Exception:
            payload = {}
    except Exception as exc:
        return 0, {"_transport_error": str(exc)}
    return code, payload


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


# ── 1. 基础与鉴权 ──
code, d = call("/health", auth=False)
check("GET /health 无鉴权可达", code == 200 and d.get("ok") is True, f"code={code}")
code, d = call("/api/ping", auth=False, expect=401)
check("GET /api/ping 无 Key 被拒 401", code == 401, f"code={code}")
code, d = call("/api/ping")
check("GET /api/ping 带 Key 200", code == 200 and d.get("ok") is True, f"code={code}")

# ── 2. 状态聚合 ──
code, d = call("/api/status")
plats = {p["name"]: p for p in d.get("platforms", [])}
check("/api/status 三平台", len(d.get("platforms", [])) == 3 and d["kpi"]["platforms_ok"] == 3)
check("/api/status 账号池非空", all(len(p["accounts"]) > 0 for p in d.get("platforms", [])),
      json.dumps({k: len(v["accounts"]) for k, v in plats.items()}))
check("/api/status 无跨平台积分合计", "credits_remain" not in d.get("kpi", {}))
check("/api/status qwenwork 真实余额", plats["qwenwork"]["accounts"][0].get("remain") is not None,
      json.dumps({a["name"]: a["remain"] for a in plats["qwenwork"]["accounts"]}))

# ── 3. 模型目录与可见性 ──
code, cat = call("/api/catalog")
total = cat.get("total", 0)
check("/api/catalog 目录非空", total > 30, f"total={total}")
# 回归调用目标动态挑「当前可见」的模型：用户随时在模型页增删隐藏，硬编码会假失败
def _pick_visible(platform, prefer=None):
    items = [i for i in cat.get("items", [])
             if i["platform"] == platform and not i.get("hidden") and not i.get("pro_locked")]
    if prefer:
        for p in prefer:
            if p in {i["id"] for i in items}:
                return p
    return items[0]["id"] if items else None
_wb_pick = _pick_visible("workbuddy", prefer=["workbuddy/glm-5.3-flash", "workbuddy/glm-5.3"])
_qoder_pick = _pick_visible("qoder", prefer=["qoder/Qwen3.8-Flash"])
check("wb/qoder 有可见模型可供回归", _wb_pick is not None and _qoder_pick is not None,
      f"wb={_wb_pick} qoder={_qoder_pick}")
# wb PRO 会员锁定：国际版 PRO 专属模型（账号池无付费账号时自动隐藏）
wb_items = [i for i in cat.get("items", []) if i["platform"] == "workbuddy"]
_locked = [i for i in wb_items if i.get("pro_locked")]
check("wb PRO 专属标注非空", len(_locked) >= 10, f"locked={len(_locked)}")
check("kimi-k3.1-pro 不锁定（国内版免费）",
      next((i for i in wb_items if i["model"] == "kimi-k3.1-pro"), {}).get("pro_locked") is False)
check("wb 目录含国际版专属模型（分区并集完整）",
      any(i["model"] == "max-model" for i in wb_items) and any(i["model"] == "gemini-4-carbon" for i in wb_items))
code, m = call("/v1/models")
_visible_ids = {x["id"] for x in m.get("data", [])}
check("/v1/models 不含 PRO 锁定模型",
      not any(i["id"] in _visible_ids for i in _locked), str(sorted(_visible_ids & {i['id'] for i in _locked})))
code, d = call("/v1/chat/completions", "POST",
               {"model": "workbuddy/gpt-6-sol", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5})
check("PRO 锁定模型调用被拒（话术含会员或已隐藏）",
      "付费会员" in str(d) or "隐藏" in str(d), json.dumps(d, ensure_ascii=False)[:120])
code, m = call("/v1/models")
visible_ids = [x["id"] for x in m.get("data", [])]
prefixed = [i for i in visible_ids if "/" in i]
bare = [i for i in visible_ids if "/" not in i]
_expected_prefix = len([i for i in cat.get("items", [])
                        if not i.get("hidden") and not i.get("pro_locked")])
check("/v1/models 全部带平台前缀（裸名不再列出）",
      len(bare) == 0 and len(prefixed) == _expected_prefix and len(visible_ids) == len(prefixed),
      f"v1={len(visible_ids)} prefix={len(prefixed)} bare={len(bare)} "
      f"期望前缀={_expected_prefix} (catalog={total} hidden={cat.get('hidden_count')})")
check("/v1/models 无双前缀套娃条目", not any(i.startswith("qwenwork/workbuddy/") for i in visible_ids))
# 隐藏/恢复闭环（先挑一个当前可见、非收藏的模型，避免污染用户已有的隐藏/收藏状态）
code, cat_now = call("/api/catalog")
_vishide = next((i["id"] for i in cat_now.get("items", [])
                 if i["platform"] == "workbuddy" and not i.get("hidden")
                 and i["id"] != "workbuddy/glm-5.3-flash"), None)
check("找到了可安全用于隐藏测试的模型", _vishide is not None, str(_vishide))
t0 = time.time()
code, d = call("/api/models/visibility", "POST", {"id": _vishide, "visible": False})
check("visibility 响应及时（不死锁）", code == 200 and time.time() - t0 < 30,
      f"code={code} 耗时={time.time() - t0:.1f}s")
check("隐藏后 hidden 列表包含该模型", code == 200 and _vishide in (d.get("hidden") or []),
      json.dumps(d, ensure_ascii=False)[:120])
code, m = call("/v1/models")
gone = not any(x["id"] == _vishide and x.get("owned_by") == "workbuddy" for x in m["data"])
check("隐藏后 /v1/models 消失（owned_by 归属判定）", gone)
code, d = call("/v1/chat/completions", "POST",
               {"model": _vishide, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5})
check("隐藏后调用被拒", "隐藏" in str(d), json.dumps(d, ensure_ascii=False)[:120])
code, d = call("/api/models/visibility", "POST", {"id": _vishide, "visible": True})
check("恢复显示（响应含 ok 且该模型已不在 hidden）",
      code == 200 and d.get("ok") is True and _vishide not in (d.get("hidden") or []),
      f"code={code} " + json.dumps(d, ensure_ascii=False)[:120])
check("调用回归可见模型", _wb_pick and call("/v1/chat/completions", "POST",
      {"model": _wb_pick, "messages": [{"role": "user", "content": "回复:OK"}], "max_tokens": 10})[0] == 200,
      str(_wb_pick))

# ── 4. 三平台路由 ──
# 各平台回归目标动态挑当前可见模型：用户随时在模型页增删隐藏，硬编码会假失败
code, cat_now = call("/api/catalog")
def _pick2(platform, prefer=None):
    items = [i for i in cat_now.get("items", [])
             if i["platform"] == platform and not i.get("hidden") and not i.get("pro_locked")]
    if prefer:
        for p in prefer:
            if p in {i["id"] for i in items}:
                return p
    return items[0]["id"] if items else None
route_targets = []
for plat, prefer in (("workbuddy", ["workbuddy/glm-5.3-flash", "workbuddy/glm-5.3"]),
                     ("qoder", ["qoder/Qwen3.8-Flash"]), ("qwenwork", None)):
    mid = _pick2(plat, prefer)
    if mid:
        route_targets.append((plat, mid))
for plat, model in route_targets:
    code, d = call("/v1/chat/completions", "POST",
                   {"model": model, "messages": [{"role": "user", "content": "回复两个字:成功"}], "max_tokens": 20})
    check(f"路由 {model}", code == 200 and "choices" in d,
          (d.get("choices", [{}])[0].get("message", {}).get("content", "")[:20] if "choices" in d
           else json.dumps(d, ensure_ascii=False)[:120]))
# 国际版专属免费模型：经 X-Realm 分区路由（Key 出口跟随面板时会跨区 400）
_sg = next((i["id"] for i in cat_now.get("items", [])
            if i["id"] == "workbuddy/deepseek-v4.1-flash-sg" and not i.get("hidden")), None)
if _sg:
    code, d = call("/v1/chat/completions", "POST",
                   {"model": _sg, "messages": [{"role": "user", "content": "回复两个字:成功"}], "max_tokens": 20},
                   timeout=300)
    check("wb 国际版专属模型分区路由可调用", code == 200 and "choices" in d,
          (d.get("choices", [{}])[0].get("message", {}).get("content", "")[:20] if "choices" in d
           else json.dumps(d, ensure_ascii=False)[:120]))
else:
    check("wb 国际版专属模型分区路由可调用", True, "deepseek-v4.1-flash-sg 已被隐藏，跳过")

# ── 5. 签到与任务 ──
for p in ("workbuddy", "qoder", "qwenwork"):
    code, d = call(f"/api/checkin/{p}", "POST", {})
    check(f"/api/checkin/{p}", code == 200 and d.get("ok") is True, json.dumps(d, ensure_ascii=False)[:100])
for action, plat in [("run", "qoder"), ("travel", "qoder"), ("claim", "workbuddy"),
                     ("streak-bonus", "workbuddy"), ("night", "workbuddy")]:
    code, d = call(f"/api/tasks/{action}/{plat}", "POST", {})
    check(f"/api/tasks/{action}/{plat}", code == 200 and d.get("ok") is True,
          json.dumps(d.get("result", d.get("error", "")), ensure_ascii=False)[:80])

# ── 6. 引擎代理（白名单） ──
for body in [{"platform": "workbuddy", "path": "accounts/refresh", "method": "POST", "body": {}},
             {"platform": "workbuddy", "path": "accounts/credits/fetch", "method": "POST", "body": {}},
             {"platform": "workbuddy", "path": "scheduler", "method": "GET"},
             {"platform": "qoder", "path": "accounts/refresh", "method": "POST", "body": {}},
             {"platform": "qwenwork", "path": "admin/credit-summary", "method": "GET"}]:
    code, d = call("/api/engine", "POST", body)
    check(f"/api/engine {body['platform']}/{body['path']}", code == 200 and d.get("ok") is True,
          json.dumps(d, ensure_ascii=False)[:100])
code, d = call("/api/engine", "POST",
               {"platform": "workbuddy", "path": "accounts/delete", "method": "POST",
                "body": {"uid": "x"}})
check("白名单外路径 403", code == 403, f"code={code}")

# ── 7. 日志与错误路径 ──
code, d = call("/api/logs")
check("/api/logs 有内容", len(d.get("lines", "")) > 100)
code, d = call("/api/login/poll", "GET", None)
check("/api/login/poll 缺参 400", code == 400)
# ── 网关自身开关 ──
code, d = call("/api/gwconfig")
check("/api/gwconfig 读取", code == 200 and d.get("ok") is True
      and isinstance(d.get("hide_pro_locked"), bool)
      and d.get("wb_dual_prefer") in ("auto", "cn", "intl"),
      json.dumps(d, ensure_ascii=False)[:150])
check("gwconfig 含两区临期概况", isinstance(d.get("wb_realms"), dict)
      and all(("min_days_left" in v and "paid" in v) for v in d.get("wb_realms", {}).values()),
      json.dumps(d.get("wb_realms"), ensure_ascii=False)[:150])
code, d = call("/api/gwconfig", "POST", {"wb_dual_prefer": "auto", "hide_pro_locked": True})
check("/api/gwconfig 写入合法值", code == 200 and d.get("ok") is True, json.dumps(d, ensure_ascii=False)[:100])
code, d = call("/api/gwconfig", "POST", {"wb_dual_prefer": "bogus"})
check("gwconfig 非法值被拒 400", code == 400, f"code={code}")
code, d = call("/api/qwenwork/reimport", "POST", {})
check("/api/qwenwork/reimport 响应明确", d.get("ok") is False or d.get("ok") is True,
      json.dumps(d, ensure_ascii=False)[:100])

# ── 汇总 ──
passed = sum(1 for _, ok, _ in results if ok)
print("=" * 60)
for name, ok, detail in results:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if not ok and detail else ""))
print("=" * 60)
print(f"通过 {passed}/{len(results)}")
fails = [(n, d) for n, ok, d in results if not ok]
if fails:
    print("失败项:", json.dumps(fails, ensure_ascii=False, indent=1))
