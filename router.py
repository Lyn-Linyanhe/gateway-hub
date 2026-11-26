#!/usr/bin/env python3
"""gateway-hub 聚合控制台：多平台 2api 网关的统一入口 + wb 风格看板。

设计原则：
- 纯标准库，无第三方依赖，与上游各引擎（workbuddy2api-hub / qoder2api-hub / Buddy2api）解耦。
- 上游各自的账号池、签到调度、看板都留在各自引擎里；这里做协议汇聚、按模型路由，
  并通过各引擎的面板 API 聚合展示账号池 / 积分 / 签到状态。
- 一个对外 API Key；模型名支持 `平台/模型` 精确指定，裸模型名在全网关唯一时可直接用。
"""

import contextlib
import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "config.json")
LOG_PATH = os.path.join(HERE, "router.log")
DASHBOARD_PATH = os.path.join(HERE, "dashboard.html")

_log_lock = threading.Lock()

# 本机引擎始终直连；urllib 的默认 opener 会缓存进程启动时的系统代理。
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Internal API redirects must never forward panel or API credentials."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
# 外部只读检查（GitHub release）优先直连：本机残留的失效代理会让更新检测
# 误报“不可达”；直连失败再回退系统代理，需要代理的网络仍能用。
_direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _urlopen(request, timeout):
    url = request.full_url if isinstance(request, urllib.request.Request) else request
    host = (urlsplit(url).hostname or "").lower()
    if host in ("127.0.0.1", "localhost", "::1"):
        return _local_opener.open(request, timeout=timeout)
    return urllib.request.urlopen(request, timeout=timeout)


# 网关视角的在飞推理请求数（/v1/*，流式算到流写完）：平台卡片「并发」展示用。
# 这是"经过网关的"并发，直连引擎的请求不计入——与各引擎自己的账号级在飞数互补。
_inflight_lock = threading.Lock()
_inflight = {}


@contextlib.contextmanager
def _platform_inflight(name):
    with _inflight_lock:
        _inflight[name] = _inflight.get(name, 0) + 1
    try:
        yield
    finally:
        with _inflight_lock:
            _inflight[name] = max(0, _inflight.get(name, 0) - 1)


def log(msg):
    line = time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg
    with _log_lock:
        try:
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass
        print(line, flush=True)


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


CONFIG = load_config()

# RLock：visibility/favorite/rate 三个 handler 在自己的 with _config_lock 块内
# 调 save_config()（其内部再取同一把锁）。Lock 不可重入 → 自死锁，整个网关
# 的配置写路径与 /api/status 全部挂死（「模型隐藏点了没反应」的根因）。
_config_lock = threading.RLock()


def save_config():
    """原子写 config.json：临时文件 + rename，全程持锁。

    并发写坏过一次的教训：visibility/rate/favorite/observe 各自 json.dump，
    无锁无原子性，两个线程交错会留下半截文件（下次启动 load_config 直接失败）。
    """
    with _config_lock:
        tmp = CONFIG_PATH + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(CONFIG, f, ensure_ascii=False, indent=2)
            os.replace(tmp, CONFIG_PATH)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


# ---------------------------------------------------------------------------
# 上游模型目录缓存：避免每次 /v1/models 都打上游。
# ---------------------------------------------------------------------------

_catalog_lock = threading.Lock()
_catalog_cache = {}  # name -> (epoch, entries)


def upstream_rows(up):
    """拉取单个上游的 /v1/models 完整行（含 credits/price_factor 等字段），带 15s 缓存。"""
    now = time.time()
    with _catalog_lock:
        hit = _catalog_cache.get(up["name"])
        if hit and now - hit[0] < 15:
            return hit[1]
    try:
        if up["name"] == "workbuddy":
            # workbuddy 按分区各拉一份再并集：realm=all 的合并目录会漏模型
            # （实测少 gemini-4-argon/-carbon/max-model）。同 id 双区都有时
            # 优先国内行——双区模型在国内版都是免费档、账号池也以国内为主。
            # 单区失败只降级用另一区（两个都失败才算上游不可用），否则引擎
            # 一抖整个平台目录就消失。
            merged = {}
            realm_errors = []
            for realm in ("cn", "intl"):
                try:
                    req = urllib.request.Request(
                        up["base_url"].rstrip("/") + f"/v1/models?realm={realm}",
                        headers=_upstream_headers(up),
                    )
                    with _urlopen(req, timeout=10) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                    for m in (data.get("data") or []):
                        if m.get("id") and m["id"] not in merged:
                            merged[m["id"]] = m
                except Exception as exc:
                    realm_errors.append(f"{realm}: {type(exc).__name__}")
            if merged:
                rows = list(merged.values())
                if realm_errors:
                    log(f"models[workbuddy] 分区降级（{'; '.join(realm_errors)}），仅 {len(rows)} 个模型")
            else:
                rows = None
                log(f"models[workbuddy] 双区全部失败: {'; '.join(realm_errors)}")
        else:
            req = urllib.request.Request(
                up["base_url"].rstrip("/") + "/v1/models",
                headers=_upstream_headers(up),
            )
            with _urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            rows = [m for m in (data.get("data") or []) if m.get("id")]
    except Exception as exc:
        log(f"models[{up['name']}] 上游不可用: {type(exc).__name__}: {exc}")
        rows = None
    if rows is not None:
        with _catalog_lock:
            _catalog_cache[up["name"]] = (now, rows)
    return rows


def upstream_models(up):
    rows = upstream_rows(up)
    return None if rows is None else [r.get("id") for r in rows]


# ---------------------------------------------------------------------------
# workbuddy 分区目录：模型 → 可用区域 / PRO 会员门槛。
# 国际版上游目录对 PRO 专属模型标注 entitlement.required=standard，免费账号
# 调用会被引擎本地拦截（「该模型仅限 PRO 账号」）。realm=all 的合并行会把
# 双区同名模型（如 kimi-k3.1-pro 国内版免费）的标注丢掉，所以按分区各拉
# 一份再合并：锁定判定 = 可用区域全部带 PRO 门槛。
# ---------------------------------------------------------------------------

_wb_realm_lock = threading.Lock()
_wb_realm_cache = {"ts": 0.0, "meta": {}, "refreshing": False}

# 与 wb 引擎 wb_proxy.INTL_MODEL_ALIASES 保持一致：国际版出口对老 id 的改名
# 映射（引擎转发时自动重写为上游真名，客户端和目录始终用逻辑 id）。
WB_INTL_ALIAS = {"deepseek-v4.1-flash": "deepseek-v4.1-flash-sg"}


def _wb_realm_fetch(up):
    """拉双区目录并写缓存；全部分区都失败时保留旧 meta（只推进 ts）。"""
    meta = {}
    ok_any = False
    if up:
        for realm in ("cn", "intl"):
            try:
                req = urllib.request.Request(
                    up["base_url"].rstrip("/") + f"/v1/models?realm={realm}",
                    headers=_upstream_headers(up))
                with _urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                ok_any = True
                for m in (data.get("data") or []):
                    mid = m.get("id")
                    if not mid:
                        continue
                    ent = meta.setdefault(mid, {"realms": [], "pro": []})
                    ent["realms"].append(realm)
                    if (m.get("entitlement") or {}).get("required"):
                        ent["pro"].append(realm)
            except Exception as exc:
                log(f"wb 分区目录[{realm}] 拉取失败: {type(exc).__name__}: {exc}")
                continue  # 分区目录拉不到就当未知：不标注、不隐藏、不注入区域
    with _wb_realm_lock:
        _wb_realm_cache["ts"] = time.time()
        if ok_any:
            _wb_realm_cache["meta"] = meta
        _wb_realm_cache["refreshing"] = False
    return meta


def _wb_realm_meta():
    """workbuddy 双区目录元数据（15s 缓存，过期后台刷新）。

    这份元数据在 /v1/chat 的路由热路径上（决定 X-Realm）。旧实现过期时**同步**
    拉 /v1/models?realm=cn+intl（实测 ~80ms，引擎忙时更久），等于每 15s 卡一个
    请求的首字。改成 stale-while-revalidate：过期先用手上的旧值，后台单飞刷新
    （模型增删晚 15s 生效不影响正确性）。
    """
    now = time.time()
    with _wb_realm_lock:
        if _wb_realm_cache["meta"] and now - _wb_realm_cache["ts"] < 15:
            return _wb_realm_cache["meta"]
        stale = _wb_realm_cache["meta"]
        start = bool(stale) and not _wb_realm_cache["refreshing"]
        if start:
            _wb_realm_cache["refreshing"] = True
    up = next((u for u in CONFIG["upstreams"] if u["name"] == "workbuddy"), None)
    if stale:
        if start and up:
            threading.Thread(target=_wb_realm_fetch, args=(up,), daemon=True).start()
        return stale
    return _wb_realm_fetch(up)  # 首次启动：没有旧值，只能同步拉一次


_wb_state_lock = threading.Lock()
_wb_state_cache = {"ts": 0.0, "state": {}, "refreshing": False}


def _wb_pool_fetch(up):
    """拉 /accounts?realm=all 并写池状态缓存；拉不到沿用上次结果。"""
    state = {"realms": {}, "paid_any": False, "known": False}
    if up:
        try:
            data = engine_request(up, "GET", "/accounts?realm=all")
            items = data.get("accounts") if isinstance(data, dict) else data
            if not isinstance(items, list) or any(not isinstance(a, dict) for a in items):
                raise ValueError("账号池响应缺少 accounts 数组")
            state["known"] = True
            for a in items:
                if not a.get("enabled", True) or a.get("accountBanned") or a.get("ready") is False:
                    continue
                realm = a.get("realm")
                if realm not in ("cn", "intl"):
                    state["known"] = False
                    continue
                paid = bool((a.get("tier") or {}).get("paid") or a.get("is_paid_user"))
                state["paid_any"] = state["paid_any"] or paid
                ent = state["realms"].setdefault(
                    realm, {"min_days_left": None, "expiring_remain": 0.0, "paid": False})
                ent["paid"] = ent["paid"] or paid
                ee = (a.get("credits") or {}).get("earliest_expiring") or {}
                try:
                    dl = float(ee.get("days_left")) if ee.get("days_left") is not None else None
                except (TypeError, ValueError):
                    dl = None
                remain = _num(ee.get("remain"))
                if dl is not None and remain > 0:
                    if ent["min_days_left"] is None or dl < ent["min_days_left"]:
                        ent["min_days_left"] = dl
                    ent["expiring_remain"] = round(ent["expiring_remain"] + remain, 2)
        except Exception:
            state = _wb_state_cache.get("state") or state  # 拉不到沿用上次结果
    with _wb_state_lock:
        _wb_state_cache["ts"] = time.time()
        _wb_state_cache["state"] = state
        _wb_state_cache["refreshing"] = False
    return state


def _wb_pool_state():
    """wb 账号池概览（60s 缓存，过期后台刷新）：各区临期包紧迫度 + 是否有付费账号。

    {"realms": {realm: {"min_days_left", "expiring_remain", "paid"}},
     "paid_any": bool} —— 供双区模型出口选择与 PRO 锁定判定共用。
    与 _wb_realm_meta 同理走 stale-while-revalidate：过期不阻塞请求首字。
    """
    now = time.time()
    with _wb_state_lock:
        if now - _wb_state_cache["ts"] < 60:
            return _wb_state_cache["state"]
        stale = _wb_state_cache["state"]
        start = bool(stale) and not _wb_state_cache["refreshing"]
        if start:
            _wb_state_cache["refreshing"] = True
    up = next((u for u in CONFIG["upstreams"] if u["name"] == "workbuddy"), None)
    if stale:
        if start and up:
            threading.Thread(target=_wb_pool_fetch, args=(up,), daemon=True).start()
        return stale
    return _wb_pool_fetch(up)


def _wb_paid_any():
    """wb 账号池里是否存在付费（PRO）账号（读 _wb_pool_state 缓存）。"""
    return bool(_wb_pool_state().get("paid_any"))


def _wb_usable_realms(upstream_id):
    meta = _wb_realm_meta().get(upstream_id) or {}
    pool = _wb_pool_state().get("realms") or {}
    pro = set(meta.get("pro") or [])
    return [realm for realm in meta.get("realms") or []
            if realm not in pro or (pool.get(realm) or {}).get("paid")]


def _wb_account_realms(upstream_id):
    """已知号池时排除空区域；部分观测优先走已观察区域。

    读取失败且没有任何旧观测时保留目录区域作为未知候选，不能把未知当成
    空区域；但只要响应中观察到某个区域，就不把未出现的区域伪装成有账号。
    """
    available = _wb_usable_realms(upstream_id)
    state = _wb_pool_state()
    observed = set(state.get("realms") or {})
    if state.get("known") or observed:
        return [realm for realm in available if realm in observed]
    return available


def _wb_entry_locked(entry):
    if entry.get("platform") != "workbuddy":
        return False
    if not CONFIG.get("hide_pro_locked", True):
        return False
    meta = _wb_realm_meta().get(entry.get("upstream"))
    if meta:
        return not _wb_usable_realms(entry["upstream"])
    return bool(entry.get("requires_pro")) and not _wb_paid_any()


def _wb_model_realm(upstream_id):
    """模型专属区域（cn/intl）；双区同名或未知返回 None。"""
    meta = _wb_realm_meta().get(upstream_id)
    if not meta:
        return None
    realms = set(meta["realms"])
    if realms == {"intl"}:
        return "intl"
    if realms == {"cn"}:
        return "cn"
    return None


def _wb_dual_prefer():
    """双区同名模型的出口偏好（config wb_dual_prefer: auto/cn/intl，默认 auto）。

    auto = 哪个区的临期积分包更紧急就走哪个区（把快过期的钱先花掉）；
    信息缺失或国际版没有临期包时回国内（账号多、响应快）。
    """
    pref = (CONFIG.get("wb_dual_prefer") or "auto").lower()
    if pref in ("cn", "intl"):
        return pref
    realms = (_wb_pool_state().get("realms") or {})
    cn_dl = (realms.get("cn") or {}).get("min_days_left")
    intl_dl = (realms.get("intl") or {}).get("min_days_left")
    if intl_dl is not None and (cn_dl is None or intl_dl < cn_dl):
        return "intl"
    return "cn"


def _wb_route_realm(upstream_id):
    """聊天请求的出口决定：区域专属 → 该区域；双区 → 按 wb_dual_prefer，
    但选中区域若对该模型 PRO 门控且池里无付费账号，则换另一区
    （如 kimi-k3.1-pro 国际版要会员、国内版免费）。"""
    meta = _wb_realm_meta().get(upstream_id)
    if not meta or not meta.get("realms"):
        return None
    available = _wb_account_realms(upstream_id)
    pref = _wb_dual_prefer()
    if pref in available:
        return pref
    return next(iter(sorted(available)), None)


# ---------------------------------------------------------------------------
# WorkBuddy 区域级调度（网关层）：会话粘性 + 区域健康度 + 首字前换区。
#
# 账号选择仍在 WorkBuddy 引擎内；网关只能决定国内/国际出口。这里的目标是
# 不因几小时的临期差反复换区（保护 prompt cache），并避开刚失败过的区域。
# ---------------------------------------------------------------------------

WB_STICKY_TTL = 1800.0          # 同一会话 30 分钟无请求后解除粘性
WB_STICKY_MAX = 2048            # 防止无界增长
WB_HEALTH_BASE_COOLDOWN = 30.0  # 首次失败冷却
WB_HEALTH_MAX_COOLDOWN = 300.0  # 连续失败最长冷却
WB_RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}

_wb_route_lock = threading.Lock()
_wb_route_sticky = {}   # session_key -> {"realm": str, "ts": float}
_wb_route_health = {}   # realm -> {"failures", "cooldown_until", "last_fail", "last_ok", "last_status"}


def _wb_header(headers, name):
    """大小写不敏感地读一个请求头（self.headers / dict 都适用）。"""
    if not headers:
        return ""
    try:
        value = headers.get(name)
        if value:
            return str(value)
    except AttributeError:
        pass
    target = name.lower()
    for key, value in getattr(headers, "items", lambda: [])():
        if str(key).lower() == target and value:
            return str(value)
    return ""


def _wb_session_key(req, path, headers, model):
    """会话粘性键：优先客户端显式 id，其次 system/tools 前缀哈希。

    只用哈希落日志，不记录原始 system prompt 或用户标识。
    """
    raw = ""
    for name in ("prompt_cache_key", "conversation_id", "session_id", "thread_id", "user"):
        value = req.get(name)
        if isinstance(value, str) and value.strip():
            raw = value.strip()
            break
    if not raw:
        for name in ("X-Session-Id", "X-Conversation-Id", "X-Thread-Id",
                     "Session-Id", "Conversation-Id"):
            value = _wb_header(headers, name)
            if value.strip():
                raw = value.strip()
                break
    if not raw:
        parts = [str(model or "")]
        if path.endswith("/responses"):
            parts.append(str(req.get("instructions") or ""))
        else:
            messages = req.get("messages") or []
            if messages and isinstance(messages[0], dict) and messages[0].get("role") == "system":
                parts.append(str(messages[0].get("content") or ""))
        tools = req.get("tools")
        if tools:
            try:
                parts.append(json.dumps(tools, sort_keys=True, ensure_ascii=False))
            except (TypeError, ValueError):
                parts.append(str(tools))
        if len(parts) == 1:
            return None  # 没有任何可稳定识别的会话特征，不做粘性
        raw = "\x1f".join(parts)
    return hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:24]


def _wb_sticky_get(key):
    if not key:
        return None
    now = time.time()
    with _wb_route_lock:
        item = _wb_route_sticky.get(key)
        if not item:
            return None
        if now - item["ts"] > WB_STICKY_TTL:
            _wb_route_sticky.pop(key, None)
            return None
        item["ts"] = now  # 滑动 TTL：活跃会话持续粘住
        return item["realm"]


def _wb_sticky_set(key, realm):
    if not key or not realm:
        return
    with _wb_route_lock:
        if len(_wb_route_sticky) >= WB_STICKY_MAX:
            # 只淘汰最旧的一批，避免每次写入都排序全表
            oldest = sorted(_wb_route_sticky.items(), key=lambda kv: kv[1]["ts"])[:WB_STICKY_MAX // 10 or 1]
            for old_key, _ in oldest:
                _wb_route_sticky.pop(old_key, None)
        _wb_route_sticky[key] = {"realm": realm, "ts": time.time()}


def _wb_sticky_clear(key):
    if not key:
        return
    with _wb_route_lock:
        _wb_route_sticky.pop(key, None)


def _wb_health_ok(realm):
    with _wb_route_lock:
        item = _wb_route_health.get(realm)
        return not item or item.get("cooldown_until", 0) <= time.time()


def _wb_health_fail(realm, status=None, retry_after=None):
    now = time.time()
    with _wb_route_lock:
        item = _wb_route_health.setdefault(realm, {
            "failures": 0, "cooldown_until": 0, "last_fail": 0,
            "last_ok": 0, "last_status": None})
        item["failures"] += 1
        item["last_fail"] = now
        item["last_status"] = status
        cooldown = min(WB_HEALTH_MAX_COOLDOWN,
                       WB_HEALTH_BASE_COOLDOWN * (2 ** (item["failures"] - 1)))
        if retry_after is not None:
            cooldown = max(cooldown, min(WB_HEALTH_MAX_COOLDOWN, float(retry_after)))
        item["cooldown_until"] = now + cooldown
        failures = item["failures"]
    log(f"wb health {realm} fail status={status} failures={failures} cooldown={int(cooldown)}s")


def _wb_health_success(realm):
    now = time.time()
    with _wb_route_lock:
        item = _wb_route_health.get(realm)
        was_bad = bool(item and (item.get("failures") or item.get("cooldown_until", 0) > now))
        if item:
            item.update(failures=0, cooldown_until=0, last_ok=now, last_status=200)
        else:
            _wb_route_health[realm] = {
                "failures": 0, "cooldown_until": 0, "last_fail": 0,
                "last_ok": now, "last_status": 200}
    if was_bad:
        log(f"wb health {realm} recovered")


def _wb_retryable(status, body):
    """只重试“上游还没开始生成”的失败；400/401/404/内容拒绝不重试。"""
    if status in WB_RETRY_STATUSES:
        return True
    if status == 403:
        try:
            text = (body.decode("utf-8", "ignore") if isinstance(body, (bytes, bytearray))
                    else str(body)).lower()
        except Exception:
            text = ""
        return any(marker in text for marker in
                   ("rate limit", "queue", "10605", "too many requests", "retryafter"))
    return False


def _wb_retry_after(headers):
    value = (headers or {}).get("Retry-After") or (headers or {}).get("retry-after")
    try:
        seconds = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return max(0, min(300, seconds))


def _wb_plan(upstream_model, req, path, headers):
    """返回本次 WB 请求的区域候选顺序与决策依据。

    order 已经按“健康优先、粘性/偏好其次”排好；账号仍由引擎选择。
    """
    available = _wb_account_realms(upstream_model)
    fixed = _wb_model_realm(upstream_model)
    key = _wb_session_key(req, path, headers, upstream_model)
    if not available:
        return {"order": [], "available": [], "preferred": fixed,
                "sticky": None, "key": key, "reason": "no-account", "fixed": fixed}
    if fixed:
        order = [fixed] if fixed in available else []
        return {"order": order, "available": available, "preferred": fixed,
                "sticky": None, "key": key, "reason": "fixed", "fixed": fixed}
    preferred = _wb_dual_prefer()
    if preferred not in available:
        preferred = sorted(available)[0]
    sticky = _wb_sticky_get(key) if key else None
    if sticky not in available:
        sticky = None
    base = []
    for realm in ([sticky] if sticky else []) + [preferred] + sorted(available):
        if realm in available and realm not in base:
            base.append(realm)
    healthy = [realm for realm in base if _wb_health_ok(realm)]
    cooling = [realm for realm in base if not _wb_health_ok(realm)]
    reason = "sticky" if sticky else "prefer"
    if cooling and healthy:
        reason += "+health"
    elif cooling:
        reason += "+all-cooling"
    return {"order": healthy + cooling, "available": available,
            "preferred": preferred, "sticky": sticky, "key": key,
            "reason": reason, "fixed": None}


def _wb_log_route(rid, model, plan, attempt, realm, status=None,
                  outcome="", next_realm="", retry_after=None):
    """结构化决策日志：只落会话哈希，不落原始 prompt / 用户标识。"""
    parts = [f"route rid={rid}", f"model={model}",
             f"session={(plan.get('key') or '-')[:8]}",
             f"pref={plan.get('preferred') or '-'}",
             f"sticky={plan.get('sticky') or '-'}",
             f"order={','.join(plan.get('order') or []) or '-'}",
             f"reason={plan.get('reason') or '-'}",
             f"attempt={attempt}", f"realm={realm or '-'}"]
    if status is not None:
        parts.append(f"status={status}")
    parts.append(f"outcome={outcome or '-'}")
    if next_realm:
        parts.append(f"next={next_realm}")
    if retry_after is not None:
        parts.append(f"retry_after={retry_after}")
    log(" ".join(parts))


def _raccoon_session_key(req):
    """从小浣熊请求派生稳定的会话键（注入 X-Session-Id 头）。

    xhx2api 的会话粘性优先吃显式 X-Session-Id；ZCode 不发这个头，退到引擎的
    消息前缀指纹又因 system 每轮都变（时间戳/工作目录）而命中率只有 ~44%，
    于是同一会话在两个账号间轮番——风控视角等于「同一段上下文在多个登录态
    下出现」，也是二次风控的诱因之一。

    会话键取「第一条 user 消息」的哈希：一次对话里 system 可能每轮都变，
    但首问不变；新会话首问不同，自然换键。没有 user 消息的请求不派生。
    """
    msgs = req.get("messages")
    if not isinstance(msgs, list):
        return ""
    for msg in msgs:
        if not isinstance(msg, dict) or msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, list):
            content = "".join(part.get("text", "") for part in content
                              if isinstance(part, dict))
        text = str(content or "").strip()
        if text:
            return "gw-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return ""


def _upstream_headers(up, extra=None):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    key = (up.get("api_key") or "").strip()
    if key:
        headers["Authorization"] = "Bearer " + key
    if extra:
        headers.update(extra)
    return headers


def _hidden_set():
    """隐藏模型集合（存 config.json 的 hidden_models，元素是 `平台/模型` 全名）。"""
    return set(CONFIG.get("hidden_models") or [])


def _model_hidden(platform, model):
    return f"{platform}/{model}" in _hidden_set()


# ---------------------------------------------------------------------------
# 平台启用开关：停用的平台不展示模型、不接受调用、不轮询状态。
# 只影响网关编排；上游引擎进程是否运行由 launch 配置与设置页的进程按钮决定。
# ---------------------------------------------------------------------------

def _disabled_platforms():
    return {str(name) for name in (CONFIG.get("disabled_platforms") or [])}


def platform_enabled(name):
    return name not in _disabled_platforms()


def active_upstreams():
    return [up for up in CONFIG["upstreams"] if platform_enabled(up["name"])]


def _upstream_by_name(name):
    return next((up for up in CONFIG["upstreams"] if up["name"] == name), None)


def _model_in_disabled_platform(model):
    """裸模型名只存在于已停用平台时，给出比「未知模型」更准确的原因。

    只读 15 秒目录缓存，不主动拉取停用平台——停用后不该再产生上游请求。
    """
    for up in CONFIG["upstreams"]:
        if platform_enabled(up["name"]):
            continue
        with _catalog_lock:
            hit = _catalog_cache.get(up["name"])
        for row in (hit[1] if hit else []):
            if _public_name(up["name"], row.get("id") or "") == model:
                return up["name"]
    return None


# 小浣熊桌面端用顶层 raccoon_thinking_level（off / level_1..level_5）。
# 网关保留原生字段，不把 reasoning_effort 猜译为等级；具体可选档位由模型目录
# 决定。旧 xhx2api 的 Responses 转换可能丢弃该字段，详见 docs/raccoon-thinking.md。


# 进程管理：只记录由本网关启动的子进程。外部手动启动的引擎只显示状态，
# 不按端口猜 PID 强杀，避免误停别的程序。
_proc_lock = threading.Lock()
_managed_procs = {}
_platform_op_locks = {}

# 自动化 job 只保存网关进程生命周期内的脱敏状态；业务原始返回不落盘。
_automation_lock = threading.RLock()
_automation_jobs = {}
_automation_idempotency = {}
_AUTOMATION_JOB_LIMIT = 40
_AUTOMATION_STEP_SPECS = (
    ("workbuddy.checkin", "WorkBuddy · 签到 + 国际打卡", "checkin", "workbuddy", ""),
    ("workbuddy.tasks.run", "WorkBuddy · 运行成长任务", "task", "workbuddy", "run"),
    ("workbuddy.tasks.claim", "WorkBuddy · 领取任务奖励", "task", "workbuddy", "claim"),
    ("workbuddy.tasks.travel", "WorkBuddy · 猫猫旅行", "task", "workbuddy", "travel"),
    ("workbuddy.tasks.streak", "WorkBuddy · 连登奖励", "task", "workbuddy", "streak-bonus"),
    ("workbuddy.tasks.night", "WorkBuddy · 夜猫子", "task", "workbuddy", "night"),
    ("qoder.checkin", "Qoder · 签到", "checkin", "qoder", ""),
    ("qoder.tasks.run", "Qoder · 领取全部福利", "task", "qoder", "run"),
    ("qoder.tasks.travel", "Qoder · 猫猫旅行", "task", "qoder", "travel"),
    ("trae.checkin", "Trae · 签到", "checkin", "trae", ""),
    ("trae.tasks.travel", "Trae · 领取 Pro 福利包", "task", "trae", "travel"),
    ("raccoon.checkin", "小浣熊 · 签到", "checkin", "raccoon", ""),
    ("codearts.checkin", "码道 CodeArts · 签到领积分", "checkin", "codearts", ""),
    ("workbuddy.scheduler", "WorkBuddy · 提交补充巡检", "engine", "workbuddy", "scheduler/trigger"),
)


def _automation_now():
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def _automation_plan():
    configured = {up.get("name") for up in CONFIG.get("upstreams", [])}
    steps = []
    for step_id, label, kind, platform, action in _AUTOMATION_STEP_SPECS:
        supported = platform in configured and platform_enabled(platform)
        reason = "" if supported else (
            "平台未配置" if platform not in configured else "平台已停用")
        dynamic = kind in ("checkin", "task")
        steps.append({"step_id": step_id, "label": label, "kind": kind,
                      "platform": platform, "action": action,
                      "supported": supported, "skip_reason": reason,
                      "dynamic": dynamic,
                      "note": "执行前会按账号/上游状态再次判断" if dynamic else
                              "提交后由引擎调度器异步完成"})
    # 千问办公没有签到动作，但必须在预览中显式出现，避免用户误以为漏跑。
    steps.insert(9, {"step_id": "qwenwork.auto-credit", "label": "千问办公 · 上游自动发放",
                      "kind": "skip", "platform": "qwenwork", "action": "",
                      "supported": False, "skip_reason": "每日积分由上游自动发放",
                      "dynamic": False, "note": "不会调用签到接口"})
    version = hashlib.sha256(json.dumps(
        [(s["step_id"], s["supported"], s["skip_reason"]) for s in steps],
        ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]
    return {"version": version, "generated_at": _automation_now(), "steps": steps}


def _automation_safe_retry(step):
    # 任务领取/旅行可能已经在上游执行，未知结果不能盲重放。
    return step.get("kind") == "checkin" or step.get("action") in (
        "accounts/refresh", "accounts/credits", "accounts/credits/fetch")


def _automation_summary(result):
    if not isinstance(result, dict):
        return "上游返回格式未知"
    if result.get("unknown"):
        return "上游执行结果未知，请检查账号或调度器状态"
    if result.get("partial"):
        return "部分账号未完成，请查看引擎状态"
    if result.get("ok") is False:
        return "引擎返回失败，请查看引擎状态"
    if result.get("skipped") or (isinstance(result.get("result"), dict)
                                  and result["result"].get("skipped")):
        return "无需执行"
    if result.get("ok") is True:
        return "已执行；积分到账以引擎/上游记录为准"
    return "已提交，等待上游确认"


def _automation_checkin(name):
    if not platform_enabled(name):
        return {"ok": False, "error": "平台已停用"}
    up = _upstream_by_name(name)
    if not up:
        return {"ok": False, "error": "平台未配置"}
    if name == "qwenwork":
        return {"ok": True, "skipped": True, "msg": "每日积分由上游自动发放"}
    path, auth_kind = CHECKIN_PATHS.get(name, (None, None))
    if not path:
        return {"ok": False, "error": "没有签到接口"}
    try:
        result = engine_request(up, "POST", path, {}, auth=auth_kind,
                                timeout=_checkin_timeout(name))
    except Exception:
        # A timed-out check-in might have completed upstream; don't enable retries.
        return {"unknown": True, "error": "签到结果未知，请刷新账号状态确认"}
    if name == "raccoon":
        result = _raccoon_checkin_result(result)
    if name == "workbuddy":
        try:
            accs = engine_request(up, "GET", "/accounts?realm=all", auth=auth_kind, timeout=30)
            alist = accs.get("accounts") if isinstance(accs, dict) else accs
            today = time.strftime("%Y-%m-%d")
            pending = [a for a in (alist or []) if isinstance(a, dict)
                       and a.get("realm") == "intl" and a.get("enabled")
                       and a.get("canDailyChat") and not str(a.get("lastDailyChat") or "").startswith(today)]
            intl = (engine_request(up, "POST", "/accounts/daily-chat",
                                   {"uids": [a.get("uid") for a in pending]},
                                   auth=auth_kind, timeout=600)
                    if pending else {"ok": True, "skipped": True, "msg": "没有待打卡的国际版账号"})
            if pending and isinstance(intl, dict) and intl.get("ok") is False:
                intl = {**intl, "unknown": True}
            result = {"cn_checkin": result, "intl_daily_chat": intl}
        except Exception as exc:
            # Upstream may have accepted the POST before transport failed; don't replay it.
            intl = {"unknown": True, "error": "国际打卡结果未知，需刷新状态确认"}
            result = {"cn_checkin": result, "intl_daily_chat": intl}
    return {**_action_status(result), "result": result}


def _automation_call_step(step):
    name, kind, action = step["platform"], step["kind"], step.get("action", "")
    if kind == "checkin":
        return _automation_checkin(name)
    up = _upstream_by_name(name)
    if not up or not platform_enabled(name):
        return {"ok": False, "error": "平台不可用"}
    if kind == "task":
        spec = {"run": "/tasks/run", "claim": "/tasks/claim", "travel": "/tasks/travel",
                "night": "/tasks/night", "streak-bonus": "/tasks/streak-bonus"}.get(action)
        if not spec:
            return {"ok": False, "error": "任务动作不支持"}
        if action == "claim" and name == "workbuddy":
            result = Handler._wb_claim_all(None, up, {})
            return {**_action_status(result), "result": result}
        result = engine_request(up, "POST", spec, {}, timeout=300)
        return {**_action_status(result), "result": result}
    if kind == "engine":
        result = engine_request(up, "POST", "/" + action, {},
                                auth="panel", timeout=300)
        return {**_action_status(result), "submitted": action.endswith("/trigger"),
                "result": result}
    return {"ok": True, "skipped": True, "msg": step.get("skip_reason") or "已跳过"}


def _automation_update(job_id, **changes):
    with _automation_lock:
        job = _automation_jobs.get(job_id)
        if job:
            job.update(changes)
            job["updated_at"] = _automation_now()
            return dict(job)
    return None


def _automation_run_step(job_id, index):
    with _automation_lock:
        job = _automation_jobs.get(job_id)
        if not job or index >= len(job["steps"]):
            return
        step = job["steps"][index]
        step["status"] = "running"
        step["started_at"] = _automation_now()
        job["status"] = "running"
        job["started_at"] = job.get("started_at") or step["started_at"]
        job["current_step"] = step["step_id"]
        job["current_index"] = index
        job["updated_at"] = _automation_now()
    try:
        if not step.get("supported", True):
            result = {"ok": True, "skipped": True, "msg": step.get("skip_reason")}
        else:
            result = _automation_call_step(step)
        status = ("unknown" if result.get("unknown") else
                  "partial" if result.get("partial") else
                  "skipped" if result.get("skipped") else
                  "submitted" if result.get("ok") is True and result.get("submitted") else
                  "succeeded" if result.get("ok") is True else "failed")
        summary = ("已提交，等待引擎调度器结果" if status == "submitted"
                   else _automation_summary(result))
    except Exception:
        # 传输失败时无法确认上游是否已经执行。
        status, summary = "unknown", "上游执行结果未知，请检查账号或调度器状态"
    with _automation_lock:
        job = _automation_jobs.get(job_id)
        if not job:
            return
        step.update(status=status, summary=summary, finished_at=_automation_now(),
                    retryable=status == "failed" and _automation_safe_retry(step))
        job["updated_at"] = step["finished_at"]
        counts = {s: sum(1 for x in job["steps"] if x.get("status") == s)
                  for s in ("succeeded", "skipped", "failed", "partial", "unknown", "submitted")}
        job["counts"] = counts
        job["completed"] = sum(counts.values())
        next_index = index + 1
        retry_only = bool(job.get("retry_only"))
        if retry_only:
            job["retry_only"] = False
        if next_index < len(job["steps"]) and not retry_only:
            job["current_index"] = next_index
        else:
            bad = counts["failed"] + counts["partial"]
            job["status"] = ("failed" if counts["failed"] and not counts["succeeded"]
                              else "partial" if bad or counts["unknown"] or counts["submitted"]
                              else "succeeded")
            job["finished_at"] = _automation_now()
            job["current_step"] = ""
    if next_index < len(job["steps"]) and not retry_only:
        try:
            threading.Thread(target=_automation_run_step, args=(job_id, next_index),
                             daemon=True).start()
        except Exception:
            # 线程资源耗尽时不能让 job 永远停在 running：落终态并标记当前步失败，
            # 让前端能通过重试接口接手，而不是只能重启网关。
            with _automation_lock:
                job = _automation_jobs.get(job_id)
                if job:
                    step = job["steps"][next_index]
                    step["status"] = "failed"
                    step["summary"] = "网关无法继续执行后续步骤（线程资源耗尽）"
                    step["finished_at"] = _automation_now()
                    step["retryable"] = False
                    job["counts"] = {s: sum(1 for x in job["steps"] if x.get("status") == s)
                                     for s in ("succeeded", "skipped", "failed", "partial",
                                               "unknown", "submitted")}
                    job["completed"] = sum(job["counts"].values())
                    job["status"] = ("partial" if job["counts"]["succeeded"]
                                     else "failed")
                    job["finished_at"] = _automation_now()
                    job["current_step"] = ""


def _automation_start(job):
    with _automation_lock:
        _automation_jobs[job["id"]] = job
    threading.Thread(target=_automation_run_step, args=(job["id"], 0), daemon=True).start()


# ---- 每日定时自动化：到点自动跑签到 ----
#
# 网关此前只有「手动触发」的自动化 job；各引擎自己的签到调度各自为政
# （xhx2api 9 点、codearts2api watchdog 30 分钟一轮）。这里补一条统一的
# 每日任务：到点按计划里所有 supported 的 checkin 步骤跑一遍。
#
# 状态落盘只为「今天已跑过」，不保存业务结果；签到接口本身都是幂等的，
# 所以即使网关在跑完后立刻重启、状态文件来不及写，重复执行也安全。

AUTOMATION_DAILY_STATE = os.path.join(HERE, "automation_daily.json")
_automation_daily_lock = threading.Lock()


def _automation_daily_cfg():
    """config.json 的 automation_daily：{enabled, time:"HH:MM", steps:[step_id]}。

    steps 为空 = 计划里所有 checkin 步骤；显式给列表则只跑列出的。
    时间非法时退回 09:00——一个笔误不该让定时器整天停摆。
    """
    cfg = CONFIG.get("automation_daily")
    if not isinstance(cfg, dict):
        return {"enabled": False, "time": "09:00", "hour": 9, "minute": 0, "steps": []}
    hour, minute = 9, 0
    text = str(cfg.get("time") or "09:00")
    try:
        head, _, tail = text.partition(":")
        hour, minute = int(head), int(tail or 0)
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise ValueError(text)
    except (TypeError, ValueError):
        hour, minute = 9, 0
    steps = [s.strip() for s in (cfg.get("steps") or [])
             if isinstance(s, str) and s.strip()]
    return {"enabled": bool(cfg.get("enabled")), "time": "%02d:%02d" % (hour, minute),
            "hour": hour, "minute": minute, "steps": steps}


def _automation_daily_last_run():
    try:
        with open(AUTOMATION_DAILY_STATE, encoding="utf-8") as handle:
            return str(json.load(handle).get("last_run") or "")
    except (OSError, ValueError):
        return ""


def _automation_daily_mark(day):
    tmp = AUTOMATION_DAILY_STATE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump({"last_run": day}, handle)
        os.replace(tmp, AUTOMATION_DAILY_STATE)
    except OSError:
        pass


def _automation_daily_due(cfg, last_run, now=None):
    """到点且今天没跑过 → True。now 固定东八区，与账号签到口径一致。"""
    if not cfg.get("enabled"):
        return False
    if now is None:
        now = datetime.now(timezone(timedelta(hours=8)))
    if last_run == now.strftime("%Y-%m-%d"):
        return False
    return (now.hour, now.minute) >= (cfg.get("hour", 9), cfg.get("minute", 0))


def _automation_daily_steps(plan, wanted):
    """从当前计划里挑出要跑的签到步骤；只保留 supported（平台启用且已配置）。"""
    by_id = {s["step_id"]: s for s in plan.get("steps", [])}
    ids = wanted or [s["step_id"] for s in plan.get("steps", []) if s.get("kind") == "checkin"]
    return [dict(by_id[step_id]) for step_id in ids
            if step_id in by_id and by_id[step_id].get("supported")]


def _automation_daily_run():
    """建一个只含签到步骤的 job 并立刻开始执行；返回 job id 或 None。"""
    cfg = _automation_daily_cfg()
    plan = _automation_plan()
    steps = _automation_daily_steps(plan, cfg["steps"])
    if not steps:
        log("automation daily: 没有可执行的签到步骤，跳过")
        return None
    day = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    job = {"id": secrets.token_urlsafe(12), "status": "queued",
           "plan_version": plan["version"], "idempotency_key": "daily-" + day,
           "created_at": _automation_now(), "updated_at": _automation_now(),
           "started_at": None, "finished_at": None, "current_step": "",
           "current_index": 0, "completed": 0,
           "counts": {s: 0 for s in ("succeeded", "skipped", "failed", "partial",
                                     "unknown", "submitted")},
           "steps": steps}
    with _automation_lock:
        _automation_jobs[job["id"]] = job
        _automation_idempotency["daily-" + day] = job["id"]
        while len(_automation_jobs) > _AUTOMATION_JOB_LIMIT:
            old = next((jid for jid, item in _automation_jobs.items()
                        if item.get("status") not in ("queued", "running")
                        and jid != job["id"]), None)
            if old is None:
                break
            _automation_jobs.pop(old, None)
            for old_key, old_job in list(_automation_idempotency.items()):
                if old_job == old:
                    _automation_idempotency.pop(old_key, None)
    _automation_daily_mark(day)
    log("automation daily: 触发 %s（%s）" % (job["id"], ",".join(s["step_id"] for s in steps)))
    threading.Thread(target=_automation_run_step, args=(job["id"], 0), daemon=True).start()
    return job["id"]


def _automation_daily_loop():
    while True:
        try:
            cfg = _automation_daily_cfg()
            if _automation_daily_due(cfg, _automation_daily_last_run()):
                _automation_daily_run()
        except Exception as exc:
            log("automation daily loop error: %s: %s" % (type(exc).__name__, exc))
        time.sleep(30)


# ---------------------------------------------------------------------------
# codearts token 保活：access token 约 2 小时过期，引擎自身的 watchdog 靠不住
# ---------------------------------------------------------------------------
#: 默认每 30 分钟续期一次；config.json 的 `codearts_keepalive_minutes` 可改，0 关闭。
CODECARTS_KEEPALIVE_MINUTES = 30
#: 启动后首次续期的延迟：网关刚起来时引擎多半也刚校验过 token，不必立刻再打一次；
#: 但也不能等到一个完整周期——如果启动时 token 只剩几分钟，等 30 分钟就晚了。
CODECARTS_KEEPALIVE_FIRST_DELAY = 20.0


def _codearts_keepalive_cfg():
    """保活间隔（秒）；0 表示关闭。非法值回退默认，下限 60 秒防打爆上游。"""
    raw = CONFIG.get("codearts_keepalive_minutes", CODECARTS_KEEPALIVE_MINUTES)
    try:
        minutes = float(raw)
    except (TypeError, ValueError):
        minutes = float(CODECARTS_KEEPALIVE_MINUTES)
    if minutes <= 0:
        return 0
    return max(60.0, minutes * 60.0)


def _codearts_keepalive_once():
    """对 codearts 调一次保活续期。返回 {ok, skipped?, error?}。

    引擎的 auth 文件不持久化 expires_at，watchdog 无法按到期时刻提前刷新——
    token 过期后只能等某次请求吃到 401 才去续，而那次 401 还会把账号打进
    10 分钟错误冷却。实测 2026-11-26 19:57–20:02 全部 503 no_healthy_account
    （all accounts unavailable (disabled/cooldown)）就是这个窗口。

    主动续期把窗口消掉：/admin/api/keepalive 会 refreshed/validated 各账号。
    """
    up = _upstream_by_name("codearts")
    if not up:
        return {"ok": True, "skipped": True, "msg": "未配置 codearts"}
    if not platform_enabled("codearts"):
        return {"ok": True, "skipped": True, "msg": "codearts 已停用"}
    try:
        result = engine_request(up, "POST", "/admin/api/keepalive", {},
                                auth="bearer", timeout=120)
    except Exception as exc:
        return {"ok": False, "error": "%s: %s" % (type(exc).__name__, exc)}
    if isinstance(result, dict) and result.get("ok") is False:
        return {"ok": False,
                "error": str(result.get("message") or result.get("error") or "引擎拒绝保活")}
    return {"ok": True, "result": result}


#: token 剩余低于这个分钟数才去续期。2 小时有效期的 token 每 90 分钟续一次足够，
#: 没必要每轮询都打一次上游。
CODECARTS_KEEPALIVE_THRESHOLD_MINUTES = 45


def _parse_token_remaining(text):
    """把引擎的 "1h32m0s" 解析成分钟；解析不了返回 None。"""
    text = str(text or "").strip()
    if not text:
        return None
    total = 0
    matched = False
    for value, unit in re.findall(r"(\d+(?:\.\d+)?)\s*([hms])", text):
        matched = True
        total += float(value) * {"h": 60.0, "m": 1.0, "s": 1.0 / 60.0}[unit]
    if not matched:
        return None
    return total


def _codearts_token_remaining(up):
    """读引擎 overview 里的 token 剩余分钟数；读不到返回 None。"""
    try:
        data = engine_request(up, "GET", "/admin/api/overview", auth="bearer", timeout=15)
    except Exception:
        return None
    accounts = (data.get("accounts") if isinstance(data, dict) else None) or []
    values = []
    for account in accounts:
        if not isinstance(account, dict) or account.get("disabled"):
            continue
        minutes = _parse_token_remaining(account.get("token_remaining"))
        if minutes is not None:
            values.append(minutes)
    return min(values) if values else None


def _codearts_keepalive_loop(interval):
    """后台续期线程：只在 token 快到期时才续，不是每个周期都打上游。

    实测 token 只有约 2 小时有效期，而引擎自己的 watchdog 在 auth 文件缺
    expires_at 时不会提前刷新（2026-11-26：14:12 启动剩 1h47m，直到 19:50
    第一次刷新，中间近 4 小时是过期状态）。这里按剩余时间判断，剩 45 分钟
    以内才续；读不到剩余时间时保守续一次（宁可多打一次也不让 token 断）。
    """
    time.sleep(CODECARTS_KEEPALIVE_FIRST_DELAY)
    while True:
        try:
            up = _upstream_by_name("codearts")
            if up and platform_enabled("codearts"):
                remaining = _codearts_token_remaining(up)
                if remaining is None or remaining <= CODECARTS_KEEPALIVE_THRESHOLD_MINUTES:
                    out = _codearts_keepalive_once()
                    if not out.get("ok"):
                        log("codearts 保活失败：%s" % (out.get("error") or "未知原因"))
                    elif not out.get("skipped"):
                        log("codearts 保活成功（token 剩余 %s 分钟）"
                            % (round(remaining, 1) if remaining is not None else "未知"))
        except Exception as exc:
            log("codearts keepalive loop error: %s: %s" % (type(exc).__name__, exc))
        time.sleep(interval)


def _automation_public(job):
    with _automation_lock:
        out = json.loads(json.dumps(job, ensure_ascii=False))
    out.pop("idempotency_key", None)
    for step in out.get("steps", []):
        step.pop("result", None)
    return out


def _platform_op_lock(name):
    with _proc_lock:
        return _platform_op_locks.setdefault(name, threading.Lock())


def _launch_spec(up):
    """读取 upstreams[].launch：{command, cwd, timeout, env}。

    command 可以是数组（可执行文件 + 参数），也可以是字符串（按 shell
    命令执行，便于调用引擎自带的 .bat/.sh 启动脚本）。字符串只来自配置
    或设置页保存前的校验，不直接来自客户端请求。
    """
    spec = up.get("launch")
    if not isinstance(spec, dict):
        return None
    command = spec.get("command")
    shell = False
    if isinstance(command, str):
        if not command.strip():
            return None
        command = (["cmd.exe", "/c", command] if os.name == "nt"
                   else ["/bin/sh", "-c", command])
        shell = True
    elif isinstance(command, list):
        if (not command
                or not all(isinstance(part, str) and part.strip() for part in command)):
            return None
    else:
        return None
    cwd = spec.get("cwd") or None
    if cwd is not None:
        if not isinstance(cwd, str):
            return None
        if not os.path.isabs(cwd):
            # 相对路径按网关目录解析，避免依赖启动时的工作目录
            cwd = os.path.normpath(os.path.join(HERE, cwd))
        if not os.path.isdir(cwd):
            return None
    env = spec.get("env")
    if env is not None:
        if (not isinstance(env, dict)
                or not all(isinstance(k, str) and k and isinstance(v, str)
                           for k, v in env.items())):
            return None
        env = dict(env)
    else:
        env = {}
    if not shell:
        exe = command[0]
        if not os.path.isabs(exe) and os.sep not in exe and "/" not in exe:
            candidate = os.path.join(cwd or HERE, exe)
            if os.path.isfile(candidate):
                command = [candidate] + command[1:]
    try:
        timeout = int(spec.get("timeout") or 30)
    except (TypeError, ValueError):
        timeout = 30
    return {"command": command, "cwd": cwd, "timeout": max(5, min(180, timeout)),
            "env": env, "shell": shell}


def _upstream_health(up, timeout=3):
    """轻量健康检查（只读 /health），用于进程启动确认和设置页状态。"""
    try:
        # xhx2api（raccoon）/ codearts2api（codearts）用 /healthz；其它 hub 系引擎用 /health。
        health_path = "/healthz" if up.get("name") in ("raccoon", "codearts") else "/health"
        req = urllib.request.Request(up["base_url"].rstrip("/") + health_path)
        with _urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace").strip()
        try:
            body = json.loads(raw)
        except ValueError:
            # codearts2api 的 /healthz 是纯文本 "ok"，不是 JSON。
            return raw.lower() == "ok"
        if not isinstance(body, dict):
            return True
        if "ok" in body:
            return bool(body.get("ok"))
        if "status" in body:
            return body.get("status") == "ok"
        return True
    except Exception:
        return False


def _platform_process(up):
    """{managed, running, pid, launch_configured}；managed 仅指网关启动的进程。"""
    name = up["name"]
    with _proc_lock:
        proc = _managed_procs.get(name)
        if proc is not None and proc.poll() is not None:
            _managed_procs.pop(name, None)
            proc = None
    # 路由启用与进程可达是两种状态。停用平台后仍探测健康端点，避免把
    # 未能安全停止的外部进程误报成已停止。
    healthy = _upstream_health(up)
    enabled = platform_enabled(name)
    observed_at = _automation_now()
    if proc is not None:
        state = "running_healthy" if healthy else "running_unhealthy"
        return {"managed": True, "running": True, "healthy": healthy,
                "state": state, "route_enabled": enabled,
                "observed_at": observed_at,
                "pid": proc.pid,
                "launch_configured": _launch_spec(up) is not None}
    state = "running_healthy" if healthy else "unknown"
    return {"managed": False, "running": healthy, "healthy": healthy,
            "state": state, "route_enabled": enabled,
            "observed_at": observed_at, "pid": None,
            "launch_configured": _launch_spec(up) is not None}


def _start_platform(up):
    """按 launch 配置启动引擎并等待健康检查通过。"""
    with _platform_op_lock(up["name"]):
        return _start_platform_locked(up)


def _start_platform_locked(up):
    name = up["name"]
    spec = _launch_spec(up)
    if spec is None:
        return {"ok": False, "error": "未配置 launch.command（config.json upstreams[].launch）"}
    with _proc_lock:
        proc = _managed_procs.get(name)
        if proc is not None and proc.poll() is None:
            return {"ok": True, "already": True, "pid": proc.pid}
    if _upstream_health(up):
        return {"ok": True, "already": True, "external": True}
    with _proc_lock:
        log_path = os.path.join(HERE, f"engine-{name}.log")
        try:
            out = open(log_path, "ab")
        except OSError as exc:
            return {"ok": False, "error": f"无法写引擎日志: {exc}"}
        flags = 0
        if os.name == "nt":
            flags = (getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                     | getattr(subprocess, "DETACHED_PROCESS", 0))
        try:
            env = {**os.environ, **spec["env"]} if spec["env"] else None
            proc = subprocess.Popen(spec["command"], cwd=spec["cwd"], env=env,
                                    stdout=out, stderr=subprocess.STDOUT,
                                    creationflags=flags, close_fds=True)
        except OSError as exc:
            out.close()
            return {"ok": False, "error": f"启动失败: {exc}"}
        out.close()
        _managed_procs[name] = proc
    deadline = time.time() + spec["timeout"]
    exited_at = None
    while time.time() < deadline:
        if _upstream_health(up):
            if proc.poll() is None:
                log(f"platform[{name}] 启动成功 pid={proc.pid}")
                return {"ok": True, "started": True, "pid": proc.pid}
            # 启动脚本把引擎拉成独立进程后自己退出：引擎已就绪，但不再由
            # 网关托管；停止时走端口识别。
            with _proc_lock:
                _managed_procs.pop(name, None)
            log(f"platform[{name}] 启动成功（脚本已退出，引擎独立运行）")
            return {"ok": True, "started": True, "detached": True, "pid": proc.pid}
        if proc.poll() is not None:
            if exited_at is None:
                exited_at = time.time()
            elif time.time() - exited_at > 5:
                with _proc_lock:
                    _managed_procs.pop(name, None)
                return {"ok": False,
                        "error": f"启动命令已退出（code {proc.returncode}），健康检查未通过，见 engine-{name}.log"}
        time.sleep(0.5)
    with _proc_lock:
        if proc.poll() is not None:
            _managed_procs.pop(name, None)
    return {"ok": False,
            "error": f"启动后 {spec['timeout']} 秒内健康检查未通过（pid {proc.pid}）"}


def _listener_port(address):
    endpoint = (address or "").strip()
    if endpoint.startswith("["):
        end = endpoint.find("]")
        if end < 0 or endpoint[end + 1:end + 2] != ":":
            return None
        raw = endpoint[end + 2:]
    else:
        _, sep, raw = endpoint.rpartition(":")
        if not sep:
            return None
    return int(raw) if raw.isdigit() else None


def _port_listener_pid(port, host=None):
    """Windows: return a listener for the requested port and optional host."""
    if os.name != "nt":
        return None
    try:
        out = subprocess.run(["netstat", "-ano", "-p", "tcp"],
                             capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 5 or parts[0].upper() != "TCP" or parts[3].upper() != "LISTENING":
            continue
        if _listener_port(parts[1]) != int(port):
            continue
        if host:
            local_host = parts[1].rsplit(":", 1)[0].strip("[]")
            if local_host not in (host, "0.0.0.0", "::"):
                continue
        try:
            return int(parts[4])
        except ValueError:
            continue
    return None


def _process_command_line(pid):
    """Windows：读进程命令行，用于停止前确认身份；失败返回空串。"""
    if os.name != "nt":
        return ""
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-CimInstance Win32_Process -Filter 'ProcessId = {int(pid)}').CommandLine"],
            capture_output=True, text=True, timeout=8).stdout
        return out.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _engine_identity_tokens(up):
    """仅返回可在命令行上精确核对的仓库名/脚本名片段，不含通用参数。"""
    tokens = set()
    repo = str(up.get("repo") or "").strip().rstrip("/")
    if repo:
        tokens.add(repo.rsplit("/", 1)[-1])
    spec = _launch_spec(up)
    if spec:
        command = spec["command"]
        if spec.get("shell"):
            script = command[-1] if command else ""
            tokens.add(os.path.basename(script))
        else:
            for part in command:
                base = os.path.basename(part)
                if base.lower().endswith((".py", ".exe", ".bat", ".cmd", ".ps1", ".sh")):
                    tokens.add(base)
    return {token for token in tokens if len(token) >= 4}


def _command_matches_engine(cmdline, tokens):
    """Match a configured script/executable as a complete command token."""
    if not cmdline or not tokens:
        return False
    try:
        parts = subprocess.list2cmdline([])  # touch stdlib cmdline support consistently
        del parts
        import shlex
        argv = shlex.split(cmdline, posix=(os.name != "nt"))
    except ValueError:
        argv = cmdline.split()
    basenames = {os.path.basename(part.strip('"')).lower() for part in argv if part}
    return any(token.lower() in basenames for token in tokens)


def _kill_tree(pid):
    """Windows：taskkill /T 收整棵进程树；其它平台按 PID 终止。"""
    if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=False)
        return
    try:
        os.kill(pid, 15)
    except OSError:
        pass


def _stop_platform(up, allow_external=False):
    """停止平台引擎。"""
    with _platform_op_lock(up["name"]):
        return _stop_platform_locked(up, allow_external)


def _stop_platform_locked(up, allow_external=False):
    """停止平台引擎。

    网关自己启动的进程直接收进程树；外部进程仅在 allow_external=True 时，
    先按 base_url 端口找到监听 PID，并确认命令行包含平台名/仓库名/launch
    路径片段后才停止，避免误杀端口上的其它程序。
    """
    name = up["name"]
    with _proc_lock:
        proc = _managed_procs.get(name)
        if proc is not None and proc.poll() is not None:
            _managed_procs.pop(name, None)
            proc = None
    if proc is not None:
        _kill_tree(proc.pid)
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            return {"ok": False, "stopped": False, "managed": True, "pid": proc.pid,
                    "error": f"停止 PID {proc.pid} 超时，仍保留托管记录以便重试"}
        if proc.poll() is None:
            return {"ok": False, "stopped": False, "managed": True, "pid": proc.pid,
                    "error": f"停止 PID {proc.pid} 未确认退出，仍保留托管记录以便重试"}
        with _proc_lock:
            if _managed_procs.get(name) is proc:
                _managed_procs.pop(name, None)
        log(f"platform[{name}] 已停止 pid={proc.pid}（网关托管）")
        return {"ok": True, "stopped": True, "managed": True, "pid": proc.pid}
    if not allow_external:
        return {"ok": False, "error": "该平台不是由网关启动的（或已退出），不自动结束外部进程"}
    try:
        base = urlsplit(up.get("base_url") or "")
        port = base.port
        host = base.hostname
    except ValueError:
        port, host = None, None
    if not port:
        return {"ok": False, "error": "上游 base_url 没有有效端口，无法定位进程"}
    pid = _port_listener_pid(port, host)
    if pid is None:
        return {"ok": True, "already": True, "note": f"端口 {port} 没有监听进程"}
    if not host or host not in ("127.0.0.1", "localhost", "::1"):
        return {"ok": False, "error": "外部引擎地址不是本机回环地址，拒绝按端口停止进程"}
    if pid == os.getpid():
        return {"ok": False, "pid": pid, "error": "目标 PID 是网关自身，拒绝停止"}
    cmdline = _process_command_line(pid)
    tokens = _engine_identity_tokens(up)
    if not cmdline or not _command_matches_engine(cmdline, tokens):
        return {"ok": False, "pid": pid,
                "error": f"端口 {port} 上的进程 PID {pid} 无法确认属于 {name}，未停止"}
    # Re-check the listener before terminating to avoid killing a PID that has
    # already stopped serving this port and may have been reused.
    if _port_listener_pid(port, host) != pid:
        return {"ok": False, "pid": pid,
                "error": f"端口 {port} 的监听进程已变化，未停止 PID {pid}"}
    _kill_tree(pid)
    time.sleep(0.5)
    if _port_listener_pid(port, host) == pid:
        return {"ok": False, "pid": pid,
                "error": f"已请求停止 PID {pid}，但端口 {port} 仍在监听"}
    log(f"platform[{name}] 已停止 pid={pid}（外部进程，端口 {port}）")
    return {"ok": True, "stopped": True, "managed": False, "pid": pid}


def _set_platform_enabled(name, enabled):
    """写 disabled_platforms 并清掉受影响的缓存。"""
    with _config_lock:
        disabled = {str(item) for item in (CONFIG.get("disabled_platforms") or [])}
        if enabled:
            disabled.discard(name)
        else:
            disabled.add(name)
        CONFIG["disabled_platforms"] = sorted(disabled)
        save_config()
    global _catalog_pub_generation
    with _catalog_pub_lock:
        _catalog_pub_generation += 1
        _catalog_pub_cache.pop("all", None)
    with _estimate_lock:
        _estimate_cache["data"] = None
        _estimate_cache["ts"] = 0.0
    with _updates_lock:
        _updates_cache["data"] = None
        _updates_cache["ts"] = 0.0
    return sorted(disabled)


def _row_multiplier(platform, row):
    """从各引擎的模型行里提取计费倍率，形状不一时返回 None。

    workbuddy: credits 字符串 "x0.69"；qoder: price_factor（峰）/ price_factor_valley（谷）；
    qwenwork（Buddy2api）目录行不含价格字段 → None。
    """
    try:
        if platform == "workbuddy":
            c = row.get("credits")
            if c:
                # 上游个别行带尾巴（实测 max-model credits="x1.33 credits"），
                # 直接 float 会 ValueError；取前导数字
                mnum = re.match(r"[xX]\s*([0-9]+(?:\.[0-9]+)?)", str(c).strip())
                if mnum:
                    return {"factor": float(mnum.group(1))}
        elif platform == "qoder":
            pf = row.get("price_factor")
            if pf is not None:
                out = {"factor": float(pf)}
                pv = row.get("price_factor_valley")
                if pv is not None:
                    out["valley"] = float(pv)
                return out
        elif platform == "raccoon":
            # xhx2api 的目录给 credits="x0.05"（相对倍率，不是积分/千tok）。
            # 只用于模型页展示，不参与额度预估。
            c = row.get("credits")
            if c:
                mnum = re.match(r"[xX]\s*([0-9]+(?:\.[0-9]+)?)", str(c).strip())
                if mnum:
                    return {"factor": float(mnum.group(1))}
        elif platform == "trae":
            # trae2api-hub 把官方 features.consumption_rate.data.rate 原样透传为
            # consumption_rate（信用消费倍率，0.06 = 基础价的 6%）。只用于模型页
            # 展示；上游 usage 不带 credits，额度预估仍显示待测。
            cr = row.get("consumption_rate")
            if cr is not None and not isinstance(cr, bool):
                return {"factor": float(cr)}
    except (TypeError, ValueError):
        pass
    return None


#: qwenwork（Buddy2api）目录里的内嵌渠道前缀：auto 通道命名会带 "workbuddy/"
#: 这类前缀，展示时剥掉，转发时保留原名。
INNER_CHANNEL_PREFIXES = ("workbuddy/", "qwenwork/")

_catalog_pub_cache = {}
_catalog_pub_generation = 0
_catalog_pub_lock = threading.Lock()


def _public_name(up_name, model):
    """上游模型名 → (对外公共名 或 None=跳过)。

    qwenwork（Buddy2api，qwenwork-only 模式）的目录会把它自己 workbuddy 渠道的
    模型清单也列出来（"workbuddy/*" 和裸名 glm 系列），但绑定 qwenwork 通道的
    Key 实际调不动它们（Unknown model）——这些条目直接跳过，不冒充千问模型。
    "qwenwork/*" 是千问渠道自己的别名（flash/pro/frontier-1/qwen3.8-*），剥前缀展示。
    """
    if up_name == "qwenwork":
        if model.startswith("workbuddy/"):
            return None
        for p in INNER_CHANNEL_PREFIXES:
            if model.startswith(p) and len(model) > len(p):
                return model[len(p):]
    return model


def _row_caps(platform, row):
    """跨平台归一的模型规格：上下文/最大输出/思考档位/多模态/工具。

    wb 字段最全（含 reasoning_efforts 档位表）；qoder 有 context_windows
    选项但无思考档位表；qwenwork 目录基本没有这些字段——缺的就缺，
    前端按 None 隐藏。返回 {} 表示上游什么都没说。
    """
    def num(v):
        return int(v) if isinstance(v, (int, float)) and v > 0 else None

    caps = {}
    # Buddy2api/Buddy 系目录用 context_window（qwenwork 上游实测 1,000,000），
    # 其它引擎用 context_length；两个都读，别再让 qwenwork 的上下文变成未知。
    ctx = num(row.get("context_length")) or num(row.get("context_window"))
    if ctx:
        caps["ctx"] = ctx
    wins = row.get("context_windows")
    if isinstance(wins, list):
        wl = sorted({num(w) for w in wins if num(w)})
        if wl:
            caps["ctx_windows"] = wl
    mi, mo = num(row.get("max_input_tokens")), num(row.get("max_output_tokens"))
    if not mo:
        mo = num(row.get("max_completion_tokens"))
    if mi:
        caps["max_in"] = mi
    if mo:
        caps["max_out"] = mo
    eff = row.get("reasoning_efforts")
    if isinstance(eff, list) and eff:
        caps["efforts"] = [str(e) for e in eff if str(e)]
    if row.get("reasoning_default_effort"):
        caps["effort_default"] = str(row["reasoning_default_effort"])
    if row.get("reasoning_can_disable") is not None:
        caps["can_disable"] = bool(row["reasoning_can_disable"])
    if row.get("always_reasoning") is not None:
        caps["always_reasoning"] = bool(row["always_reasoning"])
    cap = row.get("capabilities") if isinstance(row.get("capabilities"), dict) else {}
    vision = row.get("supports_vision")
    if vision is None:
        vision = cap.get("vision")
    if vision is not None:
        caps["vision"] = bool(vision)
    tools = row.get("supports_tool_calls")
    if tools is None:
        tools = cap.get("tool_calls")
    if tools is None:
        tools = cap.get("tools")  # xhx2api 用 capabilities.tools
    if tools is not None:
        caps["tools"] = bool(tools)
    reasoning = row.get("supports_reasoning")
    if reasoning is None:
        reasoning = cap.get("reasoning")
    if reasoning is not None:
        caps["reasoning"] = bool(reasoning)
    if row.get("temperature") is not None:
        caps["temp"] = row.get("temperature")
    return caps


def _model_caps(x):
    """_row_caps + config.model_caps 覆盖。

    上游目录（尤其 qwenwork 的供应商列表）不声明多模态/工具能力，只能靠
    实测补：覆盖表按 "平台/模型" 写 {"vision": true} 之类的硬事实，测得
    什么补什么，没测的继续留空，前端照旧不显示。改配置后无需重启之外的
    操作，目录/模型列表下次拉取即生效。
    """
    caps = _row_caps(x["platform"], x["row"])
    ov = (CONFIG.get("model_caps") or {}).get(f"{x['platform']}/{x['public']}")
    if isinstance(ov, dict):
        caps.update({k: v for k, v in ov.items() if v is not None})
    return caps


def public_catalog():
    """全平台公共模型目录（15s 缓存）：

    [{platform, public, upstream, row}] — public 是对外的模型名（剥内嵌前缀后），
    upstream 是转发给引擎的真实模型名。同一公共名多入口时优先裸名（上游 auto
    通道按名分发），目录页把其余入口显示为别名。
    """
    now = time.time()
    with _catalog_pub_lock:
        hit = _catalog_pub_cache.get("all")
        if hit and now - hit[0] < 15:
            return hit[1]
    with _catalog_pub_lock:
        generation = _catalog_pub_generation
    entries = []
    for up in active_upstreams():
        by_public = {}
        order = []
        for row in upstream_rows(up) or []:
            m = row.get("id") or ""
            if not m:
                continue
            # Buddy2api（qwenwork-only）的目录会把 workbuddy 渠道的模型也列出来，
            # 但绑定 qwenwork 的 Key 实际调不动它们（Unknown model）——按行的
            # channel 字段过滤，auto 万能别名除外（实测可用）。
            if (up["name"] == "qwenwork"
                    and row.get("channel") == "workbuddy" and m != "auto"):
                continue
            # xhx2api 会把生图/改图别名（gpt-image-*、dall-e-*）也列进 /v1/models；
            # 它们 capabilities.chat=false，不能当聊天模型暴露给客户端。
            if up["name"] == "raccoon":
                caps = row.get("capabilities")
                if isinstance(caps, dict) and caps.get("chat") is False:
                    continue
            public = _public_name(up["name"], m)
            if public is None:
                continue  # 另一渠道的清单残留，不对外
            e = {"upstream": m, "row": row}
            if public in by_public:
                # 撞名：优先裸名入口（upstream == public，上游 auto 通道按名分发）
                if by_public[public]["upstream"] != public and m == public:
                    by_public[public] = e
                continue
            by_public[public] = e
            order.append(public)
        for public in order:
            e = by_public[public]
            entry = {"platform": up["name"], "public": public,
                     "upstream": e["upstream"], "row": e["row"]}
            if up["name"] == "workbuddy":
                # PRO 门槛与可用区域来自双区目录（见 _wb_realm_meta）；
                # 每个可用区域都要求 PRO 才算锁定（kimi-k3.1-pro 国际版要
                # 会员但国内版免费，不算锁）
                meta = _wb_realm_meta().get(e["upstream"])
                if meta and meta["realms"]:
                    entry["realms"] = list(meta["realms"])
                    entry["requires_pro"] = bool(meta["pro"]) and set(meta["realms"]) <= set(meta["pro"])
            entries.append(entry)
    with _catalog_pub_lock:
        if generation != _catalog_pub_generation:
            return public_catalog()
        _catalog_pub_cache["all"] = (now, entries)
    return entries


# ---------------------------------------------------------------------------
# WorkBuddy 媒体模型（生图 / 生视频）。
#
# 引擎把媒体目录放在 /v1/media/models，并刻意不混进 /v1/models（聊天客户端
# 会把图片模型当对话模型用）。网关沿用这个边界：聊天目录不动，媒体模型走
# /api/media/models 与 /v1/media/models，生成请求按 /v1/images/*、/v1/videos*
# 原样转发给 workbuddy。媒体与文本共用引擎的 API Key 与媒体号池，网关只做
# 模型名校验、可见性过滤和任务产物 URL 回写。
# ---------------------------------------------------------------------------

def _media_supports_tag(model, tag, realm=None):
    """Check operation support in the requested/current realm, not a union across regions."""
    if not tag:
        return True
    by_realm = model.get("tags_by_realm")
    if isinstance(by_realm, dict):
        selected = realm or model.get("active_realm")
        if selected:
            return tag in (by_realm.get(selected) or [])
        return any(tag in (tags or []) for tags in by_realm.values())
    return tag in (model.get("tags") or [])


WB_MEDIA_REALMS = ("cn", "intl")
WB_MEDIA_TTL = 30.0
_wb_media_lock = threading.Lock()
_wb_media_cache = {"ts": 0.0, "models": [], "errors": {}}
_media_link_secret = secrets.token_bytes(32)


def _media_link_signature(target, expires):
    message = f"{target}|{expires}".encode("utf-8")
    return hmac.new(_media_link_secret, message, hashlib.sha256).hexdigest()


def _media_download_allowed(target):
    original, sep, signature = target.rpartition("&gw_exp=")
    if not sep or not re.fullmatch(r"/v1/videos/[A-Za-z0-9_-]+/content\?[^#]*", original):
        return False
    expires, sep, supplied = signature.partition("&gw_sig=")
    if not sep or not re.fullmatch(r"[0-9]{1,12}", expires):
        return False
    deadline = int(expires)
    return (time.time() <= deadline <= time.time() + 3601
            and hmac.compare_digest(supplied, _media_link_signature(original, expires)))


def _wb_media_fetch(force=False):
    """拉取国内/国际两区媒体目录并按模型 id 合并。

    同一模型可能两区都有（kling-video-v3）；合并后 realms 记录所有可用区域，
    区域专属模型（hunyuan-image-alpha / hy-image-3.5）只标自己那一区。
    """
    now = time.time()
    with _wb_media_lock:
        if not force and now - _wb_media_cache["ts"] < WB_MEDIA_TTL:
            return _wb_media_cache["models"], _wb_media_cache["errors"]
    up = next((u for u in CONFIG["upstreams"] if u["name"] == "workbuddy"), None)
    merged, errors, active_realm = {}, {}, None
    if up:
        for requested in (None, *WB_MEDIA_REALMS):
            query = f"?realm={requested}" if requested else ""
            try:
                req = urllib.request.Request(
                    up["base_url"].rstrip("/") + "/v1/media/models" + query,
                    headers=_upstream_headers(up))
                with _urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                if not isinstance(data, dict) or not isinstance(data.get("models"), list):
                    raise ValueError("媒体目录响应缺少 models 数组")
                realm = data.get("realm")
                if realm not in WB_MEDIA_REALMS:
                    raise ValueError("媒体目录响应缺少有效区域")
                if requested is None:
                    active_realm = realm
            except Exception as exc:
                errors[requested or "active"] = f"{type(exc).__name__}: {exc}"
                continue
            rows = data["models"]
            for row in rows or []:
                if not isinstance(row, dict) or not row.get("id"):
                    continue
                mid = str(row["id"])
                tags = [str(t) for t in (row.get("tags") or []) if str(t)]
                item = merged.get(mid)
                if item is None:
                    item = merged[mid] = {
                        "id": mid,
                        "name": str(row.get("name") or mid),
                        "tags_by_realm": {},
                        "realms": [],
                        "video": row.get("videoGeneration"),
                        "credit_per_image": _known_num(row.get("creditPerImage")),
                        "image_measured": bool(row.get("creditPerImageMeasured")),
                        "credit_per_second": _known_num(row.get("creditPerSecond")),
                        "credit_ceiling": _known_num(row.get("creditPerSecondCeiling")),
                        "credits_per_second": row.get("creditsPerSecond") or {},
                        "credits_with_reference": row.get("creditsPerSecondWithReference") or {},
                    }
                item["realms"].append(realm)
                item["tags_by_realm"][realm] = sorted(set(tags))
                item["tags"] = sorted(set(item.get("tags") or []) | set(tags))
                if item["video"] is None:
                    item["video"] = row.get("videoGeneration")
    models = []
    for item in merged.values():
        item["realms"] = sorted(set(item["realms"]))
        item["tags"] = sorted({tag for tags in item["tags_by_realm"].values() for tag in tags})
        item["kind"] = "video" if any("video" in t for t in item["tags"]) else "image"
        item["active_realm"] = active_realm
        item["available"] = active_realm in item["realms"]
        models.append(item)
    models.sort(key=lambda m: (m["kind"], m["id"]))
    with _wb_media_lock:
        _wb_media_cache.update(ts=now, models=models, errors=errors)
    return models, errors


_raccoon_media_lock = threading.Lock()
_raccoon_media_cache = {"ts": 0.0, "models": [], "errors": {}}
RACCOON_MEDIA_TTL = 30.0
_raccoon_price_lock = threading.Lock()
_raccoon_price_cache = {"ts": 0.0, "price": None}
RACCOON_PRICE_TTL = 300.0


def _raccoon_image_price(up):
    """从积分账单里取最近一笔「图片生成」的扣费，作为每张图的实测单价。

    xhx2api 的生图响应只有 token，没有 credit；真实扣费在
    /panel/api/accounts/{uid}/bills 的 event_name「任务消耗 · 图片生成」里。
    取最近一笔（账单按时间倒序），5 分钟缓存；没有样本返回 None，不猜。
    """
    now = time.time()
    with _raccoon_price_lock:
        if now - _raccoon_price_cache["ts"] < RACCOON_PRICE_TTL:
            return _raccoon_price_cache["price"]
    price = None
    latest = None
    try:
        overview = engine_request(up, "GET", "/panel/api/overview",
                                  auth="bearer", timeout=10)
        accounts = overview.get("accounts") if isinstance(overview, dict) else []
    except Exception:
        accounts = []
    for a in accounts or []:
        uid = str(a.get("uid") or "")
        if not uid:
            continue
        try:
            # 单账号账单失败只跳过该账号：整轮中止会把一次网络抖动放大成
            # 「所有模型未实测」并缓存 5 分钟。
            bills = engine_request(
                up, "GET",
                f"/panel/api/accounts/{quote(uid, safe='')}/bills?limit=100",
                auth="bearer", timeout=15)
        except Exception:
            continue
        rows = (bills.get("bills") if isinstance(bills, dict) else bills) or []
        for r in rows:
            if not isinstance(r, dict):
                continue
            try:
                pts = float(r.get("points"))
            except (TypeError, ValueError):
                continue
            name = str(r.get("event_name") or "")
            if pts < 0 and "图片" in name:
                at = str(r.get("created_at") or "")
                if latest is None or at > latest[0]:
                    latest = (at, abs(pts))
    if latest is not None:
        value = latest[1]
        price = int(value) if float(value).is_integer() else round(value, 2)
    with _raccoon_price_lock:
        # 用抓取结束的时刻记缓存：抓取耗时不吃掉有效 TTL。
        _raccoon_price_cache.update(ts=time.time(), price=price)
    return price


def _raccoon_media_fetch(force=False):
    """xhx2api 的生图模型：/v1/models 里 capabilities.image=true 的行。

    xhx2api 的生图通道忽略 model，raccoon-image-gen / gpt-image-* / dall-e-*
    都是同一条上游通道；这里把引擎列出的每个 id 都作为独立媒体模型展示，
    客户端用哪个别名都能直接选。
    """
    now = time.time()
    with _raccoon_media_lock:
        if not force and now - _raccoon_media_cache["ts"] < RACCOON_MEDIA_TTL:
            return _raccoon_media_cache["models"], _raccoon_media_cache["errors"]
    up = _upstream_by_name("raccoon")
    models, errors = [], {}
    if up and platform_enabled("raccoon"):
        try:
            rows = upstream_rows(up)
        except Exception as exc:
            rows, errors["active"] = None, f"{type(exc).__name__}: {exc}"
        if rows is None and "active" not in errors:
            # upstream_rows 失败返回 None 不抛异常；不显式标注的话「引擎挂了」
            # 和「引擎没配生图模型」在媒体页看起来一样。
            errors["active"] = "raccoon /v1/models 不可达"
        found = []
        for row in rows or []:
            caps = row.get("capabilities")
            mid = str(row.get("id") or "")
            if mid and isinstance(caps, dict) and caps.get("image"):
                found.append((mid, str(row.get("name") or mid)))
        # 规范 id 排最前，其余保持引擎返回顺序（稳定排序，不按字母重排）。
        found.sort(key=lambda item: item[0] != "raccoon-image-gen")
        price = _raccoon_image_price(up)
        # 目录抖动时（引擎重启瞬间少列了某个别名）resolve_media_model 仍能
        # 通过别名 fallback 找到规范行——引擎本就忽略 model，走同一条通道。
        aliases = [mid for mid, _ in found if mid != "raccoon-image-gen"]
        for mid, name in found:
            models.append({
                "id": mid,
                "name": name,
                "tags": ["text-to-image", "image-to-image"],
                # xhx2api 的生图通道按账号池选号，不按模型分区。
                "realms": ["cn", "intl"],
                "video": None,
                "credit_per_image": price,
                "image_measured": price is not None,
                "credit_per_second": None,
                "credit_ceiling": None,
                "credits_per_second": {},
                "credits_with_reference": {},
                "kind": "image",
                "active_realm": None,
                "available": True,
                "aliases": aliases if mid == "raccoon-image-gen" else [],
            })
    with _raccoon_media_lock:
        _raccoon_media_cache.update(ts=now, models=models, errors=errors)
    return models, errors


def public_media_catalog(force=False):
    out = []
    if platform_enabled("workbuddy"):
        models, _ = _wb_media_fetch(force=force)
        out.extend(dict(m, platform="workbuddy", public=m["id"], upstream=m["id"])
                   for m in models)
    if platform_enabled("raccoon"):
        models, _ = _raccoon_media_fetch(force=force)
        out.extend(dict(m, platform="raccoon", public=m["id"], upstream=m["id"])
                   for m in models)
    return out


def resolve_media_model(model, tag=None):
    """只从当前媒体出口中挑可见且支持目标操作的模型（workbuddy / raccoon）。"""
    model = (model or "").strip()
    platform, sep, rest = model.partition("/")
    platform = platform.lower() if sep else ""
    if sep and platform not in ("workbuddy", "raccoon"):
        return None, None, None, f"未知媒体平台 `{platform}`（可用: workbuddy, raccoon）"
    wanted = rest if sep else model
    if sep and not wanted:
        return None, None, None, f"模型名 `{model}` 缺少 `/` 后的模型部分"
    entries = public_media_catalog()
    candidates = [platform] if platform else ["workbuddy", "raccoon"]
    for name in candidates:
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up or not platform_enabled(name):
            continue
        pool = [m for m in entries if (m.get("platform") or "workbuddy") == name]
        active_realm = next((m.get("active_realm") for m in pool
                             if m.get("active_realm")), None)
        entry = None
        wanted_name = wanted
        if not wanted:
            entry = next((m for m in pool if m.get("available")
                          and not _model_hidden(name, m["public"])
                          and _media_supports_tag(m, tag, active_realm)), None)
            if entry is None:
                continue
            wanted_name = entry["public"]
        else:
            entry = next((m for m in pool if m["public"] == wanted), None)
            if entry is None and name == "raccoon":
                # 生图别名（gpt-image-1 等）也能调；引擎忽略 model，统一走同一条通道。
                aliases = next((m.get("aliases") for m in pool if m.get("aliases")), [])
                if wanted in aliases:
                    entry = pool[0]
            if entry is None:
                continue
        if _model_hidden(name, entry["public"]):
            return None, None, None, f"媒体模型 `{model}` 已在控制台隐藏（媒体页可恢复显示）"
        if not entry.get("available"):
            return None, None, None, (f"媒体模型 `{wanted_name}` 不在当前媒体出口，"
                                      f"请检查 {name} 引擎的区域与账号池")
        if tag and not _media_supports_tag(entry, tag, active_realm):
            return None, None, None, f"媒体模型 `{wanted_name}` 不支持 `{tag}` 的当前区域出口"
        return up, wanted_name, (model or f"{name}/{wanted_name}"), None
    return None, None, None, f"未知媒体模型 `{model}`"


#: 媒体生成入口（与引擎 MEDIA_POST_PATHS 对齐）。视频任务的状态/产物/取消
#: 是 /v1/videos/... 的动态路径，单独在 do_POST / do_GET 里判。
MEDIA_POST_PATHS = {
    "/v1/images/generations", "/images/generations",
    "/v1/images/edits", "/images/edits",
    "/v1/videos", "/videos",
}


def _media_multipart_reject(path, content_type):
    """OpenAI 规范的 /v1/images/edits 是 multipart 文件上传；本网关媒体接口
    只收 JSON（image 传 URL 或 data URL）。multipart 放进 _read_post_object
    只会得到误导性的「请求体不是合法 JSON」，所以先识别并让调用方拦下说明。"""
    p = (path or "").split("?")[0]
    if p not in MEDIA_POST_PATHS \
            and not (p.startswith("/v1/videos/") and p.endswith("/cancel")):
        return False
    return "multipart/form-data" in (content_type or "").lower()


def _row_alt_desc(platform, row):
    """给目录页展示的「真实名称 / 描述」。"""
    if platform == "qoder":
        alt = row.get("name_local") or row.get("name")
        desc = row.get("description")
    elif platform == "workbuddy":
        alt = row.get("name")
        desc = row.get("description")
    else:
        alt, desc = None, None
    return alt, desc


_qwen_names_lock = threading.Lock()
_qwen_names_cache = {"ts": 0, "names": {}}


def _qwenwork_display_names(up):
    """千问官方套餐名（flash→标准、frontier-1→Qwen4-Max 预览版…），60s 缓存。

    来源是 Buddy2api 的 /admin/models/catalogs（供应目录 live 快照，带官方 name）。
    """
    now = time.time()
    with _qwen_names_lock:
        if now - _qwen_names_cache["ts"] < 60:
            return _qwen_names_cache["names"]
    names = {}
    try:
        data = engine_request(up, "GET", "/admin/models/catalogs", auth="none")
        for src in (data.get("sources") or []):
            if src.get("channel") != "qwenwork":
                continue
            for m in (src.get("models") or []):
                if isinstance(m, dict) and m.get("id"):
                    names[m["id"]] = str(m.get("name") or "")
    except Exception:
        names = {}
    # ts 与 names 必须在锁内一起写：先写 ts 后写 names 会让并发读者
    # 拿到「新时间戳 + 旧名单」组合，缓存陈旧值长达 60 秒
    with _qwen_names_lock:
        _qwen_names_cache["ts"] = now
        _qwen_names_cache["names"] = names
    return names


def resolve_model(model):
    """把客户端发来的模型名解析成 (upstream, 上游模型名, 客户端可见名, 错误原因)。

    成功时 error 为 None；失败时 upstream 为 None 并在 error 给出原因。
    已在控制台隐藏的模型一律拒绝，客户端既拉不到也调不了。
    """
    model = (model or "").strip()
    entries = public_catalog()
    if "/" in model:
        platform, _, rest = model.partition("/")
        up = next((u for u in CONFIG["upstreams"] if u["name"].lower() == platform.lower()), None)
        if not up:
            return (None, None, None,
                    f"未知平台前缀 `{platform}`（可用: {', '.join(u['name'] for u in CONFIG['upstreams'])}）")
        if not platform_enabled(up["name"]):
            return (None, None, None,
                    f"平台 `{up['name']}` 已在控制台停用（设置页可重新启用）")
        if not rest:
            return None, None, None, f"模型名 `{model}` 缺少 `/` 后的模型部分"
        e = next((x for x in entries
                  if x["platform"] == up["name"] and x["public"] == rest), None)
        if e is None:
            return None, None, None, f"未知模型 `{model}`"
        if _model_hidden(up["name"], rest):
            return (None, None, None,
                    f"模型 `{model}` 已在控制台隐藏（模型页可恢复显示）")
        if _wb_entry_locked(e):
            return (None, None, None,
                    f"模型 `{model}` 需要 WorkBuddy 付费会员（PRO），当前账号池没有付费账号；"
                    "已自动从 /v1/models 隐藏，会员账号入池后自动恢复")
        return up, e["upstream"], model, None

    # 裸模型名：全网关唯一才放行，撞名时要求带前缀。隐藏/PRO 锁定的实例不参与。
    hits, hidden_elsewhere, locked_elsewhere = [], False, False
    for x in entries:
        if x["public"] == model:
            if _model_hidden(x["platform"], model):
                hidden_elsewhere = True
            elif _wb_entry_locked(x):
                locked_elsewhere = True
            else:
                hits.append(x)
    if len(hits) == 1:
        x = hits[0]
        up = next(u for u in CONFIG["upstreams"] if u["name"] == x["platform"])
        return up, x["upstream"], model, None
    if len(hits) > 1:
        prefixes = ", ".join(x["platform"] + "/" + model for x in hits)
        return None, None, None, f"模型 `{model}` 在多个平台存在，请用带前缀的名字: {prefixes}"
    if hidden_elsewhere:
        return None, None, None, f"模型 `{model}` 已在控制台隐藏（模型页可恢复显示）"
    if locked_elsewhere:
        return (None, None, None,
                f"模型 `{model}` 需要 WorkBuddy 付费会员（PRO），当前账号池没有付费账号；"
                "已自动从 /v1/models 隐藏，会员账号入池后自动恢复")
    disabled_platform = _model_in_disabled_platform(model)
    if disabled_platform:
        return (None, None, None,
                f"模型 `{model}` 所属平台 `{disabled_platform}` 已在控制台停用（设置页可重新启用）")
    return None, None, None, f"未知模型 `{model}`"


# ---------------------------------------------------------------------------
# 请求转发。
# ---------------------------------------------------------------------------

HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}


def _connect(up):
    parts = urlsplit(up["base_url"])
    if parts.scheme == "https":
        return HTTPSConnection(parts.netloc, timeout=600)
    return HTTPConnection(parts.netloc, timeout=600)


def _upstream_path(up, path):
    base = urlsplit(up["base_url"]).path.rstrip("/")
    return (base + path) if base else path


def forward(up, method, path, body, client_headers, stream):
    """把请求转发到上游，返回 (status, headers, 响应迭代器/字节)。

    stream=True 走 SSE 分块；stream="raw" 用于二进制/大文件透传（视频产物），
    不缓冲、不改 Content-Type、不覆盖 Accept。
    """
    conn = _connect(up)
    streaming = False  # relay 生成器路径由生成器自己的 finally 关闭连接
    try:
        try:
            headers = _upstream_headers(up)
            for k, v in (client_headers or {}).items():
                lower = k.lower()
                if lower in ("x-api-key", "x-panel-token"):
                    continue
                if lower in ("accept", "user-agent", "anthropic-version", "anthropic-beta") or lower.startswith("x-"):
                    if not any(key.lower() == lower for key in headers):
                        headers[k] = v
            if stream is True:
                headers["Accept"] = "text/event-stream"
            payload = body if body else None
            if payload is not None and isinstance(payload, (bytes, bytearray)):
                headers["Content-Length"] = str(len(payload))
            conn.request(method, _upstream_path(up, path), body=payload, headers=headers)
            resp = conn.getresponse()
            resp_headers = {k.title(): v for k, v in resp.getheaders() if k.lower() not in HOP_HEADERS}
        except Exception:
            conn.close()  # 上游连接失败/超时时显式关闭，不留 socket 等 GC
            raise
        # 客户端要流式、但上游回了非流式响应（如 400/401 错误 JSON）时退回缓冲发送：
        # 否则响应无 Content-Length 也无分帧，客户端会挂到自己超时。
        if stream is True and "event-stream" not in (resp_headers.get("Content-Type") or "").lower():
            data = resp.read()
            return resp.status, resp_headers, data
        if stream:
            streaming = True
            # 关键：上游 SSE 可能是 Connection: close 且**非 chunked**（wb 引擎就是）。
            # 此时 resp.read(8192) 会阻塞到攒满 8192 字节才返回——等于把首字推迟
            # 到一整批 token 生成完（实测 ds-flash +200ms、kimi +800ms）。
            # read1() 只做一次底层读，有多少给多少，首字立刻就能往下传。
            read1 = getattr(resp, "read1", None)

            def relay():
                try:
                    while True:
                        if read1 is not None:
                            chunk = read1(65536)
                        else:  # 老 Python：退回小块 read，至少不会等 8KB
                            chunk = resp.read(1024)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    conn.close()
            return resp.status, resp_headers, relay()
        data = resp.read()
        return resp.status, resp_headers, data
    finally:
        # 覆盖 getresponse 之后的 getheaders/read 抛错（上游中途断开）与缓冲路径兜底；
        # 生成器路径交由 relay() 的 finally
        if not streaming:
            conn.close()


# ---------------------------------------------------------------------------
# 面板会话：登录各引擎的 Web 面板，聚合账号池 / 积分 / 签到。
# ---------------------------------------------------------------------------

_panel_lock = threading.Lock()
_panel_sessions = {}  # name -> {"token": str, "kind": "panel"}


def _panel_login(up):
    """登录 hub 系引擎的面板，返回 session token。"""
    password = (up.get("panel_password") or "admin").strip()
    body = json.dumps({"password": password}).encode("utf-8")
    req = urllib.request.Request(
        up["base_url"].rstrip("/") + "/panel/login",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with _urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    token = data.get("token") or data.get("panel_token") or ""
    if not token:
        raise ValueError("面板登录成功但没有返回 token")
    return token


def _panel_token(up, force=False):
    name = up["name"]
    with _panel_lock:
        hit = _panel_sessions.get(name)
        if hit and not force:
            return hit["token"]
    token = _panel_login(up)
    with _panel_lock:
        _panel_sessions[name] = {"token": token, "ts": time.time()}
    return token


def _http_error_detail(exc, limit=300):
    """Read an HTTPError body once so callers can preserve upstream diagnostics."""
    cached = getattr(exc, "_gateway_detail", None)
    if cached is not None:
        return cached[:limit]
    try:
        raw = exc.read(64 * 1024)
    except Exception:
        raw = b""
    if isinstance(raw, bytes):
        detail = raw.decode("utf-8", "ignore")
    else:
        detail = str(raw or "")
    try:
        exc._gateway_detail = detail
    except Exception:
        pass
    return detail[:limit]


def engine_request(up, method, path, body=None, auth="auto", timeout=15):
    """对引擎发管理请求。auth: panel（hub 面板）/ bearer（引擎 API Key）/
    none（Buddy2api 本地免鉴权）/ auto。

    返回解析后的 JSON（或原始 dict）。面板 401/403 时自动重登一次。
    """
    kind = auth
    if kind == "auto":
        kind = "none" if up.get("admin_kind") == "open" else "panel"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    for attempt in (1, 2):
        if kind == "panel":
            headers["X-Panel-Token"] = _panel_token(up, force=(attempt == 2))
        elif kind == "bearer":
            key = (up.get("api_key") or "").strip()
            if key:
                headers["Authorization"] = "Bearer " + key
        req = urllib.request.Request(
            up["base_url"].rstrip("/") + path,
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            with _urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            _http_error_detail(exc, 64 * 1024)  # consume once; callers may include the cached detail
            if exc.code in (401, 403) and kind == "panel" and attempt == 1:
                continue  # 会话失效，强刷重登
            raise
    raise RuntimeError("unreachable")


# ---------------------------------------------------------------------------
# 各引擎账号行归一化：字段名各引擎不同，这里统一成看板用的形状。
# ---------------------------------------------------------------------------

def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _known_num(v):
    """None/invalid means unknown; a real 0 stays 0."""
    if v is None or isinstance(v, bool):
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n != n or n in (float("inf"), float("-inf")):
        return None
    return n


def _epoch_seconds(value):
    """epoch 秒；毫秒时间戳自动归一，非法值返回 None（不猜时间）。

    各引擎的到期字段单位不统一：小浣熊的积分到期是秒、它的凭证 expires_at
    是毫秒，Trae 的 expire_time 是秒。混着用会把 2026 年算成 5 万年以后，
    所以统一在这里归一。
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts != ts or ts <= 0:
        return None
    if ts > 4e9:
        ts /= 1000.0
    if not (0 < ts < 4e9):  # 越界值会让 localtime 在 Windows 抛错
        return None
    return ts


def _expiry_row(account, name, remain, ts, burn_days=None):
    """统一的临期行：days_left 一律由到期时间现算。

    上游的「可用天数」多数是「按当前燃烧速度还能撑多久」（小浣熊
    audit.days_left 就是 remain ÷ burn_per_day），跟「还有几天过期」是两件
    事；直接塞进同一列会出现「还剩 374 天」而实际 6 天后清零。所以速度
    预测单独放 burn_days，由前端另列展示。
    """
    burn = _known_num(burn_days)
    return {"account": str(account or ""),
            "name": str(name or "积分包"),
            "remain": round(float(remain), 2),
            "expire": time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)),
            "days_left": round((ts - time.time()) / 86400, 1),
            "burn_days": (round(burn, 1) if burn is not None else None)}


def _trae_pack_name(base):
    """Trae 权益包的展示名：display_desc 偶尔为空，退回 product_extra 里的包名。"""
    extra = (base or {}).get("product_extra")
    if not isinstance(extra, dict):
        return ""
    for key in ("package_extra", "subscription_extra"):
        inner = extra.get(key)
        if isinstance(inner, dict) and inner.get("package_name"):
            return str(inner["package_name"])
    return ""


def normalize_wb(a):
    c = a.get("credits") or {}
    ck = c.get("checkin") or {}
    tier = a.get("tier") or {}
    # 国际版「今天打过没有」以引擎的 lastDailyChat 为准。日期比较必须放在服务端：
    # 浏览器时区与引擎不一致时，前端自己比会一直显示「待打卡」。
    last_dc = str(a.get("lastDailyChat") or "")
    today = time.strftime("%Y-%m-%d")
    return {
        "uid": a.get("uid"),
        "name": a.get("nickname") or (a.get("uid") or "")[:8],
        "sub": tier.get("package") or a.get("realm", ""),
        "enabled": bool(a.get("enabled", True)) and not a.get("accountBanned"),
        "remain": _known_num(c.get("remain")), "size": _known_num(c.get("size")),
        "pct": c.get("remain_percent") or "",
        "checked_in": bool(ck.get("today_checked_in")),
        "can_checkin": bool(a.get("canCheckin")),
        "can_daily_chat": bool(a.get("canDailyChat")),
        # canDailyChat 是静态标记（realm==intl 恒 true），「今天打过没有」的
        # 真实闸门是 lastDailyChat 的日期（引擎 can_daily_chat() 的判定口径）
        "last_daily_chat": a.get("lastDailyChat"),
        "daily_chatted_today": bool(last_dc.startswith(today)),
        "realm": a.get("realm"),
        "streak": ck.get("streak_days"),
        # 引擎已经记录的错误标记（面板「当前禁用」的同源数据）：
        # lastError=最近一次失败；modelCaps=6004 配额耗尽（含上游给的恢复墙钟，
        # 跨重启成立）；disabledModels=面板手动禁用的模型。之前全被丢掉，
        # 账号页看不到任何异常标记。
        "last_error": (str(a.get("lastError") or "").strip()[:200] or None),
        "cooldown_until": _known_num(a.get("cooldownUntil")),
        "model_caps": a.get("modelCaps") if isinstance(a.get("modelCaps"), dict) else {},
        "disabled_models": [str(m) for m in (a.get("disabledModels") or [])],
    }


def normalize_qoder(a):
    c = a.get("credits") or {}
    return {
        "uid": a.get("uid"),
        "name": a.get("nickname") or (a.get("uid") or "")[:8],
        "sub": a.get("plan") or a.get("realm", ""),
        "enabled": bool(a.get("enabled", True)) and not a.get("accountBanned"),
        "remain": _known_num(c.get("remain")), "size": _known_num(c.get("size")),
        "pct": c.get("usage_pct") if isinstance(c.get("usage_pct"), str) else "",
        "checked_in": bool(a.get("checkedInToday")),
        "can_checkin": bool(a.get("canCheckin")),
        "can_daily_chat": None,
        "realm": a.get("realm"),
        "streak": None,
        # 账号级并发：引擎 account_views() 附带（在飞租约数 / 闸门上限，0=不限）
        "inflight": a.get("inflight"),
        "concurrency_limit": a.get("concurrencyLimit"),
    }


def _buddy_realm(domain):
    """千问办公账号的区域：引擎只给域名（qwenwork 行的 domain 形如
    gateway.qwenwork.cn），按后缀判国内/国际；没有域名就返回 None（显示 —）。"""
    d = str(domain or "").strip().lower()
    if not d:
        return None
    return "cn" if d.endswith(".cn") else "intl"


def normalize_buddy(a):
    # Buddy2api 账号行的快照来自刷新流程，而 qwenwork 的刷新令牌被官方客户端
    # 设备绑定、续期必被拒，快照永远是 0；真实余额走 /admin/accounts/{id}/resources
    # （读全部钱包，含企业分配包）。resources 没拿到时显示「以官方客户端为准」。
    res = a.get("_resources") or {}
    remaining = res.get("remaining")
    limit = res.get("limit")
    if remaining is not None:
        remain = _num(remaining)
        size = _num(limit) if limit is not None else None
    else:
        remain, size = None, None
    acct_type = str(a.get("account_type") or "")
    org = str(a.get("org_name") or "")
    plan = str(a.get("plan_name") or "")
    if acct_type == "enterprise":
        sub = "企业 · " + (org or plan or "企业版")
    else:
        sub = "个人" + ((" · " + plan) if plan else "")
    return {
        "uid": a.get("id"),
        "name": a.get("nickname") or a.get("name") or str(a.get("id")),
        "sub": sub,
        "enabled": a.get("status") == "active",
        "remain": remain,
        "size": size,
        "pct": ("%.1f%%" % (remain / size * 100)) if size else "",
        "checked_in": None,
        "can_checkin": True,
        "can_daily_chat": None,
        "realm": _buddy_realm(a.get("domain")),
        "domain": a.get("domain"),
        "account_type": acct_type,
        "enterprise_id": a.get("enterprise_id") or "",
        "org_name": org,
        "plan_name": plan,
        # 引擎导入时发现「资料是企业、token 是个人」会写这条；页面据此提示
        "identity_mismatch": a.get("identity_mismatch"),
        "streak": None,
    }


def _shanghai_today():
    """东八区日期：xhx2api 的 checked_in 用上海时区记日期，不能拿本机时区直接比。"""
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + 8 * 3600))


def normalize_raccoon(a):
    """xhx2api /panel/api/overview 的账号行 → 看板形状。

    credits 是上游积分余额（未知/缺失保持 None）；checked_in 是东八区日期，
    只用来判断「今天领过没有」；paused/disabled 都表示当前不参与调度。
    """
    uid = str(a.get("uid") or "")
    return {
        "uid": uid,
        "name": a.get("nickname") or (uid[:8] if uid else "?"),
        "sub": "小浣熊",
        "enabled": not a.get("disabled") and not a.get("paused"),
        "remain": _known_num(a.get("credits")),
        "size": None,
        "pct": "",
        "checked_in": str(a.get("checked_in") or "") == _shanghai_today(),
        "can_checkin": True,
        "can_daily_chat": None,
        "realm": a.get("realm") if a.get("realm") in ("cn", "intl") else None,
        "streak": None,
        "inflight": a.get("in_flight"),
        "concurrency_limit": None,
    }


def normalize_trae(a):
    """trae2api-hub /accounts 的账号行 → 看板形状。

    credits 来自官方 v2 entitlement 的 usage_summary（remain/limit），
    checked_in 是引擎记录的当日签到；uid 已由引擎脱敏（只留后 4 位），
    看板用 uidHash 做稳定标识。国际版另有 intlQuota，这里不并入积分。
    """
    c = a.get("credits") or {}
    profile = a.get("account") or {}
    status = str(a.get("status") or "")
    remain = _known_num(c.get("remain"))
    size = _known_num(c.get("limit"))
    realm = a.get("realm") if a.get("realm") in ("cn", "intl") else None
    realm_name = a.get("realmName") or ("国内版" if realm == "cn" else
                                        ("国际版" if realm == "intl" else ""))
    return {
        "uid": a.get("uidHash") or a.get("uid"),
        "name": profile.get("username") or a.get("uid") or "Trae 账号",
        "sub": " · ".join(x for x in [realm_name, str(c.get("plan") or "")] if x),
        "enabled": bool(a.get("enabled")) and status not in ("disabled", "expired"),
        "remain": remain,
        "size": size,
        "pct": ("%.1f%%" % (remain / size * 100)) if (remain is not None and size) else "",
        "checked_in": (bool(c.get("checked_in")) if realm == "cn" else None),
        # Trae 只有国内版有每日签到；国际版没有该接口（引擎如实跳过）。
        "can_checkin": bool(realm == "cn" and a.get("enabled")
                            and not c.get("checked_in")),
        "can_daily_chat": None,
        "realm": realm,
        "domain": "trae.com.cn" if realm == "cn" else ("trae.ai" if realm == "intl" else None),
        "streak": None,
        "inflight": a.get("inflight"),
        "concurrency_limit": None,
        "account_status": status,
        "cooldown_for": a.get("cooldownFor"),
        "last_error": a.get("lastError") or "",
    }


#: 浏览器同步的 CodeArts 积分快照（由 cftk 会话取数后落盘）。
#:
#: 为什么不能直接调上游：`/portal/snap-manager/v1/statistics/plugin` 只认真实
#: 浏览器会话——同样的 cookie 与 header，curl/python/Go 客户端一律拿到
#: **HTTP 200 空响应体**（华为云 WAF 的 TLS/JS 指纹静默拦截，不是 403）。
#: 所以余额只能由浏览器取一次、写进这个文件，网关读它展示。
CODEARTS_CREDITS_FILE = os.path.join(HERE, "codearts_credits.json")
#: 快照超过这个时长就标注「数据陈旧」，避免把旧余额当实时值。
CODEARTS_CREDITS_STALE = 24 * 3600


def _codearts_credits_snapshot():
    """读浏览器同步的积分快照；没有或损坏返回 None。"""
    try:
        with open(CODEARTS_CREDITS_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    total = data.get("total") if isinstance(data.get("total"), dict) else {}
    remain = _known_num(total.get("remain"))
    if remain is None:
        return None
    synced_at = None
    try:
        synced_at = datetime.fromisoformat(
            str(data.get("synced_at") or "").replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        synced_at = None
    return {
        "remain": remain,
        "size": _known_num(total.get("amount")),
        "used": _known_num(total.get("used")),
        "expiring": _known_num(total.get("expiring")) or 0,
        "end_date": str(data.get("end_date") or ""),
        "synced_at": synced_at,
        "stale": bool(synced_at and time.time() - synced_at > CODEARTS_CREDITS_STALE),
        "basic": data.get("basic") if isinstance(data.get("basic"), dict) else {},
        "bonus": data.get("bonus") if isinstance(data.get("bonus"), dict) else {},
    }


def normalize_codearts(a):
    """codearts2api /admin/api/overview 的账号行 → 看板形状。

    引擎不查积分余额（`credits` 恒为 0，只记录领取动作成功与否），所以
    `remain` 不能取引擎字段。真实余额来自浏览器同步的快照文件
    （见 CODEARTS_CREDITS_FILE 的说明：上游只认真实浏览器，WAF 指纹拦截
    其他客户端）。没有快照时 remain 保持 None——填 0 会被当成「已用完」。
    能展示的登录态：token 剩余有效期、冷却、禁用、最近错误、并发占用。

    `checkin` 是 {ok, message, checked_at}；checked_at 为 RFC3339，取日期部分
    与东八区今天比较。
    """
    uid = str(a.get("uid") or a.get("name") or "")
    ck = a.get("checkin") if isinstance(a.get("checkin"), dict) else {}
    checked_at = str(ck.get("checked_at") or "")[:10]
    cool_until = _iso_epoch(a.get("cool_until")) if a.get("cooling") else None
    snap = _codearts_credits_snapshot()
    remain = snap["remain"] if snap else None
    size = (snap["size"] if snap and snap.get("size") else None)
    sub = "码道 · token " + str(a.get("token_remaining") or "?")
    if snap:
        sub += " · 余额同步于 " + time.strftime(
            "%m-%d %H:%M", time.localtime(snap["synced_at"])) if snap.get("synced_at") else ""
    return {
        "uid": uid,
        "name": a.get("nickname") or a.get("user_name") or (uid[:8] if uid else "?"),
        "sub": sub,
        "enabled": not a.get("disabled"),
        "remain": remain,
        "size": size,
        "pct": ("%.1f%%" % (remain / size * 100)) if (remain is not None and size) else "",
        "checked_in": bool(ck.get("ok")) and checked_at == _shanghai_today(),
        "can_checkin": True,
        "can_daily_chat": None,
        "realm": "cn",
        "streak": None,
        "inflight": a.get("active_concurrent"),
        "concurrency_limit": a.get("max_concurrent"),
        "cooldown_until": cool_until or None,
        "cooldown_kind": a.get("cool_kind") or None,
        "last_error": (str(a.get("reason") or a.get("last_error") or "").strip()[:200] or None),
        # 积分快照的元信息：让前端能标注「数据陈旧」与到期日
        "credits_synced_at": snap["synced_at"] if snap else None,
        "credits_stale": snap["stale"] if snap else None,
        "credits_expiring": snap["expiring"] if snap else None,
        "credits_end_date": snap["end_date"] if snap else None,
    }


NORMALIZERS = {"workbuddy": normalize_wb, "qoder": normalize_qoder,
               "qwenwork": normalize_buddy, "raccoon": normalize_raccoon,
               "trae": normalize_trae, "codearts": normalize_codearts}


def _signable_account(a):
    """这个账号今天是否有「每日一次」的动作要完成。

    wb 国内/qoder 看 checked_in 是否有值（签到体系存在），wb 国际看活跃打卡
    资格；qwenwork 的 checked_in 恒为 None（上游自动发放），不计入分母。
    qoder 签完后 canCheckin 会变 false，所以不能用 can_checkin 判分母。
    """
    if a.get("realm") == "intl" and a.get("can_daily_chat"):
        return True
    return a.get("checked_in") is not None


# ---------------------------------------------------------------------------
# 上游版本更新检测：qoder/buddy 走 GitHub Releases 的 302 跳转（HEAD 请求拿
# 最终 URL 里的 tag，不吃 API 60次/小时的匿名配额）；workbuddy 用引擎自带的
# 更新检查端点（官方 GitHub 仓库已转 Telegram 分发，无稳定发布源）。
# ---------------------------------------------------------------------------

_updates_lock = threading.Lock()
_updates_cache = {"ts": 0.0, "data": None}
UPDATES_TTL = 3600.0

# 小浣熊积分到期（/panel/api/points）比 overview 慢一个量级，单独缓存。
# 失败也要短暂退避：否则端点挂掉时每轮 /api/status 都要等一次超时。
_raccoon_points_lock = threading.Lock()
_raccoon_points_cache = {"ts": 0.0, "by_uid": {}, "fail_ts": 0.0}
RACCOON_POINTS_TTL = 300.0
RACCOON_POINTS_FAIL_TTL = 60.0

# 官方 wb 仓库已下线（GitHub 404），引擎自带检查因此永远拿不到 latest。
# 回退到社区镜像的 release tag：只作参照，明确标注镜像可能滞后；可用
# config.json 的 wb_update_mirror（owner/repo）覆盖这一默认值。
WB_UPDATE_MIRRORS = ("niyouqianma/workbuddy2api-hub",)


def _gateway_update_error(exc):
    return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:240]}"}


_gateway_update_lock = threading.Lock()
_gateway_update_cache = {"ts": 0.0, "data": None}
GATEWAY_UPDATE_TTL = 300.0
# 部署任务状态：idle → preflight → scheduled（随后本网关退出，由独立更新进程接管）；
# preflight 失败回到 failed，可重试。scheduled 之后进程即将退出，状态仅供退出前轮询。
_gateway_update_job = {"status": "idle", "version": "", "commit": "", "error": "", "ts": 0.0}
_SERVER = None  # main() 里赋值；自更新调度成功后用它优雅退出


def _gateway_update_snapshot(force=False):
    """Read-only official Release check; never touches the checkout."""
    now = time.time()
    with _gateway_update_lock:
        if (not force and _gateway_update_cache["data"] is not None
                and now - _gateway_update_cache["ts"] < GATEWAY_UPDATE_TTL):
            return _gateway_update_cache["data"]
    try:
        import gateway_update
        current = gateway_update.get_current_version(HERE)
        status = gateway_update.repository_status(HERE)
        latest = gateway_update.fetch_latest_release()
        if latest.get("error"):
            data = {"ok": True, "current": current, "repo": status,
                    "latest": None, "has_update": None, "error": latest["error"]}
        else:
            data = {"ok": True, "current": current, "repo": status,
                    "latest": latest, "has_update": gateway_update.is_newer(
                        current, latest.get("version")), "error": ""}
        with _gateway_update_lock:
            _gateway_update_cache.update(ts=now, data=data)
        return data
    except Exception as exc:
        data = {"ok": False, "current": None, "latest": None,
                "has_update": None, "error": str(exc)[:240]}
        with _gateway_update_lock:
            _gateway_update_cache.update(ts=now, data=data)
        return data


def _gateway_update_job_state():
    with _gateway_update_lock:
        return dict(_gateway_update_job)


def _gateway_update_deploy(snapshot):
    """确认后才允许的一键部署：后台预检并拉起独立更新进程，随后本网关退出。"""
    if not isinstance(snapshot, dict) or not snapshot.get("ok") or snapshot.get("has_update") is not True:
        raise ValueError("没有可部署的更新：请先检查到更新的官方正式版 Release")
    latest = snapshot.get("latest") or {}
    with _gateway_update_lock:
        if _gateway_update_job["status"] in ("preflight", "scheduled"):
            raise ValueError("已有更新部署在进行中，请等待完成")
        _gateway_update_job.update(status="preflight", version=latest.get("version", ""),
                                   commit=latest.get("commit", ""), error="", ts=time.time())
    threading.Thread(target=_gateway_update_deploy_thread, args=(snapshot,),
                     name="gateway-update-deploy", daemon=True).start()
    return {"ok": True, "status": "preflight"}


def _gateway_update_deploy_thread(snapshot):
    try:
        import gateway_update
        result = gateway_update.schedule_update(HERE, snapshot, os.getpid())
        with _gateway_update_lock:
            _gateway_update_job.update(status="scheduled", version=result["version"],
                                       commit=result["commit"], error="", ts=time.time())
        log(f"gateway self-update scheduled: {json.dumps(result, ensure_ascii=False)}；即将退出交给更新进程")
        time.sleep(1.0)  # 让 202 响应先冲刷出去，再停 serve_forever
        if _SERVER is not None:
            _SERVER.shutdown()
    except Exception as exc:
        message = f"{type(exc).__name__}: {str(exc)[:240]}"
        with _gateway_update_lock:
            _gateway_update_job.update(status="failed", error=message, ts=time.time())
        log(f"gateway self-update preflight failed: {message}")

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")


def _wb_mirror_update(row):
    """官方源不可用时用镜像 tag 补齐 latest；成功返回 True。"""
    mirrors = []
    configured = str(CONFIG.get("wb_update_mirror") or "").strip()
    for repo in ((configured,) if configured else ()) + WB_UPDATE_MIRRORS:
        if repo and repo not in mirrors and _REPO_RE.fullmatch(repo):
            mirrors.append(repo)
    last_error = ""
    for repo in mirrors:
        try:
            latest = _github_latest(repo)
        except Exception as exc:
            last_error = f"{repo}: {type(exc).__name__}: {exc}"
            continue
        row["latest"] = latest
        row["source"] = f"mirror:{repo}"
        row["mirror"] = True
        cur_t, lat_t = _ver_tuple(row.get("current")), _ver_tuple(str(latest).lstrip("vV"))
        if cur_t and lat_t:
            row["has_update"] = lat_t > cur_t
        return True
    if last_error:
        row["error"] = (row.get("error") or "") + f"；社区镜像也失败: {last_error}"[:180]
    return False


def _ver_tuple(s):
    m = re.search(r"(\d+(?:\.\d+){0,3})", str(s or ""))
    return tuple(int(x) for x in m.group(1).split(".")) if m else ()


def _github_latest(repo):
    """GitHub releases/latest 的 302 目标 → tag 名；失败抛异常。"""
    req = urllib.request.Request(
        f"https://github.com/{repo}/releases/latest", method="HEAD",
        headers={"User-Agent": "gateway-hub-update-check"})
    try:
        with _direct_opener.open(req, timeout=15) as resp:
            final = resp.geturl()
    except Exception:
        with _urlopen(req, timeout=15) as resp:
            final = resp.geturl()
    m = re.search(r"/releases/tag/([^/?#]+)", final)
    if not m:
        raise RuntimeError("release 页面没有 tag")
    return m.group(1)


def _engine_version(up):
    """引擎当前版本：/health JSON 的 version 字段，或 Server 头里的 x.y.z。"""
    if up.get("name") == "raccoon":
        # xhx2api 的 /healthz 不带版本，/status 才有（Bearer = 引擎 api_key）。
        data = engine_request(up, "GET", "/status", auth="bearer", timeout=8)
        return data.get("version"), data
    req = urllib.request.Request(up["base_url"].rstrip("/") + "/health")
    with _urlopen(req, timeout=8) as resp:
        raw = resp.read().decode("utf-8", "ignore") or "{}"
        data = json.loads(raw)
        ver = data.get("version")
        if not ver:
            m = re.search(r"/(\d+(?:\.\d+){0,3})", resp.headers.get("Server") or "")
            ver = m.group(1) if m else None
    return ver, data


def _collect_updates(force=False):
    now = time.time()
    if not force:
        with _updates_lock:
            if now - _updates_cache["ts"] < UPDATES_TTL and _updates_cache["data"] is not None:
                return _updates_cache["data"]
    rows = []
    for up in active_upstreams():
        row = {"platform": up["name"], "current": None, "latest": None,
               "has_update": None, "source": "", "error": ""}
        try:
            ver, _health = _engine_version(up)
            row["current"] = ver
        except Exception as exc:
            row["error"] = f"引擎不可达: {type(exc).__name__}"
        if up["name"] == "workbuddy":
            try:
                st = engine_request(up, "GET", "/updates")
                row["current"] = st.get("current_version") or row["current"]
                row["latest"] = st.get("latest_version") or None
                row["source"] = "engine"
                row["checked_at"] = st.get("last_attempt")
                engine_error = ""
                if st.get("last_error"):
                    engine_error = "引擎更新检测失败: " + str(st["last_error"])[:140]
                elif st.get("checking"):
                    engine_error = "引擎正在检查更新，请稍后刷新"
                elif st.get("last_attempt") and not row["latest"]:
                    engine_error = "引擎检测未返回可用发布版本"
                elif not st.get("last_attempt"):
                    engine_error = "尚未执行引擎更新检测"
                if row["latest"]:
                    row["has_update"] = bool(st.get("update_available"))
                    row["error"] = engine_error
                elif _wb_mirror_update(row):
                    # 官方源 404：镜像 tag 只作参照，绝不标成官方结论。
                    row["error"] = (engine_error + "；已改用社区镜像检测，镜像可能滞后，仅供参照"
                                    if engine_error else "已改用社区镜像检测，镜像可能滞后")
                else:
                    row["error"] = engine_error + "（官方 Telegram 渠道仍是权威来源）"
            except Exception as exc:
                row["error"] = f"引擎 /updates 不可用: {type(exc).__name__}"
                if _wb_mirror_update(row):
                    row["error"] += "；已改用社区镜像检测，镜像可能滞后"
        else:
            repo = (up.get("repo") or "").strip()
            if not repo:
                row["error"] = "未配置 repo（config.json upstreams[].repo）"
            else:
                try:
                    latest = _github_latest(repo)
                    row["latest"] = latest
                    row["source"] = f"github:{repo}"
                    cur_t, lat_t = _ver_tuple(row["current"]), _ver_tuple(str(latest).lstrip("vV"))
                    if cur_t and lat_t:
                        row["has_update"] = lat_t > cur_t
                except Exception as exc:
                    row["error"] = f"GitHub 检查失败: {type(exc).__name__}: {exc}"[:180]
        rows.append(row)
    with _updates_lock:
        _updates_cache["ts"] = now
        _updates_cache["data"] = rows
    return rows


def _raccoon_points_audit(up):
    """uid → audit（含积分到期 expire）。5 分钟缓存，失败返回空 dict。

    该端点在引擎侧要聚合全部账号积分流水，实测 400-660ms；而 /api/status 被
    前端高频轮询，直连会把总览拖慢。成功后缓存 5 分钟；失败退避 60 秒再重试，
    避免端点挂掉时每轮轮询都卡一次超时。拿不到时只让临期行缺省，不影响余额
    与账号展示。
    """
    now = time.time()
    with _raccoon_points_lock:
        cache = _raccoon_points_cache
        if now - cache["ts"] < RACCOON_POINTS_TTL:
            return cache["by_uid"]
        if now - float(cache.get("fail_ts") or 0.0) < RACCOON_POINTS_FAIL_TTL:
            return {}
    try:
        data = engine_request(up, "GET", "/panel/api/points", auth="bearer")
    except Exception:
        with _raccoon_points_lock:
            _raccoon_points_cache["fail_ts"] = time.time()
        return {}
    by_uid = {}
    for row in ((data.get("accounts") if isinstance(data, dict) else data) or []):
        if isinstance(row, dict) and isinstance(row.get("audit"), dict):
            by_uid[str(row.get("uid") or "")] = row["audit"]
    with _raccoon_points_lock:
        _raccoon_points_cache.update(ts=time.time(), by_uid=by_uid)
    return by_uid


def platform_status(up):
    """单个引擎的健康 + 账号池聚合。"""
    name = up["name"]
    expiry_sources = {"workbuddy": "credits.earliest_expiring",
                      "qoder": "credits.expires_at",
                      "raccoon": "points audit",
                      "trae": "entitlement packs",
                      "codearts": "engine 不提供积分余额/到期",
                      "qwenwork": "qwenwork_reset_at configuration"}
    out = {"name": name, "base_url": up["base_url"], "health": False, "accounts": [],
           "inflight": _inflight.get(name, 0), "error": "",
           "observed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
           "data_state": "unknown", "balance_state": "unknown",
           "expiry_state": "unknown", "expiry_source": expiry_sources.get(name, "")}
    try:
        # hub 系引擎是 /health；xhx2api 与 codearts2api 用 /healthz（同样是 JSON status=ok）。
        health_path = "/healthz" if name in ("raccoon", "codearts") else "/health"
        req = urllib.request.Request(up["base_url"].rstrip("/") + health_path)
        with _urlopen(req, timeout=5) as resp:
            raw = resp.read().decode("utf-8", "replace").strip()
        try:
            body = json.loads(raw)
        except ValueError:
            # codearts2api 的 /healthz 是纯文本 "ok"，不是 JSON。
            body = raw
        # 各引擎 health 形状不同：hub 系 {"ok": true}，Buddy2api {"status": "ok"}，
        # codearts2api 纯文本 "ok"。
        if isinstance(body, str):
            out["health"] = body.lower() == "ok"
        elif "ok" in body:
            out["health"] = bool(body.get("ok"))
        elif "status" in body:
            out["health"] = body.get("status") == "ok"
        else:
            out["health"] = True  # 200 且无标准字段，视为可达
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["data_state"] = "offline"
        out["expiry_state"] = "error"
        return out
    try:
        if name == "qwenwork":
            data = engine_request(up, "GET", "/admin/accounts", auth="none")
            items = data if isinstance(data, list) else data.get("accounts") or []
            # Buddy2api 里可能混着误导入的 workbuddy 渠道账号（真实扣费在 8788
            # 引擎），混在千问卡片下只会让人误读，这里只留 qwenwork 渠道。
            items = [a for a in items if isinstance(a, dict) and a.get("provider") == "qwenwork"]
            for a in items:
                try:  # 真实余额在 wallets（含企业分配包），resources 端点会读全钱包
                    res = engine_request(up, "GET", f"/admin/accounts/{a.get('id')}/resources", auth="none")
                    if isinstance(res, dict) and res.get("remaining") is not None:
                        a["_resources"] = res
                        # 余额历史落盘：后续可用「两次快照差 ÷ 期间调用的 token」
                        # 反推千问办公各模型的实测单价。
                        _append_wallet_snapshot(
                            os.path.join(HERE, "qwenwork_wallet_history.jsonl"),
                            a.get("nickname") or str(a.get("id")),
                            res.get("remaining"))
                except Exception:
                    pass

        elif name == "raccoon":
            # xhx2api 的逐账号清单在 /panel/api/overview（Bearer = 引擎 api_key）；
            # /status 只回聚合计数，不能拿来展示余额。
            data = engine_request(up, "GET", "/panel/api/overview", auth="bearer")
            items = data.get("accounts") if isinstance(data, dict) else data
            items = items or []
            # 积分到期只在 /panel/api/points 的 audit 里（overview 的 expires_at
            # 是登录凭证过期，不是积分）。该端点单次要 400-660ms（overview 只有
            # 15ms），而 /api/status 每轮询都走这里，所以单独缓存 5 分钟；失败
            # 只让临期行缺省，不拖垮账号与余额聚合。
            by_uid = _raccoon_points_audit(up)
            with _raccoon_points_lock:
                points_cache = dict(_raccoon_points_cache)
            if points_cache.get("fail_ts", 0) > points_cache.get("ts", 0):
                out["expiry_state"] = "error"
            else:
                out["expiry_state"] = "known"
            for a in items:
                if isinstance(a, dict) and by_uid:
                    a["_audit"] = by_uid.get(str(a.get("uid") or ""))
        elif name == "codearts":
            # codearts2api 的账号清单在 /admin/api/overview（Bearer = 引擎 api_key）。
            # 引擎不查积分余额（credits 恒 0），所以这里只取账号状态；临期行为空。
            data = engine_request(up, "GET", "/admin/api/overview", auth="bearer")
            items = data.get("accounts") if isinstance(data, dict) else data
            items = items or []
        else:
            # hub 系 /accounts 默认只返回当前区域视图，realm=all 才是全池
            data = engine_request(up, "GET", "/accounts?realm=all")
            items = data.get("accounts") if isinstance(data, dict) else data
            items = items or []
        norm = NORMALIZERS.get(name, normalize_buddy)
        out["accounts"] = [norm(a) for a in items if isinstance(a, dict)]
        if name == "trae":
            # Trae 的 usage 行不带逐请求扣费（credit_reported 恒 false）。真正可用的
            # 实扣来源是 entitlement 的 usage_summary.consumed_amount —— 平台自己
            # 结算的累计已消耗积分，只增不减，不受充值/签到干扰。
            #
            # 但引擎 /accounts 返回的是缓存快照，只有 POST /accounts/credits 才会
            # 去上游刷新。所以这里节流地主动刷新一次（默认 10 分钟），再记录
            # 计数器快照；否则 consumed_amount 永远是旧值，区间增量恒为 0。
            if (time.time() - _trae_refresh_at.get("ts", 0.0) >= TRAE_REFRESH_INTERVAL
                    and _trae_refresh_lock.acquire(blocking=False)):
                # 非阻塞单飞：/api/status 会被前端轮询，慢刷新（90s 超时）期间
                # 其余轮询线程直接跳过，不再按轮询频率放大成几十个并发 POST。
                try:
                    engine_request(up, "POST", "/accounts/credits", body={}, timeout=90)
                    data = engine_request(up, "GET", "/accounts?realm=all")
                    items = data.get("accounts") if isinstance(data, dict) else data
                    items = items or []
                    out["accounts"] = [norm(a) for a in items if isinstance(a, dict)]
                    # POST+GET 都成功才算完成刷新；GET 失败时保留旧 ts，
                    # 下个轮询周期可以立刻重试而不是白等一个完整窗口。
                    _trae_refresh_at["ts"] = time.time()
                except Exception as exc:
                    # 刷新失败不影响账号展示：用缓存值继续，并把原因留在卡片上。
                    out["credits_error"] = f"{type(exc).__name__}: {exc}"[:200]
                finally:
                    _trae_refresh_lock.release()
            for raw, row in zip(items, out["accounts"]):
                if not isinstance(raw, dict):
                    continue
                # 快照键用 uid 尾 4 位：引擎 /accounts 脱敏成「…4052」，而 usage
                # 行里是完整 uid（1414412225494052），两边只有后 4 位一致。
                account_id = _trae_wallet_account(raw, row)
                _append_spend_snapshot(account_id, _trae_consumed_amount(raw))
        # 账号并发：wb 引擎逐账号给在飞数（/settings.account_inflight）+ 每账号
        # 上限（account_max_concurrent，0=不限）；qwenwork 只有全局 rate_limit 上限
        # （0=不限，Buddy2api 不暴露逐账号在飞数）；qoder 由引擎 account_views 附带。
        if name == "workbuddy":
            try:
                st = engine_request(up, "GET", "/settings")
                limit = int(st.get("account_max_concurrent") or 0)
                snap = st.get("account_inflight") or {}
                if not isinstance(snap, dict):
                    snap = {}
            except Exception:
                limit, snap = None, {}
            for row in out["accounts"]:
                row["concurrency_limit"] = limit
                row["inflight"] = (int(snap.get(row.get("uid")) or 0)
                                   if limit is not None else None)
            # 执行记录与奖励到账是两种状态；历史文案不能证明到账。
            intl_rows = [r for r in out["accounts"] if r.get("realm") == "intl"]
            if intl_rows:
                try:
                    hist = engine_request(
                        up, "GET", "/activity/history?range=today&task=daily_chat&limit=100",
                        timeout=8)
                    rows = (hist.get("rows") or []) if isinstance(hist, dict) else []
                except Exception:
                    rows = []
                latest = {}
                for hr in rows:
                    if isinstance(hr, dict) and hr.get("uid"):
                        latest.setdefault(str(hr["uid"]), hr)
                # 到账证据只能来自积分包发放记录：引擎 /accounts/credits/grants 会把
                # 每个包的 create_time 与最近一次本机打卡动作做时间关联（窗口
                # -2h/+10min），action.task == daily_chat 才算「这次打卡带来的发放」。
                # 活动历史里的 ok=true 只证明「对话跑完了」——实测有「0 段输出」也算
                # 成功，与发分无关；只看它会把「跑过但没发分」误报成已到账。
                granted = {}
                try:
                    grants = engine_request(up, "GET", "/accounts/credits/grants", timeout=8)
                    grows = (grants.get("rows") or []) if isinstance(grants, dict) else []
                    today = time.strftime("%Y-%m-%d", time.localtime())
                    for gr in grows:
                        if not isinstance(gr, dict):
                            continue
                        act = gr.get("action")
                        if not isinstance(act, dict) or act.get("task") != "daily_chat":
                            continue
                        if not str(gr.get("create_iso") or "").startswith(today):
                            continue
                        gid = str(gr.get("uid") or "")
                        if gid:
                            granted.setdefault(gid, []).append(gr)
                except Exception:
                    granted = {}
                for row in intl_rows:
                    uid = str(row.get("uid") or "")
                    hit = latest.get(uid)
                    # confirmed=有本机打卡触发的今日发放；executed=对话跑完但无对应
                    # 发放（奖励可能延迟或未发）；failed 走下面的 error 分支。
                    if granted.get(uid):
                        row["daily_chat_verified"] = True
                        row["daily_chat_reward_status"] = "confirmed"
                        g = granted[uid][0]
                        row["daily_chat_reward_name"] = str(g.get("name") or "")
                        row["daily_chat_reward_size"] = g.get("size")
                    else:
                        row["daily_chat_verified"] = False
                        row["daily_chat_reward_status"] = (
                            "executed" if (hit and hit.get("ok") is not False) else "unknown")
                    if hit and hit.get("ok") is False and not row.get("daily_chatted_today"):
                        row["daily_chat_error"] = str(hit.get("message") or "")[:240]
                        row["daily_chat_error_at"] = hit.get("ts")
        elif name == "qwenwork":
            try:
                st = engine_request(up, "GET", "/admin/settings", auth="none")
                limit = int(st.get("rate_limit_max_concurrent") or 0)
            except Exception:
                limit = None
            for row in out["accounts"]:
                row["concurrency_limit"] = limit
                row["inflight"] = None
        # 即将过期的积分：wb 直接给 earliest_expiring（最临期包 + 已算好的天数），
        # qoder 给整池 expires_at；raccoon 的 audit.expire 是积分到期；
        # trae 的每个权益包都带 expire_time。qwenwork 上游不透出过期时间，如实缺省。
        expiring = []
        if name == "workbuddy":
            for a in items:
                if not isinstance(a, dict):
                    continue
                c = a.get("credits") or {}
                ee = c.get("earliest_expiring") or {}
                remain = _num(ee.get("remain"))
                if not ee or remain <= 0:
                    continue
                try:  # days_left 归一成数值：上游若给字符串，排序混型会 TypeError
                    dl = round(float(ee.get("days_left")), 1)
                except (TypeError, ValueError):
                    dl = None
                expiring.append({"account": a.get("nickname") or "",
                                 "name": str(ee.get("name") or "积分包"),
                                 "remain": remain,
                                 "expire": str(ee.get("cycle_end_time") or ""),
                                 "days_left": dl})
        elif name == "qoder":
            for a in items:
                if not isinstance(a, dict):
                    continue
                c = a.get("credits") or {}
                exp = c.get("expires_at")
                if not exp:
                    continue
                try:
                    ts = float(exp)
                except (TypeError, ValueError):
                    continue
                if not (0 < ts < 4e9):
                    continue  # 毫秒时间戳/负数会让 localtime 在 Windows 抛错，毁掉整平台聚合
                expiring.append({"account": a.get("nickname") or "",
                                 "name": "积分池（套餐 + 签到包）",
                                 "remain": _num(c.get("remain")),
                                 "expire": time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)),
                                 "days_left": round((ts - time.time()) / 86400, 1)})
        elif name == "raccoon":
            # audit.expire 是积分有效期（秒）；audit.days_left 是「按当前燃烧
            # 速度还能用几天」，两者语义不同，分开给前端。
            #
            # 金额必须用 audit.expire_points（最早那批每日积分的额度），不能
            # 用 audit.remain：remain 是账号总余额，混着不会过期的奖励/充值
            # 积分。拿 remain 配 expire 展示会报出「5972 分 2 天后全没」的假
            # 警报——实际只有最早一批（通常 300 分）会作废。老引擎不返回该
            # 字段，此时跳过临期行（宁可不显示，也不报错的数）。
            for a in items:
                if not isinstance(a, dict):
                    continue
                audit = a.get("_audit") or {}
                ts = _epoch_seconds(audit.get("expire"))
                if not ts:
                    continue
                remain = _known_num(audit.get("expire_points"))
                if remain is None or remain <= 0:
                    continue
                expiring.append(_expiry_row(
                    a.get("nickname") or a.get("uid"), "每日积分（最早到期批次）",
                    remain, ts, audit.get("days_left")))
        elif name == "trae":
            # 每个权益包独立到期；is_hide 是上游标记为不展示的包，跳过。
            for a in items:
                if not isinstance(a, dict):
                    continue
                ent = a.get("entitlement") or {}
                usage = ent.get("ent_usage") if isinstance(ent.get("ent_usage"), dict) else {}
                packs = usage.get("user_entitlement_pack_list") or []
                if not isinstance(packs, list):
                    continue
                for pack in packs:
                    if not isinstance(pack, dict) or pack.get("is_hide"):
                        continue
                    ts = _epoch_seconds(pack.get("expire_time")
                                        or pack.get("yearly_expire_time"))
                    if not ts:
                        continue
                    base = pack.get("entitlement_base_info") or {}
                    quota = base.get("quota") if isinstance(base.get("quota"), dict) else {}
                    used = pack.get("usage") if isinstance(pack.get("usage"), dict) else {}
                    limit = _known_num(quota.get("credits_limit"))
                    if limit is None:
                        continue  # 没有额度上限的包（如「免费」套餐）不可计量，不猜
                    consumed = _known_num(used.get("credits_amount")) or 0.0
                    remain = limit - consumed
                    if remain <= 0:
                        continue  # 已用尽的包不再提示，否则表里全是 0
                    expiring.append(_expiry_row(
                        (a.get("account") or {}).get("username") or a.get("uid"),
                        pack.get("display_desc") or _trae_pack_name(base) or "权益包",
                        remain, ts))
        elif name == "qwenwork":
            # 千问办公企业配额按月重置（客户端显示「配额下次重置时间」），
            # 上游 resources 不透出 valid_to，日期由 config qwenwork_reset_at 提供
            reset_at = str(CONFIG.get("qwenwork_reset_at") or "").strip()
            if reset_at:
                try:
                    ts = time.mktime(time.strptime(reset_at, "%Y-%m-%d"))
                    for a in items:
                        res = (a.get("_resources") or {})
                        remain = _num(res.get("remaining"))
                        if remain > 0:
                            expiring.append({"account": a.get("nickname") or "",
                                             "name": "企业配额（按月重置）",
                                             "remain": round(remain, 2),
                                             "expire": reset_at + " 00:00",
                                             "days_left": round((ts - time.time()) / 86400, 1)})
                except ValueError:
                    pass
        out["expiring"] = expiring
        enabled_accounts = [a for a in out["accounts"] if a.get("enabled")]
        known_balances = sum(1 for a in enabled_accounts
                             if _known_num(a.get("remain")) is not None)
        out["balance_state"] = ("unknown" if not enabled_accounts or not known_balances
                                 else "complete" if known_balances == len(enabled_accounts)
                                 else "partial")
        out["data_state"] = "partial" if out.get("credits_error") else "ok"
        if name != "raccoon":
            if name == "qwenwork" and not str(CONFIG.get("qwenwork_reset_at") or "").strip():
                out["expiry_state"] = "unsupported"
            else:
                out["expiry_state"] = "known" if expiring else "unknown"
    except Exception as exc:
        out["error"] = f"账号聚合失败: {type(exc).__name__}: {exc}"
        out["data_state"] = "partial" if out["health"] else "offline"
        if out["expiry_state"] == "unknown":
            out["expiry_state"] = "error"
    return out


# ---------------------------------------------------------------------------
# 额度可用量预估：按最近 24h 实扣外推可用天数（不是 token 数）
# ---------------------------------------------------------------------------
_estimate_lock = threading.Lock()
_estimate_cache = {"ts": 0.0, "data": None}
ESTIMATE_TTL = 120.0
_raccoon_billing_lock = threading.Lock()
_raccoon_billing_cache = {"ts": 0.0, "rows": []}
RACCOON_BILLING_TTL = 300.0

# 千问办公没有逐请求扣费：per-model 预估用短样本全价（未计缓存），
# 平台钱包实测均价只作对照；WorkBuddy/Qoder 仍按模型实测扣费。
QWEN_MODEL_PRICES = {
    "qwork-auto":          {"public": "flash",              "miss_per_ktok": 0.01562},
    "ling-3.1-flash":      {"public": "ling-3.1-flash",     "miss_per_ktok": 0.004115},
    "glm-5.3-flash":       {"public": "glm-5.3-flash",      "miss_per_ktok": 0.010425},
    "deepseek-flash":      {"public": "deepseek-flash",     "miss_per_ktok": 0.02057},
    "qwen3.8-flash":       {"public": "qwen3.8-flash",      "miss_per_ktok": 0.01521},
    "qwen4-max":           {"public": "qwen4-max",          "miss_per_ktok": 0.05102},
    "step-5-preview":      {"public": "step-5-preview",     "miss_per_ktok": 0.1009},
    "qwen3.8-max-preview": {"public": "qwen3.8-max-preview","miss_per_ktok": 0.51554},
}


def _qwen_sample(upstream, public):
    """千问模型 → 短样本全价；上游 id / 公开名都匹配，兼容内嵌前缀。"""
    candidates = [upstream, public]
    for raw in (upstream, public):
        text = str(raw or "").strip()
        while text.startswith("qwenwork/"):
            text = text[len("qwenwork/"):]
        candidates.append(text)
    for key in candidates:
        spec = QWEN_MODEL_PRICES.get(key)
        if spec:
            return spec
    for spec in QWEN_MODEL_PRICES.values():
        if spec.get("public") == public:
            return spec
    return None


def _estimate_public_model(platform, model):
    """引擎内部模型名 -> 网关公开 id，用于把用量行归到模型。"""
    model = str(model or "")
    while model.startswith(platform + "/"):
        model = model[len(platform + "/"):]
    if platform == "qwenwork":
        spec = _qwen_sample(model, "")
        if spec:
            return spec["public"]
        if model == "qwork-advanced":
            return "auto"
    return model


def _iso_epoch(value):
    """RFC3339/ISO 时间 → epoch 秒；解析不了返回 0（不猜时间）。"""
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


#: CodeArts 上游对 function name 的硬限制（超限报 InferHub.001001017.413）。
CODECARTS_FUNCTION_NAME_LIMIT = 64


def _oversized_function_names(payload, limit=CODECARTS_FUNCTION_NAME_LIMIT):
    """列出请求里超过 name 长度上限的 function / tool 名（去重保序）。

    覆盖三处：`tools[].function.name`（也容忍 `tools[].name` 的扁平形状）、
    `messages[].tool_calls[].function.name`、`messages[].function_call.name`。

    为什么要在网关侧拦：CodeArts 对 function name 硬限 64 字符，超了直接回
    502（InferHub.001001017.413），而引擎把这个 502 当成账号异常、冷却 10
    分钟。MCP 工具名普遍是 `mcp__plugin_<server>__<tool>` 形状，70+ 字符很常见
    ——不拦的话，客户端每发一次就把唯一账号锁死 10 分钟，看起来像「引擎用不了」。
    """
    if not isinstance(payload, dict):
        return []
    found = []

    def note(name):
        if isinstance(name, str) and len(name) > limit:
            found.append(name)

    tools = payload.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            fn = tool.get("function") if isinstance(tool.get("function"), dict) else tool
            note(fn.get("name"))

    messages = payload.get("messages")
    if isinstance(messages, list):
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            calls = msg.get("tool_calls")
            if isinstance(calls, list):
                for call in calls:
                    if isinstance(call, dict) and isinstance(call.get("function"), dict):
                        note(call["function"].get("name"))
            legacy = msg.get("function_call")
            if isinstance(legacy, dict):
                note(legacy.get("name"))

    seen = set()
    unique = []
    for name in found:
        if name not in seen:
            seen.add(name)
            unique.append(name)
    return unique


def _codearts_usage_rows(events):
    """codearts2api /admin/api/recent 条目 → 用量页行。

    该端点是内存里的最近 100 条（重启即清空），且不返回 token 计数：
    prompt/completion 按未知记为 0，与 raccoon 用量行的既有口径一致，
    前端不会拿它参与单价估算。outcome 优先用引擎的错误分类（code/kind），
    比裸状态码更能解释「为什么失败」。
    """
    rows = []
    for event in events or []:
        if not isinstance(event, dict):
            continue
        status = event.get("status")
        outcome = ("ok" if status == 200
                   else (event.get("code") or event.get("kind") or "http %s" % status))
        rows.append({
            "at": _iso_epoch(event.get("time")),
            "model": event.get("model") or "",
            "account": event.get("account") or "",
            "stream": event.get("stream"),
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "elapsed_ms": event.get("duration_ms"),
            "ttft_ms": None,
            "outcome": outcome,
        })
    return rows


RACCOON_BILLING_LINKS = os.path.join(HERE, "raccoon_billing_links.jsonl")
_raccoon_billing_link_lock = threading.Lock()


def _raccoon_bill_link_for_request(model, at=None):
    """Create a random billing marker; never put prompt or caller title in the ledger."""
    request_id = secrets.token_hex(16)
    return {"request_id": request_id, "marker": "gw-" + request_id,
            "model": str(model or "")[:200], "at": float(at or time.time())}


def _raccoon_response_id(status, headers, result):
    """Read xhx2api's request ID from the response header or JSON body."""
    if not (200 <= int(status or 0) < 300):
        return ""
    for key, value in (headers or {}).items():
        if str(key).lower() == "x-request-id" and value:
            return str(value).strip()
    if isinstance(result, (bytes, bytearray)):
        try:
            obj = json.loads(bytes(result).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return ""
        return str(obj.get("id") or "") if isinstance(obj, dict) else ""
    return ""


def _record_raccoon_billing_link(request_id, marker, model, at=None):
    """Remember the xhx2api request id ↔ unique bill title marker, never prompt text."""
    request_id = str(request_id or "").strip()
    marker = str(marker or "").strip()
    if not request_id or not marker:
        return False
    row = {"request_id": request_id[:200], "marker": marker[:64],
           "model": str(model or "")[:200], "at": float(at or time.time())}
    try:
        with _raccoon_billing_link_lock:
            with open(RACCOON_BILLING_LINKS, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            if os.path.getsize(RACCOON_BILLING_LINKS) > 512 * 1024:
                with open(RACCOON_BILLING_LINKS, encoding="utf-8") as fh:
                    keep = fh.readlines()[-2000:]
                temp = RACCOON_BILLING_LINKS + ".tmp"
                with open(temp, "w", encoding="utf-8") as fh:
                    fh.writelines(keep)
                os.replace(temp, RACCOON_BILLING_LINKS)
        return True
    except OSError:
        return False


def _read_raccoon_billing_links():
    """Read recent non-sensitive request-id/title links; malformed rows are skipped."""
    rows = []
    try:
        with open(RACCOON_BILLING_LINKS, encoding="utf-8") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(row, dict) or not row.get("request_id") or not row.get("marker"):
                    continue
                rows.append(row)
    except OSError:
        return []
    return rows[-2000:]


def _attach_raccoon_bills(events, bills, links):
    """Attach a bill only through a unique marker, unique xhx request id and same UID."""
    requests_by_id = {}
    duplicate_requests = set()
    for event in events:
        request_id = str(event.get("id") or "")
        if not request_id:
            continue
        if request_id in requests_by_id:
            duplicate_requests.add(request_id)
        requests_by_id[request_id] = event

    links_by_id = {}
    duplicate_links = set()
    for link in links:
        request_id = str(link.get("request_id") or "")
        if not request_id:
            continue
        if request_id in links_by_id:
            duplicate_links.add(request_id)
        links_by_id[request_id] = link

    bills_by_marker = {}
    for bill in bills:
        event_name = str(bill.get("event_name") or "")
        for link in links:
            marker = str(link.get("marker") or "")
            if marker and marker in event_name:
                bills_by_marker.setdefault(marker, []).append(bill)

    for request_id, link in links_by_id.items():
        if request_id in duplicate_links or request_id in duplicate_requests:
            continue
        event = requests_by_id.get(request_id)
        marker = str(link.get("marker") or "")
        matches = bills_by_marker.get(marker) or []
        if event is None or len(matches) != 1:
            continue
        bill = matches[0]
        link_at = _known_num(link.get("at")) or 0
        event_uid = str(event.get("uid") or "")
        bill_uid = str(bill.get("account") or "")
        if ((not event_uid or event_uid != bill_uid)
                or not bill_uid or not bill.get("bill_id") or not event.get("model")
                or _num(event.get("total_tokens")) <= 0
                or len(marker) < 20
                or not link_at
                or _estimate_at(event) < link_at - 1
                or abs(_estimate_at(event) - link_at) > 900
                or abs(_estimate_at(event) - _estimate_at(bill)) > 900):
            continue
        points = _known_num(bill.get("points"))
        if points is None or points >= 0:
            continue
        event["credit"] = -points
        event["credit_source"] = "matched_bill"
    return events


def _raccoon_billing_rows(up, limit=200):
    """读取小浣熊积分流水；只返回可安全对账的消费行。

    账单行按账号查询，字段只有 bill_id/biz_id/event_name/points/time，没有
    model。event_name 里的 title 也不是稳定的请求 ID，所以这里只做原始账单
    归档；除非后续能和请求一一对应，否则不把它摊到 token 模型单价。
    """
    now = time.time()
    with _raccoon_billing_lock:
        cached = _raccoon_billing_cache.get("rows") or []
        if now - _raccoon_billing_cache.get("ts", 0.0) < RACCOON_BILLING_TTL:
            return cached
    try:
        overview = engine_request(up, "GET", "/panel/api/overview",
                                  auth="bearer", timeout=10)
        accounts = overview.get("accounts") if isinstance(overview, dict) else []
    except Exception:
        accounts = []
    rows = []
    for account in accounts or []:
        if not isinstance(account, dict):
            continue
        uid = str(account.get("uid") or "")
        if not uid:
            continue
        try:
            data = engine_request(
                up, "GET", f"/panel/api/accounts/{quote(uid, safe='')}/bills?limit={limit}",
                auth="bearer", timeout=15)
        except Exception:
            continue
        bills = (data.get("bills") if isinstance(data, dict) else data) or []
        for bill in bills:
            if not isinstance(bill, dict):
                continue
            points = _known_num(bill.get("points"))
            if points is None or points >= 0:
                continue
            rows.append({
                "account": uid,
                "bill_id": str(bill.get("bill_id") or ""),
                "biz_id": str(bill.get("biz_id") or ""),
                "event_name": str(bill.get("event_name") or ""),
                "points": points,
                "created_at": _iso_epoch(bill.get("created_at")),
            })
    rows.sort(key=lambda row: row["created_at"], reverse=True)
    with _raccoon_billing_lock:
        _raccoon_billing_cache.update(ts=time.time(), rows=rows)
    return rows


def _estimate_rows(up):
    """各平台最近的用量行（每引擎最多 1000 条；wb 双区合并）。"""
    name = up["name"]
    limit = 1000
    if name == "qwenwork":
        data = engine_request(up, "GET", f"/admin/logs?limit={limit}", auth="none")
        return data if isinstance(data, list) else (data.get("items") or data.get("logs") or [])
    if name == "raccoon":
        # xhx2api 的请求日志是逐条事件；没有 credit / cached_tokens 字段，
        # 这两项保持未知，不能拿 0 冒充「零扣费/零缓存」。
        data = engine_request(up, "GET", f"/panel/api/requests?limit={limit}", auth="bearer")
        events = (data.get("events") if isinstance(data, dict) else data) or []
        rows = []
        all_events = []
        for e in events:
            if not isinstance(e, dict) or e.get("status") not in (None, 200):
                continue
            event = {
                "id": str(e.get("id") or ""),
                "created_at": _iso_epoch(e.get("time")),
                "model": e.get("model") or "",
                "uid": str(e.get("uid") or ""),
                "account": e.get("account") or "",
                "total_tokens": e.get("total_tokens") or 0,
                "prompt_tokens": e.get("prompt_tokens") or 0,
                "completion_tokens": e.get("completion_tokens") or 0,
                "cached_tokens": None,
                "credit": None,
            }
            all_events.append(event)
            rows.append(event)
        bills = _raccoon_billing_rows(up)
        return _attach_raccoon_bills(all_events, bills, _read_raccoon_billing_links())
    if name == "workbuddy":
        rows = []
        for realm in ("cn", "intl"):
            d = engine_request(up, "GET", f"/usage/recent?realm={realm}&limit={limit}")
            rows.extend((d.get("rows") if isinstance(d, dict) else d) or [])
        return rows
    if name == "codearts":
        # codearts2api 的用量端点是 /admin/api/recent（不是 /usage/recent）。
        # 该端点只给 body_bytes，没有 token 计数，所以这些行只能进「最近请求」
        # 展示，不能参与单价估算——total_tokens 保持 0 而不是猜。
        d = engine_request(up, "GET", f"/admin/api/recent?limit={limit}", auth="bearer")
        events = (d.get("items") if isinstance(d, dict) else d) or []
        return _codearts_usage_rows(events)
    d = engine_request(up, "GET", f"/usage/recent?limit={limit}")
    return (d.get("rows") if isinstance(d, dict) else d) or []


def _estimate_at(row):
    for key in ("at", "ts", "time", "created_at"):
        try:
            v = float(row.get(key))
        except (TypeError, ValueError):
            continue
        if v > 0:
            return v
    return 0.0


QWEN_WALLET_HISTORY = os.path.join(HERE, "qwenwork_wallet_history.jsonl")
#: Trae 的累计实扣计数器历史（consumed_amount 快照）。
#:
#: trae2api-hub 对每个请求都写 credit_reported=false，逐请求实扣拿不到；但它
#: 的 entitlement 里有一个单调递增的 usage_summary.consumed_amount（账号累计
#: 已消耗积分），是平台自己结算出来的真实扣费。取相邻快照的增量 ÷ 同期
#: token，就得到与 WorkBuddy 完全同口径的 Σ实扣 ÷ Σtoken，而且不像余额差
#: 那样会被充值/签到包发放污染（consumed 只增不减）。
TRAE_SPEND_HISTORY = os.path.join(HERE, "trae_spend_history.jsonl")
#: Trae 积分刷新的节流状态与间隔（秒）。引擎 /accounts 只是缓存，必须主动
#: 调 /accounts/credits 才会去上游拉最新的 consumed_amount；5 分钟一次既能
#: 让估值在一个观测周期内收敛，又不会把上游打成刷新风暴。
_trae_refresh_at = {"ts": 0.0}
_trae_refresh_lock = threading.Lock()
TRAE_REFRESH_INTERVAL = 300.0


def _trae_consumed_amount(raw_account):
    """Trae 账号的累计实扣积分（entitlement.usage_summary.consumed_amount）。

    这是平台自己结算的「已消耗积分」，单调递增，等价于 WorkBuddy 的逐请求
    实扣求和。取不到时返回 None（不能拿 0 冒充「零消耗」）。
    """
    ent = (raw_account or {}).get("entitlement") or {}
    usage = ent.get("ent_usage") if isinstance(ent.get("ent_usage"), dict) else {}
    summary = usage.get("usage_summary") if isinstance(usage.get("usage_summary"), dict) else {}
    return _known_num(summary.get("consumed_amount"))


def _append_spend_snapshot(account, value, path=None):
    """累计计数器快照：**只在值变化时**追加。

    计数器是批量结算的——两次刷新之间平台可能已经扣了几十次请求，但
    consumed_amount 只在刷新时跳一次。如果每轮询都写一条相同值的快照，那么
    「上次变化」到「这次变化」之间的 token 会被切成两段：前一段所在区间的
    增量是 0（被跳过），后一段的增量却包含了前一段的消费 → 单价虚高。

    只在值变化时写，相邻快照就严格是「一次结算的完整边界」。
    """
    if not account or value is None:
        return
    path = path or TRAE_SPEND_HISTORY
    try:
        with _wallet_lock:
            last = None
            try:
                with open(path, encoding="utf-8") as rf:
                    for line in rf:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            item = json.loads(line)
                        except ValueError:
                            continue
                        if str(item.get("account") or "") == str(account):
                            last = item
            except OSError:
                last = None
            if last is not None:
                prev = _known_num(last.get("balance"))
                if prev is not None and prev == value:
                    return  # 计数器没动：不写，保持区间边界干净
            with open(path, "a", encoding="utf-8") as wf:
                wf.write(json.dumps({"ts": time.time(), "account": str(account),
                                     "metric": "consumed", "balance": value},
                                    ensure_ascii=False) + "\n")
            if os.path.getsize(path) > 512 * 1024:
                with open(path, encoding="utf-8") as rf:
                    keep = rf.readlines()[-2000:]
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as wf:
                    wf.writelines(keep)
                os.replace(tmp, path)
    except OSError:
        pass


def _read_wallet_history(path=None, metric="balance"):
    """读取余额快照历史；坏行跳过，读不到返回空列表。

    metric 用于区分同一文件里的不同计量：Trae 的 `balance` 是余额、`consumed`
    是累计实扣计数器。两种口径不能混算，读的时候必须只取一种。
    """
    path = path or QWEN_WALLET_HISTORY
    rows = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(item, dict):
                    continue
                ts = _known_num(item.get("ts"))
                balance = _known_num(item.get("balance"))
                account = str(item.get("account") or "")
                row_metric = str(item.get("metric") or "balance")
                if metric and row_metric != metric:
                    continue
                if ts and balance is not None and account:
                    rows.append({"ts": ts, "account": account, "metric": row_metric,
                                 "balance": balance})
    except OSError:
        return []
    return rows


def _credit_rate(rows):
    """Σ实扣积分 ÷ Σtoken → (分/Mtok, 样本行数, 样本 token)。

    只统计同时有 token 和有效 credit 的行；整批扣费为 0 时没有可换算的
    单价（免费模型），返回 None。
    """
    total_credit = 0.0
    total_tokens = 0.0
    priced_rows = 0
    for row in rows:
        tokens = _num(row.get("total_tokens") or row.get("total"))
        credit = _known_num(row.get("credit"))
        if credit is None or credit < 0 or tokens <= 0:
            continue
        total_credit += credit
        total_tokens += tokens
        priced_rows += 1
    if priced_rows < 3 or total_tokens < 500 or total_credit <= 0:
        return None, priced_rows, total_tokens
    return total_credit / total_tokens * 1_000_000, priced_rows, total_tokens


def _wallet_rate(rows, history):
    """钱包快照差 ÷ 同期 token → (分/Mtok, 账号数, 样本 token, 覆盖小时)。

    只使用**余额真正下降**的相邻快照对：上游余额常是缓存值，只有强制刷新
    积分时才更新。若拿「首末快照」算，静止期（余额不变但 token 一直在涨）
    会被当成零成本，把单价稀释到毫无意义（实测把 10.66 点摊到 3422 万
    token 上，算出 0.31 分/Mtok 这种假单价）。

    口径：把快照按时间排序，逐对相邻快照取 (prev.ts, cur.ts] 区间，只在该
    区间余额下降时计入；token 也严格只数落在该区间内的请求。
    """
    per_account = {}
    for item in history:
        per_account.setdefault(item["account"], []).append(item)
    total_credit = 0.0
    total_tokens = 0.0
    accounts = 0
    hours = 0.0
    for account, snapshots in per_account.items():
        snapshots.sort(key=lambda item: item["ts"])
        if len(snapshots) < 2:
            continue
        account_tokens = 0.0
        account_credit = 0.0
        account_hours = 0.0
        for prev, cur in zip(snapshots, snapshots[1:]):
            spent = prev["balance"] - cur["balance"]
            if spent <= 0:
                continue  # 余额未变（缓存）或增加（发放/充值）：这一对不作样本
            tokens = 0.0
            for row in rows:
                row_account = str(row.get("account_name") or row.get("account") or "")
                if row_account != account:
                    # Trae 的快照键是脱敏 uid 的尾 4 位（引擎 /accounts 只给「…4052」），
                    # usage 行里是完整 uid；仅对短数字键做后缀匹配，避免误合并。
                    if not (len(account) == 4 and account.isdigit()
                            and row_account.endswith(account)):
                        continue
                at = _estimate_at(row)
                if at and prev["ts"] < at <= cur["ts"]:
                    tokens += _num(row.get("total_tokens") or row.get("total"))
            if tokens <= 0:
                continue  # 扣了费但区间内没有 token 记录：来源不明，不用它算单价
            account_credit += spent
            account_tokens += tokens
            account_hours += (cur["ts"] - prev["ts"]) / 3600
        if account_tokens <= 0 or account_credit <= 0:
            continue
        total_credit += account_credit
        total_tokens += account_tokens
        accounts += 1
        hours = max(hours, account_hours)
    if accounts < 1 or total_tokens < 100_000 or total_credit <= 0:
        return None, accounts, total_tokens, hours
    return total_credit / total_tokens * 1_000_000, accounts, total_tokens, hours


def _trae_spend_rate(rows, history):
    """Trae 累计实扣计数器增量 ÷ 同期 token → (分/Mtok, 账号数, 样本 token, 覆盖小时)。

    与 `_credit_rate` 同口径（Σ实扣 ÷ Σtoken），只是「实扣」不是逐请求字段，
    而是平台自己维护的累计计数器 `usage_summary.consumed_amount`。取相邻快照
    的增量即该区间的真实扣费。

    为什么不用余额差（`_wallet_rate`）：Trae 的 remain 同时受扣费和发放影响，
    签到 +100、福利包 +1500 都会进余额，「同区间既扣费又充值」无法识别。实测
    一次 166 token 的区间里记了 10.66 积分，反推出 6.4 万 分/Mtok 的假单价。
    consumed_amount 只增不减，不受发放影响。

    门槛：至少 1 个有效区间、累计 10 万 token、总扣费为正。区间本身就是聚合
    单位（一次刷新周期内可能包含上百个请求），所以不像 `_credit_rate` 那样
    要求 3 条。
    """
    per_account = {}
    for item in history:
        per_account.setdefault(item["account"], []).append(item)
    total_credit = 0.0
    total_tokens = 0.0
    accounts = 0
    hours = 0.0
    intervals = 0
    for account, snapshots in per_account.items():
        snapshots.sort(key=lambda item: item["ts"])
        if len(snapshots) < 2:
            continue
        account_tokens = 0.0
        account_credit = 0.0
        account_hours = 0.0
        account_intervals = 0
        for prev, cur in zip(snapshots, snapshots[1:]):
            spent = cur["balance"] - prev["balance"]
            if spent <= 0:
                continue  # 计数器没动：区间内没有结算，不能当成零成本样本
            tokens = 0.0
            for row in rows:
                row_account = str(row.get("account_name") or row.get("account") or "")
                if row_account != account:
                    # 快照键是脱敏 uid 尾 4 位，usage 行是完整 uid；仅对短数字键
                    # 做后缀匹配，避免把不同账号误合并。
                    if not (len(account) == 4 and account.isdigit()
                            and row_account.endswith(account)):
                        continue
                at = _estimate_at(row)
                if at and prev["ts"] < at <= cur["ts"]:
                    tokens += _num(row.get("total_tokens") or row.get("total"))
            if tokens <= 0:
                continue  # 扣了费但区间内没有 token 记录：来源不明，不用它算单价
            account_credit += spent
            account_tokens += tokens
            account_hours += (cur["ts"] - prev["ts"]) / 3600
            account_intervals += 1
        if account_tokens <= 0 or account_credit <= 0:
            continue
        total_credit += account_credit
        total_tokens += account_tokens
        accounts += 1
        intervals += account_intervals
        hours = max(hours, account_hours)
    if intervals < 1 or total_tokens < 100_000 or total_credit <= 0:
        return None, accounts, total_tokens, hours
    return total_credit / total_tokens * 1_000_000, accounts, total_tokens, hours


def _trae_wallet_account(raw_account, normalized):
    """Trae 余额快照与 usage 行之间统一的账号匹配键。

    引擎 /accounts 把 uid 脱敏成「…4052」，而 usage 行的 account 是完整 uid
    （如 1414412225494052）。两边只有后 4 位一致，所以用 uid 尾段作键；
    取不到时退回 uidHash（前端展示用的稳定标识）。
    """
    raw_uid = str((raw_account or {}).get("uid") or "").strip()
    digits = "".join(ch for ch in raw_uid if ch.isdigit())
    if len(digits) >= 4:
        return digits[-4:]
    if raw_uid and not raw_uid.startswith("…"):
        return raw_uid
    return str((normalized or {}).get("uid") or "")


def _trae_consumption_rate(catalog, upstream_model):
    """Trae 官方消费倍率（features.consumption_rate.data.rate）。

    目录行里由 trae2api-hub 原样透传为 consumption_rate，如 DeepSeek-V4.1-Flash
    是 0.06、Kimi-K3 是 1.83。它只是模型之间的相对倍率，不是「分/Mtok」，
    必须乘上钱包反推的锚定系数才是单价。
    """
    for entry in catalog:
        if entry["platform"] != "trae":
            continue
        if upstream_model not in (entry.get("upstream"), entry.get("public")):
            continue
        row = entry.get("row") or {}
        value = row.get("consumption_rate")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value) if value > 0 else None
    return None


#: 各平台余额的计费单位说明。数值不可跨平台比较——它们不是同一种货币。
PLATFORM_BALANCE_UNIT = {
    "workbuddy": "WorkBuddy 积分",
    "qoder": "Qoder 积分",
    "qwenwork": "千问办公企业配额",
    "raccoon": "小浣熊积分点",
    "trae": "Trae 积分（信用）",
    "codearts": "码道积分",
}

#: 不参与「额度可用量预估」的平台。
#:
#: 留空：codearts 现在有余额了（浏览器同步快照，见 CODEARTS_CREDITS_FILE），
#: 虽然它的 /admin/api/recent 不返回 token 计数、算不出单价，但预估循环本来
#: 就能把「有余额、无单价」的模型显示成「待测」，不需要整块排除——那样连余额
#: 都看不到。只有「既无余额又无用量」的平台才值得排除。
ESTIMATE_EXCLUDED = set()


def estimate_snapshot(force=False):
    """额度预估：每个平台、每个模型分别按实测有效单价（分/Mtok）估算。

    预计 token = 模型可用余额 ÷ 单价 × 1,000,000。
    WorkBuddy/Qoder 用明确的逐请求扣费字段；模型样本不足时显示待测。
    千问办公没有逐请求扣费，按模型短样本相对价用钱包实测均价锚定；
    Trae 同样没有逐请求扣费，但平台提供累计实扣计数器 consumed_amount，
    取区间增量得到与 WorkBuddy 同口径的 Σ实扣 ÷ Σtoken，再用官方
    consumption_rate 作模型间相对倍率分配；
    小浣熊只有唯一账单关联的聊天样本才参与估值，历史未关联请求保持待测。
    """
    now = time.time()
    with _estimate_lock:
        hit = _estimate_cache["data"]
        if hit and not force and now - _estimate_cache["ts"] < ESTIMATE_TTL:
            return hit
    catalog = public_catalog()
    out = []
    for up in active_upstreams():
        name = up["name"]
        if name in ESTIMATE_EXCLUDED:
            # 引擎不提供余额与用量，估不出来（见 ESTIMATE_EXCLUDED 注释）。
            continue
        st = platform_status(up)
        accounts = [a for a in st.get("accounts", []) if a.get("enabled")]
        known_balances = [v for v in (_known_num(a.get("remain")) for a in accounts) if v is not None]
        known_balance = round(sum(known_balances), 2) if known_balances else None
        balance_complete = bool(accounts) and len(known_balances) == len(accounts)
        balance = known_balance if balance_complete else None
        realm_totals, realm_values = {}, {}
        unknown_realm_total, unknown_realm_values = 0, []
        for a in accounts:
            realm = a.get("realm") if a.get("realm") in ("cn", "intl") else None
            value = _known_num(a.get("remain"))
            if realm is None:
                unknown_realm_total += 1
                if value is not None:
                    unknown_realm_values.append(value)
                continue
            realm_totals[realm] = realm_totals.get(realm, 0) + 1
            if value is not None:
                realm_values.setdefault(realm, []).append(value)
        realm_balances = {
            realm: (round(sum(realm_values[realm]), 2)
                    if len(realm_values.get(realm, [])) == realm_totals[realm] else None)
            for realm in realm_totals
        }
        realm_known = {
            realm: {"known": len(realm_values.get(realm, [])), "total": realm_totals[realm]}
            for realm in realm_totals
        }
        if unknown_realm_total:
            realm_balances["unknown"] = (
                round(sum(unknown_realm_values), 2)
                if len(unknown_realm_values) == unknown_realm_total else None)
            realm_known["unknown"] = {"known": len(unknown_realm_values),
                                      "total": unknown_realm_total}
        item = {"name": name, "balance": balance, "known_balance": known_balance,
                "balance_complete": balance_complete,
                "known": len(known_balances), "total": len(accounts),
                "enabled": len(accounts),
                "health": st["health"], "error": st.get("error", ""),
                "realm_balances": realm_balances, "realm_known": realm_known,
                # 各平台的「分」是平台自有积分单位，彼此不可比：小浣熊 1 点 ≈ 2.1 万
                # token，WorkBuddy 1 分 ≈ 67 万 token，数值大小没有跨平台含义。
                "balance_unit": PLATFORM_BALANCE_UNIT.get(name, "积分")}
        try:
            rows = _estimate_rows(up)
        except Exception as exc:
            rows = []
            item["usage_error"] = type(exc).__name__
        if name == "raccoon":
            # 账单只补齐逐请求实扣来源；之后与 WorkBuddy 共用同一计价流程。
            rows = [r for r in rows if r.get("credit_source") == "matched_bill"
                    and r.get("credit") is not None]

        # 平台级实测均价：单模型样本不足时的回退，也是千问的对照值。
        if name == "qwenwork":
            platform_rate, platform_samples, platform_tokens, platform_hours = _wallet_rate(
                rows, _read_wallet_history())
            platform_source = "wallet"
        elif name == "trae":
            # Trae 的 usage 行不带逐请求扣费（credit_reported 恒 false），所以
            # 「Σ实扣 ÷ Σtoken」的实扣取自平台自己的累计计数器
            # usage_summary.consumed_amount 的区间增量——与 WorkBuddy 同口径，
            # 且不受充值/签到发放干扰。
            priced = [row for row in rows
                      if not row.get("usage_missing")
                      and row.get("credit_reported") is True
                      and row.get("credit") is not None]
            platform_rate, platform_samples, platform_tokens = _credit_rate(priced)
            spend_history = _read_wallet_history(TRAE_SPEND_HISTORY, metric="consumed")
            if platform_rate is not None:
                first_at = min((_estimate_at(r) for r in priced if _estimate_at(r)), default=0.0)
                platform_hours = (round(max(0.0, (now - first_at) / 3600), 1)
                                  if first_at else None)
                platform_source = "measured_credits"
            else:
                platform_rate, platform_samples, platform_tokens, platform_hours = (
                    _trae_spend_rate(rows, spend_history))
                platform_source = "measured_credits"
                if platform_rate is None:
                    # 刻意不退回余额快照差：Trae 的 remain 混着签到/福利包发放，
                    # 实测 166 token 的区间记了 10.66 积分，反推出 6.4 万 分/Mtok。
                    # 宁可待测，也不给一个会让人误判的假单价。
                    # 把采集进度说清楚，用户才知道这是「还在攒样本」而非故障。
                    points = len(spend_history)
                    if points < 2:
                        item["rate_note"] = (
                            "Trae 累计实扣计数器已记录 %d 个快照，还需一次余额刷新"
                            "（每 %d 分钟）才能算出单价" % (points, int(TRAE_REFRESH_INTERVAL // 60)))
                    else:
                        item["rate_note"] = (
                            "Trae 累计实扣计数器有 %d 个快照，但尚未观察到「扣费增长 + "
                            "同期有 token 记录」的完整区间，继续采集" % points)
        else:
            platform_rate, platform_samples, platform_tokens = _credit_rate(rows)
            first_at = min((_estimate_at(r) for r in rows if _estimate_at(r)), default=0.0)
            platform_hours = (round(max(0.0, (now - first_at) / 3600), 1)
                              if first_at else None)
            platform_source = "measured"
        item.update({
            "rate_per_mtok": (round(platform_rate, 4) if platform_rate is not None else None),
            "rate_source": (platform_source if platform_rate is not None else "unknown"),
            "samples": platform_samples,
            "sample_tokens": int(platform_tokens),
            "sample_hours": platform_hours,
            "note": (item.get("rate_note")
                     or ("平台实测均价（累计实扣计数器增量 ÷ 同期 token）"
                         if platform_rate is not None and name == "trae"
                         else ("平台实测均价" if platform_rate is not None else "平台样本不足"))),
        })
        item.pop("rate_note", None)

        by_model = {}
        for r in rows:
            model = _estimate_public_model(name, r.get("resolved_model") or r.get("model"))
            by_model.setdefault(model, []).append(r)

        # 千问：用观测到的模型 token 结构，把短样本相对价整体缩放到钱包实测
        # 均价。这样各模型仍有相对差异，但总量与实际扣费一致。
        qwen_anchor = None
        if name == "qwenwork" and platform_rate is not None:
            weighted = 0.0
            priced_tokens = 0.0
            for model_name, model_rows in by_model.items():
                spec = _qwen_sample(model_name, model_name)
                if not spec:
                    continue
                tokens = sum(_num(r.get("total_tokens") or r.get("total"))
                             for r in model_rows)
                if tokens <= 0:
                    continue
                weighted += spec["miss_per_ktok"] * 1000 * tokens
                priced_tokens += tokens
            if priced_tokens > 0 and weighted > 0:
                qwen_anchor = platform_rate / (weighted / priced_tokens)

        # Trae：没有逐请求扣费，也没有「短样本相对价」。用官方 consumption_rate
        # 作为模型间相对倍率，把累计实扣计数器反推的平台均价按 token 加权缩放到
        # 各模型。
        trae_anchor = None
        if name == "trae" and platform_rate is not None:
            weighted = 0.0
            priced_tokens = 0.0
            for model_name, model_rows in by_model.items():
                factor = _trae_consumption_rate(catalog, model_name)
                if factor is None:
                    continue
                tokens = sum(_num(r.get("total_tokens") or r.get("total"))
                             for r in model_rows)
                if tokens <= 0:
                    continue
                weighted += factor * tokens
                priced_tokens += tokens
            if priced_tokens > 0 and weighted > 0:
                trae_anchor = platform_rate / (weighted / priced_tokens)

        models = []
        for entry in catalog:
            if entry["platform"] != name or _model_hidden(name, entry["public"]):
                continue
            public = entry["public"]
            model_balance = balance
            scope = ""
            if name == "workbuddy":
                realms = [realm for realm in _wb_usable_realms(entry["upstream"])
                          if realm in realm_totals]
                if realms:
                    model_balance = (round(sum(realm_balances[realm] for realm in realms), 2)
                                     if all(realm_balances.get(realm) is not None
                                            for realm in realms) else None)
                else:
                    model_balance = None
                scope = ("双区合计" if len(realms) > 1 else
                         ("仅国际" if realms == ["intl"] else "仅国内") if realms
                         else "无已知可用区域")
            mrows = by_model.get(public) or []
            if name == "trae":
                mrate, msamples, mtokens = _credit_rate(
                    [r for r in mrows if not r.get("usage_missing")
                     and r.get("credit_reported") is True])
            else:
                mrate, msamples, mtokens = _credit_rate(mrows)
            mfirst = min((_estimate_at(r) for r in mrows if _estimate_at(r)), default=0.0)
            mhours = round(max(0.0, (now - mfirst) / 3600), 1) if mfirst else None
            rate = None
            source = "unknown"
            samples = 0
            sample_tokens = 0
            sample_hours = None
            note = "样本不足，暂不估算"
            if name == "qwenwork":
                spec = _qwen_sample(entry["upstream"], public)
                if spec and qwen_anchor is not None:
                    rate = spec["miss_per_ktok"] * 1000 * qwen_anchor  # 分/Mtok
                    source = "anchored"
                    samples = len(mrows)
                    sample_tokens = int(sum(_num(r.get("total_tokens") or r.get("total"))
                                            for r in mrows))
                    sample_hours = mhours
                    note = (f"短样本相对价 × 钱包实测均价锚定"
                            f"（k={qwen_anchor:.2f}）")
                elif spec:
                    samples = len(mrows)
                    sample_tokens = int(sum(_num(r.get("total_tokens") or r.get("total"))
                                            for r in mrows))
                    sample_hours = mhours
                    note = "钱包样本不足，无法锚定"
                else:
                    note = "该模型无短样本价，暂不估算"
            elif name == "trae":
                if mrate is not None:
                    # 上游若真给了逐请求扣费，实测优先，不用倍率推测。
                    rate = mrate
                    source = "measured"
                    samples = msamples
                    sample_tokens = int(mtokens)
                    sample_hours = mhours
                    note = f"按最近 {msamples} 条扣费样本：Σ实扣 ÷ Σtoken"
                else:
                    # 官方 consumption_rate（信用消费倍率）× 平台实测均价锚定系数
                    # → 分/Mtok。锚定系数来自累计实扣计数器，与 WB 同口径。
                    factor = _trae_consumption_rate(catalog, entry["upstream"])
                    if factor is not None and trae_anchor is not None:
                        rate = factor * trae_anchor
                        source = "anchored"
                        samples = len(mrows)
                        sample_tokens = int(sum(_num(r.get("total_tokens") or r.get("total"))
                                                for r in mrows))
                        sample_hours = mhours
                        note = (f"官方倍率 x{factor} × 平台实测均价锚定"
                                f"（k={trae_anchor:.4f}）")
                    elif factor is not None:
                        samples = len(mrows)
                        sample_tokens = int(sum(_num(r.get("total_tokens") or r.get("total"))
                                                for r in mrows))
                        sample_hours = mhours
                        note = "累计实扣样本不足，无法锚定"
                    else:
                        note = "该模型无官方消费倍率，暂不估算"
            elif mrate is not None:
                rate = mrate
                source = "measured"
                samples = msamples
                sample_tokens = int(mtokens)
                sample_hours = mhours
                note = f"按最近 {msamples} 条扣费样本：Σ实扣 ÷ Σtoken"
            est = None
            if model_balance is None:
                note = "余额待测，暂不估算"
            elif rate is not None:
                est = int(model_balance / rate * 1_000_000)
            models.append({
                "model": public,
                "rate_per_mtok": (round(rate, 4) if rate is not None else None),
                "rate_source": source,
                "samples": samples,
                "sample_tokens": sample_tokens,
                "sample_hours": sample_hours,
                "est_tokens": est,
                "model_balance": (round(model_balance, 2)
                                  if model_balance is not None else None),
                "balance_scope": scope,
                "note": note,
            })
        models.sort(key=lambda m: (m["est_tokens"] is None, -(m["est_tokens"] or 0), m["model"]))
        item["models"] = models
        out.append(item)
    data = {"ok": True, "generated_at": int(now),
            "sample_basis": ("按模型 Σ实扣 ÷ Σtoken；千问用短样本相对价 × 钱包实测均价锚定；"
                             "Trae 用累计实扣计数器增量 ÷ 同期 token，再按官方消费倍率分配到各模型"),
            "unit_note": ("各平台「分/Mtok」的分是平台自有积分单位，"
                          "跨平台的单价与余额都不可直接比较大小"),
            "platforms": out}
    with _estimate_lock:
        _estimate_cache.update(ts=now, data=data)
    return data

#: 签到要逐个账号去上游领活动积分的平台，超时放宽到 10 分钟；其余 15s 足够。
CHECKIN_TIMEOUTS = {"raccoon": 600, "codearts": 600}


def _checkin_timeout(name):
    return CHECKIN_TIMEOUTS.get(name, 15)


CHECKIN_PATHS = {
    "workbuddy": ("/accounts/checkin", "panel"),
    "qoder": ("/accounts/checkin", "panel"),
    "trae": ("/accounts/checkin", "panel"),
    "raccoon": ("/panel/api/checkin_all", "bearer"),
    # codearts2api 的签到要逐个账号去上游领活动积分，走管理端点（Bearer）。
    "codearts": ("/admin/api/checkin", "bearer"),
    # qwenwork 没有签到动作：每日积分由上游自动发放（见 _api_checkin 特判）。
    # 不放进这里，避免把「令牌保活」当签到去调用（那条路 100% 超时/失败）。
}


def _raccoon_checkin_result(result):
    """xhx2api 的 JobResult（ok/failed/skipped 计数）→ 共享 results 行形状。

    三项计数都必须是非负整数；无效响应保留为显式失败，避免空 results
    被 _action_status 当作一次成功的空签到。
    """
    if not isinstance(result, dict):
        return {"error": "小浣熊签到响应格式无效：预期 JobResult 对象"}
    if result.get("ok") is False or result.get("error"):
        return result
    counts = {}
    for key in ("ok", "failed", "skipped"):
        value = result.get(key)
        if type(value) is not int or value < 0:
            return {"error": f"小浣熊签到响应格式无效：{key} 计数缺失或不是非负整数"}
        counts[key] = value
    ok, failed, skipped = counts["ok"], counts["failed"], counts["skipped"]
    rows = [{"ok": True} for _ in range(ok)]
    rows += [{"ok": False} for _ in range(failed)]
    rows += [{"ok": True, "skipped": True} for _ in range(skipped)]
    return {"results": rows, "details": result.get("details") or [],
            "job": result.get("job") or "checkin"}


def _checkin_tally(result):
    """从签到响应里抽出 (成功账号数, 账号总数)，抽不到返回 None。

    各引擎形状不同：wb 是 {cn_checkin:{results:[...]}, intl_daily_chat:{...}}，
    qoder 直接是 {results:[...]}。全部失败时顶层 ok 必须给 false——旧实现
    恒定 ok=True，账号 token 全失效时前端仍弹「签到成功」，掩盖真实故障。
    """
    if not isinstance(result, dict):
        return None
    groups = []
    if isinstance(result.get("results"), list):
        groups.append(result["results"])
    for value in result.values():
        if isinstance(value, dict) and isinstance(value.get("results"), list):
            groups.append(value["results"])
    rows = [row for group in groups for row in group if isinstance(row, dict)]
    if not rows:
        return None
    return sum(1 for row in rows if row.get("ok")), len(rows)


def _action_status(result):
    """汇总已知业务结果容器；不把附带账号快照当成执行结果。

    结果行（results/claimed 下的条目）缺少布尔 ok 时按未知处理：既不算成功，
    也不会让顶层 ok=true。engine/tasks 的显式 ok/error 语义保持不变。
    """
    successes, failures, unknown = 0, 0, 0
    errors = []

    def visit(value, is_result_row=False):
        nonlocal successes, failures, unknown
        if isinstance(value, list):
            for child in value:
                visit(child, is_result_row)
            return
        if not isinstance(value, dict):
            return
        result_keys = ("results", "claimed", "result", "cn_checkin", "intl_daily_chat")
        before = successes + failures + unknown
        for key in result_keys:
            if isinstance(value.get(key), (dict, list)):
                visit(value[key], key in ("results", "claimed"))
        if value.get("unknown") is True:
            unknown += 1
            errors.append(str(value.get("error") or value.get("msg")
                              or value.get("message") or "上游执行结果未知"))
            return
        failed = value.get("ok") is False or bool(value.get("error"))
        if failed:
            failures += 1
            errors.append(str(value.get("error") or value.get("msg")
                              or value.get("message") or "上游业务失败"))
        elif before != successes + failures + unknown:
            return  # 容器：结果已由子节点记账
        elif value.get("ok") is True or value.get("skipped"):
            successes += 1
        elif is_result_row:
            unknown += 1
            errors.append(str(value.get("msg") or value.get("message")
                              or "结果缺少 ok 判定"))

    visit(result)
    status = {"ok": failures == 0 and unknown == 0,
              "partial": successes > 0 and (failures > 0 or unknown > 0)}
    if unknown:
        status["unknown"] = unknown
    if errors:
        status["error"] = errors[0][:240]
    return status


def tail_log(lines=120):
    """只读日志尾部，避免每次把整份 router.log 读进内存。"""
    try:
        with open(LOG_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            start = max(0, size - 256 * 1024)
            f.seek(start)
            data = f.read().decode("utf-8", "ignore")
        rows = data.splitlines()[-lines:]
        return [re.sub(r"API Key:.*", "API Key: [redacted]", line) for line in rows]
    except OSError:
        return []


# ---------------------------------------------------------------------------
# 钱包历史写入锁；历史仅作余额观测，不自动推算模型单价。
_wallet_lock = threading.Lock()


def _append_wallet_snapshot(path, account, balance, metric="balance"):
    """追加一条余额快照（并发安全 + 512KB 轮转到最近 2000 行）。

    千问办公与 Trae 共用：两者都没有逐请求扣费字段，只能靠「两次快照的
    余额差 ÷ 同期真实消耗 token」反推平台实测均价。Trae 另有一路
    metric="consumed" 的累计实扣计数器快照（见 `_trae_spend_rate`）。
    """
    if not account or balance is None:
        return
    try:
        with _wallet_lock:
            with open(path, "a", encoding="utf-8") as wf:
                wf.write(json.dumps({"ts": time.time(), "account": str(account),
                                     "metric": metric, "balance": balance},
                                    ensure_ascii=False) + "\n")
            if os.path.getsize(path) > 512 * 1024:
                with open(path, encoding="utf-8") as rf:
                    keep = rf.readlines()[-2000:]
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as wf:
                    wf.writelines(keep)
                os.replace(tmp, path)
    except OSError:
        pass


def _request_target(target):
    """Normalize origin-form and proxy absolute-form request targets."""
    parsed = urlsplit(target or "")
    if parsed.scheme and parsed.netloc:
        return parsed.path or "/", parsed.query
    path, _, query = (target or "").partition("?")
    return path or "/", query


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "gateway-hub/1.1"

    def log_message(self, fmt, *args):  # 静默默认 stderr 访问日志，统一进文件
        log("%s %s" % (self.address_string(), fmt % args))

    # ---- 鉴权 ----

    def _authorized(self):
        want = (CONFIG.get("api_key") or "").strip()
        if not want:
            return True
        got = self.headers.get("Authorization", "")
        api_key = self.headers.get("x-api-key", "")
        bearer_key = ""
        parts = str(got).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            bearer_key = parts[1].strip()
        return (secrets.compare_digest(bearer_key.encode(), want.encode())
                or secrets.compare_digest(str(got).strip().encode(), want.encode())
                or secrets.compare_digest(str(api_key).strip().encode(), want.encode()))

    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def _read_post_object(self):
        """Consume one framed JSON object before dispatching any operation."""
        lengths = self.headers.get_all("Content-Length", [])
        transfer = self.headers.get("Transfer-Encoding", "")
        if len(lengths) > 1 or (lengths and transfer):
            self.close_connection = True
            self._deny(400, "请求体长度与传输编码不合法")
            return None
        try:
            if transfer:
                codings = [part.strip().lower() for part in transfer.split(",") if part.strip()]
                if codings != ["chunked"]:
                    self.close_connection = True
                    self._deny(400, "不支持该 Transfer-Encoding")
                    return None
                raw = self._read_chunked_body()
                if raw is None:
                    return None
            else:
                raw_length = lengths[0].strip() if lengths else "0"
                if not re.fullmatch(r"[0-9]+", raw_length):
                    self.close_connection = True
                    self._deny(400, "Content-Length 非法")
                    return None
                length = int(raw_length)
                if length > 32 * 1024 * 1024:
                    self.close_connection = True
                    self._deny(413, "请求体过大（上限 32MB）")
                    return None
                raw = self.rfile.read(length) if length else b""
                if len(raw) != length:
                    raise ValueError("请求体未完整接收")
        except (OSError, ValueError, OverflowError) as exc:
            self.close_connection = True
            self._deny(400, f"请求体 framing 错误: {exc}")
            return None
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, RecursionError):
            self._deny(400, "请求体不是合法 JSON")
            return None
        if not isinstance(body, dict):
            self._deny(400, "请求体必须是 JSON 对象")
            return None
        return body

    def _read_chunked_body(self):
        limit = 32 * 1024 * 1024
        out = bytearray()
        while True:
            line = self.rfile.readline(8193)
            if len(line) > 8192 or not line.endswith(b"\r\n"):
                raise ValueError("chunk size 行无效或过长")
            raw_size = line[:-2].split(b";", 1)[0].strip()
            if not raw_size or not re.fullmatch(rb"[0-9A-Fa-f]+", raw_size):
                raise ValueError("chunk size 不是十六进制")
            size = int(raw_size, 16)
            if size == 0:
                trailer_bytes = 0
                while True:
                    trailer = self.rfile.readline(8193)
                    trailer_bytes += len(trailer)
                    if (len(trailer) > 8192 or trailer_bytes > 64 * 1024
                            or not trailer.endswith(b"\r\n")):
                        raise ValueError("chunk trailer 无效或过长")
                    if trailer == b"\r\n":
                        return bytes(out)
                    if b":" not in trailer[:-2]:
                        raise ValueError("chunk trailer 字段无效")
            if len(out) + size > limit:
                self.close_connection = True
                self._deny(413, "请求体过大（上限 32MB）")
                return None
            chunk = self.rfile.read(size)
            if len(chunk) != size or self.rfile.read(2) != b"\r\n":
                raise ValueError("chunk 数据未完整接收")
            out.extend(chunk)

    def _deny(self, code, message, err_type="invalid_request_error"):
        request_path, _ = _request_target(getattr(self, "path", ""))
        if request_path == "/v1/messages":
            anthropic_type = "authentication_error" if code == 401 else err_type
            self._send_json(code, {"type": "error", "error": {
                "type": anthropic_type, "message": message}})
            return
        self._send_json(code, {"error": {"message": message, "type": err_type}})

    def _send_json(self, code, obj, model=None):
        if model is not None:
            obj["model"] = model
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_bytes(self, code, data, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---- 路由 ----

    def do_GET(self):
        path, query = _request_target(self.path)
        if path in ("/", "/index.html"):
            try:
                with open(DASHBOARD_PATH, "rb") as f:
                    data = f.read()
                # 禁缓存：改版后浏览器启发式缓存旧 JS 会造成「改了没生效」的错觉
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return  # 不 return 会落到下面的鉴权/404 分支，在 keep-alive 连接上再写第二个响应
            except OSError:
                return self._deny(500, "dashboard.html 缺失")
        if path == "/favicon.ico":
            return self._send_bytes(204, b"", "image/x-icon")
        if path == "/health":
            return self._health()
        normalized_target = path + ("?" + query if query else "")
        if _media_download_allowed(normalized_target):
            return self._media_video_get()
        if not self._authorized():
            return self._deny(401, "API Key 缺失或不正确")
        if path in ("/v1/models", "/models"):
            return self._models()
        if path in ("/v1/media/models", "/media/models"):
            return self._media_models()
        if path == "/api/media/models":
            return self._api_media_models()
        if path == "/v1/media/tasks":
            return self._media_tasks()
        if path.startswith("/v1/videos/"):
            return self._media_video_get()
        if path == "/api/ping":
            return self._send_json(200, {"ok": True})
        if path == "/api/gwconfig":
            return self._api_gwconfig()
        if path == "/api/platforms":
            return self._api_platforms()
        if path == "/api/automation/plan":
            return self._api_automation_plan()
        if path == "/api/automation/jobs":
            key = self.headers.get("Idempotency-Key", "").strip()
            if not key:
                return self._deny(400, "需要 Idempotency-Key 请求头")
            with _automation_lock:
                job_id = _automation_idempotency.get(key)
            if not job_id:
                return self._deny(404, "未找到对应的自动化任务")
            return self._api_automation_job_get(job_id)
        if path.startswith("/api/automation/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4:
                return self._api_automation_job_get(parts[3])
            return self._deny(404, "自动化任务路径无效")
        if path == "/api/updates":
            force = "force=1" in query
            return self._send_json(200, {"ok": True, "rows": _collect_updates(force=force)})
        if path == "/api/gateway-update":
            data = dict(_gateway_update_snapshot(force="force=1" in query))
            data["job"] = _gateway_update_job_state()
            return self._send_json(200, data)
        if path == "/api/catalog":
            return self._api_catalog()
        if path == "/api/status":
            return self._api_status()
        if path == "/api/usage":
            return self._api_usage()
        if path == "/api/estimate":
            return self._api_estimate()
        if path == "/api/raccoon/scan":
            return self._api_raccoon_scan()
        if path == "/api/raccoon/login/poll":
            return self._api_raccoon_login_poll()
        if path == "/api/login/poll":
            return self._api_login_poll()
        if path.startswith("/api/tasks/"):
            return self._api_tasks_get(path.rsplit("/", 1)[-1])
        if path == "/api/logs":
            return self._send_json(200, {"lines": "".join(tail_log())})
        return self._deny(404, "未知路径")

    def do_POST(self):
        t0 = time.time()
        path, query = _request_target(self.path)
        self._target = path + ("?" + query if query else "")
        if not self._authorized():
            self.close_connection = True
            return self._deny(401, "API Key 缺失或不正确")
        if _media_multipart_reject(path, self.headers.get("Content-Type")):
            self.close_connection = True  # 请求体未读取，不得在同一连接解析成下一次请求
            return self._deny(400, "媒体接口只接受 JSON 请求体：不支持 multipart 文件上传，"
                                   "改图请用 JSON 的 /v1/images/edits（image 传图片 URL 或 data URL）")
        self._post_body = self._read_post_object()
        if self._post_body is None:
            return
        if path.startswith("/api/checkin/"):
            return self._api_checkin(path.rsplit("/", 1)[-1])
        if path == "/api/automation/jobs":
            return self._api_automation_job_post()
        if path.startswith("/api/automation/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 6 and parts[4] == "steps":
                return self._api_automation_job_post(parts[3], parts[5])
            return self._deny(404, "自动化任务路径无效")
        if path.startswith("/api/tasks/"):
            # /api/tasks/{action}/{platform}
            parts = path.strip("/").split("/")
            if len(parts) == 4:
                return self._api_tasks_post(parts[2], parts[3])
            return self._deny(404, "任务操作格式: /api/tasks/{action}/{platform}")
        if path == "/api/models/visibility":
            return self._api_visibility()
        if path == "/api/models/rate":
            return self._api_model_rate()
        if path == "/api/models/favorite":
            return self._api_model_favorite()
        if path.startswith("/api/login/start/"):
            return self._api_login_start(path.rsplit("/", 1)[-1])
        if path.startswith("/api/qwenwork/reimport"):
            return self._api_qwenwork_reimport()
        if path == "/api/raccoon/import":
            return self._api_raccoon_import()
        if path == "/api/raccoon/login/start":
            return self._api_raccoon_login_start()
        if path == "/api/engine":
            return self._api_engine()
        if path == "/api/gwconfig":
            return self._api_gwconfig()
        if path == "/api/gateway-update":
            return self._api_gateway_update_deploy()
        if path in ("/api/platforms", "/api/platforms/process"):
            return self._api_platforms()
        if (path in MEDIA_POST_PATHS
                or (path.startswith("/v1/videos/") and path.endswith("/cancel"))):
            return self._media_post()
        if path not in ("/v1/chat/completions", "/v1/responses", "/v1/messages"):
            return self._deny(404, "仅支持 /v1/* 与 /api/*")

        req = self._post_body
        t_parse = time.time()
        model = req.get("model", "")
        if not isinstance(model, str):
            return self._deny(400, "model 必须是字符串")
        up, upstream_model, client_facing, reason = resolve_model(model)
        if up is None:
            return self._deny(400, reason)

        # codearts 上游对 function name 硬限 64 字符（InferHub.001001017.413），
        # 超了直接 502，而引擎会把这次 502 当成账号异常、冷却 10 分钟——客户端
        # 只要带着长名字的 MCP 工具（mcp__plugin_xxx__yyy 常见 70+ 字符），就再也
        # 用不了 codearts，而且每次重试都会把账号重新打进冷却。转发前拦下，
        # 由我们明确告诉调用方原因，不让上游与账号健康背这个锅。
        if up["name"] == "codearts":
            oversized = _oversized_function_names(req)
            if oversized:
                shown = "、".join("%s(%d)" % (n, len(n)) for n in oversized[:5])
                more = "…" if len(oversized) > 5 else ""
                return self._deny(400, (
                    "codearts 上游限制 function name ≤ %d 字符，以下工具名超长：%s%s。"
                    "请停用对应的 MCP 插件，或改用其他平台的模型；"
                    "直接转发会被上游判为 502 并让账号冷却 10 分钟。"
                ) % (CODECARTS_FUNCTION_NAME_LIMIT, shown, more))

        # 小浣熊账单不暴露模型或请求 ID：只对新 Chat Completions 生成随机标题标记，
        # 并在成功响应带回 X-Request-Id 后记录关联。Responses 路径的上游转换不保留
        # title，故不在该入口猜测账单归属。
        raccoon_bill_link = None
        raccoon_session_key = ""
        if up["name"] == "raccoon":
            raccoon_session_key = _raccoon_session_key(req)
            if path == "/v1/chat/completions" and "title" not in req:
                raccoon_bill_link = _raccoon_bill_link_for_request(upstream_model, t_parse)
                req["title"] = raccoon_bill_link["marker"]

        # 客户端视角的名字回显给客户端；上游拿真实模型名。
        req["model"] = upstream_model
        payload = json.dumps(req, ensure_ascii=False).encode("utf-8")
        t_payload = time.time()
        stream = bool(req.get("stream"))
        log(f"{path} model={model} -> {up['name']}/{upstream_model} stream={stream}")

        client_h = dict(self.headers.items())
        if raccoon_session_key:
            # 覆盖客户端可能自带的同名头：会话键由网关统一派生，保证同会话
            # 同键。forward 只透传 x-* 白名单头，会原样到达引擎的粘性路由。
            client_h["X-Session-Id"] = raccoon_session_key
        wb_plan = None
        rid = ""
        if up["name"] == "workbuddy":
            rid = secrets.token_hex(4)
            wb_plan = _wb_plan(upstream_model, req, path, self.headers)
            if not wb_plan["order"]:
                _wb_log_route(rid, model, wb_plan, 0, None, outcome="no-account")
                return self._deny(503, "WorkBuddy 当前没有可用区域/账号，请检查账号池",
                                  "upstream_error")
            # 客户端误带的 X-Realm 不覆盖网关判定；每次尝试再写具体区域。
            client_h = {k: v for k, v in client_h.items() if k.lower() != "x-realm"}

        t_route = time.time()
        attempts = wb_plan["order"] if wb_plan else [None]

        # 平台在飞计数：从发起到响应体写完（流式 = 整个流）都算
        with _platform_inflight(up["name"]):
            for idx, realm in enumerate(attempts):
                attempt_no = idx + 1
                headers = dict(client_h)
                if realm:
                    headers["X-Realm"] = realm
                try:
                    status, resp_headers, result = forward(
                        up, "POST", path, payload, headers, stream
                    )
                except (OSError, TimeoutError, HTTPException) as exc:
                    log(f"upstream[{up['name']}] 连接失败: {type(exc).__name__}: {exc}")
                    if wb_plan and realm:
                        _wb_health_fail(realm, "connect")
                        if idx + 1 >= len(attempts):
                            _wb_sticky_clear(wb_plan.get("key"))
                        _wb_log_route(rid, model, wb_plan, attempt_no, realm,
                                      outcome="connect_error",
                                      next_realm=(attempts[idx + 1] if idx + 1 < len(attempts) else ""))
                    if wb_plan and idx + 1 < len(attempts):
                        continue
                    return self._deny(502, f"上游 {up['name']} 不可达: {exc}", "upstream_error")

                if wb_plan and realm:
                    retryable = _wb_retryable(status, result)
                    retry_after = _wb_retry_after(resp_headers) if retryable else None
                    if status < 400:
                        _wb_health_success(realm)
                        _wb_sticky_set(wb_plan.get("key"), realm)
                        _wb_log_route(rid, model, wb_plan, attempt_no, realm,
                                      status=status, outcome="ok")
                    elif retryable:
                        _wb_health_fail(realm, status, retry_after=retry_after)
                        if idx + 1 < len(attempts):
                            _wb_log_route(rid, model, wb_plan, attempt_no, realm,
                                          status=status, outcome="retry",
                                          next_realm=attempts[idx + 1],
                                          retry_after=retry_after)
                            close = getattr(result, "close", None)
                            if close:
                                close()
                            continue
                        _wb_sticky_clear(wb_plan.get("key"))
                        _wb_log_route(rid, model, wb_plan, attempt_no, realm,
                                      status=status, outcome="fail",
                                      retry_after=retry_after)
                    else:
                        # 400/401/404/内容拒绝：请求本身的问题，不惩罚区域、不换区。
                        _wb_log_route(rid, model, wb_plan, attempt_no, realm,
                                      status=status, outcome="client-error")
                break

            # HTTP 200 + 上游 request id 允许把唯一账单标题和请求日志配对，
            # 流式请求也通过响应头获取 id；无 id 时宁可保持待测。
            if raccoon_bill_link and up["name"] == "raccoon" and status < 300:
                response_id = _raccoon_response_id(status, resp_headers, result)
                if response_id:
                    _record_raccoon_billing_link(
                        response_id, raccoon_bill_link["marker"],
                        raccoon_bill_link["model"], raccoon_bill_link["at"])
            # forward 会把「客户端要流式但上游回了非流式」退化为缓冲字节
            if stream and not isinstance(result, (bytes, bytearray)):
                return self._relay_stream(status, resp_headers, result, client_facing, up,
                                          t0=t0, t_head=time.time(), label=model,
                                          t_parse=t_parse, t_payload=t_payload,
                                          t_route=t_route)
            return self._relay_json(status, resp_headers, result, client_facing)

    # ---- 转发实现 ----

    def _relay_stream(self, status, headers, chunks, client_facing, up,
                      t0=None, t_head=None, label="", t_parse=None, t_payload=None,
                      t_route=None):
        self.send_response(status)
        ctype = headers.get("Content-Type", "text/event-stream")
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        # 流式响应无 Content-Length：HTTP/1.1 下必须声明关闭连接，
        # 否则客户端读完 SSE 后仍等下一帧，挂到自己超时
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        first_data_at = None
        try:
            for chunk in _rewrite_sse_chunks(chunks, client_facing):
                if first_data_at is None and b"data:" in chunk:
                    first_data_at = time.time()
                self.wfile.write(chunk)
                self.wfile.flush()
        except (ConnectionError, TimeoutError, HTTPException) as exc:
            log(f"连接中断（{up['name']}）: {type(exc).__name__}")
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()
        log(f"stream 完成 -> {client_facing}")
        if t0:
            total_ms = int((time.time() - t0) * 1000)
            head_ms = int((t_head - t0) * 1000) if t_head else -1
            first_ms = int((first_data_at - t0) * 1000) if first_data_at else -1
            parse_ms = int((t_parse - t0) * 1000) if t_parse else -1
            body_ms = int((t_payload - t_parse) * 1000) if (t_payload and t_parse) else -1
            route_ms = int((t_route - t_payload) * 1000) if (t_route and t_payload) else -1
            # 首字拆解：parse=读 body+JSON 解析，body=重新序列化，route=双区/账号池
            # 路由判定，head=解析+引擎响应头，first=第一个 data: 块，total=整条流
            log(f"timing {label} head={head_ms}ms first={first_ms}ms "
                f"parse={parse_ms}ms body={body_ms}ms route={route_ms}ms "
                f"total={total_ms}ms")

    def _relay_json(self, status, headers, data, client_facing):
        if status >= 400:
            self.send_response(status)
            for k, v in headers.items():
                if k.lower() != "content-length":
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        try:
            obj = json.loads(data.decode("utf-8"))
            if client_facing:
                obj["model"] = client_facing
            out = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)
        except ValueError:
            # 上游给了非 JSON，原样透传
            self.send_response(status)
            self.send_header("Content-Type", headers.get("Content-Type", "application/octet-stream"))
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    # ---- 媒体（生图 / 生视频） ----

    def _media_catalog_combined(self, force=False):
        """媒体目录合并：workbuddy 的图片/视频 + xhx2api 的图片。"""
        rows, errors = [], {}
        if platform_enabled("workbuddy"):
            models, err = _wb_media_fetch(force=force)
            rows.extend(("workbuddy", m) for m in models)
            errors.update(err)
        if platform_enabled("raccoon"):
            models, err = _raccoon_media_fetch(force=force)
            rows.extend(("raccoon", m) for m in models)
            errors.update({f"raccoon:{k}": v for k, v in err.items()})
        return rows, errors

    def _media_models(self):
        """GET /v1/media/models：媒体模型清单（OpenAI 风格外壳）。

        媒体模型刻意不进 /v1/models——聊天客户端会把它们当对话模型调用。
        支持 ?tag=text-to-image / image-to-video 等与 ?realm=cn|intl 过滤。
        """
        from urllib.parse import parse_qs
        qs = parse_qs(urlsplit(self.path).query)
        tag = (qs.get("tag") or [""])[0]
        realm = (qs.get("realm") or [""])[0]
        if not platform_enabled("workbuddy") and not platform_enabled("raccoon"):
            return self._send_json(200, {"object": "list", "data": [], "total": 0,
                                         "disabled": True})
        rows, errors = self._media_catalog_combined()
        data = []
        for platform, m in rows:
            if _model_hidden(platform, m["id"]):
                continue
            realm_tags = m.get("tags_by_realm") or {}
            supported_realms = [r for r in m["realms"]
                                if _media_supports_tag(m, tag, r)]
            if realm and realm not in supported_realms:
                continue
            if tag and not realm and not supported_realms:
                continue
            data.append({
                "id": f"{platform}/{m['id']}",
                "object": "model",
                "owned_by": platform,
                "name": m["name"],
                "type": m["kind"],
                "tags": m["tags"],
                "realms": m["realms"],
                "available": m["available"],
                "active_realm": m["active_realm"],
                "videoGeneration": m["video"],
                "creditPerImage": m["credit_per_image"],
                "creditPerImageMeasured": m["image_measured"],
                "creditPerSecond": m["credit_per_second"],
                "creditPerSecondCeiling": m["credit_ceiling"],
                "creditsPerSecond": m["credits_per_second"],
                "creditsPerSecondWithReference": m["credits_with_reference"],
            })
        payload = {"object": "list", "data": data, "total": len(data)}
        if errors:
            payload["errors"] = errors
        self._send_json(200, payload)

    def _api_media_models(self):
        """GET /api/media/models：控制台用（带 hidden 标记与刷新）。"""
        if not platform_enabled("workbuddy") and not platform_enabled("raccoon"):
            return self._send_json(200, {"ok": True, "items": [], "total": 0,
                                         "hidden_count": 0, "hidden": sorted(_hidden_set()), "errors": {},
                                         "disabled": True})
        rows, errors = self._media_catalog_combined(force="force=1" in self.path)
        items = []
        for platform, m in rows:
            items.append({
                "id": f"{platform}/{m['id']}",
                "platform": platform,
                "model": m["id"],
                "name": m["name"],
                "kind": m["kind"],
                "tags": m["tags"],
                "realms": m["realms"],
                "video": m["video"],
                "credit_per_image": m["credit_per_image"],
                "image_measured": m["image_measured"],
                "credit_per_second": m["credit_per_second"],
                "credit_ceiling": m["credit_ceiling"],
                "credits_per_second": m["credits_per_second"],
                "credits_with_reference": m["credits_with_reference"],
                "hidden": _model_hidden(platform, m["id"]),
                "available": m["available"],
                "active_realm": m["active_realm"],
            })
        self._send_json(200, {"ok": True, "items": items, "total": len(items),
                              "hidden_count": sum(1 for i in items if i["hidden"]),
                              "hidden": sorted(_hidden_set()), "errors": errors})

    def _abs_media_url(self, url):
        """引擎回的是相对产物路径（/v1/videos/...?t=签名）；转成网关直链。

        签名 token 自带鉴权，<video>/浏览器直接打开也能下载；不带网关 Key。
        """
        if (isinstance(url, str)
                and re.fullmatch(r"/v1/videos/[A-Za-z0-9_-]+/content\?[^#]*", url)):
            port = CONFIG.get("listen_port") or 8787
            expires = str(int(time.time()) + 3600)
            signature = _media_link_signature(url, expires)
            return f"http://127.0.0.1:{port}{url}&gw_exp={expires}&gw_sig={signature}"
        return url

    def _media_rewrite(self, obj, client_facing):
        if not isinstance(obj, dict):
            return obj
        if client_facing and isinstance(obj.get("model"), str):
            obj["model"] = client_facing
        if isinstance(obj.get("url"), str):
            obj["url"] = self._abs_media_url(obj["url"])
        for key in ("data", "tasks"):
            for row in obj.get(key) or []:
                if isinstance(row, dict) and isinstance(row.get("url"), str):
                    row["url"] = self._abs_media_url(row["url"])
        return obj

    def _relay_media_json(self, status, headers, data, client_facing=None):
        """媒体 JSON 透传：改写模型名与产物直链；错误体原样保留。"""
        if status >= 400:
            return self._relay_json(status, headers, data, None)
        try:
            obj = json.loads(data.decode("utf-8"))
            if not isinstance(obj, dict):
                raise ValueError
        except ValueError:
            return self._relay_json(status, headers, data, None)
        obj = self._media_rewrite(obj, client_facing)
        out = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def _relay_binary(self, status, headers, chunks):
        """视频产物直传：不缓冲、不改 Content-Type，连接关闭即结束。"""
        self.send_response(status)
        for key in ("Content-Type", "Content-Length", "Content-Disposition",
                    "Cache-Control", "ETag", "Last-Modified"):
            if headers.get(key):
                self.send_header(key, headers[key])
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        try:
            for chunk in chunks:
                self.wfile.write(chunk)
                self.wfile.flush()
        except (ConnectionError, TimeoutError, HTTPException) as exc:
            log(f"媒体下载中断: {type(exc).__name__}")
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()

    def _media_upstream(self):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == "workbuddy"), None)
        if not up:
            self._deny(404, "未配置 workbuddy 上游，媒体不可用")
            return None
        if not platform_enabled("workbuddy"):
            self._deny(409, "平台 `workbuddy` 已在控制台停用（设置页可重新启用）")
            return None
        return up

    def _media_post(self):
        path, query = _request_target(self.path)
        cancel = path.startswith("/v1/videos/") and path.endswith("/cancel")
        client_facing = None
        if cancel:
            # 取消只对 workbuddy 的异步视频任务有效。
            up = self._media_upstream()
            if up is None:
                return
        else:
            req = self._post_body
            model = req.get("model", "")
            if not isinstance(model, str):
                return self._deny(400, "model 必须是字符串")
            if path in ("/v1/videos", "/videos"):
                has_images = any(req.get(k) for k in ("image", "input_reference", "reference_images", "images"))
                tag = "image-to-video" if has_images else "text-to-video"
            else:
                tag = "image-to-image" if path.endswith("/edits") else "text-to-image"
            up, upstream_model, client_facing, reason = resolve_media_model(model, tag)
            if up is None:
                return self._deny(400, reason)
            req["model"] = upstream_model
            if up["name"] == "raccoon" and not path.startswith("/v1/videos"):
                # xhx2api 会把 n 统一转成字符串再转发，而上游生图接口只收 int；
                # 默认单张可以省略 n，但多张会被改成默认 1，必须明确拒绝。
                n = req.get("n", 1)
                if isinstance(n, bool) or not isinstance(n, int) or n < 1:
                    return self._deny(400, "小浣熊生图参数 n 必须是正整数")
                if n > 1:
                    return self._deny(400, "小浣熊上游当前不支持一次生成多张图片（n > 1）")
                req.pop("n", None)
            self._post_body = req
        payload = json.dumps(self._post_body, ensure_ascii=False).encode("utf-8")
        try:
            target = path + ("?" + query if query else "")
            status, headers, data = forward(up, "POST", target, payload,
                                            dict(self.headers.items()), False)
        except (OSError, TimeoutError, HTTPException) as exc:
            log(f"upstream[{up['name']}] 媒体请求失败: {type(exc).__name__}: {exc}")
            return self._send_json(502, {"error": {
                "message": f"上游 {up['name']} 不可达: {exc}", "type": "upstream_error"}})
        log(f"media {path} model={client_facing or '(默认)'} -> HTTP {status}")
        self._relay_media_json(status, headers, data, client_facing)

    def _media_video_get(self):
        up = self._media_upstream()
        if up is None:
            return
        path, _ = _request_target(self.path)
        try:
            if path.endswith("/content"):
                target, query = _request_target(self.path)
                target = target + ("?" + query if query else "")
                status, headers, chunks = forward(up, "GET", target, None, {}, "raw")
                return self._relay_binary(status, headers, chunks)
            target, query = _request_target(self.path)
            target = target + ("?" + query if query else "")
            status, headers, data = forward(up, "GET", target, None, {}, False)
        except (OSError, TimeoutError, HTTPException) as exc:
            return self._send_json(502, {"error": {
                "message": f"上游 workbuddy 不可达: {exc}", "type": "upstream_error"}})
        self._relay_media_json(status, headers, data)

    def _media_tasks(self):
        up = self._media_upstream()
        if up is None:
            return
        try:
            target, query = _request_target(self.path)
            target = target + ("?" + query if query else "")
            status, headers, data = forward(up, "GET", target, None, {}, False)
        except (OSError, TimeoutError, HTTPException) as exc:
            return self._send_json(502, {"error": {
                "message": f"上游 workbuddy 不可达: {exc}", "type": "upstream_error"}})
        self._relay_media_json(status, headers, data)

    # ---- 控制台 API ----

    def _api_status(self):
        platforms = [platform_status(up) for up in active_upstreams()]
        # 「今日签到」KPI 把国际版活跃打卡也算进来：它没有签到动作，但同样
        # 是每天一次的收益；只统计 can_checkin 会让国际号永远不出现在分母里。
        # 分子分母都走同一个资格判定，避免出现「5/4」这种 checked > checkable。
        checked = sum(1 for p in platforms for a in p["accounts"]
                      if _signable_account(a)
                      and (a["checked_in"] or (a.get("daily_chatted_today")
                                               and a.get("daily_chat_verified") is not False)))
        checkable = sum(1 for p in platforms for a in p["accounts"]
                        if _signable_account(a))
        # 即将过期积分聚合：跨平台不加总余额，但过期时间线是统一的时间轴，可排序
        expiring_all = []
        for p in platforms:
            for e in p.get("expiring", []):
                e2 = dict(e)
                e2["platform"] = p["name"]
                expiring_all.append(e2)
        expiring_all.sort(key=lambda e: (e.get("days_left") is None,
                                         e.get("days_left") if e.get("days_left") is not None else 0))
        # 不做跨平台积分合计：三家对积分/额度的口径完全不同，加总没有意义。
        self._send_json(200, {
            "ok": True,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
            "platforms": platforms,
            "expiring": expiring_all,
            "kpi": {
                "platforms_ok": sum(1 for p in platforms if p["health"]),
                "platforms_total": len(platforms),
                "accounts": sum(len(p["accounts"]) for p in platforms),
                "checked_in": checked,
                "checkable": checkable,
            },
        })

    def _api_checkin(self, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(404, f"未知平台 `{name}`")
        if not platform_enabled(name):
            return self._deny(409, f"平台 `{name}` 已在控制台停用（设置页可重新启用）")
        if name == "qwenwork":
            # 千问办公没有签到：每日积分由上游自动发放。端点保留但直接回
            # 「跳过」，客户端/审计不会误判失败，也不用等一次注定超时的保活。
            return self._send_json(200, {"ok": True, "platform": name,
                                         "result": {"skipped": True,
                                                    "msg": "千问办公每日积分由上游自动发放，无需签到"}})
        spec = CHECKIN_PATHS.get(name)
        if not spec:
            return self._deny(400, f"平台 `{name}` 没有可代执行的签到")
        path, kind = spec
        # 小浣熊/码道要逐个账号领积分，可能跑几分钟；其余平台保持 15s 默认。
        timeout = _checkin_timeout(name)
        try:
            result = engine_request(up, "POST", path, {}, auth=kind, timeout=timeout)
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc, 300)
            return self._send_json(502, {"ok": False, "platform": name,
                                         "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            return self._send_json(502, {"ok": False, "platform": name,
                                         "error": f"{type(exc).__name__}: {exc}"})
        if name == "raccoon":
            result = _raccoon_checkin_result(result)
        # wb 国际版没有每日签到：它的每日收益是活跃对话打卡（30-50 分），
        # 由引擎对显式 uid 名单同步执行。签到动作在这里顺带把国际版未打卡的
        # 账号一并打卡，一次点击覆盖国内+国际。目标筛选必须用 lastDailyChat
        # 日期（引擎 can_daily_chat() 的口径）——canDailyChat 是静态标记，
        # 打过卡也是 true，全发一遍只会让引擎逐个回「今日活跃已完成」。
        intl_part = None
        if name == "workbuddy":
            try:
                accs = engine_request(up, "GET", "/accounts?realm=all",
                                      auth=kind, timeout=30)
                alist = accs.get("accounts") if isinstance(accs, dict) else accs
                today = time.strftime("%Y-%m-%d")
                pending = [a for a in (alist or [])
                           if isinstance(a, dict) and a.get("realm") == "intl"
                           and a.get("enabled") and not a.get("accountBanned")
                           and a.get("canDailyChat")
                           and not str(a.get("lastDailyChat") or "").startswith(today)]
                if pending:
                    dc = engine_request(up, "POST", "/accounts/daily-chat",
                                        {"uids": [a.get("uid") for a in pending]},
                                        auth=kind, timeout=600)
                    intl_part = dc
                else:
                    intl_part = {"ok": True, "skipped": True,
                                 "msg": "所有国际版账号今日已打卡，跳过"}
            except Exception as exc:
                intl_part = {"error": f"{type(exc).__name__}: {exc}"}
        if intl_part is not None:
            result = {"cn_checkin": result, "intl_daily_chat": intl_part}
        status = _action_status(result)
        tally = _checkin_tally(result)
        if tally is not None:
            checked, total = tally
            if checked < total:
                status["ok"] = False
                status["partial"] = checked > 0
                if not status.get("error"):
                    status["error"] = f"{checked}/{total} 个账号成功"
        payload = {"platform": name, "result": result, **status}
        if tally is not None:
            payload["checked"], payload["total"] = tally
        log(f"checkin[{name}] ok={status['ok']} partial={status['partial']}")
        self._send_json(200, payload)

    def _api_automation_plan(self):
        plan = _automation_plan()
        return self._send_json(200, {"ok": True, **plan})

    def _api_automation_job_get(self, job_id):
        with _automation_lock:
            job = _automation_jobs.get(job_id)
        if not job:
            return self._deny(404, "自动化任务不存在")
        return self._send_json(200, {"ok": True, "job": _automation_public(job)})

    def _api_automation_job_post(self, job_id=None, step_id=None):
        body = self._post_body or {}
        if job_id and step_id:
            with _automation_lock:
                job = _automation_jobs.get(job_id)
                step = next((s for s in (job or {}).get("steps", [])
                             if s.get("step_id") == step_id), None)
                if not job or not step:
                    return self._deny(404, "自动化任务或步骤不存在")
                if job.get("status") in ("queued", "running"):
                    return self._deny(409, "自动化任务仍在执行，不能单独重试")
                if step.get("status") != "failed" or not step.get("retryable"):
                    return self._deny(409, "该步骤不是明确可安全重试的失败")
                step.update(status="queued", summary="等待重试", retryable=False)
                job.update(status="running", current_step=step_id, retry_only=True,
                           finished_at=None, updated_at=_automation_now())
                index = next(i for i, item in enumerate(job["steps"])
                             if item.get("step_id") == step_id)
            threading.Thread(target=_automation_run_step, args=(job_id, index),
                             daemon=True).start()
            return self._send_json(202, {"ok": True, "job": _automation_public(job)})

        if job_id:
            return self._deny(404, "自动化任务路径无效")
        key = str(body.get("idempotency_key") or "").strip()
        if not key or len(key) > 160:
            return self._deny(400, "需要 1-160 个字符的 idempotency_key")
        plan = _automation_plan()
        requested_version = str(body.get("plan_version") or "")
        if requested_version != plan["version"]:
            return self._send_json(409, {"ok": False, "error": "执行计划已变化，请重新预览",
                                         "plan": plan})
        with _automation_lock:
            old_id = _automation_idempotency.get(key)
            if old_id and old_id in _automation_jobs:
                return self._send_json(200, {"ok": True, "reused": True,
                                             "job": _automation_public(_automation_jobs[old_id])})
        wanted = body.get("steps")
        if not isinstance(wanted, list) or not wanted:
            return self._deny(400, "需要至少一个明确的执行步骤")
        if any(not isinstance(step_id, str) for step_id in wanted):
            return self._deny(400, "执行步骤必须是字符串")
        if len(set(wanted)) != len(wanted):
            return self._deny(400, "执行步骤不能重复")
        by_id = {s["step_id"]: s for s in plan["steps"]}
        if any(not isinstance(step_id, str) or step_id not in by_id for step_id in wanted):
            return self._deny(400, "执行步骤不在当前计划中")
        steps = [dict(by_id[step_id]) for step_id in wanted]
        job = {"id": secrets.token_urlsafe(12), "status": "queued",
               "plan_version": plan["version"], "idempotency_key": key,
               "created_at": _automation_now(), "updated_at": _automation_now(),
               "started_at": None, "finished_at": None, "current_step": "",
               "current_index": 0, "completed": 0,
               "counts": {s: 0 for s in ("succeeded", "skipped", "failed", "partial",
                                         "unknown", "submitted")},
               "steps": steps}
        with _automation_lock:
            old_id = _automation_idempotency.get(key)
            if old_id and old_id in _automation_jobs:
                return self._send_json(200, {"ok": True, "reused": True,
                                             "job": _automation_public(_automation_jobs[old_id])})
            _automation_jobs[job["id"]] = job
            _automation_idempotency[key] = job["id"]
            while len(_automation_jobs) > _AUTOMATION_JOB_LIMIT:
                old = next((job_id for job_id, item in _automation_jobs.items()
                            if item.get("status") not in ("queued", "running")
                            and job_id != job["id"]), None)
                if old is None:
                    break
                _automation_jobs.pop(old, None)
                for old_key, old_job_id in list(_automation_idempotency.items()):
                    if old_job_id == old:
                        _automation_idempotency.pop(old_key, None)
        threading.Thread(target=_automation_run_step, args=(job["id"], 0),
                         daemon=True).start()
        return self._send_json(202, {"ok": True, "job": _automation_public(job)})

    # ---- 自动化任务计划与生命周期 ----

    TASK_ACTIONS = {
        # action -> {平台: (引擎路径, 说明)}
        "run": {"workbuddy": ("/tasks/run", "运行成长任务（全部国内任务账号）"),
                "qoder": ("/tasks/run", "领取全部福利（每日 Credits + 活动 + Pro 包）")},
        "claim": {"workbuddy": ("/tasks/claim", "领取已完成任务的奖励")},
        "travel": {"workbuddy": ("/tasks/travel", "猫猫旅行（派出/领奖闭环）"),
                   "qoder": ("/tasks/travel", "猫猫旅行（派出/领奖闭环）"),
                   "trae": ("/tasks/travel", "领取 Pro 福利包（官方国内版）")},
        "night": {"workbuddy": ("/tasks/night", "夜猫子任务（仅 23:00-08:00 计数）")},
        "streak-bonus": {"workbuddy": ("/tasks/streak-bonus", "连登管家奖励")},
        "daily-web": {"workbuddy": ("/accounts/daily-chat-web", "国际版网页通道补打卡")},
    }

    def _api_tasks_get(self, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(404, f"未知平台 `{name}`（可用: workbuddy, qoder, qwenwork, raccoon, trae）")
        if not platform_enabled(name):
            return self._deny(409, f"平台 `{name}` 已在控制台停用（设置页可重新启用）")
        if name == "qwenwork":
            return self._send_json(200, {"ok": True, "platform": name, "tasks": [],
                "note": "千问办公的每日积分由上游自动发放，无任务体系"})
        try:
            result = engine_request(up, "GET", "/tasks")
            self._send_json(200, {"ok": True, "platform": name, "result": result})
        except Exception as exc:
            self._send_json(502, {"ok": False, "platform": name, "error": f"{type(exc).__name__}: {exc}"})

    def _wb_claim_all(self, up, body):
        """遍历所有启用的国内任务账号领取已完成任务。

        旧实现只挑第一个 cn 账号、且用不带 uid 的 /tasks 判定，多账号池里
        其余账号的奖励永远领不到。返回引擎风格的结果容器；显式 uid+code
        的单任务请求返回 None，交给原逻辑直接转发。
        """
        from urllib.parse import quote
        wanted = str(body.get("uid") or "").strip()
        code = str(body.get("code") or "").strip()
        if code and not wanted:
            return {"ok": False, "error": "指定任务 code 时必须同时指定 uid"}
        if wanted and code:
            return None
        accs = engine_request(up, "GET", "/accounts?realm=all", timeout=30)
        alist = accs.get("accounts") if isinstance(accs, dict) else accs
        targets = [a for a in (alist or [])
                   if isinstance(a, dict) and a.get("realm") == "cn"
                   and a.get("enabled") and not a.get("accountBanned")
                   and a.get("tasksEnabled") is not False
                   and (not wanted or a.get("uid") == wanted)]
        if not targets:
            return {"ok": False, "error": "没有启用的国内版任务账号可领取"}
        claimed, accounts = [], []
        for acct in targets:
            uid = acct.get("uid")
            row = {"uid": uid, "nickname": acct.get("nickname") or ""}
            try:
                status = engine_request(
                    up, "GET", f"/tasks?uid={quote(str(uid), safe='')}", timeout=60)
                tasks = status.get("tasks") if isinstance(status, dict) else status
                if isinstance(status, dict) and status.get("ok") is False:
                    row["error"] = str(status.get("error") or "任务列表查询失败")
                    accounts.append(row)
                    continue
                if tasks is None:
                    # 明确的空列表是「没有待领奖任务」；缺失/非法字段是
                    # 查询响应格式错误，不能把业务失败伪装成成功跳过。
                    if isinstance(status, dict) and isinstance(status.get("tasks"), list):
                        tasks = []
                    else:
                        row["error"] = "任务列表响应缺少 tasks 数组"
                        accounts.append(row)
                        continue
                if not isinstance(tasks, list):
                    row["error"] = "任务列表响应 tasks 不是数组"
                    accounts.append(row)
                    continue
                codes = [t.get("task_code") for t in tasks
                         if isinstance(t, dict) and t.get("status") == "accepted"
                         and _num(t.get("current")) >= _num(t.get("target")) > 0]
                if not codes:
                    row["skipped"] = True
                    row["msg"] = "没有已完成待领取的任务"
                    accounts.append(row)
                    continue
                row["codes"] = codes
                for one_code in codes:
                    try:
                        one = engine_request(up, "POST", "/tasks/claim",
                                             {"uid": uid, "code": one_code}, timeout=300)
                        claimed.append({"uid": uid, "code": one_code, "result": one})
                    except Exception as exc:
                        claimed.append({"uid": uid, "code": one_code,
                                        "error": f"{type(exc).__name__}: {exc}"})
                accounts.append(row)
            except Exception as exc:
                row["error"] = f"{type(exc).__name__}: {exc}"
                accounts.append(row)
        results = [row for row in accounts if row.get("error") or row.get("skipped")]
        return {"uid": wanted or "all", "accounts": accounts,
                "results": results, "claimed": claimed}

    def _api_tasks_post(self, action, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(404, f"未知平台 `{name}`")
        if not platform_enabled(name):
            return self._deny(409, f"平台 `{name}` 已在控制台停用（设置页可重新启用）")
        spec = self.TASK_ACTIONS.get(action, {}).get(name)
        if not spec:
            return self._deny(400, f"平台 `{name}` 不支持任务操作 `{action}`")
        engine_path, label = spec
        body = self._post_body
        # wb 的 /tasks/claim 必须指定 {uid, code}：控制台不带 code 时按账号
        # 逐个拉 /tasks、把「已完成未领奖」的任务全部领掉（不再只领第一个号）。
        if action == "claim" and name == "workbuddy":
            try:
                result = self._wb_claim_all(up, body)
            except Exception as exc:
                log(f"tasks/claim[{name}] 预备阶段失败: {type(exc).__name__}: {exc}")
                return self._send_json(502, {"ok": False, "platform": name, "action": action,
                                             "error": f"{type(exc).__name__}: {exc}"})
            if result is not None:
                log(f"tasks/claim[{name}] auto -> {json.dumps(result, ensure_ascii=False)[:180]}")
                return self._send_json(200, {"platform": name, "action": action,
                                             "label": label, "result": result,
                                             **_action_status(result)})
            # 只剩「显式 uid + code」这一种：原 body 已带全，直接转发。
        try:
            # run/claim/travel 在引擎里跑真实会话，分钟级耗时；默认 15s 会 502 而
            # 引擎仍在跑，用户重复点击造成重复执行
            result = engine_request(up, "POST", engine_path, body, timeout=300)
            log(f"tasks/{action}[{name}] completed")
            self._send_json(200, {"platform": name, "action": action,
                                  "label": label, "result": result, **_action_status(result)})
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc, 300)
            self._send_json(502, {"ok": False, "platform": name, "action": action,
                                  "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            self._send_json(502, {"ok": False, "platform": name, "action": action,
                                  "error": f"{type(exc).__name__}: {exc}"})

    # ---- 引擎自动化代理：白名单内的面板接口可直接代调 ----

    ENGINE_WHITELIST = {
        "workbuddy": {
            "accounts/refresh", "accounts/credits/fetch", "accounts/credits",
            "accounts/credits/grants", "accounts/sync-profile", "accounts/test",
            "accounts/daily-chat", "accounts/daily-chat-web", "scheduler",
            "scheduler/trigger", "scheduler/toggle",
            "activity/history", "tasks/progress", "quota-scan/run",
            "settings", "settings/save",
            "updates", "updates/check",
        },
        "qoder": {"accounts/refresh", "accounts/credits", "accounts/test"},
        "trae": {"accounts/refresh", "accounts/credits", "accounts/test",
                 "scheduler/trigger", "tasks/run", "tasks/travel"},
        "qwenwork": {"admin/accounts/keepalive-all", "admin/credit-summary"},
        "raccoon": {"panel/api/checkin_all", "panel/api/balance_all",
                    "panel/api/keepalive_all"},
    }

    def _api_engine(self):
        """POST /api/engine  {platform, path, method?, body?}
        代理调用引擎面板接口（仅白名单路径），供自动化页的保活/积分/活跃按钮使用。
        """
        body = self._post_body
        name = str(body.get("platform") or "").strip()
        path = str(body.get("path") or "").strip().strip("/")
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(404, f"未知平台 `{name}`")
        if not platform_enabled(name):
            return self._deny(409, f"平台 `{name}` 已在控制台停用（设置页可重新启用）")
        allowed = self.ENGINE_WHITELIST.get(name, set())
        if path not in allowed:
            return self._deny(403, f"路径 `{path}` 不在 {name} 的自动化白名单内")
        method = str(body.get("method") or "POST").upper()
        if method not in ("GET", "POST"):
            # 白名单按路径放行的是只读/动作类面板接口，不是任意方法网关
            return self._deny(405, f"引擎代理不支持 `{method}`（仅 GET/POST）")
        payload = body.get("body") if isinstance(body.get("body"), dict) else {}
        # 每日活跃/成长任务这类要在引擎里跑真实会话的操作，耗时可能几分钟
        timeout = 300 if ("daily-chat" in path or "checkin_all" in path
                          or path.startswith("tasks/")) else 60
        try:
            auth = ("none" if name == "qwenwork"
                    else "bearer" if name == "raccoon" else "panel")
            result = engine_request(up, method, "/" + path, payload, auth=auth, timeout=timeout)
            log(f"engine[{name}] {method} /{path} completed")
            self._send_json(200, {"platform": name, "path": path,
                                  "result": result, **_action_status(result)})
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc, 300)
            self._send_json(502, {"ok": False, "platform": name, "path": path,
                                  "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            self._send_json(502, {"ok": False, "platform": name, "path": path,
                                  "error": f"{type(exc).__name__}: {exc}"})

    # ---- 网关自身开关（区别于引擎 settings） ----

    def _api_gwconfig(self):
        """GET /api/gwconfig 读、POST /api/gwconfig 写（白名单字段）。

        hide_pro_locked: PRO 专属模型在账号池无付费账号时是否自动隐藏；
        wb_dual_prefer: 双区模型出口偏好 auto（按临期紧迫度）/cn/intl。
        """
        if self.command == "GET":
            return self._send_json(200, {"ok": True,
                "hide_pro_locked": bool(CONFIG.get("hide_pro_locked", True)),
                "wb_dual_prefer": (CONFIG.get("wb_dual_prefer") or "auto").lower(),
                "disabled_platforms": sorted(_disabled_platforms()),
                "wb_realms": ((_wb_pool_state().get("realms") or {})
                              if platform_enabled("workbuddy") else {})})
        body = self._post_body
        updates = {}
        if "hide_pro_locked" in body:
            updates["hide_pro_locked"] = bool(body.get("hide_pro_locked"))
        if "wb_dual_prefer" in body:
            v = str(body.get("wb_dual_prefer") or "auto").lower()
            if v not in ("auto", "cn", "intl"):
                return self._deny(400, "wb_dual_prefer 只能是 auto/cn/intl")
            updates["wb_dual_prefer"] = v
        if not updates:
            return self._deny(400, "没有可更新的字段（白名单: hide_pro_locked, wb_dual_prefer）")
        with _config_lock:
            CONFIG.update(updates)
            save_config()
        log(f"gwconfig 更新: {json.dumps(updates, ensure_ascii=False)}")
        self._send_json(200, {"ok": True, **updates})

    def _api_gateway_update_deploy(self):
        """POST /api/gateway-update：显式确认后一键部署官方正式版 Release。

        预检（拉取 tag、解包候选、跑回归）在后台线程执行，接口立即返回 202；
        前端轮询 GET /api/gateway-update 的 job 字段。预检通过并拉起独立更新
        进程后，本网关优雅退出，由更新进程 fast-forward 并重新拉起网关。
        """
        body = self._post_body or {}
        if body.get("confirm") is not True:
            return self._deny(400, '部署需要显式确认：请求体 {"confirm": true}')
        try:
            snapshot = _gateway_update_snapshot(force=True)
            result = _gateway_update_deploy(snapshot)
        except Exception as exc:
            return self._send_json(409, _gateway_update_error(exc))
        self._send_json(202, result)

    def _api_platforms(self):
        """GET /api/platforms 读平台开关与进程状态；POST 切换启用或启停进程。

        POST 两种形状：
          {name, enabled: bool}        网关层启用/停用；停用会同时尝试停止引擎进程
          {name, action: start|stop}   按 launch 配置启停引擎进程
        """
        if self.command == "GET":
            items = []
            for up in CONFIG["upstreams"]:
                proc = _platform_process(up)
                raw_launch = up.get("launch") if isinstance(up.get("launch"), dict) else {}
                items.append({
                    "name": up["name"],
                    "enabled": platform_enabled(up["name"]),
                    "base_url": up.get("base_url", ""),
                    "launch_configured": proc.pop("launch_configured", False),
                    "launch": ({"command": raw_launch.get("command"),
                                "cwd": raw_launch.get("cwd"),
                                "timeout": raw_launch.get("timeout")}
                               if raw_launch else None),
                    "process": proc,
                })
            return self._send_json(200, {"ok": True, "platforms": items,
                                         "disabled": sorted(_disabled_platforms()),
                                         "observed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())})
        body = self._post_body
        name = str(body.get("name") or "").strip()
        up = _upstream_by_name(name)
        if not up:
            return self._deny(404, f"未知平台 `{name}`")
        if "launch" in body:
            launch = body.get("launch")
            if launch is None:
                up.pop("launch", None)
            elif isinstance(launch, dict):
                candidate = {"command": launch.get("command"),
                             "cwd": launch.get("cwd") or None,
                             "timeout": launch.get("timeout") or 30}
                # 控制台只编辑命令/目录；env 由 config.json 维护，别在保存时丢掉
                # （例如 Trae 的 QODER_DISABLE / 默认区域）。
                old_env = (up.get("launch") or {}).get("env") \
                    if isinstance(up.get("launch"), dict) else None
                if isinstance(old_env, dict) and old_env:
                    candidate["env"] = old_env
                if _launch_spec({"name": name, "launch": candidate}) is None:
                    return self._deny(400, "启动命令无效：command 需要字符串或非空数组，cwd 必须存在")
                up["launch"] = candidate
            else:
                return self._deny(400, "launch 必须是对象或 null")
            with _config_lock:
                save_config()
            log(f"platform[{name}] launch 已更新")
            return self._send_json(200, {"ok": True, "platform": name,
                                         "launch_configured": _launch_spec(up) is not None})
        action = str(body.get("action") or "").strip().lower()
        if action in ("start", "stop"):
            result = _start_platform(up) if action == "start" else _stop_platform(up)
            if not result.get("ok"):
                return self._send_json(409, {"ok": False, "platform": name,
                                             "action": action,
                                             "error": result.get("error", "")})
            log(f"platform[{name}] {action}: {json.dumps(result, ensure_ascii=False)}")
            return self._send_json(200, {"ok": True, "platform": name,
                                         "action": action, **result})
        if "enabled" in body:
            enabled = bool(body.get("enabled"))
            disabled = _set_platform_enabled(name, enabled)
            process = None
            if enabled:
                # 打开开关 = 引擎也要起来：没在跑就按 launch 启动。
                if not _upstream_health(up):
                    if _launch_spec(up):
                        process = _start_platform(up)
                    else:
                        process = {"ok": False,
                                   "error": "未配置启动命令，平台已启用但引擎未启动"}
                log(f"platform[{name}] 启用后启动进程: "
                    f"{json.dumps(process, ensure_ascii=False)}")
            else:
                # 关掉开关 = 引擎也停。先确认端口上的进程属于该平台再停；
                # 确认不了只停网关侧，把原因带回给前端。
                process = _stop_platform(up, allow_external=True)
                log(f"platform[{name}] 停用后停止进程: "
                    f"{json.dumps(process, ensure_ascii=False)}")
            log(f"platform[{name}] {'启用' if enabled else '停用'}; disabled={disabled}")
            process_ok = process is None or process.get("ok", True)
            warning = ""
            if process and not process_ok:
                warning = str(process.get("error") or "进程状态未确认")
            return self._send_json(200, {"ok": True, "platform": name,
                                         "enabled": enabled, "route_enabled": enabled,
                                         "config_applied": True, "process_ok": process_ok,
                                         "warning": warning, "disabled": disabled,
                                         "process": process})
        return self._deny(400, "需要 enabled 或 action=start|stop")

    # ---- 聚合请求日志 ----

    def _norm_usage_row(self, name, row):
        at = row.get("at") or row.get("ts") or row.get("time") or row.get("created_at") or 0
        try:
            at = float(at)
        except (TypeError, ValueError):
            at = 0.0
        iso = row.get("iso") or (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(at)) if at else "")
        # 结果口径：hub 系 outcome 字符串；Buddy2api status_code（200=成功）
        outcome = row.get("outcome") or row.get("status")
        if not outcome:
            code = row.get("status_code")
            outcome = ("ok" if code == 200 else f"http {code}") if code is not None else ("ok" if row.get("ok") else "")
        model = row.get("resolved_model") or row.get("model") or row.get("name") or "?"
        gen_ms = row.get("gen_ms")
        speed = None
        try:
            if gen_ms and _num(row.get("completion_tokens")):
                speed = round(_num(row.get("completion_tokens")) / (_num(gen_ms) / 1000), 1)
        except ZeroDivisionError:
            speed = None
        # credit 数值化：上游若回字符串会让前端聚合变字符串拼接、toFixed 崩溃
        raw_credit = row.get("credit")
        if not isinstance(raw_credit, (int, float)) or isinstance(raw_credit, bool):
            raw_credit = None
        # Trae usage_missing 行或未明确报告 credit 的旧行都不能视为免费。
        # estimated cost_cny 是公开价参考，不是积分结算，不参与估值。
        if name == "trae" and (row.get("usage_missing") or not row.get("credit_reported")):
            raw_credit = None
        try:
            credit = None if raw_credit is None else float(raw_credit)
        except (TypeError, ValueError):
            credit = None
        # 引擎的货币成本是参考估价，与平台实际积分结算区分。
        raw_cny = row.get("cost_cny")
        try:
            cost_cny = None if raw_cny is None else float(raw_cny)
        except (TypeError, ValueError):
            cost_cny = None
        # 缓存口径与 credit 同构：引擎明确说「上游没报缓存」（cached_reported
        # 为假）或整行 usage 缺失时，cached_tokens 里的 0 是占位而不是实测值。
        # 当成已知 0 会让看板算出 100% 覆盖、0% 命中，把「不知道」伪装成结论。
        # 旧行没有该键（None）沿用历史语义：视为已报。
        cached = (_known_num(row.get("cached_tokens"))
                  if row.get("cached_tokens") is not None else None)
        if name == "trae" and (row.get("usage_missing")
                               or row.get("cached_reported") is False):
            cached = None
        return {
            "platform": name,
            "at": at,
            "iso": iso,
            "model": model,
            "account": row.get("account") or row.get("account_name") or row.get("nickname") or "",
            "key_name": row.get("key_name") or row.get("api_key_name") or "",
            # 思考强度：上游给具体档位就用档位；只给了开关状态（Buddy2api 的
            # reasoning mode）就折成 thinking/none；都没有 → None（前端显示 —）
            "reasoning_effort": (
                row.get("reasoning_effort")
                or {"enabled": "thinking", "disabled": "none",
                    "effort": "thinking"}.get(str(row.get("reasoning") or ""))
                or None),
            "stream": bool(row.get("stream")) if row.get("stream") is not None else None,
            "prompt": _num(row.get("prompt_tokens")),
            "completion": _num(row.get("completion_tokens")),
            "cached": cached,
            "reasoning": _num(row.get("reasoning_tokens")),
            "total": _num(row.get("total_tokens")) or (_num(row.get("prompt_tokens")) + _num(row.get("completion_tokens"))),
            "credit": credit,
            "credit_reported": bool(row.get("credit_reported")) if name == "trae" else credit is not None,
            "credit_source": ("upstream_usage" if name == "trae" and row.get("credit_reported")
                              else row.get("credit_source")),
            "cost_cny": cost_cny,
            "elapsed_ms": row.get("elapsed_ms") if row.get("elapsed_ms") is not None else row.get("duration_ms"),
            "ttft_ms": row.get("ttft_ms"),
            "speed": speed,
            "outcome": outcome,
        }

    def _api_estimate(self):
        try:
            self._send_json(200, estimate_snapshot(force="force=1" in self.path))
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _api_usage(self):
        from urllib.parse import parse_qs
        try:
            # 规范解析 query，避免裸子串匹配吞掉 output_limit= 这类以 limit 结尾的参数名
            params = parse_qs(urlsplit(self.path).query)
            limit = int((params.get("limit") or ["60"])[0])
        except (ValueError, TypeError):
            limit = 60
        limit = max(1, min(500, limit))
        requested_limit = limit
        rows, errors, usd_cny = [], {}, None
        coverage = {}
        for up in active_upstreams():
            name = up["name"]
            before_count = len(rows)
            realm_coverage = {}
            try:
                uid_names = {}
                if name == "qwenwork":
                    data = engine_request(up, "GET", f"/admin/logs?limit={limit}", auth="none")
                    items = data if isinstance(data, list) else data.get("items") or data.get("logs") or []
                    realm_coverage["all"] = {"ok": True, "rows": len(items)}
                elif name == "raccoon":
                    # xhx2api 没有 /usage/recent：逐条请求在 /panel/api/requests，
                    # 事件里没有 credit / cached，保持未知而不是补 0。
                    data = engine_request(up, "GET", f"/panel/api/requests?limit={limit}",
                                          auth="bearer", timeout=30)
                    events = (data.get("events") if isinstance(data, dict) else data) or []
                    realm_coverage["all"] = {"ok": True, "rows": len(events)}
                    items = []
                    for e in events:
                        if not isinstance(e, dict):
                            continue
                        status = e.get("status")
                        items.append({
                            "at": _iso_epoch(e.get("time")),
                            "model": e.get("model") or "",
                            "account": e.get("account") or e.get("uid") or "",
                            "stream": e.get("stream"),
                            "prompt_tokens": e.get("prompt_tokens") or 0,
                            "completion_tokens": e.get("completion_tokens") or 0,
                            "total_tokens": e.get("total_tokens") or 0,
                            "elapsed_ms": e.get("duration_ms"),
                            "ttft_ms": e.get("ttfb_ms"),
                            "outcome": ("ok" if status == 200
                                        else (e.get("error_kind") or f"http {status}")),
                        })
                elif name == "codearts":
                    # codearts2api 没有 /usage/recent；/admin/api/recent 是内存里
                    # 最近 100 条（重启清空），字段比 hub 系少：没有 token 计数。
                    data = engine_request(up, "GET", f"/admin/api/recent?limit={limit}",
                                          auth="bearer", timeout=30)
                    events = (data.get("items") if isinstance(data, dict) else data) or []
                    realm_coverage["all"] = {"ok": True, "rows": len(events)}
                    items = _codearts_usage_rows(events)
                else:
                    try:  # uid -> 昵称映射，让日志显示人名而不是一串 uuid；
                        # realm=all 才是全池（默认视图漏国际账号）
                        accs = engine_request(up, "GET", "/accounts?realm=all")
                        alist = accs.get("accounts") if isinstance(accs, dict) else accs
                        uid_names = {a.get("uid"): a.get("nickname") for a in (alist or [])
                                     if isinstance(a, dict) and a.get("uid")}
                    except Exception:
                        pass
                    if name == "workbuddy":
                        # wb 的 /usage/recent 只回当前面板区域，必须双区各拉一份合并，
                        # 否则国际版调用在用量页彻底消失
                        items = []
                        realm_errors = []
                        for realm in ("cn", "intl"):
                            try:
                                d2 = engine_request(
                                    up, "GET", f"/usage/recent?realm={realm}&limit={limit}")
                                r2 = d2.get("rows") if isinstance(d2, dict) else d2
                                r2 = r2 or []
                                realm_coverage[realm] = {"ok": True, "rows": len(r2)}
                                items.extend(r2)
                                if isinstance(d2, dict) and d2.get("usd_cny"):
                                    usd_cny = d2.get("usd_cny")
                            except Exception as exc:
                                realm_coverage[realm] = {"ok": False, "rows": 0,
                                                         "error": f"{type(exc).__name__}: {exc}"}
                                realm_errors.append(f"{realm}: {type(exc).__name__}: {exc}")
                        if realm_errors:
                            errors[name] = "; ".join(realm_errors)
                        normed = [self._norm_usage_row(name, r) for r in items if isinstance(r, dict)]
                        for r in normed:
                            r["account"] = uid_names.get(r["account"]) or r["account"]
                        rows.extend(normed)
                        coverage[name] = realm_coverage
                        continue

                    else:
                        data = engine_request(up, "GET", f"/usage/recent?limit={limit}")
                        items = data.get("rows") if isinstance(data, dict) else data
                        items = items or []
                        realm_coverage["all"] = {"ok": True, "rows": len(items)}
                        if isinstance(data, dict) and data.get("usd_cny"):
                            usd_cny = data.get("usd_cny")
                        if name == "trae":
                            # trae2api-hub 只把成功请求写进 usage.jsonl；行里没有
                            # outcome/status 字段，补成 ok 才能被前端算进成功数。
                            for item in items:
                                if isinstance(item, dict) and not item.get("outcome"):
                                    item["outcome"] = "ok"
                normed = [self._norm_usage_row(name, r) for r in items if isinstance(r, dict)]
                for r in normed:
                    r["account"] = uid_names.get(r["account"]) or r["account"]
                rows.extend(normed)
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"
                realm_coverage["all"] = {"ok": False, "rows": 0,
                                          "error": f"{type(exc).__name__}: {exc}"}
            # wb 双区部分失败时也保留 coverage：前端需要展示每个源的真实行数，
            # 只留 errors 里一条拼串会让该平台在覆盖面板上凭空消失。
            coverage[name] = realm_coverage
        truncated = False
        for sources in coverage.values():
            for src in sources.values():
                if isinstance(src, dict) and src.get("ok") and (src.get("rows") or 0) >= limit:
                    truncated = True
        rows.sort(key=lambda r: r["at"], reverse=True)
        # Upstreams expose only recent windows, and Trae omits failed requests entirely.
        # Keep the result explicit so the dashboard never labels this as a complete daily count.
        self._send_json(200, {"ok": True, "rows": rows, "errors": errors,
                              "usd_cny": usd_cny,
                              "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
                              "requested_limit": requested_limit, "returned": len(rows),
                              "truncated": truncated,
                              "failure_coverage": {"complete": False,
                                  "note": "上游日志窗口有限；Trae 上游仅记录成功请求"},
                              "coverage": coverage})

    # ---- OAuth 添加账号 ----

    LOGIN_START = {"workbuddy": {"platform": "CLI"}, "qoder": {}, "trae": {}}

    def _api_login_start(self, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(400, f"未知平台 `{name}`（OAuth 支持: workbuddy, qoder, trae）")
        if name == "qwenwork":
            return self._deny(400, "千问办公没有 OAuth 流程：在官方客户端登录后点「重新检测并导入」")
        if not platform_enabled(name):
            return self._deny(409, f"平台 `{name}` 已在控制台停用（设置页可重新启用）")
        body = self._post_body
        spec = self.LOGIN_START.get(name)
        if spec is None:  # 配置了上游但没配 OAuth 流程的平台名，别 KeyError 崩线程；
            # 注意不能用 not spec——qoder 的配置是空字典 {}，falsy 但合法
            return self._deny(400, f"平台 `{name}` 没有 OAuth 登录配置")
        payload = dict(spec)
        realm = (body.get("realm") or "cn").lower()
        if realm in ("cn", "intl"):
            payload["realm"] = realm
        try:
            result = engine_request(up, "POST", "/accounts/login/start", payload)
            log(f"login/start[{name}] realm={realm} state={result.get('state')}")
            # hub 系引擎用 authUrl；trae2api-hub 的 OAuth 返回
            # authorization_url / verification_uri，这里统一成 authUrl。
            auth_url = (result.get("authUrl") or result.get("auth_url")
                        or result.get("authorization_url")
                        or result.get("verification_uri"))
            self._send_json(200, {"ok": True, "platform": name, "state": result.get("state"),
                                  "authUrl": auth_url})
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _api_login_poll(self):
        qs = urlsplit(self.path).query
        from urllib.parse import parse_qs, quote  # 局部导入避免顶部命名冲突
        params = parse_qs(qs)
        up_name = (params.get("platform") or [""])[0].strip()
        state = (params.get("state") or [""])[0].strip()
        up = next((u for u in CONFIG["upstreams"] if u["name"] == up_name), None)
        if not up or not state:
            return self._deny(400, "需要 platform 与 state 参数")
        if up_name == "qwenwork":
            return self._deny(400, "千问办公没有 OAuth 轮询：在官方客户端登录后点「重新检测并导入」")
        if not platform_enabled(up_name):
            return self._deny(409, f"平台 `{up_name}` 已在控制台停用（设置页可重新启用）")
        try:
            result = engine_request(up, "GET",
                                    "/accounts/login/poll?state=" + quote(state, safe=""))
            self._send_json(200, {"ok": True, "platform": up_name, "result": result})
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _api_qwenwork_reimport(self):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == "qwenwork"), None)
        if not up:
            return self._deny(404, "未配置 qwenwork 上游")
        if not platform_enabled("qwenwork"):
            return self._deny(409, "平台 `qwenwork` 已在控制台停用（设置页可重新启用）")
        try:
            disc = engine_request(up, "GET", "/admin/accounts/discover?channel=qwenwork", auth="none")
            token = disc.get("preview_token") or ""
            if not token or not disc.get("importable_count"):
                # preview_token 可直接调 /admin/accounts/import，绝不能进 HTTP 响应；
                # 只回非敏感的计数字段（对齐 raccoon scan「token 不带到浏览器」的约定）。
                return self._send_json(200, {"ok": False,
                    "error": "没有发现可导入的新凭证（确认 QwenWork 客户端已登录企业工作区）",
                    "discover": {"importable_count": disc.get("importable_count") or 0,
                                 "total_count": disc.get("total_count")}})
            result = engine_request(up, "POST", "/admin/accounts/import",
                                    {"channel": "qwenwork", "preview_token": token}, auth="none")
            log(f"qwenwork reimport -> {json.dumps(result, ensure_ascii=False)[:150]}")
            self._send_json(200, {"ok": True, "result": result})
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _raccoon_upstream(self):
        up = _upstream_by_name("raccoon")
        if not up:
            self._deny(404, "未配置 raccoon 上游（config.json upstreams[]）")
            return None
        if not platform_enabled("raccoon"):
            self._deny(409, "平台 `raccoon` 已在控制台停用（设置页可重新启用）")
            return None
        return up

    def _api_raccoon_scan(self):
        """扫描本机小浣熊客户端登录态；只回候选元数据，token 不带到浏览器。"""
        up = self._raccoon_upstream()
        if up is None:
            return
        try:
            data = engine_request(up, "GET", "/panel/api/accounts/scan",
                                  auth="bearer", timeout=30)
        except Exception as exc:
            return self._send_json(502, {"ok": False,
                                         "error": f"{type(exc).__name__}: {exc}"})
        items = (data.get("candidates") if isinstance(data, dict) else data) or []
        self._send_json(200, {
            "ok": True, "items": items,
            "locations": (data.get("locations") or []) if isinstance(data, dict) else [],
        })

    def _api_raccoon_import(self):
        """导入扫描到的客户端凭证；路径由前端从扫描结果带回，凭证不经过网关。"""
        up = self._raccoon_upstream()
        if up is None:
            return
        paths = self._post_body.get("paths")
        if (not isinstance(paths, list) or not paths
                or not all(isinstance(p, str) and p.strip() for p in paths)):
            return self._deny(400, "需要 paths 数组（扫描结果里的文件路径）")
        if len(paths) > 20:
            return self._deny(400, "一次最多导入 20 个文件")
        try:
            result = engine_request(up, "POST", "/panel/api/accounts/scan/import",
                                    {"paths": paths}, auth="bearer", timeout=120)
        except urllib.error.HTTPError as exc:
            # 引擎在“一个都没导入成功”时回 400，但 body 里仍有逐条原因；
            # 转成 200 + ok=false，前端才能把每个文件的失败原因显示出来。
            try:
                data = json.loads(_http_error_detail(exc, 64 * 1024))
            except (ValueError, OSError):
                data = {}
            return self._send_json(200, {
                "ok": False,
                "imported": data.get("imported") or 0,
                "results": data.get("results") or [],
                "error": data.get("error") or f"HTTP {exc.code}",
            })
        except Exception as exc:
            return self._send_json(502, {"ok": False,
                                         "error": f"{type(exc).__name__}: {exc}"})
        imported = int(result.get("imported") or 0) if isinstance(result, dict) else 0
        log(f"raccoon import -> {imported} account(s)")
        self._send_json(200, {
            "ok": imported > 0, "imported": imported,
            "results": (result.get("results") or []) if isinstance(result, dict) else [],
        })

    def _api_raccoon_login_start(self):
        """生成小浣熊官方浏览器授权链接（引擎 /panel/api/login/device/start）。"""
        up = self._raccoon_upstream()
        if up is None:
            return
        try:
            result = engine_request(up, "POST", "/panel/api/login/device/start", {},
                                    auth="bearer", timeout=20)
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc, 300)
            return self._send_json(502, {"ok": False,
                                         "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            return self._send_json(502, {"ok": False,
                                         "error": f"{type(exc).__name__}: {exc}"})
        if not isinstance(result, dict):  # 引擎回 200 但 body 不是对象时别让 .get 崩掉
            result = {}
        state = str(result.get("id") or "").strip()
        auth_url = str(result.get("url") or "").strip()
        if not state or not auth_url:
            return self._send_json(502, {"ok": False,
                                         "error": "小浣熊授权响应缺少会话 ID 或授权链接"})
        self._send_json(200, {"ok": True, "state": state,
                              "authUrl": auth_url})

    def _api_raccoon_login_poll(self):
        """轮询浏览器授权是否完成；完成后引擎自动把账号入池。"""
        up = self._raccoon_upstream()
        if up is None:
            return
        from urllib.parse import parse_qs, quote  # 局部导入，和 _api_login_poll 一致
        params = parse_qs(urlsplit(self.path).query)
        state = (params.get("state") or [""])[0].strip()
        if not state:
            return self._deny(400, "需要 state 参数")
        try:
            result = engine_request(
                up, "GET", "/panel/api/login/device/poll?id=" + quote(state, safe=""),
                auth="bearer", timeout=35)
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc, 300)
            return self._send_json(502, {"ok": False,
                                         "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            return self._send_json(502, {"ok": False,
                                         "error": f"{type(exc).__name__}: {exc}"})
        if not isinstance(result, dict) or type(result.get("done")) is not bool:
            return self._send_json(502, {"ok": False,
                                         "error": "小浣熊授权轮询响应格式无效：缺少 done 状态"})
        self._send_json(200, {"ok": True, "result": result})

    def _models(self):
        hidden_count = len(_hidden_set())
        merged, seen_platforms = [], {}
        for x in public_catalog():
            seen_platforms[x["platform"]] = "ok"
            if _model_hidden(x["platform"], x["public"]):
                continue  # 隐藏的不进目录，客户端拉取列表时直接看不到
            if _wb_entry_locked(x):
                continue  # PRO 专属且池里无付费账号：客户端拉不到也调不了
            # 统一带平台前缀：裸名（如 kimi-k3/max-model）让用户看不出模型
            # 归属哪个平台；裸名调用仍兼容（resolve_model 唯一性放行）
            entry = {"id": f"{x['platform']}/{x['public']}", "object": "model",
                     "owned_by": x["platform"]}
            # 能力字段（vision/tools/reasoning）：OpenAI 官方 /v1/models 没有，
            # 但多数客户端会读；客户端不认识的字段直接忽略，无副作用。
            caps = _model_caps(x)
            ability = {k: caps[k] for k in ("vision", "tools", "reasoning") if k in caps}
            if ability:
                entry["capabilities"] = ability
            merged.append(entry)
        self._send_json(200, {"object": "list", "data": merged,
                              "platforms": seen_platforms, "hidden_count": hidden_count})

    def _api_catalog(self):
        """模型管理页用：全量目录 + 可见性标记 + 计费倍率 + 真实名称 + 收藏标记。

        qwenwork 目录不暴露单价：展示 config.json custom_multipliers 里用户
        手工记录的实测倍率（观察官方「用量明细」后填入），没有则 None。
        """
        custom = CONFIG.get("custom_multipliers") or {}
        favorites = set(CONFIG.get("favorite_models") or [])
        qwen_names = None
        items = []
        for x in public_catalog():
            alt, desc = _row_alt_desc(x["platform"], x["row"])
            full = f"{x['platform']}/{x['public']}"
            mult = _row_multiplier(x["platform"], x["row"])
            if mult is None and full in custom:
                mult = {"factor": custom[full].get("factor"), "source": "manual",
                        "note": custom[full].get("note", ""),
                        "recorded_at": custom[full].get("recorded_at", "")}
            # 无可靠逐请求价格的渠道保留手工倍率，不展示余额差推导值。
            # 千问办公：把官方套餐名（标准/高级/Qwen4-Max 预览版…）并进展示名
            if x["platform"] == "qwenwork":
                if qwen_names is None:
                    qwen_names = _qwenwork_display_names(
                        next(u for u in CONFIG["upstreams"] if u["name"] == "qwenwork"))
                official = qwen_names.get(x["public"])
                if official and official != x["public"]:
                    alt = f"{official}（{alt or x['public']}）" if alt else official
            item = {"id": full, "platform": x["platform"],
                    "model": x["public"], "upstream": x["upstream"],
                    "alt": alt, "desc": desc,
                    "hidden": _model_hidden(x["platform"], x["public"]),
                    "fav": full in favorites,
                    "requires_pro": bool(x.get("requires_pro")),
                    "realms": x.get("realms") or [],
                    "pro_locked": x["platform"] == "workbuddy" and _wb_entry_locked(x),
                    "multiplier": mult}
            item["caps"] = _model_caps(x)
            if x["platform"] == "workbuddy" and item["realms"]:
                # 当前实际出口：区域专属 → 固定；双区 → wb_dual_prefer 决定
                #（PRO 避让在 _wb_route_realm 里）。route_auto 表示出口由调度
                # 策略现场决定，不是模型 id 锁死的——这是「选 id = 选区域」
                # 误解的正面回答。
                dual = len(item["realms"]) > 1
                item["route"] = _wb_route_realm(x["upstream"]) if dual else item["realms"][0]
                item["route_auto"] = bool(dual and (CONFIG.get("wb_dual_prefer") or "auto") == "auto")
                aliased_to = WB_INTL_ALIAS.get(x["public"])
                if aliased_to:
                    item["aliased_to"] = aliased_to  # 国际版出口会把该 id 改写成上游新名
                alias_of = next((k for k, v in WB_INTL_ALIAS.items() if v == x["public"]), None)
                if alias_of:
                    item["alias_of"] = alias_of  # 本 id 是某逻辑模型在国际版出口的改名
            items.append(item)
        # 媒体模型并进同一张目录表（带 media 标记），可见性/批量操作与聊天
        # 模型共用 hidden_models；但 /v1/models 不会列出它们。
        for m in public_media_catalog():
            platform = m.get("platform") or "workbuddy"
            full = f"{platform}/{m['public']}"
            mult = None
            if full in custom:
                mult = {"factor": custom[full].get("factor"), "source": "manual",
                        "note": custom[full].get("note", ""),
                        "recorded_at": custom[full].get("recorded_at", "")}
            items.append({
                "id": full, "platform": platform, "model": m["public"],
                "upstream": m["upstream"], "alt": m["name"], "desc": "",
                "hidden": _model_hidden(platform, m["public"]),
                "fav": full in favorites,
                "requires_pro": False,
                "realms": m["realms"],
                "pro_locked": False,
                "multiplier": mult,
                "media": m["kind"],
                "tags": m["tags"],
                "video": m["video"],
                "credit_per_image": m["credit_per_image"],
                "image_measured": m["image_measured"],
                "credit_per_second": m["credit_per_second"],
                "credit_ceiling": m["credit_ceiling"],
                "credits_per_second": m["credits_per_second"],
                "caps": {},
            })
        items.sort(key=lambda i: (i["platform"], i["model"]))
        hidden = _hidden_set()
        # hidden 数组是前端实时同步行状态的依据：只回 hidden_count 会让
        # 已隐藏模型在每次加载模型页时被错误覆写成「显示中」
        self._send_json(200, {"ok": True, "items": items,
                              "total": len(items), "hidden_count": len(hidden),
                              "hidden": sorted(hidden)})

    def _api_model_favorite(self):
        """POST /api/models/favorite {id, fav} — 标记常用模型（置顶展示）。"""
        body = self._post_body
        mid = str(body.get("id") or "").strip()
        if not mid:
            return self._deny(400, "需要 id（平台/模型 全名）")
        with _config_lock:
            favs = set(CONFIG.get("favorite_models") or [])
            if body.get("fav"):
                favs.add(mid)
            else:
                favs.discard(mid)
            CONFIG["favorite_models"] = sorted(favs)
            save_config()
        self._send_json(200, {"ok": True, "favorites": sorted(favs)})

    def _api_model_rate(self):
        """POST /api/models/rate {id, factor, note?} — 记录手工实测的单价倍率。

        主要给千问办公用（目录不暴露单价）：观察官方用量明细后把「积分/请求」
        折算成倍率记进来。factor 传 0 或空表示清除记录。
        """
        body = self._post_body
        mid = str(body.get("id") or "").strip()
        if not mid:
            return self._deny(400, "需要 id（平台/模型 全名）")
        # 校验 id 在公共目录里存在，防止手滑记到不存在的模型上变孤儿数据
        known = (any(f"{x['platform']}/{x['public']}" == mid for x in public_catalog())
                 or any(f"{m.get('platform') or 'workbuddy'}/{m['public']}" == mid
                        for m in public_media_catalog()))
        if not known:
            return self._deny(404, f"模型 `{mid}` 不在当前目录里（千问办公渠道只有 qwen 系模型）")
        try:
            factor = float(body.get("factor"))
        except (TypeError, ValueError):
            factor = 0.0
        with _config_lock:
            custom = CONFIG.get("custom_multipliers") or {}
            if factor > 0:
                custom[mid] = {"factor": round(factor, 8),
                               "note": str(body.get("note") or "")[:120],
                               "recorded_at": time.strftime("%Y-%m-%d %H:%M")}
            else:
                custom.pop(mid, None)
            CONFIG["custom_multipliers"] = custom
            save_config()
        log(f"单价记录: {mid} = x{factor if factor > 0 else '(清除)'}")
        self._send_json(200, {"ok": True, "id": mid, "factor": factor if factor > 0 else None,
                              "custom": custom})

    def _api_visibility(self):
        """更新模型可见性。四种写法：
        {"id": "平台/模型", "visible": bool}     单个
        {"ids": ["平台/模型", ...], "visible": bool}  批量（勾选多选时用）
        {"platform": "qoder", "visible": bool}   整平台
        {"visible": true}                         全部恢复显示
        """
        body = self._post_body
        # 整平台操作要先枚举目录（可能打上游），放在配置锁外，避免一次
        # 网络抖动把 /api/status 和配置写全部拖住。
        platform_names = None
        if "platform" in body:
            pf = str(body["platform"] or "").strip().lower()
            up = next((u for u in CONFIG["upstreams"] if u["name"].lower() == pf), None)
            if not up:
                return self._deny(400, f"未知平台 `{pf}`")
            # 按公共目录名整平台操作：直接用上游原始 id 会写出
            # `qwenwork/qwenwork/flash` 这类垃圾键且对剥前缀的条目不生效
            platform_names = [f"{x['platform']}/{x['public']}" for x in public_catalog()
                              if x["platform"] == up["name"]]
            if up["name"] in ("workbuddy", "raccoon"):
                platform_names += [f"{m.get('platform') or 'workbuddy'}/{m['public']}"
                                   for m in public_media_catalog()
                                   if (m.get("platform") or "workbuddy") == up["name"]]
        def _known_model(mid):
            # 隐藏操作只接受目录里真实存在的模型，防止拼错的 id 写成永远
            # 清不掉的孤儿键、虚增 hidden_count；显示路径不校验，
            # 保留清理历史孤儿键的能力。
            return (any(f"{x['platform']}/{x['public']}" == mid for x in public_catalog())
                    or any(f"{m.get('platform') or 'workbuddy'}/{m['public']}" == mid
                           for m in public_media_catalog()))

        with _config_lock:
            hidden = _hidden_set()
            if "ids" in body:
                ids = body.get("ids")
                if not isinstance(ids, list) or not all(isinstance(i, str) and "/" in i for i in ids):
                    return self._deny(400, "ids 必须是 `平台/模型` 全名数组")
                if body.get("visible") is False:
                    unknown = [mid for mid in ids if not _known_model(mid)]
                    if unknown:
                        return self._deny(400, f"模型 `{unknown[0]}` 不在当前目录里，无法隐藏")
                for mid in ids:
                    if body.get("visible") is False:
                        hidden.add(mid)
                    else:
                        hidden.discard(mid)
            elif "id" in body:
                mid = str(body["id"] or "").strip()
                if not mid or "/" not in mid:
                    return self._deny(400, "id 必须是 `平台/模型` 形式")
                if body.get("visible") is False and not _known_model(mid):
                    return self._deny(400, f"模型 `{mid}` 不在当前目录里，无法隐藏")
                if body.get("visible") is False:
                    hidden.add(mid)
                else:
                    hidden.discard(mid)
            elif "platform" in body:
                for full in platform_names or []:
                    if body.get("visible") is False:
                        hidden.add(full)
                    else:
                        hidden.discard(full)
            elif body.get("visible") is True:
                hidden = set()
            else:
                return self._deny(400, "需要 id/platform/visible 之一")
            CONFIG["hidden_models"] = sorted(hidden)
            save_config()
        log(f"模型可见性更新: 隐藏 {len(hidden)} 个")
        self._send_json(200, {"ok": True, "hidden": sorted(hidden), "hidden_count": len(hidden)})

    def _health(self):
        platforms = {}
        for up in active_upstreams():
            # 与平台状态共用同一套健康检查（raccoon 用 /healthz，其余用 /health）。
            platforms[up["name"]] = _upstream_health(up, timeout=5)
        self._send_json(200, {"ok": all(platforms.values()), "version": VERSION,
                              "platforms": platforms,
                              "disabled": sorted(_disabled_platforms())})


def _rewrite_sse_model(chunk, model):
    """只改协议中的模型字段，保留事件名、心跳与换行格式。"""
    if not model:
        return chunk
    try:
        text = chunk.decode("utf-8")
        ending = text[len(text.rstrip("\r\n")):]
        line = text.rstrip("\r\n")
        if not line.startswith("data:"):
            return chunk
        obj = json.loads(line[5:].strip())
        if not isinstance(obj, dict):
            return chunk
        changed = False
        for target in (obj, obj.get("message"), obj.get("response")):
            if isinstance(target, dict) and "model" in target:
                target["model"] = model
                changed = True
        if changed:
            return ("data: " + json.dumps(obj, ensure_ascii=False) + ending).encode("utf-8")
    except (ValueError, UnicodeError):
        pass
    return chunk


def _rewrite_sse_chunks(chunks, model):
    """按 SSE 行增量处理；大行直接透传，避免无限缓存。"""
    buf = b""
    passthrough = False
    for chunk in chunks:
        buf += chunk
        while True:
            boundary = re.search(rb"\r\n|\n|\r(?!$)", buf)
            if boundary is None:
                break
            end = boundary.end()
            line, buf = buf[:end], buf[end:]
            yield line if passthrough else _rewrite_sse_model(line, model)
            passthrough = False
        if len(buf) > 65536:
            # 保留尾部 CR，以免将跨块 CRLF 拆成两个换行。
            keep = 1 if buf.endswith(b"\r") else 0
            yield buf[:-keep] if keep else buf
            buf = buf[-keep:] if keep else b""
            passthrough = True
    if buf:
        yield buf if passthrough else _rewrite_sse_model(buf, model)


def ensure_api_key():
    key = (CONFIG.get("api_key") or "").strip()
    if not key:
        key = "agg-" + secrets.token_urlsafe(24)
        CONFIG["api_key"] = key
        save_config()
        log("已生成聚合 API Key 并写回 config.json")
    return key


class Server(ThreadingHTTPServer):
    # Windows 上 SO_REUSEADDR 允许多个实例同时绑同一端口（连接随机分发），
    # 旧实例不杀干净就会出现「改了配置不生效」的诡异现象。显式关掉复用，
    # 第二个实例绑定会直接失败退出。
    allow_reuse_address = False


VERSION = "1.12.0"


def main():
    global _SERVER
    # 内存优化：请求线程栈从默认 1-2MB 降到 512KB（处理器的调用深度很浅，
    # json/url 处理不会撞底），配合启动后立即回收一次。
    threading.stack_size(512 * 1024)
    import gc
    gc.collect()
    port = int(CONFIG.get("listen_port") or 8787)
    key = ensure_api_key()
    names = ", ".join(u["name"] + ":" + u["base_url"] for u in CONFIG["upstreams"])
    log(f"gateway-hub v{VERSION} 启动 port={port} upstreams=[{names}]")
    log("API Key 已配置（请在本机 config.json 中查看）")
    daily = _automation_daily_cfg()
    if daily["enabled"]:
        threading.Thread(target=_automation_daily_loop, daemon=True).start()
        log("每日自动化已启用：%s（%s）" % (
            daily["time"], ",".join(daily["steps"]) or "计划内全部签到步骤"))
    else:
        log("每日自动化未启用（config.json 的 automation_daily.enabled）")
    keepalive = _codearts_keepalive_cfg()
    if keepalive:
        threading.Thread(target=_codearts_keepalive_loop, args=(keepalive,),
                         daemon=True).start()
        log("codearts 保活已启用：每 %.0f 分钟续期一次 token" % (keepalive / 60))
    try:
        server = Server(("127.0.0.1", port), Handler)
    except OSError as exc:
        raise SystemExit(f"端口 {port} 已被占用（可能已有 gateway-hub 在运行）: {exc}")
    _SERVER = server
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
