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
import threading
import time
import urllib.error
import urllib.request
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

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
    state = {"realms": {}, "paid_any": False}
    if up:
        try:
            data = engine_request(up, "GET", "/accounts?realm=all")
            items = data.get("accounts") if isinstance(data, dict) else data
            for a in (items or []):
                if not isinstance(a, dict) or not a.get("enabled"):
                    continue
                realm = a.get("realm")
                if realm not in ("cn", "intl"):
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
    """模型专属区域（cn/intl）；双区同名或未知返回 None（交给 _wb_route_realm）。"""
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
    available = _wb_usable_realms(upstream_id)
    pref = _wb_dual_prefer()
    if pref in available:
        return pref
    return next(iter(sorted(available or meta["realms"])), None)


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
    except (TypeError, ValueError):
        pass
    return None


#: qwenwork（Buddy2api）目录里的内嵌渠道前缀：auto 通道命名会带 "workbuddy/"
#: 这类前缀，展示时剥掉，转发时保留原名。
INNER_CHANNEL_PREFIXES = ("workbuddy/", "qwenwork/")

_catalog_pub_cache = {}
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
    entries = []
    for up in CONFIG["upstreams"]:
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
                        "tags": tags,
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
                item["tags"] = sorted(set(item["tags"]) | set(tags))
                if item["video"] is None:
                    item["video"] = row.get("videoGeneration")
    models = []
    for item in merged.values():
        item["realms"] = sorted(set(item["realms"]))
        item["kind"] = "video" if any("video" in t for t in item["tags"]) else "image"
        item["active_realm"] = active_realm
        item["available"] = active_realm in item["realms"]
        models.append(item)
    models.sort(key=lambda m: (m["kind"], m["id"]))
    with _wb_media_lock:
        _wb_media_cache.update(ts=now, models=models, errors=errors)
    return models, errors


def public_media_catalog(force=False):
    models, _ = _wb_media_fetch(force=force)
    return [dict(m, platform="workbuddy", public=m["id"], upstream=m["id"])
            for m in models]


def resolve_media_model(model, tag=None):
    """只从当前媒体出口中挑可见且支持目标操作的模型。"""
    model = (model or "").strip()
    up = next((u for u in CONFIG["upstreams"] if u["name"] == "workbuddy"), None)
    if up is None:
        return None, None, None, "未配置 workbuddy 上游，媒体模型不可用"
    entries = public_media_catalog()
    platform, sep, rest = model.partition("/")
    if sep and platform.lower() != "workbuddy":
        return None, None, None, f"媒体生成目前只由 workbuddy 上游提供，不支持 `{model}`"
    wanted = rest if sep else model
    if sep and not wanted:
        return None, None, None, f"模型名 `{model}` 缺少 `/` 后的模型部分"
    if not wanted:
        entry = next((m for m in entries if m.get("available")
                      and not _model_hidden("workbuddy", m["public"])
                      and (not tag or tag in m["tags"])), None)
        if entry is None:
            return None, None, None, "当前媒体出口没有可见且支持该操作的模型"
        wanted = entry["public"]
    else:
        entry = next((m for m in entries if m["public"] == wanted), None)
        if entry is None:
            return None, None, None, f"未知媒体模型 `{model}`"
    if _model_hidden("workbuddy", wanted):
        return None, None, None, f"媒体模型 `{model}` 已在控制台隐藏（媒体页可恢复显示）"
    if not entry.get("available"):
        return None, None, None, f"媒体模型 `{wanted}` 不在当前媒体出口，请检查 WorkBuddy 引擎的媒体区域与账号池"
    if tag and tag not in entry["tags"]:
        return None, None, None, f"媒体模型 `{wanted}` 不支持 `{tag}`"
    return up, wanted, (model or f"workbuddy/{wanted}"), None


#: 媒体生成入口（与引擎 MEDIA_POST_PATHS 对齐）。视频任务的状态/产物/取消
#: 是 /v1/videos/... 的动态路径，单独在 do_POST / do_GET 里判。
MEDIA_POST_PATHS = {
    "/v1/images/generations", "/images/generations",
    "/v1/images/edits", "/images/edits",
    "/v1/videos", "/videos",
}


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


def engine_request(up, method, path, body=None, auth="auto", timeout=15):
    """对引擎发管理请求。auth: panel（hub 面板）/ none（Buddy2api 本地免鉴权）/ auto。

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
            try:
                exc.read()  # 读完错误体再关闭，连接不留到 GC
            except Exception:
                pass
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
        "remain": _num(c.get("remain")), "size": _num(c.get("size")),
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
    }


def normalize_qoder(a):
    c = a.get("credits") or {}
    return {
        "uid": a.get("uid"),
        "name": a.get("nickname") or (a.get("uid") or "")[:8],
        "sub": a.get("plan") or a.get("realm", ""),
        "enabled": bool(a.get("enabled", True)) and not a.get("accountBanned"),
        "remain": _num(c.get("remain")), "size": _num(c.get("size")),
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


NORMALIZERS = {"workbuddy": normalize_wb, "qoder": normalize_qoder, "qwenwork": normalize_buddy}


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

# 官方 wb 仓库已下线（GitHub 404），引擎自带检查因此永远拿不到 latest。
# 回退到社区镜像的 release tag：只作参照，明确标注镜像可能滞后；可用
# config.json 的 wb_update_mirror（owner/repo）覆盖这一默认值。
WB_UPDATE_MIRRORS = ("niyouqianma/workbuddy2api-hub",)
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
    with _urlopen(req, timeout=15) as resp:
        final = resp.geturl()
    m = re.search(r"/releases/tag/([^/?#]+)", final)
    if not m:
        raise RuntimeError("release 页面没有 tag")
    return m.group(1)


def _engine_version(up):
    """引擎当前版本：/health JSON 的 version 字段，或 Server 头里的 x.y.z。"""
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
    for up in CONFIG["upstreams"]:
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


def platform_status(up):
    """单个引擎的健康 + 账号池聚合。"""
    name = up["name"]
    out = {"name": name, "base_url": up["base_url"], "health": False, "accounts": [],
           "inflight": _inflight.get(name, 0), "error": ""}
    try:
        req = urllib.request.Request(up["base_url"].rstrip("/") + "/health")
        with _urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        # 各引擎 health 形状不同：hub 系 {"ok": true}，Buddy2api {"status": "ok"}。
        if "ok" in body:
            out["health"] = bool(body.get("ok"))
        elif "status" in body:
            out["health"] = body.get("status") == "ok"
        else:
            out["health"] = True  # 200 且无标准字段，视为可达
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
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
                        # 反推千问办公各模型的实测单价。超过 512KB 裁到最近 2000 行，
                        # 否则 30s 一次的轮询会让它无限增长（~6900 行/天）。
                        try:
                            hist = os.path.join(HERE, "qwenwork_wallet_history.jsonl")
                            with _wallet_lock:  # 并发 /api/status 下追加+轮转不能交错
                                with open(hist, "a", encoding="utf-8") as wf:
                                    wf.write(json.dumps({
                                        "ts": time.time(),
                                        "account": a.get("nickname") or str(a.get("id")),
                                        "balance": res.get("remaining"),
                                    }, ensure_ascii=False) + "\n")
                                if os.path.getsize(hist) > 512 * 1024:
                                    with open(hist, encoding="utf-8") as rf:
                                        keep = rf.readlines()[-2000:]
                                    tmp = hist + ".tmp"
                                    with open(tmp, "w", encoding="utf-8") as wf:
                                        wf.writelines(keep)
                                    os.replace(tmp, hist)
                        except OSError:
                            pass
                except Exception:
                    pass

        else:
            # hub 系 /accounts 默认只返回当前区域视图，realm=all 才是全池
            data = engine_request(up, "GET", "/accounts?realm=all")
            items = data.get("accounts") if isinstance(data, dict) else data
            items = items or []
        norm = NORMALIZERS.get(name, normalize_buddy)
        out["accounts"] = [norm(a) for a in items if isinstance(a, dict)]
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
                for row in intl_rows:
                    row["daily_chat_verified"] = None
                    row["daily_chat_reward_status"] = "unknown"
                    hit = latest.get(str(row.get("uid") or ""))
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
        # qoder 给整池 expires_at；qwenwork 上游不透出过期时间，如实缺省。
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
    except Exception as exc:
        out["error"] = f"账号聚合失败: {type(exc).__name__}: {exc}"
    return out


# ---------------------------------------------------------------------------
# 额度可用量预估：按最近 24h 实扣外推可用天数（不是 token 数）
# ---------------------------------------------------------------------------
_estimate_lock = threading.Lock()
_estimate_cache = {"ts": 0.0, "data": None}
ESTIMATE_TTL = 120.0

# 千问短样本参考单价（2026-11-16，柳业宏号；冷/热各一次）。
# 这不是官方价，也不保证对任意输入/输出构成成立；只作选型参考，界面必须标注。
QWEN_MODEL_RATES = {
    "qwork-auto":          {"public": "flash",              "hit_per_ktok": 0.00159, "miss_per_ktok": 0.01562, "hit_rate": 0.90},
    "ling-3.1-flash":      {"public": "ling-3.1-flash",     "hit_per_ktok": 0.004115, "miss_per_ktok": 0.004115, "hit_rate": 0.0},
    "glm-5.3-flash":       {"public": "glm-5.3-flash",      "hit_per_ktok": 0.010425, "miss_per_ktok": 0.010425, "hit_rate": 0.0},
    "deepseek-flash":      {"public": "deepseek-flash",     "hit_per_ktok": 0.02057, "miss_per_ktok": 0.02057, "hit_rate": 0.0},
    "qwen3.8-flash":       {"public": "qwen3.8-flash",      "hit_per_ktok": 0.00444, "miss_per_ktok": 0.01521, "hit_rate": 0.90},
    "qwen4-max":           {"public": "qwen4-max",          "hit_per_ktok": 0.00317, "miss_per_ktok": 0.05102, "hit_rate": 0.90},
    "step-5-preview":      {"public": "step-5-preview",     "hit_per_ktok": 0.03197, "miss_per_ktok": 0.1009, "hit_rate": 0.90},
    "qwen3.8-max-preview": {"public": "qwen3.8-max-preview","hit_per_ktok": 0.51554, "miss_per_ktok": 0.51554, "hit_rate": 0.0},
}


def _estimate_public_model(platform, model):
    """引擎内部模型名 -> 网关公开 id，用于可见性过滤和展示。"""
    model = str(model or "")
    while model.startswith(platform + "/"):
        model = model[len(platform + "/"):]
    if platform == "qwenwork":
        spec = _qwen_rate_spec(model, "")
        if spec:
            return spec["public"]
        if model == "qwork-advanced":
            return "auto"
    return model


def _qwen_rate_spec(upstream, public):
    """Match upstream ids with or without extra platform prefixes, then public name."""
    candidates = [upstream, public]
    for raw in (upstream, public):
        text = str(raw or "").strip()
        while text.startswith("qwenwork/"):
            text = text[len("qwenwork/"):]
        candidates.append(text)
    for key in candidates:
        spec = QWEN_MODEL_RATES.get(key)
        if spec:
            return spec
    for spec in QWEN_MODEL_RATES.values():
        if spec.get("public") == public:
            return spec
    return None


def _estimate_rows(up):
    """各平台最近的用量行（每引擎最多 1000 条；wb 双区合并）。"""
    name = up["name"]
    limit = 1000
    if name == "qwenwork":
        data = engine_request(up, "GET", f"/admin/logs?limit={limit}", auth="none")
        return data if isinstance(data, list) else (data.get("items") or data.get("logs") or [])
    if name == "workbuddy":
        rows = []
        for realm in ("cn", "intl"):
            d = engine_request(up, "GET", f"/usage/recent?realm={realm}&limit={limit}")
            rows.extend((d.get("rows") if isinstance(d, dict) else d) or [])
        return rows
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


def estimate_snapshot(force=False):
    """额度预估：目录单价优先，其次有扣费证据的样本；未知不冒充零。"""
    now = time.time()
    with _estimate_lock:
        hit = _estimate_cache["data"]
        if hit and not force and now - _estimate_cache["ts"] < ESTIMATE_TTL:
            return hit
    catalog = public_catalog()
    out = []
    for up in CONFIG["upstreams"]:
        name = up["name"]
        st = platform_status(up)
        accounts = [a for a in st.get("accounts", []) if a.get("enabled")]
        known_balances = [v for v in (_known_num(a.get("remain")) for a in accounts) if v is not None]
        balance = round(sum(known_balances), 2) if known_balances else None
        realm_values, realm_totals = {}, {}
        for a in accounts:
            realm = a.get("realm") or "cn"
            realm_totals[realm] = realm_totals.get(realm, 0) + 1
            value = _known_num(a.get("remain"))
            if value is not None:
                realm_values.setdefault(realm, []).append(value)
        realm_balances = {
            realm: (round(sum(realm_values[realm]), 2) if realm_values.get(realm) else None)
            for realm in realm_totals
        }
        realm_known = {
            realm: {"known": len(realm_values.get(realm, [])), "total": realm_totals[realm]}
            for realm in realm_totals
        }
        item = {"name": name, "balance": balance,
                "known": len(known_balances), "total": len(accounts),
                "enabled": len(accounts),
                "health": st["health"], "error": st.get("error", ""),
                "realm_balances": realm_balances, "realm_known": realm_known}
        try:
            rows = _estimate_rows(up)
        except Exception as exc:
            rows = []
            item["usage_error"] = type(exc).__name__
        samples = {}
        for r in rows:
            model = _estimate_public_model(name, r.get("resolved_model") or r.get("model"))
            sample = samples.setdefault(model, {
                "rows": 0, "tokens": 0.0, "prompt": 0.0, "completion": 0.0,
                "cached": 0.0, "cached_rows": 0,
                "priced_rows": 0, "priced_tokens": 0.0, "credit": 0.0,
                "billed_priced": 0.0, "first_at": None})
            sample["rows"] += 1
            at = _estimate_at(r)
            if at and (sample["first_at"] is None or at < sample["first_at"]):
                sample["first_at"] = at
            tokens = _num(r.get("total_tokens") or r.get("total"))
            prompt = _num(r.get("prompt_tokens") or r.get("prompt"))
            completion = _num(r.get("completion_tokens") or r.get("completion"))
            sample["tokens"] += tokens
            sample["prompt"] += prompt
            sample["completion"] += completion
            cached = None
            raw_cached = r.get("cached_tokens")
            if raw_cached is not None:
                try:
                    cached = float(raw_cached)
                except (TypeError, ValueError):
                    cached = None
                if cached is not None and (cached < 0 or tokens <= 0):
                    cached = None
            billed = None
            if cached is not None:
                sample["cached"] += cached
                sample["cached_rows"] += 1
                # 计费口径：非缓存输入 + 全部输出。billed/tokens 就是缓存折扣率。
                billed = max(0.0, prompt - cached) + completion
            raw_credit = r.get("credit")
            if raw_credit is not None and tokens > 0:
                try:
                    credit = float(raw_credit)
                except (TypeError, ValueError):
                    credit = None
                if credit is not None and 0 <= credit < float("inf"):
                    sample["priced_rows"] += 1
                    sample["priced_tokens"] += tokens
                    sample["credit"] += credit
                    if billed is not None:
                        sample["billed_priced"] += billed
        models = []
        for entry in catalog:
            if entry["platform"] != name or _model_hidden(name, entry["public"]):
                continue
            public = entry["public"]
            model_balance = balance
            scope = ""
            if name == "workbuddy":
                realms = _wb_usable_realms(entry["upstream"])
                if not realms:
                    continue
                scoped_balances = [realm_balances.get(realm) for realm in realms]
                model_balance = (None if any(v is None for v in scoped_balances)
                                 else sum(scoped_balances))
                scope = "双区合计" if len(realms) > 1 else ("仅国际" if realms[0] == "intl" else "仅国内")
            sample = samples.get(public) or {}
            priced = sample.get("priced_tokens", 0.0)
            credit = sample.get("credit", 0.0)
            coverage = priced / sample["tokens"] if sample.get("tokens") else 0
            # 实测有效单价 = 扣费 / 全部 token（天然含缓存折扣）；
            # 实测全价 = 扣费 / 计费 token（非缓存输入 + 输出），即零缓存下限。
            measured_eff = (credit / priced * 1000
                            if sample.get("priced_rows", 0) >= 3 and priced >= 500 and credit > 0
                            else None)
            measured_full = (credit / sample["billed_priced"] * 1000
                             if sample.get("billed_priced", 0.0) >= 500 and credit > 0
                             else None)
            spec = _qwen_rate_spec(entry["upstream"], entry["public"]) if name == "qwenwork" else None
            rate = None          # 支撑 est_tokens 的单价（积分/千tok）
            floor_rate = None    # 零缓存全价（积分/千tok）
            rate_src = "unknown"
            note = "缺少可靠单价，暂不估算"
            extra = {}
            if measured_eff is not None:
                rate = measured_eff
                rate_src = "measured"
                note = (f"按最近 {sample['priced_rows']} 条扣费样本实测（含实际缓存表现），非保证容量")
                if measured_full is not None:
                    floor_rate = measured_full
                else:
                    try:
                        floor_rate = (_row_multiplier(name, entry["row"]) or {}).get("factor")
                    except Exception:
                        floor_rate = None
                    if floor_rate:
                        note += "；下限按目录全价（未经本机校准）"
                    else:
                        floor_rate = None
            elif spec:
                miss = spec["miss_per_ktok"]
                hit_rate = spec["hit_rate"]
                eff = hit_rate * spec["hit_per_ktok"] + (1 - hit_rate) * miss
                # 千问没有逐请求扣费：headline 取全价（零缓存）作保守下限，
                # 缓存假设只作乐观参考，不冒充主估算。
                rate = miss
                rate_src = "cache_assumed"
                hit_pct = int(hit_rate * 100)
                note = f"短样本全价下限（未计缓存）；乐观值按 {hit_pct}% 缓存假设，未经持续校准"
                extra = {"cache_hit_rate": hit_rate, "cache_assumed": True,
                         "rate_opt_per_ktok": round(eff, 6),
                         "est_tokens_opt": None}
            else:
                try:
                    factor = (_row_multiplier(name, entry["row"]) or {}).get("factor")
                except Exception:
                    factor = None
                if factor:
                    floor_rate = float(factor)
                    rate_src = "catalog_floor"
                    note = "无扣费样本：目录全价仅作零缓存下限，未经本机校准"
            est = floor_est = None
            if model_balance is None:
                note = "余额待测，暂不估算"
            elif model_balance > 0:
                if rate:
                    est = int(model_balance / rate * 1000)
                if floor_rate:
                    floor_est = int(model_balance / floor_rate * 1000)
                if extra.get("cache_assumed") and extra.get("rate_opt_per_ktok"):
                    extra["est_tokens_opt"] = int(model_balance / extra["rate_opt_per_ktok"] * 1000)
            sample_hours = None
            if sample.get("first_at"):
                sample_hours = round(max(0.0, (now - sample["first_at"]) / 3600), 1)
            row = {"model": public, "tokens": sample.get("tokens", 0),
                   "samples": sample.get("rows", 0), "coverage": coverage,
                   "sample_hours": sample_hours,
                   "rate_per_ktok": rate, "rate_source": rate_src,
                   "rate_floor_per_ktok": (round(floor_rate, 6) if floor_rate else None),
                   "est_tokens": est, "est_tokens_floor": floor_est,
                   "model_balance": (round(model_balance, 2)
                                     if model_balance is not None else None),
                   "balance_scope": scope,
                   "balance_unknown": model_balance is None,
                   "note": note}
            row.update(extra)
            models.append(row)
        models.sort(key=lambda m: (m["est_tokens"] is None, -(m["est_tokens"] or 0), m["model"]))
        item["models"] = models
        # 不从分散到期的积分包推断整个池的寿命。
        item.update(burn_day=None, days=None, effective_days=None, pool_all=False)
        out.append(item)
    data = {"ok": True, "generated_at": int(now),
            "sample_basis": "各引擎最近返回的用量行（每引擎最多 1000 条，不限 24 小时）",
            "platforms": out}
    with _estimate_lock:
        _estimate_cache.update(ts=now, data=data)
    return data


CHECKIN_PATHS = {
    "workbuddy": ("/accounts/checkin", "panel"),
    "qoder": ("/accounts/checkin", "panel"),
    # qwenwork 没有签到动作：每日积分由上游自动发放（见 _api_checkin 特判）。
    # 不放进这里，避免把「令牌保活」当签到去调用（那条路 100% 超时/失败）。
}


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
        return (secrets.compare_digest(got.encode(), ("Bearer " + want).encode())
                or secrets.compare_digest(got.encode(), want.encode())
                or secrets.compare_digest(api_key.encode(), want.encode()))

    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def _read_post_object(self):
        """Consume one framed object before dispatching any operation."""
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) > 1 or self.headers.get("Transfer-Encoding"):
            self.close_connection = True
            self._deny(400, "不支持的请求体长度或传输编码")
            return None
        raw_length = lengths[0].strip() if lengths else "0"
        if not re.fullmatch(r"[0-9]+", raw_length):
            self.close_connection = True
            self._deny(400, "Content-Length 非法")
            return None
        try:
            length = int(raw_length)
        except ValueError:
            length = 32 * 1024 * 1024 + 1
        if length > 32 * 1024 * 1024:
            self.close_connection = True
            self._deny(413, "请求体过大（上限 32MB）")
            return None
        try:
            raw = self.rfile.read(length) if length else b""
            if len(raw) != length:
                raise ValueError("请求体未完整接收")
        except (OSError, ValueError):
            self.close_connection = True
            self._deny(400, "请求体未完整接收")
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

    def _deny(self, code, message, err_type="invalid_request_error"):
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
        path = self.path.split("?")[0]
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
        if _media_download_allowed(self.path):
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
        if path == "/api/updates":
            force = "force=1" in self.path
            return self._send_json(200, {"ok": True, "rows": _collect_updates(force=force)})
        if path == "/api/catalog":
            return self._api_catalog()
        if path == "/api/status":
            return self._api_status()
        if path == "/api/usage":
            return self._api_usage()
        if path == "/api/estimate":
            return self._api_estimate()
        if path == "/api/login/poll":
            return self._api_login_poll()
        if path.startswith("/api/tasks/"):
            return self._api_tasks_get(path.rsplit("/", 1)[-1])
        if path == "/api/logs":
            return self._send_json(200, {"lines": "".join(tail_log())})
        return self._deny(404, "未知路径")

    def do_POST(self):
        t0 = time.time()
        path = self.path.split("?")[0]
        if not self._authorized():
            self.close_connection = True
            return self._deny(401, "API Key 缺失或不正确")
        self._post_body = self._read_post_object()
        if self._post_body is None:
            return
        if path.startswith("/api/checkin/"):
            return self._api_checkin(path.rsplit("/", 1)[-1])
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
        if path == "/api/engine":
            return self._api_engine()
        if path == "/api/gwconfig":
            return self._api_gwconfig()
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

        # 客户端视角的名字回显给客户端；上游拿真实模型名。
        req["model"] = upstream_model
        payload = json.dumps(req, ensure_ascii=False).encode("utf-8")
        t_payload = time.time()
        stream = bool(req.get("stream"))
        log(f"{path} model={model} -> {up['name']}/{upstream_model} stream={stream}")

        client_h = dict(self.headers.items())
        if up["name"] == "workbuddy":
            # 区域专属模型显式指路：wb Key 的出口是「跟随面板切换」，面板停在
            # 国内版时 intl 专属模型会被引擎以跨区 400 拒掉。双区模型按
            # wb_dual_prefer（默认 auto：临期积分包更紧急的区优先消耗）。
            realm = _wb_model_realm(upstream_model) or _wb_route_realm(upstream_model)
            if realm:
                client_h = {k: v for k, v in client_h.items() if k.lower() != "x-realm"}  # 客户端误带的区域头不覆盖路由判定
                client_h["X-Realm"] = realm
                log(f"wb 分区路由: {upstream_model} -> {realm}")

        t_route = time.time()

        # 平台在飞计数：从发起到响应体写完（流式 = 整个流）都算
        with _platform_inflight(up["name"]):
            try:
                status, headers, result = forward(
                    up, "POST", path, payload, client_h, stream
                )
            except (OSError, TimeoutError, HTTPException) as exc:
                log(f"upstream[{up['name']}] 连接失败: {type(exc).__name__}: {exc}")
                return self._deny(502, f"上游 {up['name']} 不可达: {exc}", "upstream_error")

            # forward 会把「客户端要流式但上游回了非流式」退化为缓冲字节
            if stream and not isinstance(result, (bytes, bytearray)):
                return self._relay_stream(status, headers, result, client_facing, up,
                                          t0=t0, t_head=time.time(), label=model,
                                          t_parse=t_parse, t_payload=t_payload,
                                          t_route=t_route)
            return self._relay_json(status, headers, result, client_facing)

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

    def _media_models(self):
        """GET /v1/media/models：媒体模型清单（OpenAI 风格外壳）。

        媒体模型刻意不进 /v1/models——聊天客户端会把它们当对话模型调用。
        支持 ?tag=text-to-image / image-to-video 等与 ?realm=cn|intl 过滤。
        """
        from urllib.parse import parse_qs
        qs = parse_qs(urlsplit(self.path).query)
        tag = (qs.get("tag") or [""])[0]
        realm = (qs.get("realm") or [""])[0]
        models, errors = _wb_media_fetch()
        data = []
        for m in models:
            if _model_hidden("workbuddy", m["id"]):
                continue
            if tag and tag not in m["tags"]:
                continue
            if realm and realm not in m["realms"]:
                continue
            data.append({
                "id": f"workbuddy/{m['id']}",
                "object": "model",
                "owned_by": "workbuddy",
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
        models, errors = _wb_media_fetch(force="force=1" in self.path)
        items = []
        for m in models:
            items.append({
                "id": f"workbuddy/{m['id']}",
                "platform": "workbuddy",
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
                "hidden": _model_hidden("workbuddy", m["id"]),
                "available": m["available"],
                "active_realm": m["active_realm"],
            })
        self._send_json(200, {"ok": True, "items": items, "total": len(items),
                              "hidden_count": sum(1 for i in items if i["hidden"]),
                              "errors": errors})

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
        return up

    def _media_post(self):
        up = self._media_upstream()
        if up is None:
            return
        path = self.path.split("?")[0]
        cancel = path.startswith("/v1/videos/") and path.endswith("/cancel")
        client_facing = None
        if not cancel:
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
            self._post_body = req
        payload = json.dumps(self._post_body, ensure_ascii=False).encode("utf-8")
        try:
            status, headers, data = forward(up, "POST", self.path, payload,
                                            dict(self.headers.items()), False)
        except (OSError, TimeoutError, HTTPException) as exc:
            log(f"upstream[workbuddy] 媒体请求失败: {type(exc).__name__}: {exc}")
            return self._send_json(502, {"error": {
                "message": f"上游 workbuddy 不可达: {exc}", "type": "upstream_error"}})
        log(f"media {path} model={client_facing or '(默认)'} -> HTTP {status}")
        self._relay_media_json(status, headers, data, client_facing)

    def _media_video_get(self):
        up = self._media_upstream()
        if up is None:
            return
        path = self.path.split("?")[0]
        try:
            if path.endswith("/content"):
                status, headers, chunks = forward(up, "GET", self.path, None, {}, "raw")
                return self._relay_binary(status, headers, chunks)
            status, headers, data = forward(up, "GET", self.path, None, {}, False)
        except (OSError, TimeoutError, HTTPException) as exc:
            return self._send_json(502, {"error": {
                "message": f"上游 workbuddy 不可达: {exc}", "type": "upstream_error"}})
        self._relay_media_json(status, headers, data)

    def _media_tasks(self):
        up = self._media_upstream()
        if up is None:
            return
        try:
            status, headers, data = forward(up, "GET", self.path, None, {}, False)
        except (OSError, TimeoutError, HTTPException) as exc:
            return self._send_json(502, {"error": {
                "message": f"上游 workbuddy 不可达: {exc}", "type": "upstream_error"}})
        self._relay_media_json(status, headers, data)

    # ---- 控制台 API ----

    def _api_status(self):
        platforms = [platform_status(up) for up in CONFIG["upstreams"]]
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
        try:
            result = engine_request(up, "POST", path, {}, auth=kind)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "ignore")[:300]
            return self._send_json(502, {"ok": False, "platform": name,
                                         "error": f"HTTP {exc.code}: {detail}"})
        except Exception as exc:
            return self._send_json(502, {"ok": False, "platform": name,
                                         "error": f"{type(exc).__name__}: {exc}"})
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

    # ---- 任务中心（成长任务 / 猫猫旅行 / 福利领取，代理到各引擎） ----

    TASK_ACTIONS = {
        # action -> {平台: (引擎路径, 说明)}
        "run": {"workbuddy": ("/tasks/run", "运行成长任务（全部国内任务账号）"),
                "qoder": ("/tasks/run", "领取全部福利（每日 Credits + 活动 + Pro 包）")},
        "claim": {"workbuddy": ("/tasks/claim", "领取已完成任务的奖励")},
        "travel": {"workbuddy": ("/tasks/travel", "猫猫旅行（派出/领奖闭环）"),
                   "qoder": ("/tasks/travel", "猫猫旅行（派出/领奖闭环）")},
        "night": {"workbuddy": ("/tasks/night", "夜猫子任务（仅 23:00-08:00 计数）")},
        "streak-bonus": {"workbuddy": ("/tasks/streak-bonus", "连登管家奖励")},
        "daily-web": {"workbuddy": ("/accounts/daily-chat-web", "国际版网页通道补打卡")},
    }

    def _api_tasks_get(self, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up:
            return self._deny(404, f"未知平台 `{name}`（可用: workbuddy, qoder, qwenwork）")
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
                codes = [t.get("task_code") for t in (tasks or [])
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
            detail = exc.read().decode("utf-8", "ignore")[:300]
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
        "qwenwork": {"admin/accounts/keepalive-all", "admin/credit-summary"},
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
        allowed = self.ENGINE_WHITELIST.get(name, set())
        if path not in allowed:
            return self._deny(403, f"路径 `{path}` 不在 {name} 的自动化白名单内")
        method = str(body.get("method") or "POST").upper()
        payload = body.get("body") if isinstance(body.get("body"), dict) else {}
        # 每日活跃/成长任务这类要在引擎里跑真实会话的操作，耗时可能几分钟
        timeout = 300 if ("daily-chat" in path or path.startswith("tasks/")) else 60
        try:
            auth = "none" if name == "qwenwork" else "panel"
            result = engine_request(up, method, "/" + path, payload, auth=auth, timeout=timeout)
            log(f"engine[{name}] {method} /{path} completed")
            self._send_json(200, {"platform": name, "path": path,
                                  "result": result, **_action_status(result)})
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "ignore")[:300]
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
                "wb_realms": (_wb_pool_state().get("realms") or {})})
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
            "cached": (_num(row["cached_tokens"]) if row.get("cached_tokens") is not None else None),
            "reasoning": _num(row.get("reasoning_tokens")),
            "total": _num(row.get("total_tokens")) or (_num(row.get("prompt_tokens")) + _num(row.get("completion_tokens"))),
            "credit": credit,
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
        try:
            limit = max(1, min(500, int(self.path.split("limit=")[-1].split("&")[0]) if "limit=" in self.path else 60))
        except ValueError:
            limit = 60
        rows, errors, usd_cny = [], {}, None
        for up in CONFIG["upstreams"]:
            name = up["name"]
            try:
                uid_names = {}
                if name == "qwenwork":
                    data = engine_request(up, "GET", f"/admin/logs?limit={limit}", auth="none")
                    items = data if isinstance(data, list) else data.get("items") or data.get("logs") or []
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
                        for realm in ("cn", "intl"):
                            d2 = engine_request(up, "GET", f"/usage/recent?realm={realm}&limit={limit}")
                            r2 = d2.get("rows") if isinstance(d2, dict) else d2
                            items.extend(r2 or [])
                            if isinstance(d2, dict) and d2.get("usd_cny"):
                                usd_cny = d2.get("usd_cny")
                    else:
                        data = engine_request(up, "GET", f"/usage/recent?limit={limit}")
                        items = data.get("rows") if isinstance(data, dict) else data
                        items = items or []
                        if isinstance(data, dict) and data.get("usd_cny"):
                            usd_cny = data.get("usd_cny")
                normed = [self._norm_usage_row(name, r) for r in items if isinstance(r, dict)]
                for r in normed:
                    r["account"] = uid_names.get(r["account"]) or r["account"]
                rows.extend(normed)
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"
        rows.sort(key=lambda r: r["at"], reverse=True)
        self._send_json(200, {"ok": True, "rows": rows, "errors": errors, "usd_cny": usd_cny})

    # ---- OAuth 添加账号 ----

    LOGIN_START = {"workbuddy": {"platform": "CLI"}, "qoder": {}}

    def _api_login_start(self, name):
        up = next((u for u in CONFIG["upstreams"] if u["name"] == name), None)
        if not up or name == "qwenwork":
            return self._deny(400, "千问办公没有 OAuth 流程：在官方客户端登录后点「重新检测并导入」")
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
            self._send_json(200, {"ok": True, "platform": name, "state": result.get("state"),
                                  "authUrl": result.get("authUrl")})
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _api_login_poll(self):
        qs = urlsplit(self.path).query
        from urllib.parse import parse_qs, quote  # 局部导入避免顶部命名冲突
        params = parse_qs(qs)
        up_name = (params.get("platform") or [""])[0].strip()
        state = (params.get("state") or [""])[0].strip()
        up = next((u for u in CONFIG["upstreams"] if u["name"] == up_name), None)
        if not up or not state or up_name == "qwenwork":
            return self._deny(400, "需要 platform 与 state 参数")
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
        try:
            disc = engine_request(up, "GET", "/admin/accounts/discover?channel=qwenwork", auth="none")
            token = disc.get("preview_token") or ""
            if not token or not disc.get("importable_count"):
                return self._send_json(200, {"ok": False,
                    "error": "没有发现可导入的新凭证（确认 QwenWork 客户端已登录企业工作区）",
                    "discover": disc})
            result = engine_request(up, "POST", "/admin/accounts/import",
                                    {"channel": "qwenwork", "preview_token": token}, auth="none")
            log(f"qwenwork reimport -> {json.dumps(result, ensure_ascii=False)[:150]}")
            self._send_json(200, {"ok": True, "result": result})
        except Exception as exc:
            self._send_json(502, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

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
            full = f"workbuddy/{m['public']}"
            mult = None
            if full in custom:
                mult = {"factor": custom[full].get("factor"), "source": "manual",
                        "note": custom[full].get("note", ""),
                        "recorded_at": custom[full].get("recorded_at", "")}
            items.append({
                "id": full, "platform": "workbuddy", "model": m["public"],
                "upstream": m["upstream"], "alt": m["name"], "desc": "",
                "hidden": _model_hidden("workbuddy", m["public"]),
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
                 or any(f"workbuddy/{m['public']}" == mid for m in public_media_catalog()))
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
            if up["name"] == "workbuddy":
                platform_names += [f"workbuddy/{m['public']}" for m in public_media_catalog()]
        with _config_lock:
            hidden = _hidden_set()
            if "ids" in body:
                ids = body.get("ids")
                if not isinstance(ids, list) or not all(isinstance(i, str) and "/" in i for i in ids):
                    return self._deny(400, "ids 必须是 `平台/模型` 全名数组")
                for mid in ids:
                    if body.get("visible") is False:
                        hidden.add(mid)
                    else:
                        hidden.discard(mid)
            elif "id" in body:
                mid = str(body["id"] or "").strip()
                if not mid or "/" not in mid:
                    return self._deny(400, "id 必须是 `平台/模型` 形式")
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
        for up in CONFIG["upstreams"]:
            try:
                req = urllib.request.Request(up["base_url"].rstrip("/") + "/health")
                with _urlopen(req, timeout=5) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                # 各引擎 health 形状不同：hub 系 {"ok": true}，Buddy2api {"status": "ok"}。
                if "ok" in body:
                    platforms[up["name"]] = bool(body.get("ok"))
                elif "status" in body:
                    platforms[up["name"]] = body.get("status") == "ok"
                else:
                    platforms[up["name"]] = True
            except Exception:
                platforms[up["name"]] = False
        self._send_json(200, {"ok": all(platforms.values()), "version": VERSION, "platforms": platforms})


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


VERSION = "1.9.0"


def main():
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
    try:
        server = Server(("127.0.0.1", port), Handler)
    except OSError as exc:
        raise SystemExit(f"端口 {port} 已被占用（可能已有 gateway-hub 在运行）: {exc}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
