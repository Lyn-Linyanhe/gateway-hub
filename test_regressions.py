"""Offline regression tests. No credentials, sockets or upstream requests."""
import ast
import hashlib
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import secrets
import threading
import time
import unittest
from urllib.parse import quote
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler

ROOT = Path(__file__).resolve().parent


def isolated(path, names, extra=None):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
    scope = dict(json=json, re=re, secrets=secrets, time=time, BaseHTTPRequestHandler=BaseHTTPRequestHandler,
                 HTTPException=HTTPException, CONFIG={}, log=lambda *_: None)
    scope.update(extra or {})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), scope)
    return scope


def constant(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(name)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.ns = isolated(ROOT / "router.py", {
            "_rewrite_sse_model", "_rewrite_sse_chunks", "forward", "_action_status", "Handler",
            "_num", "_known_num", "_checkin_tally", "_wb_route_realm", "_wb_usable_realms",
            "_wb_known_no_account", "estimate_snapshot", "_credit_rate", "_wallet_rate",
            "_read_wallet_history", "_estimate_public_model", "_qwen_sample",
            "_row_multiplier", "normalize_wb", "normalize_qoder", "_wb_account_realms",
            "_wb_pool_fetch", "_wb_dual_prefer", "normalize_raccoon", "_shanghai_today",
            "_raccoon_checkin_result", "_iso_epoch", "normalize_trae",
            "_disabled_platforms", "platform_enabled", "active_upstreams", "_upstream_by_name",
            "_launch_spec", "_raccoon_billing_rows", "_read_raccoon_billing_links",
            "_attach_raccoon_bills", "_record_raccoon_billing_link",
            "_raccoon_bill_link_for_request", "_raccoon_response_id",
            "_raccoon_session_key",
            "_estimate_at", "_action_status", "_request_target",
            "_media_supports_tag", "_trae_consumption_rate", "_trae_spend_rate",
            "_trae_consumed_amount",
            "_media_link_signature", "_media_download_allowed",
            "_media_multipart_reject", "_oversized_function_names"},
            {"QWEN_WALLET_HISTORY": "", "RACCOON_WALLET_HISTORY": "",
             "TRAE_WALLET_HISTORY": "", "TRAE_SPEND_HISTORY": "",
             "RACCOON_BILLING_LINKS": "",
             "PLATFORM_BALANCE_UNIT": constant(ROOT / "router.py", "PLATFORM_BALANCE_UNIT"),
             "ESTIMATE_EXCLUDED": constant(ROOT / "router.py", "ESTIMATE_EXCLUDED"),
             "CODECARTS_FUNCTION_NAME_LIMIT":
                 constant(ROOT / "router.py", "CODECARTS_FUNCTION_NAME_LIMIT"),
             "_estimate_context": type("EstimateContext", (), {})(),
             "os": __import__('os'), "HERE": str(ROOT),
             "_config_lock": threading.RLock(),
             "QWEN_MODEL_PRICES": constant(ROOT / "router.py", "QWEN_MODEL_PRICES"),
             "hashlib": __import__('hashlib'),
             "hmac": __import__('hmac'), "_media_link_secret": b'offline-test-only',
             "datetime": datetime,
             "urlsplit": __import__("urllib.parse", fromlist=["urlsplit"]).urlsplit,
             "MEDIA_POST_PATHS": constant(ROOT / "router.py", "MEDIA_POST_PATHS")})

    def test_loopback_bypasses_proxy(self):
        import urllib.request
        from urllib.parse import urlsplit
        from unittest.mock import Mock, patch
        opener = Mock()
        scope = isolated(ROOT / 'router.py', {'_urlopen'},
                         {'urllib': __import__('urllib.request'), 'urlsplit': urlsplit, '_local_opener': opener})
        with patch('urllib.request.urlopen') as external:
            for url in ('http://127.0.0.1:8788/health', 'http://localhost:8790/health', 'http://[::1]:8792/health'):
                scope['_urlopen'](urllib.request.Request(url), timeout=5)
            self.assertEqual(opener.open.call_count, 3)
            external.assert_not_called()
            scope['_urlopen']('https://example.com/updates', timeout=5)
            external.assert_called_once()

    def test_sse_newlines_and_nested_models(self):
        for nl in (b"\n", b"\r\n", b"\r"):
            stream = nl.join([b": ping", b"", b'data: {"message":{"model":"old"}}', b"",
                              b'data: {"response":{"model":"old"}}', b"", b'data: {"model":"old"}', b"", b""])
            chunks = [stream[i:i+1] for i in range(len(stream))]
            result = b"".join(self.ns['_rewrite_sse_chunks'](chunks, 'public/m'))
            self.assertEqual(result.count(b'public/m'), 3)
            self.assertIn(b': ping' + nl, result)

    def test_sse_yields_before_end(self):
        def upstream():
            yield b'data: {"model":"old"}\r\n\r\n'
            raise AssertionError('must not fetch next chunk before yielding')
        it = self.ns['_rewrite_sse_chunks'](upstream(), 'public/m')
        self.assertIn(b'public/m', next(it))

    def test_lowercase_content_type(self):
        class Response:
            status = 200
            def getheaders(self): return [('content-type', 'text/event-stream')]
            def read(self): raise AssertionError('must not buffer SSE')
            def read1(self, _): return b''
        class Connection:
            def request(self, *args, **kwargs): pass
            def getresponse(self): return Response()
            def close(self): pass
        self.ns.update(_connect=lambda _: Connection(), _upstream_headers=lambda _: {},
                       _upstream_path=lambda _, p:p, HOP_HEADERS=set())
        status, headers, body = self.ns['forward']({}, 'POST', '/', b'{}', {}, True)
        self.assertEqual(status, 200)
        self.assertNotIsInstance(body, bytes)
        list(body)

    def test_business_status(self):
        status = self.ns['_action_status']
        self.assertFalse(status({'ok':False, 'message':'denied'})['ok'])
        self.assertTrue(status({'cn_checkin':{'results':[{'ok':True}]},
                                'intl_daily_chat':{'error':'timeout'}})['partial'])
        self.assertFalse(status({'claimed':[{'result':{'ok':False}}]})['ok'])
        self.assertTrue(status({'ok':True, 'accounts':[{'error':'old'}]})['ok'])

    def test_cache_unknown(self):
        row = self.ns['Handler']._norm_usage_row(None, 'qwenwork', {'prompt_tokens':100,'total_tokens':100})
        self.assertIsNone(row['cached'])

    def test_cache_known(self):
        for value, expected in ((80, 80), (0, 0)):
            row = self.ns['Handler']._norm_usage_row(
                None, 'qwenwork',
                {'prompt_tokens':100,'total_tokens':100,'cached_tokens':value})
            self.assertEqual(row['cached'], expected)

    def test_trae_unreported_cache_is_unknown_not_zero(self):
        """引擎报 cached_reported=False 时，cached_tokens 的 0 是占位不是实测。

        当成已知 0 会让看板算出 100% 覆盖 / 0% 命中，把「不知道」说成结论。
        """
        norm = self.ns['Handler']._norm_usage_row
        unreported = norm(None, 'trae', {'prompt_tokens': 80000, 'total_tokens': 80000,
                                         'cached_tokens': 0, 'cached_reported': False})
        reported_zero = norm(None, 'trae', {'prompt_tokens': 80000, 'total_tokens': 80000,
                                            'cached_tokens': 0, 'cached_reported': True})
        reported_hit = norm(None, 'trae', {'prompt_tokens': 80000, 'total_tokens': 80000,
                                           'cached_tokens': 64000, 'cached_reported': True})
        legacy = norm(None, 'trae', {'prompt_tokens': 80000, 'total_tokens': 80000,
                                     'cached_tokens': 0})
        missing = norm(None, 'trae', {'usage_missing': True, 'prompt_tokens': 0,
                                      'total_tokens': 0, 'cached_tokens': 0,
                                      'cached_reported': False})
        self.assertIsNone(unreported['cached'])
        self.assertEqual(reported_zero['cached'], 0)
        self.assertEqual(reported_hit['cached'], 64000)
        self.assertEqual(legacy['cached'], 0)  # 旧行无该键：沿用历史语义
        self.assertIsNone(missing['cached'])
        # 其它平台不受 trae 的 cached_reported 语义影响
        self.assertEqual(norm(None, 'qwenwork',
                              {'prompt_tokens': 100, 'total_tokens': 100,
                               'cached_tokens': 0})['cached'], 0)

    def test_bearer_scheme_is_case_insensitive(self):
        import email.message
        self.ns['CONFIG'] = {'api_key': 'synthetic-only'}
        handler = object.__new__(self.ns['Handler'])
        for header in ('Bearer synthetic-only', 'bearer synthetic-only',
                       'BEARER synthetic-only', 'Bearer  synthetic-only'):
            handler.headers = email.message.Message()
            handler.headers['Authorization'] = header
            with self.subTest(header=header):
                self.assertTrue(handler._authorized())

    def test_anthropic_key(self):
        self.ns['CONFIG']={'api_key':'synthetic-only'}
        obj=object.__new__(self.ns['Handler'])
        obj.headers={'x-api-key':'synthetic-only'}
        self.assertTrue(obj._authorized())

    def test_realm_membership(self):
        self.ns.update(_wb_realm_meta=lambda:{'m':{'realms':['cn','intl'],'pro':['intl']}},
                       _wb_pool_state=lambda:{'realms':{'cn':{'paid':True},'intl':{'paid':False}}},
                       _wb_dual_prefer=lambda:'intl')
        self.assertEqual(self.ns['_wb_route_realm']('m'), 'cn')

    def test_unknown_cost_is_not_estimated(self):
        self.ns.update(_estimate_lock=threading.Lock(),_estimate_cache={'data':None},ESTIMATE_TTL=120,
            CONFIG={'upstreams':[{'name':'qwenwork'}]},
            public_catalog=lambda:[{'platform':'qwenwork','public':'pro','upstream':'pro','row':{}}],
            platform_status=lambda _: {'health':True,'accounts':[{'enabled':True,'remain':100}]},
            _estimate_rows=lambda _: [{'created_at':time.time(),'model':'pro','total_tokens':2000}],
            _estimate_at=lambda r:r['created_at'],_model_hidden=lambda *_:False,
            _read_wallet_history=lambda:[])
        model=self.ns['estimate_snapshot'](True)['platforms'][0]['models'][0]
        self.assertIsNone(model['rate_per_mtok'])
        self.assertIsNone(model['est_tokens'])
        self.assertIn('暂不估算',model['note'])

    def estimate(self, accounts, rows=(), history=(), platform='qwenwork',
                 catalog=None):
        self.ns.update(
            _estimate_lock=threading.Lock(), _estimate_cache={'data':None}, ESTIMATE_TTL=120,
            CONFIG={'upstreams':[{'name':platform}]},
            public_catalog=lambda: catalog or [
                {'platform':platform,'public':'pro','upstream':'pro','row':{}}],
            platform_status=lambda _: {'health':True,'accounts':accounts},
            _estimate_rows=lambda _: list(rows), _estimate_at=lambda r:r['created_at'],
            _model_hidden=lambda *_:False, _read_wallet_history=lambda:list(history),
            _wb_usable_realms=lambda _:['cn'])
        return self.ns['estimate_snapshot'](True)['platforms'][0]

    def test_unknown_balance_stays_unknown(self):
        rows=[{'created_at':time.time(),'model':'q','total_tokens':1000,'credit':1}
              for _ in range(3)]
        catalog=[{'platform':'workbuddy','public':'q','upstream':'q','row':{}}]
        item=self.estimate([{'enabled':True,'remain':None}], rows,
                           platform='workbuddy', catalog=catalog)
        self.assertIsNone(item['balance'])
        self.assertEqual((item['known'],item['total']),(0,1))
        model=item['models'][0]
        # 单价仍可由扣费样本算出，但余额未知时不出预计 token。
        self.assertEqual(model['rate_per_mtok'],1000.0)
        self.assertIsNone(model['est_tokens'])
        self.assertIn('待测',model['note'])
        item=self.estimate([{'enabled':True,'remain':None},{'enabled':True,'remain':5}],
                           rows, platform='workbuddy', catalog=catalog)
        self.assertIsNone(item['balance'])
        self.assertEqual(item['known_balance'],5.0)
        self.assertFalse(item['balance_complete'])
        self.assertIsNone(item['models'][0]['est_tokens'])

    def test_estimate_uses_measured_credit_rate(self):
        rows=[{'created_at':time.time(),'model':'ds','total_tokens':10000,'credit':0.02}
              for _ in range(4)]
        item=self.estimate([{'enabled':True,'remain':100,'realm':'cn'}], rows,
                           platform='workbuddy',
                           catalog=[{'platform':'workbuddy','public':'ds',
                                     'upstream':'ds','row':{}}])
        model=item['models'][0]
        self.assertEqual(model['rate_source'],'measured')
        self.assertEqual(model['rate_per_mtok'],2.0)
        self.assertEqual(model['samples'],4)
        self.assertEqual(model['sample_tokens'],40000)
        self.assertEqual(model['est_tokens'],50_000_000)
        self.assertIn('Σ实扣',model['note'])

    def test_qwen_model_without_sample_price_is_unknown(self):
        now=time.time()
        rows=[{'created_at':now-3600,'account_name':'a','total_tokens':5_000_000}]
        history=[{'ts':now-7200,'account':'a','balance':1000.0},
                 {'ts':now-60,'account':'a','balance':500.0}]
        item=self.estimate([{'enabled':True,'remain':900}], rows, history,
                           catalog=[{'platform':'qwenwork','public':'pro',
                                     'upstream':'pro','row':{}}])
        model=item['models'][0]
        self.assertIsNone(model['rate_per_mtok'])
        self.assertIsNone(model['est_tokens'])
        self.assertIn('无短样本价',model['note'])

    def test_qwen_prices_are_anchored_to_wallet_rate(self):
        self.ns['QWEN_MODEL_PRICES'] = {
            'm1': {'public':'m1','miss_per_ktok':0.05},
            'm2': {'public':'m2','miss_per_ktok':0.01},
        }
        now=time.time()
        rows=[{'created_at':now-3600,'account_name':'a','total_tokens':5_000_000,'model':'m1'},
              {'created_at':now-3600,'account_name':'a','total_tokens':5_000_000,'model':'m2'}]
        history=[{'ts':now-7200,'account':'a','balance':1000.0},
                 {'ts':now-60,'account':'a','balance':400.0}]
        catalog=[{'platform':'qwenwork','public':'m1','upstream':'m1','row':{}},
                 {'platform':'qwenwork','public':'m2','upstream':'m2','row':{}}]
        item=self.estimate([{'enabled':True,'remain':100}], rows, history,
                           catalog=catalog)
        by={m['model']:m for m in item['models']}
        self.assertEqual(by['m1']['rate_source'],'anchored')
        self.assertAlmostEqual(by['m1']['rate_per_mtok'],100.0)
        self.assertAlmostEqual(by['m2']['rate_per_mtok'],20.0)
        self.assertEqual(by['m1']['est_tokens'],1_000_000)
        self.assertEqual(by['m2']['est_tokens'],5_000_000)
        self.assertIn('锚定',by['m1']['note'])

    def test_model_with_insufficient_samples_is_unknown(self):
        rows=[{'created_at':time.time(),'model':'other','total_tokens':10000,'credit':0.02}
              for _ in range(4)]
        catalog=[{'platform':'workbuddy','public':'other','upstream':'other','row':{}},
                 {'platform':'workbuddy','public':'thin','upstream':'thin','row':{}}]
        item=self.estimate([{'enabled':True,'remain':100,'realm':'cn'}], rows,
                           platform='workbuddy', catalog=catalog)
        by={m['model']:m for m in item['models']}
        self.assertIsNotNone(by['other']['est_tokens'])
        self.assertIsNone(by['thin']['rate_per_mtok'])
        self.assertIsNone(by['thin']['est_tokens'])
        self.assertIn('样本不足',by['thin']['note'])

    def test_wallet_rate_ignores_stagnant_balance_window(self):
        """余额是缓存值时，静止期不能被当成零成本。

        实测场景：Trae 余额只在最初 5 秒从 1982.66 降到 1972.00，之后 2500 秒
        一直是 1972.00，但 token 持续累积到 3422 万。用「首末快照」会把
        10.66 点摊到 3422 万 token 上，算出 0.31 分/Mtok 这种假单价。
        正确口径：只统计余额真正下降的那一对相邻快照。
        """
        now=time.time()
        history=[{'ts':now-1000,'account':'a','balance':100.0},
                 {'ts':now-990,'account':'a','balance':90.0},   # 真实扣费 10 点
                 {'ts':now-500,'account':'a','balance':90.0},   # 静止
                 {'ts':now,'account':'a','balance':90.0}]       # 静止
        rows=[{'created_at':now-995,'account_name':'a','total_tokens':200_000},  # 下降区间内
              {'created_at':now-400,'account_name':'a','total_tokens':5_000_000}, # 静止区间：必须排除
              {'created_at':now-10,'account_name':'a','total_tokens':5_000_000}]
        rate, accounts, tokens, hours = self.ns['_wallet_rate'](rows, history)
        self.assertEqual(accounts, 1)
        self.assertEqual(tokens, 200_000)          # 只算下降区间内的 token
        self.assertAlmostEqual(rate, 50.0, places=6)  # 10 / 200000 * 1e6

    def test_wallet_rate_requires_real_decline(self):
        """余额全程不变：不能产出任何单价（引擎没在刷新积分快照）。"""
        now=time.time()
        history=[{'ts':now-1000,'account':'a','balance':90.0},
                 {'ts':now,'account':'a','balance':90.0}]
        rows=[{'created_at':now-500,'account_name':'a','total_tokens':900_000}]
        rate, accounts, tokens, hours = self.ns['_wallet_rate'](rows, history)
        self.assertIsNone(rate)
        self.assertEqual(accounts, 0)

    def test_estimate_reports_unit_and_incomparable_note(self):
        """每个平台必须带自己的余额单位；顶层要说明跨平台不可比。"""
        self.estimate([{'enabled':True,'remain':100}])
        self.ns.update(CONFIG={'upstreams':[{'name':'raccoon'}]},
            public_catalog=lambda:[{'platform':'raccoon','public':'m','upstream':'m','row':{}}],
            _estimate_rows=lambda _: [],
            _estimate_at=lambda r:0)
        data=self.ns['estimate_snapshot'](True)
        platform=data['platforms'][0]
        self.assertEqual(platform['balance_unit'],'小浣熊积分点')
        self.assertIn('不可直接比较', data['unit_note'])

    def test_wallet_rate_skips_topups_and_insufficient_samples(self):
        now=time.time()
        rows=[{'created_at':now-3600,'account_name':'a','total_tokens':200_000}]
        history=[{'ts':now-7200,'account':'a','balance':100.0},
                 {'ts':now-60,'account':'a','balance':150.0}]
        item=self.estimate([{'enabled':True,'remain':100}], rows, history,
                           catalog=[{'platform':'qwenwork','public':'pro',
                                     'upstream':'pro','row':{}}])
        self.assertIsNone(item['models'][0]['rate_per_mtok'])
        self.assertIsNone(item['models'][0]['est_tokens'])

    def test_estimate_zero_balance_is_zero_tokens(self):
        rows=[{'created_at':time.time(),'model':'q','total_tokens':1000,'credit':1}
              for _ in range(3)]
        item=self.estimate([{'enabled':True,'remain':0}], rows,
                           platform='qoder',
                           catalog=[{'platform':'qoder','public':'q',
                                     'upstream':'q','row':{}}])
        self.assertEqual(item['balance'],0.0)
        self.assertEqual(item['models'][0]['est_tokens'],0)

    def test_catalog_multiplier_is_not_absolute_price(self):
        self.estimate([{'enabled':True,'remain':100}])
        self.ns.update(CONFIG={'upstreams':[{'name':'qoder'}]},
            public_catalog=lambda:[{'platform':'qoder','public':'m','upstream':'m',
                                    'row':{'price_factor':0.5}}],
            _estimate_rows=lambda _: [{'created_at':time.time(),'model':'m',
                                       'total_tokens':2000}],
            _estimate_at=lambda r:r['created_at'])
        model=self.ns['estimate_snapshot'](True)['platforms'][0]['models'][0]
        self.assertIsNone(model['rate_per_mtok'])
        self.assertIsNone(model['est_tokens'])

    def test_account_balance_unknown_and_zero(self):
        for name in ('normalize_wb','normalize_qoder'):
            normalize=self.ns[name]
            for value in (None, '', 'invalid', True, float('nan'), float('inf')):
                self.assertIsNone(normalize({'credits':{'remain':value}})['remain'])
            self.assertIsNone(normalize({})['remain'])
            self.assertEqual(normalize({'credits':{'remain':0}})['remain'],0)

    def test_wb_normalize_passes_engine_error_marks(self):
        """引擎账号行的错误标记必须透传：面板「当前禁用」的数据源同款。

        引擎 /accounts 已输出 lastError/modelCaps(6004)/disabledModels/cooldownUntil，
        但 normalize_wb 此前全部丢弃——十二月账号的 ds 配额耗尽在网关账号页
        看不到任何标记。
        """
        normalize = self.ns['normalize_wb']
        row = normalize({
            'nickname': '十二月',
            'lastError': 'stream limit (model throttled)',
            'cooldownUntil': 1000.5,
            'modelCaps': {'deepseek-v4.1-flash': {'at': 111.0, 'reset': 222.0}},
            'disabledModels': ['hy4preview'],
        })
        self.assertEqual(row['last_error'], 'stream limit (model throttled)')
        self.assertEqual(row['cooldown_until'], 1000.5)
        self.assertEqual(row['model_caps'],
                         {'deepseek-v4.1-flash': {'at': 111.0, 'reset': 222.0}})
        self.assertEqual(row['disabled_models'], ['hy4preview'])
        # 没有错误标记的账号字段保持空态，不制造假异常
        clean = normalize({'nickname': '青'})
        self.assertIsNone(clean['last_error'])
        self.assertEqual(clean['model_caps'], {})
        self.assertEqual(clean['disabled_models'], [])

    def test_known_pool_routes_only_to_present_accounts(self):
        self.ns.update(_wb_realm_meta=lambda:{'m':{'realms':['cn','intl'],'pro':[]}},
            _wb_pool_state=lambda:{'known':True,'realms':{'intl':{'paid':False}}})
        self.assertEqual(self.ns['_wb_route_realm']('m'),'intl')
        self.ns['_wb_pool_state']=lambda:{'known':False,'realms':{}}
        self.assertEqual(self.ns['_wb_route_realm']('m'),'cn')
        self.ns['_wb_pool_state']=lambda:{'known':True,'realms':{}}
        self.assertIsNone(self.ns['_wb_route_realm']('m'))

    def test_pool_failure_is_unknown_not_empty(self):
        self.ns.update(_wb_state_lock=threading.Lock(),_wb_state_cache={'state':{}},
            engine_request=lambda *args:{'unexpected':[]})
        self.assertFalse(self.ns['_wb_pool_fetch']({})['known'])
        self.ns['engine_request']=lambda *args:{'accounts':[]}
        self.assertTrue(self.ns['_wb_pool_fetch']({'name':'workbuddy'})['known'])
        self.ns['engine_request']=lambda *args:{'accounts':[{'realm':'intl'}]}
        self.assertIn('intl',self.ns['_wb_pool_fetch']({'name':'workbuddy'})['realms'])

    def test_estimate_keeps_realm_balances_and_unknown_region(self):
        self.estimate([{'enabled':True,'remain':100}])
        self.ns.update(CONFIG={'upstreams':[{'name':'workbuddy'}]},
            public_catalog=lambda:[{'platform':'workbuddy','public':'m',
                                    'upstream':'m','row':{}}],
            _wb_usable_realms=lambda _:['intl'],
            platform_status=lambda _: {'health':True,'accounts':[
                {'enabled':True,'remain':100,'realm':'intl'},
                {'enabled':True,'remain':50}]},
            _estimate_rows=lambda _: [])
        item=self.ns['estimate_snapshot'](True)['platforms'][0]
        self.assertEqual(item['realm_balances']['intl'],100)
        self.assertEqual(item['realm_balances']['unknown'],50)
        self.assertEqual(item['balance'],150)
        self.assertEqual(item['models'][0]['balance_scope'],'仅国际')

    def test_raccoon_account_normalization(self):
        norm = self.ns['normalize_raccoon']
        today = self.ns['_shanghai_today']()
        a = norm({'uid': 'u1', 'nickname': 'nick', 'credits': 120,
                  'checked_in': today, 'realm': 'cn', 'in_flight': 2})
        self.assertEqual((a['name'], a['remain'], a['checked_in'], a['enabled'], a['realm']),
                         ('nick', 120, True, True, 'cn'))
        self.assertEqual(a['inflight'], 2)
        paused = norm({'uid': 'u2', 'credits': None, 'checked_in': '2020-01-01',
                       'paused': True})
        self.assertFalse(paused['enabled'])
        self.assertIsNone(paused['remain'])
        self.assertFalse(paused['checked_in'])
        self.assertEqual(paused['name'], 'u2')

    def test_raccoon_multiplier_and_iso_time(self):
        self.assertEqual(self.ns['_row_multiplier']('raccoon', {'credits': 'x0.05'}),
                         {'factor': 0.05})
        self.assertIsNone(self.ns['_row_multiplier']('raccoon', {}))
        self.assertGreater(self.ns['_iso_epoch']('2026-11-18T22:00:00+08:00'), 0)
        self.assertEqual(self.ns['_iso_epoch']('not-a-time'), 0.0)
        self.assertEqual(self.ns['_iso_epoch'](None), 0.0)

    def test_raccoon_checkin_result_counts(self):
        result = self.ns['_raccoon_checkin_result']({'ok': 2, 'failed': 1, 'skipped': 3})
        self.assertEqual(len(result['results']), 6)
        self.assertEqual(self.ns['_checkin_tally'](result), (5, 6))
        status = self.ns['_action_status'](result)
        self.assertFalse(status['ok'])
        self.assertTrue(status['partial'])

    def test_raccoon_checkin_result_requires_valid_counts(self):
        conv = self.ns['_raccoon_checkin_result']
        for bad in ({}, {'error': 'boom'}, {'ok': True, 'failed': 0, 'skipped': 0},
                    {'ok': 1, 'failed': 0}, {'ok': 0, 'failed': -1, 'skipped': 0},
                    {'ok': 'true', 'failed': 0, 'skipped': 0}, None):
            result = conv(bad)
            self.assertIsInstance(result, dict)
            self.assertTrue(result.get('error'))
            self.assertFalse(self.ns['_action_status'](result)['ok'])
        zero = conv({'ok': 0, 'failed': 0, 'skipped': 0})
        self.assertEqual(zero, {'results': [], 'details': [], 'job': 'checkin'})

    def test_raccoon_checkin_reports_missing_results(self):
        import urllib.error
        from unittest.mock import Mock

        handler = object.__new__(self.ns['Handler'])
        handler._send_json = Mock()
        self.ns.update(urllib=urllib,
                       CONFIG={'upstreams': [{'name': 'raccoon'}]},
                       CHECKIN_PATHS={'raccoon': ('/panel/api/checkin_all', 'bearer')},
                       _checkin_timeout=lambda name: 600)
        for upstream_result in ({}, {'ok': False, 'failed': 0}, None):
            with self.subTest(result=upstream_result):
                self.ns['engine_request'] = Mock(return_value=upstream_result)
                handler._api_checkin('raccoon')
                body = handler._send_json.call_args.args[1]
                self.assertFalse(body['ok'])
                self.assertTrue(body['error'])

    def test_raccoon_login_requires_state_and_link(self):
        import urllib.error
        from urllib.parse import urlsplit
        from unittest.mock import Mock

        handler = object.__new__(self.ns['Handler'])
        handler._raccoon_upstream = lambda: {'name': 'raccoon'}
        handler.path = '/api/raccoon/login/poll?state=flow'
        handler._send_json = Mock()
        handler._deny = Mock()
        self.ns.update(urllib=urllib, urlsplit=urlsplit)
        for upstream_result in (None, {'id': 'flow'}, {'id': '', 'url': 'https://example.invalid/login'}):
            with self.subTest(result=upstream_result):
                self.ns['engine_request'] = Mock(return_value=upstream_result)
                handler._api_raccoon_login_start()
                code, body = handler._send_json.call_args.args
                self.assertEqual(code, 502)
                self.assertFalse(body['ok'])
                self.assertTrue(body['error'])
        self.ns['engine_request'] = Mock(return_value={'id': 'flow', 'url': 'https://example.invalid/login'})
        handler._api_raccoon_login_start()
        self.assertEqual(handler._send_json.call_args.args,
                         (200, {'ok': True, 'state': 'flow', 'authUrl': 'https://example.invalid/login'}))
        for upstream_result in ({}, {'status': '等待浏览器授权'}, None):
            with self.subTest(poll_result=upstream_result):
                self.ns['engine_request'] = Mock(return_value=upstream_result)
                handler._api_raccoon_login_poll()
                code, body = handler._send_json.call_args.args
                self.assertEqual(code, 502)
                self.assertFalse(body['ok'])
                self.assertTrue(body['error'])
        pending = {'done': False, 'status': '等待浏览器授权'}
        self.ns['engine_request'] = Mock(return_value=pending)
        handler._api_raccoon_login_poll()
        self.assertEqual(handler._send_json.call_args.args, (200, {'ok': True, 'result': pending}))

    def test_workbuddy_usage_keeps_successful_realm_when_other_fails(self):
        from unittest.mock import Mock

        handler = object.__new__(self.ns['Handler'])
        handler.path = '/api/usage?limit=10'
        handler._send_json = Mock()
        account = {'uid': 'u1', 'nickname': 'User'}
        row = {'at': 1000, 'model': 'm', 'account': 'u1', 'total_tokens': 5,
               'prompt_tokens': 3, 'completion_tokens': 2, 'status_code': 200}

        def fake(up, method, path, body=None, auth=None, timeout=None):
            if path == '/accounts?realm=all':
                return {'accounts': [account]}
            if 'realm=cn' in path:
                return {'rows': [row]}
            if 'realm=intl' in path:
                raise TimeoutError('intl unavailable')
            raise AssertionError(path)

        self.ns.update(active_upstreams=lambda: [{'name': 'workbuddy'}],
                       engine_request=fake, _num=lambda n: float(n or 0), time=time)
        handler._api_usage()
        code, payload = handler._send_json.call_args.args
        self.assertEqual(code, 200)
        self.assertEqual(len(payload['rows']), 1)
        self.assertEqual(payload['rows'][0]['account'], 'User')
        self.assertIn('intl', payload['errors']['workbuddy'])
        # intl 失败时 coverage 仍要带每个源的真实行数，不能整平台消失
        self.assertIn('cn', payload['coverage']['workbuddy'])
        self.assertIn('intl', payload['coverage']['workbuddy'])
        self.assertFalse(payload['truncated'])

    def test_trae_spend_rate_uses_consumed_counter_delta(self):
        """Trae 的 Σ实扣来自累计计数器增量，不是余额差。

        实测：引擎对每个请求都写 credit_reported=false，逐请求实扣拿不到；
        但 entitlement.usage_summary.consumed_amount 单调递增（单次请求后
        369.86 → 371.25），是平台结算的真实扣费。取增量 ÷ 同期 token 就是
        WorkBuddy 同口径的 Σ实扣 ÷ Σtoken。
        """
        now = time.time()
        history = [{'ts': now - 300, 'account': '4052', 'balance': 100.0},
                   {'ts': now - 200, 'account': '4052', 'balance': 120.0},
                   {'ts': now - 100, 'account': '4052', 'balance': 150.0},
                   {'ts': now, 'account': '4052', 'balance': 200.0}]
        rows = [{'at': now - 250, 'account': '1414412225494052', 'total_tokens': 100_000},
                {'at': now - 150, 'account': '1414412225494052', 'total_tokens': 100_000},
                {'at': now - 50, 'account': '1414412225494052', 'total_tokens': 100_000}]
        rate, accounts, tokens, hours = self.ns['_trae_spend_rate'](rows, history)
        self.assertEqual(accounts, 1)
        self.assertEqual(tokens, 300_000)
        # 三段增量 20+30+50 = 100 点，摊到 30 万 token
        self.assertAlmostEqual(rate, 100 / 300_000 * 1_000_000, places=6)

    def test_trae_spend_rate_ignores_flat_counter(self):
        """计数器没动说明区间内没有结算，不能当成零成本样本。"""
        now = time.time()
        history = [{'ts': now - 300, 'account': '4052', 'balance': 100.0},
                   {'ts': now - 200, 'account': '4052', 'balance': 100.0},
                   {'ts': now - 100, 'account': '4052', 'balance': 100.0},
                   {'ts': now, 'account': '4052', 'balance': 100.0}]
        rows = [{'at': now - 250, 'account': '1414412225494052', 'total_tokens': 900_000}]
        rate, accounts, tokens, hours = self.ns['_trae_spend_rate'](rows, history)
        self.assertIsNone(rate)
        self.assertEqual(accounts, 0)

    def test_trae_spend_rate_requires_minimum_sample(self):
        """不足 10 万 token 保持待测：单区间样本太薄，单价没有统计意义。"""
        now = time.time()
        history = [{'ts': now - 200, 'account': '4052', 'balance': 100.0},
                   {'ts': now - 100, 'account': '4052', 'balance': 130.0},
                   {'ts': now, 'account': '4052', 'balance': 170.0}]
        rows = [{'at': now - 150, 'account': '1414412225494052', 'total_tokens': 1_000},
                {'at': now - 50, 'account': '1414412225494052', 'total_tokens': 1_000}]
        rate, accounts, tokens, hours = self.ns['_trae_spend_rate'](rows, history)
        self.assertIsNone(rate)
        self.assertEqual(tokens, 2_000)

    def test_trae_consumed_amount_reads_entitlement(self):
        read = self.ns['_trae_consumed_amount']
        raw = {'entitlement': {'ent_usage': {'usage_summary': {'consumed_amount': 371.25}}}}
        self.assertEqual(read(raw), 371.25)
        self.assertIsNone(read({}))                       # 缺失不是 0
        self.assertIsNone(read({'entitlement': {'ent_usage': {}}}))
        self.assertIsNone(read({'entitlement': None}))

    def test_trae_consumption_rate_lookup(self):
        lookup = self.ns['_trae_consumption_rate']
        catalog = [
            {'platform': 'trae', 'public': 'DeepSeek-V4.1-Flash',
             'upstream': 'deepseek-v4.1-flash',
             'row': {'consumption_rate': 0.06}},
            {'platform': 'trae', 'public': 'Kimi-K3', 'upstream': 'kimi-k3',
             'row': {'consumption_rate': 1.83}},
            {'platform': 'trae', 'public': 'NoRate', 'upstream': 'no-rate', 'row': {}},
            {'platform': 'trae', 'public': 'Bool', 'upstream': 'bool-rate',
             'row': {'consumption_rate': True}},
            {'platform': 'workbuddy', 'public': 'Other', 'upstream': 'other',
             'row': {'consumption_rate': 9.9}},
        ]
        self.assertEqual(lookup(catalog, 'deepseek-v4.1-flash'), 0.06)
        self.assertEqual(lookup(catalog, 'DeepSeek-V4.1-Flash'), 0.06)
        self.assertEqual(lookup(catalog, 'kimi-k3'), 1.83)
        self.assertIsNone(lookup(catalog, 'no-rate'))
        self.assertIsNone(lookup(catalog, 'bool-rate'))
        self.assertIsNone(lookup(catalog, 'other'))
        self.assertIsNone(lookup(catalog, 'missing'))

    def test_trae_estimate_anchors_by_official_rate(self):
        """Trae 无逐请求扣费时，用官方倍率 × 平台实测均价锚定出单价。"""
        scope = isolated(ROOT / 'router.py', {'estimate_snapshot', '_trae_consumption_rate',
                                              '_trae_spend_rate',
                                              '_wallet_rate', '_credit_rate',
                                              '_estimate_public_model', '_estimate_at',
                                              '_num', '_known_num'},
                         {'time': time, 'json': json,
                          'ESTIMATE_TTL': 120.0,
                          '_estimate_lock': threading.Lock(),
                          '_estimate_cache': {'ts': 0.0, 'data': None},
                          'CONFIG': {'hidden_models': []},
                          'ESTIMATE_EXCLUDED': constant(ROOT / "router.py", "ESTIMATE_EXCLUDED"),
                          'active_upstreams': lambda: [{'name': 'trae'}],
                          'public_catalog': lambda: [
                              {'platform': 'trae', 'public': 'Flash',
                               'upstream': 'flash', 'row': {'consumption_rate': 0.06}},
                              {'platform': 'trae', 'public': 'Kimi',
                               'upstream': 'kimi', 'row': {'consumption_rate': 1.83}}],
                          'platform_status': lambda _: {'health': True, 'accounts': [
                              {'enabled': True, 'remain': 1000.0, 'realm': 'cn'}]},
                          '_estimate_rows': lambda _: [
                              {'model': 'flash', 'total_tokens': 100_000,
                               'account': 'a', 'at': 100.0, 'credit': None},
                              {'model': 'kimi', 'total_tokens': 100_000,
                               'account': 'a', 'at': 200.0, 'credit': None}],
                          '_read_wallet_history': lambda path=None, metric='balance': [
                              {'ts': 100.0, 'account': 'a', 'balance': 1000.0},
                              {'ts': 200.0, 'account': 'a', 'balance': 1010.0}],
                          '_model_hidden': lambda *a: False,
                          'TRAE_SPEND_HISTORY': '',
                          'QWEN_WALLET_HISTORY': '',
                          'PLATFORM_BALANCE_UNIT': constant(ROOT / 'router.py',
                                                            'PLATFORM_BALANCE_UNIT'),
                          '_wb_usable_realms': lambda *a: []})
        data = scope['estimate_snapshot'](True)
        platform = data['platforms'][0]
        # 实扣取自累计计数器增量（1000→1010），不是余额差。区间是开区间
        # (prev.ts, cur.ts]，所以 at=100.0 那条不计入，只剩 at=200.0 的 10 万 token。
        self.assertEqual(platform['rate_source'], 'measured_credits')
        self.assertAlmostEqual(platform['rate_per_mtok'], 10 / 100_000 * 1_000_000, places=4)
        models = {m['model']: m for m in platform['models']}
        flash, kimi = models['Flash'], models['Kimi']
        self.assertEqual(flash['rate_source'], 'anchored')
        self.assertEqual(kimi['rate_source'], 'anchored')
        # 官方倍率 1.83 / 0.06 ≈ 30.5，模型间相对关系必须保留
        self.assertAlmostEqual(kimi['rate_per_mtok'] / flash['rate_per_mtok'],
                               1.83 / 0.06, places=4)
        self.assertIsNotNone(flash['est_tokens'])

    def test_trae_multiplier(self):
        self.assertEqual(self.ns['_row_multiplier']('trae', {'consumption_rate': 0.78}),
                         {'factor': 0.78})
        self.assertEqual(self.ns['_row_multiplier']('trae', {'consumption_rate': 0}),
                         {'factor': 0.0})
        self.assertIsNone(self.ns['_row_multiplier']('trae', {}))
        self.assertIsNone(self.ns['_row_multiplier']('trae', {'consumption_rate': 'bad'}))

    def test_trae_account_normalization(self):
        norm = self.ns['normalize_trae']
        cn = norm({'uid': '…1234', 'uidHash': 'hash', 'realm': 'cn',
                   'realmName': '国内版', 'enabled': True, 'status': 'ready',
                   'inflight': 3,
                   'credits': {'remain': 120.5, 'limit': 200, 'plan': 'Pro',
                               'checked_in': False},
                   'account': {'username': 'u***@example.com'}})
        self.assertEqual(cn['name'], 'u***@example.com')
        self.assertEqual(cn['uid'], 'hash')
        self.assertEqual((cn['remain'], cn['size'], cn['realm']),
                         (120.5, 200, 'cn'))
        self.assertEqual(cn['inflight'], 3)
        self.assertTrue(cn['can_checkin'])
        self.assertFalse(cn['checked_in'])
        checked = norm({'realm': 'cn', 'enabled': True, 'status': 'ready',
                        'credits': {'remain': 0, 'limit': 0, 'checked_in': True}})
        self.assertTrue(checked['checked_in'])
        self.assertFalse(checked['can_checkin'])
        # 国际版没有签到接口：checked_in 必须是 None，才不会进「今日签到」分母
        intl = norm({'realm': 'intl', 'enabled': True, 'status': 'cooldown',
                     'credits': {'remain': 10, 'limit': 10, 'checked_in': False}})
        self.assertIsNone(intl['checked_in'])
        self.assertFalse(intl['can_checkin'])
        disabled = norm({'realm': 'cn', 'enabled': False, 'status': 'disabled',
                         'credits': {}})
        self.assertFalse(disabled['enabled'])
        self.assertIsNone(disabled['remain'])

    def test_trae_wiring(self):
        paths = constant(ROOT / 'router.py', 'CHECKIN_PATHS')
        self.assertEqual(paths['trae'], ('/accounts/checkin', 'panel'))
        handler = self.ns['Handler']
        self.assertEqual(handler.LOGIN_START['trae'], {})
        self.assertIn('trae', handler.ENGINE_WHITELIST)
        self.assertIn('accounts/refresh', handler.ENGINE_WHITELIST['trae'])
        self.assertIn('tasks/travel', handler.ENGINE_WHITELIST['trae'])

    def test_raccoon_billing_rows_are_read_only_and_account_scoped(self):
        calls = []
        now = time.time()

        def fake(up, method, path, body=None, auth=None, timeout=None):
            calls.append((method, path, auth))
            if path.endswith("/overview"):
                return {"accounts": [{"uid": "acct-a"}, {"uid": "acct-b"}]}
            if "acct-a" in path:
                return {"bills": [{"bill_id": "bill-a", "biz_id": "biz-a",
                                   "event_name": "任务消耗 · gw-marker",
                                   "points": -2, "created_at": "2026-11-19T01:00:00Z"},
                                  {"bill_id": "grant-a", "points": 10}]}
            return {"bills": [{"bill_id": "bill-b", "event_name": "对话消耗",
                                "points": -3, "created_at": "2026-11-19T01:01:00Z"}]}

        scope = isolated(ROOT / "router.py", {"_raccoon_billing_rows"}, {
            "engine_request": fake, "_raccoon_billing_lock": threading.Lock(),
            "_raccoon_billing_cache": {"ts": 0.0, "rows": []},
            "RACCOON_BILLING_TTL": 300,
            "RACCOON_BILLING_LINKS": str(ROOT / "raccoon_billing_links-test.jsonl"),
            "quote": quote, "time": time,
            "_known_num": self.ns["_known_num"], "_iso_epoch": self.ns["_iso_epoch"]})
        rows = scope["_raccoon_billing_rows"]({"name": "raccoon"})
        self.assertEqual([(r["account"], r["bill_id"], r["points"]) for r in rows],
                         [("acct-b", "bill-b", -3), ("acct-a", "bill-a", -2)])
        self.assertTrue(all(method == "GET" and auth == "bearer" for method, _, auth in calls))
        self.assertEqual(sum(path.endswith("/overview") for _, path, _ in calls), 1)
        self.assertEqual(len(calls), 3)

    def test_raccoon_billing_matches_only_unique_id_marker_account_and_time(self):
        scope = isolated(ROOT / "router.py", {"_attach_raccoon_bills", "_estimate_at"}, {
            "_num": self.ns["_num"], "_known_num": self.ns["_known_num"]})
        marker = "gw-0123456789abcdef0123456789abcdef"
        event = {"id": "req-1", "uid": "acct-a", "model": "sn-qwen4-max",
                 "created_at": 2000.0, "total_tokens": 250, "credit": None}
        bill = {"account": "acct-a", "bill_id": "bill-1", "event_name": f"任务消耗 · {marker}",
                "points": -2, "created_at": 2001.0}
        link = {"request_id": "req-1", "marker": marker,
                "model": "sn-qwen4-max", "at": 2000.0}
        attached = scope["_attach_raccoon_bills"]([dict(event)], [bill], [link])[0]
        self.assertEqual(attached["credit"], 2)
        self.assertEqual(attached["credit_source"], "matched_bill")
        for bad_event, bad_bills, bad_links in (
                ([dict(event)], [bill, {**bill, "bill_id": "bill-2"}], [link]),
                ([dict(event)], [bill], [link, {**link, "request_id": "req-2"}]),
                ([{**event, "uid": "other"}], [bill], [link]),
                ([{**event, "uid": ""}], [bill], [link]),
                ([{**event, "created_at": 5000.0}], [bill], [link]),
                ([{**event, "id": "legacy"}], [bill], [link]),
                ([{**event, "total_tokens": 0}], [bill], [link])):
            self.assertIsNone(scope["_attach_raccoon_bills"](
                bad_event, bad_bills, bad_links)[0].get("credit"))

    def test_raccoon_request_link_uses_random_marker_without_prompt(self):
        scope = isolated(ROOT / "router.py", {"_raccoon_bill_link_for_request"}, {
            "secrets": secrets, "time": time})
        first = scope["_raccoon_bill_link_for_request"]("sn-qwen4-max", 1)
        second = scope["_raccoon_bill_link_for_request"]("sn-qwen4-max", 1)
        self.assertNotEqual(first["marker"], second["marker"])
        self.assertEqual(len(first["request_id"]), 32)
        self.assertTrue(first["marker"].startswith("gw-"))
        self.assertNotIn("prompt", first)

    def test_raccoon_and_wb_use_identical_measured_estimates(self):
        now=time.time()
        rows=[
            {'created_at':now-30,'model':'ds','total_tokens':100_000,
             'credit':1,'credit_source':'matched_bill'},
            {'created_at':now-20,'model':'ds','total_tokens':200_000,
             'credit':3,'credit_source':'matched_bill'},
            # A large request stays in the same token-weighted sample set.
            {'created_at':now-10,'model':'ds','total_tokens':700_000,
             'credit':6,'credit_source':'matched_bill'},
            {'created_at':now-5,'model':'qwen','total_tokens':1_000_000,'credit':None},
        ]
        estimates=[]
        for platform in ('workbuddy','raccoon'):
            catalog=[{'platform':platform,'public':model,'upstream':model,'row':{}}
                     for model in ('ds','qwen')]
            item=self.estimate([{'enabled':True,'remain':100,'realm':'cn'}],rows,
                               platform=platform,catalog=catalog)
            models={m['model']:m for m in item['models']}
            ds=models['ds']
            # Ten credits buy one million sampled tokens: 100 credits buy ten million.
            self.assertEqual(ds['rate_per_mtok'],10.0)
            self.assertEqual(ds['est_tokens'],10_000_000)
            self.assertEqual(ds['samples'],3)
            self.assertEqual(ds['sample_tokens'],1_000_000)
            self.assertEqual(ds['rate_source'],'measured')
            self.assertIsNone(models['qwen']['rate_per_mtok'])
            self.assertIsNone(models['qwen']['est_tokens'])
            estimates.append(tuple(ds[k] for k in (
                'rate_per_mtok','est_tokens','samples','sample_tokens','rate_source')))
        self.assertEqual(estimates[0],estimates[1])

    def test_raccoon_measured_rate_uses_only_unique_matched_bills(self):
        """小浣熊按 WB 的 Σ实扣/Σtoken 汇总，只计入明确关联到账单的请求。"""
        now=time.time()
        rows=[
            {'created_at':now-30,'model':'sn-deepseek-v4-1-flash-1017',
             'total_tokens':1000,'credit':2,'credit_source':'matched_bill'},
            {'created_at':now-20,'model':'sn-deepseek-v4-1-flash-1017',
             'total_tokens':1000,'credit':1,'credit_source':'matched_bill'},
            {'created_at':now-10,'model':'sn-deepseek-v4-1-flash-1017',
             'total_tokens':1000,'credit':1,'credit_source':'matched_bill'},
            # Numeric credit without the strict request↔bill marker link is untrusted.
            {'created_at':now-5,'model':'sn-deepseek-v4-1-flash-1017',
             'total_tokens':10_000,'credit':100,'credit_source':None},
        ]
        catalog=[{'platform':'raccoon','public':'sn-deepseek-v4-1-flash-1017',
                  'upstream':'sn-deepseek-v4-1-flash-1017','row':{}}]
        platform=self.estimate([{'enabled':True,'remain':18_420}],rows,
                               platform='raccoon',catalog=catalog)
        model=platform['models'][0]
        expected=4/3000*1_000_000
        self.assertEqual(platform['rate_per_mtok'],round(expected,4))
        self.assertEqual(platform['samples'],3)
        self.assertEqual(platform['sample_tokens'],3000)
        self.assertEqual(model['rate_per_mtok'],round(expected,4))
        self.assertEqual(model['samples'],3)
        self.assertEqual(model['sample_tokens'],3000)
        self.assertEqual(model['rate_source'],'measured')

    def test_raccoon_response_id_requires_success(self):
        scope = isolated(ROOT / "router.py", {"_raccoon_response_id"}, {"json": json})
        self.assertEqual(scope["_raccoon_response_id"](500, {}, b'{"id":"req"}'), "")
        self.assertEqual(scope["_raccoon_response_id"](
            200, {"x-request-id": "req-header"}, b'{"id":"req-body"}'), "req-header")
        self.assertEqual(scope["_raccoon_response_id"](200, {}, b'{"id":"req-body"}'), "req-body")

    def test_trae_usage_missing_credit_is_unknown_not_free(self):
        norm = self.ns["Handler"]._norm_usage_row
        missing = norm(None, "trae", {"usage_missing": True, "credit": 0,
                                         "credit_reported": False, "total_tokens": 1000, "model": "m"})
        old = norm(None, "trae", {"usage_missing": False, "credit": 0,
                                     "total_tokens": 1000, "model": "m"})
        known_zero = norm(None, "trae", {"usage_missing": False, "credit": 0,
                                           "credit_reported": True, "total_tokens": 1000, "model": "m"})
        priced = norm(None, "trae", {"usage_missing": False, "credit": 2,
                                      "credit_reported": True, "total_tokens": 1000, "model": "m"})
        self.assertIsNone(missing["credit"])
        self.assertIsNone(old["credit"])
        self.assertEqual(known_zero["credit"], 0)
        self.assertEqual(priced["credit"], 2)
        rate, samples, tokens = self.ns["_credit_rate"]([missing, old, known_zero, priced])
        self.assertIsNone(rate)
        self.assertEqual((samples, tokens), (2, 2000))

    def test_estimate_filters_trae_usage_missing_rows(self):
        self.ns.update(_estimate_lock=threading.Lock(), _estimate_cache={"data": None}, ESTIMATE_TTL=120,
            CONFIG={"upstreams": [{"name": "trae"}]},
            public_catalog=lambda: [{"platform":"trae","public":"m","upstream":"m","row":{}}],
            platform_status=lambda _: {"health":True,"accounts":[{"enabled":True,"remain":100}]},
            _estimate_rows=lambda _: [
                {"created_at":time.time(),"model":"m","total_tokens":1000,"credit":0,
                 "usage_missing":True,"credit_reported":False},
                {"created_at":time.time(),"model":"m","total_tokens":1000,"credit":2,
                 "usage_missing":False,"credit_reported":False},
                *[{"created_at":time.time(),"model":"m","total_tokens":1000,"credit":1,
                   "usage_missing":False,"credit_reported":True} for _ in range(3)]],
            _estimate_at=lambda r:r["created_at"], _model_hidden=lambda *_:False,
            _read_wallet_history=lambda *a, **k:[])
        model=self.ns["estimate_snapshot"](True)["platforms"][0]["models"][0]
        self.assertEqual(model["rate_per_mtok"],1000.0)
        self.assertEqual(model["samples"],3)
        self.assertEqual(model["sample_tokens"],3000)
        self.assertEqual(model["rate_source"],"measured")

    def test_raccoon_estimate_rows_join_only_unique_new_bill_link(self):
        calls=[]
        def fake(up, method, path, body=None, auth=None, timeout=None):
            calls.append(path)
            if path.endswith("/requests?limit=1000"):
                return {"events":[
                    {"id":"req-1","uid":"acct-a","status":200,"time":"2026-11-19T01:00:00Z",
                     "model":"sn-qwen4-max","total_tokens":1000},
                    {"uid":"legacy","status":200,"time":"2026-11-19T01:01:00Z",
                     "model":"sn-qwen4-max","total_tokens":1000}]}
            if path.endswith("/overview"):
                return {"accounts":[{"uid":"acct-a"}]}
            if path.endswith("/bills?limit=200"):
                return {"bills":[{"bill_id":"b1","points":-2,
                                   "event_name":"任务消耗 · gw-0123456789abcdef0123456789abcdef",
                                   "created_at":"2026-11-19T01:00:01Z"}]}
            raise AssertionError(path)
        scope = isolated(ROOT / "router.py", {"_estimate_rows", "_raccoon_billing_rows", "_read_raccoon_billing_links", "_attach_raccoon_bills", "_iso_epoch", "_estimate_at"}, {
            "datetime": datetime, "engine_request": fake, "time":time, "json":json,
            "RACCOON_BILLING_LINKS": str(ROOT / "raccoon_billing_links-test.jsonl"),
            "quote": quote, "_num": self.ns["_num"], "_known_num": self.ns["_known_num"],
            "_estimate_at": self.ns["_estimate_at"], "RACCOON_BILLING_TTL":300,
            "_raccoon_billing_lock":threading.Lock(), "_raccoon_billing_cache":{"ts":0,"rows":[]},
            "_read_raccoon_billing_links":lambda:[]})
        scope["_read_raccoon_billing_links"] = lambda: [{
            "request_id": "req-1", "marker": "gw-0123456789abcdef0123456789abcdef",
            "model": "sn-qwen4-max", "at": self.ns["_iso_epoch"]("2026-11-19T01:00:00Z")
        }]
        rows=scope["_estimate_rows"]({"name":"raccoon"})
        self.assertEqual(rows[0]["credit"],2)
        self.assertEqual(rows[0]["credit_source"],"matched_bill")
        self.assertIsNone(rows[1]["credit"])
        self.assertTrue(all(path.endswith("/requests?limit=1000") or path.endswith("/overview")
                            or path.endswith("/bills?limit=200") for path in calls))


    def test_raccoon_estimate_rows_keep_credit_unknown(self):
        scope = isolated(ROOT / 'router.py', {'_estimate_rows', '_iso_epoch'},
            {'datetime': datetime, '_raccoon_billing_rows': lambda *_: [],
             '_attach_raccoon_bills': self.ns['_attach_raccoon_bills'],
             '_read_raccoon_billing_links': lambda: [],
             '_estimate_at': lambda row: self.ns['_iso_epoch'](row.get('time')),
             '_num': self.ns['_num'], '_known_num': self.ns['_known_num'],
             'engine_request': lambda up, method, path, body=None, auth=None, timeout=None: {
                 'events': [
                     {'time': '2026-11-18T22:00:00+08:00', 'status': 200,
                      'model': 'raccoon-chat-ml-5-5', 'total_tokens': 100,
                      'prompt_tokens': 80, 'completion_tokens': 20},
                     {'time': '2026-11-18T22:01:00+08:00', 'status': 502,
                      'model': 'raccoon-chat-ml-5-5', 'total_tokens': 10},
                 ]}})
        rows = scope['_estimate_rows']({'name': 'raccoon'})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['model'], 'raccoon-chat-ml-5-5')
        self.assertEqual(rows[0]['total_tokens'], 100)
        self.assertIsNone(rows[0]['credit'])
        self.assertIsNone(rows[0]['cached_tokens'])
        self.assertGreater(rows[0]['created_at'], 0)

    def test_raccoon_platform_status_reads_overview(self):
        import urllib.request
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'{"status":"ok"}'
        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_raccoon', '_shanghai_today',
                          '_known_num', '_raccoon_points_audit'},
                         {'urllib': __import__('urllib.request'),
                          'CONFIG': {'upstreams': [{'name': 'raccoon',
                                                    'base_url': 'http://127.0.0.1:7867',
                                                    'api_key': 'synthetic'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': lambda req, timeout: Resp(),
                          '_raccoon_points_lock': threading.Lock(),
                          '_raccoon_points_cache': {'ts': 0.0, 'by_uid': {}, 'fail_ts': 0.0},
                          'RACCOON_POINTS_TTL': 300.0,
                          'RACCOON_POINTS_FAIL_TTL': 60.0,
                          'engine_request': lambda up, method, path, body=None,
                                                    auth=None, timeout=None: {
                              'accounts': [{'uid': 'u1', 'nickname': 'nick',
                                            'credits': 5, 'realm': 'cn',
                                            'checked_in': self.ns['_shanghai_today']()}]}})
        scope['NORMALIZERS']['raccoon'] = scope['normalize_raccoon']
        out = scope['platform_status']({'name': 'raccoon',
                                        'base_url': 'http://127.0.0.1:7867'})
        self.assertTrue(out['health'])
        self.assertEqual(len(out['accounts']), 1)
        self.assertEqual(out['accounts'][0]['remain'], 5)
        self.assertTrue(out['accounts'][0]['checked_in'])

    def test_expiry_helpers_normalize_units_and_keep_burn_separate(self):
        scope = isolated(ROOT / "router.py",
                         {"_epoch_seconds", "_expiry_row", "_trae_pack_name", "_known_num"})
        epoch = scope["_epoch_seconds"]
        self.assertEqual(epoch(1795622399), 1795622399.0)
        # 毫秒（小浣熊 overview 的凭证 expires_at）归一成秒
        self.assertEqual(epoch(1797606605000), 1797606605.0)
        for bad in (None, "", 0, -5, True, "abc", 5e12, float("nan")):
            self.assertIsNone(epoch(bad), bad)

        now = time.time()
        row = scope["_expiry_row"]("nick", "包", 100.004, now + 6.4 * 86400, 374.7)
        self.assertEqual(row["remain"], 100.0)
        self.assertEqual(row["days_left"], 6.4)
        # 上游的 days_left（按燃烧速度）必须单独放，不能顶替距过期天数
        self.assertEqual(row["burn_days"], 374.7)
        self.assertNotEqual(row["days_left"], row["burn_days"])
        self.assertRegex(row["expire"], r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$")
        self.assertIsNone(scope["_expiry_row"]("a", "b", 1, now)["burn_days"])

        name = scope["_trae_pack_name"]
        self.assertEqual(name({"product_extra": {"package_extra":
                                                 {"package_name": "福利积分"}}}), "福利积分")
        self.assertEqual(name({"product_extra": {"subscription_extra":
                                                 {"package_name": "订阅"}}}), "订阅")
        self.assertEqual(name({}), "")

    def test_raccoon_expiring_uses_point_expiry_not_burn_days(self):
        import urllib.request
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'{"status":"ok"}'
        expire = int(time.time() + 6.4 * 86400)

        def fake(up, method, path, body=None, auth=None, timeout=None):
            if path.endswith("/panel/api/points"):
                # remain 是总余额（含不会过期的奖励分），expire_points 才是
                # 最早那批每日积分的额度。临期行必须用后者。
                return {"accounts": [{"uid": "u1", "audit": {
                    "remain": 9421, "expire": expire, "expire_points": 300,
                    "days_left": 374.7}}]}
            return {"accounts": [{"uid": "u1", "nickname": "nick", "credits": 9421,
                                  "realm": "cn", "expires_at": 1797606605000}]}

        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_raccoon', '_shanghai_today',
                          '_known_num', '_epoch_seconds', '_expiry_row',
                          '_raccoon_points_audit'},
                         {'urllib': __import__('urllib.request'),
                          'CONFIG': {'upstreams': [{'name': 'raccoon',
                                                    'base_url': 'http://127.0.0.1:7867'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': lambda req, timeout: Resp(),
                          '_raccoon_points_lock': threading.Lock(),
                          '_raccoon_points_cache': {'ts': 0.0, 'by_uid': {}, 'fail_ts': 0.0},
                          'RACCOON_POINTS_TTL': 300.0,
                          'RACCOON_POINTS_FAIL_TTL': 60.0,
                          'engine_request': fake})
        scope['NORMALIZERS']['raccoon'] = scope['normalize_raccoon']
        out = scope['platform_status']({'name': 'raccoon',
                                        'base_url': 'http://127.0.0.1:7867'})
        self.assertEqual(len(out['expiring']), 1)
        row = out['expiring'][0]
        self.assertEqual(row['account'], 'nick')
        self.assertEqual(row['remain'], 300.0)    # 最早到期批次，不是总余额 9421
        self.assertEqual(row['days_left'], 6.4)   # 积分到期，不是 374 天
        self.assertEqual(row['burn_days'], 374.7)
        self.assertIn('每日积分', row['name'])

    def test_raccoon_expiring_skips_when_engine_lacks_expire_points(self):
        """老引擎的 audit 没有 expire_points：宁可不显示临期行，也不拿总余额冒充。"""
        import urllib.request
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'{"status":"ok"}'
        expire = int(time.time() + 2.4 * 86400)

        def fake(up, method, path, body=None, auth=None, timeout=None):
            if path.endswith("/panel/api/points"):
                return {"accounts": [{"uid": "u1", "audit": {
                    "remain": 5972, "expire": expire, "days_left": 140.8}}]}
            return {"accounts": [{"uid": "u1", "nickname": "nick", "credits": 5972}]}

        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_raccoon', '_shanghai_today',
                          '_known_num', '_epoch_seconds', '_expiry_row',
                          '_raccoon_points_audit'},
                         {'urllib': __import__('urllib.request'),
                          'CONFIG': {'upstreams': [{'name': 'raccoon',
                                                    'base_url': 'http://127.0.0.1:7867'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': lambda req, timeout: Resp(),
                          '_raccoon_points_lock': threading.Lock(),
                          '_raccoon_points_cache': {'ts': 0.0, 'by_uid': {}, 'fail_ts': 0.0},
                          'RACCOON_POINTS_TTL': 300.0,
                          'RACCOON_POINTS_FAIL_TTL': 60.0,
                          'engine_request': fake})
        scope['NORMALIZERS']['raccoon'] = scope['normalize_raccoon']
        out = scope['platform_status']({'name': 'raccoon',
                                        'base_url': 'http://127.0.0.1:7867'})
        self.assertEqual(out['expiring'], [])      # 不显示，而不是显示 5972
        self.assertEqual(out['accounts'][0]['remain'], 5972)  # 余额照常展示

    def test_raccoon_expiring_survives_points_endpoint_failure(self):
        import urllib.request
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'{"status":"ok"}'

        def fake(up, method, path, body=None, auth=None, timeout=None):
            if path.endswith("/panel/api/points"):
                raise OSError("points endpoint down")
            return {"accounts": [{"uid": "u1", "nickname": "nick", "credits": 5}]}

        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_raccoon', '_shanghai_today',
                          '_known_num', '_epoch_seconds', '_expiry_row',
                          '_raccoon_points_audit'},
                         {'urllib': __import__('urllib.request'),
                          'CONFIG': {'upstreams': [{'name': 'raccoon',
                                                    'base_url': 'http://127.0.0.1:7867'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': lambda req, timeout: Resp(),
                          '_raccoon_points_lock': threading.Lock(),
                          '_raccoon_points_cache': {'ts': 0.0, 'by_uid': {}, 'fail_ts': 0.0},
                          'RACCOON_POINTS_TTL': 300.0,
                          'RACCOON_POINTS_FAIL_TTL': 60.0,
                          'engine_request': fake})
        scope['NORMALIZERS']['raccoon'] = scope['normalize_raccoon']
        out = scope['platform_status']({'name': 'raccoon',
                                        'base_url': 'http://127.0.0.1:7867'})
        self.assertEqual(len(out['accounts']), 1)   # 账号聚合不受影响
        self.assertEqual(out['accounts'][0]['remain'], 5)
        self.assertEqual(out['expiring'], [])       # 只是缺临期行

    def test_codearts_normalize_keeps_balance_unknown(self):
        """没有浏览器同步快照时，remain 必须留 None 而不是 0。

        引擎的 credits=0 是占位；真实余额只能来自浏览器同步的快照文件
        （上游 WAF 拦截非浏览器客户端）。快照缺失 ≠ 余额为零。
        """
        scope = isolated(ROOT / 'router.py',
                         {'normalize_codearts', '_shanghai_today', '_iso_epoch'},
                         {'datetime': datetime, 'time': time, 'os': __import__('os'),
                          'json': json,
                          '_codearts_credits_snapshot': lambda: None})
        norm, today = scope['normalize_codearts'], scope['_shanghai_today']()
        row = norm({"uid": "8063", "name": "acct", "nickname": "用户abc",
                    "disabled": False, "cooling": False, "cool_until": "",
                    "cool_kind": "", "credits": 0, "token_remaining": "23h0m0s",
                    "checkin": {"ok": True, "checked_at": today + "T09:00:00+08:00"},
                    "active_concurrent": 1, "max_concurrent": 3})
        self.assertIsNone(row['remain'])          # 不能显示成「已用完」
        self.assertIsNone(row['size'])
        self.assertTrue(row['enabled'])
        self.assertTrue(row['checked_in'])
        self.assertEqual(row['inflight'], 1)
        self.assertEqual(row['concurrency_limit'], 3)
        self.assertIn("23h0m0s", row['sub'])
        # 冷却中的账号要把 RFC3339 冷却截止转成 epoch 秒
        cooled = norm({"uid": "u2", "nickname": "n2", "disabled": False,
                       "cooling": True, "cool_until": "2026-11-27T00:00:00+08:00",
                       "cool_kind": "soft", "token_remaining": "1h"})
        self.assertGreater(cooled['cooldown_until'], 0)
        self.assertEqual(cooled['cooldown_kind'], 'soft')
        # 禁用账号即使有 checkin 记录也不参与调度
        self.assertFalse(norm({"uid": "u3", "disabled": True})['enabled'])

    def test_codearts_normalize_uses_browser_credits_snapshot(self):
        """有快照时用真实余额（16455.19），并带上同步时间与到期日。"""
        now = time.time()
        snap = {"remain": 16455.19, "size": 16600.0, "used": 144.81,
                "expiring": 0.0, "end_date": "2026-12-27",
                "synced_at": now - 60, "stale": False, "basic": {}, "bonus": {}}
        scope = isolated(ROOT / 'router.py',
                         {'normalize_codearts', '_shanghai_today', '_iso_epoch'},
                         {'datetime': datetime, 'time': time, 'os': __import__('os'),
                          'json': json,
                          '_codearts_credits_snapshot': lambda: snap})
        row = scope['normalize_codearts']({"uid": "8063", "nickname": "acct",
                                           "credits": 0, "token_remaining": "23h0m0s"})
        self.assertEqual(row['remain'], 16455.19)   # 不是引擎的占位 0
        self.assertEqual(row['size'], 16600.0)
        self.assertAlmostEqual(float(row['pct'].rstrip('%')), 99.1, places=1)
        self.assertEqual(row['credits_end_date'], '2026-12-27')
        self.assertFalse(row['credits_stale'])
        self.assertEqual(row['credits_expiring'], 0.0)

    def test_codearts_credits_snapshot_marks_stale_and_rejects_garbage(self):
        """快照超过 24h 标 stale；文件缺失/损坏/无 remain 一律返回 None。"""
        import tempfile
        now = time.time()
        cases = [
            ({"total": {"remain": 100, "amount": 200}, "synced_at": "2026-01-01T00:00:00Z"},
             True, 100),          # 旧快照 → stale
            ({"total": {"remain": 100, "amount": 200},
              "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))},
             False, 100),         # 新快照
            ({"total": {}}, None, None),          # 没有 remain
            ({"synced_at": "x"}, None, None),     # 没有 total
        ]
        scope = isolated(ROOT / 'router.py', {'_codearts_credits_snapshot'},
                         {'datetime': datetime, 'time': time, 'os': __import__('os'),
                          'json': json,
                          '_known_num': lambda v: (float(v) if isinstance(v, (int, float))
                                                   and not isinstance(v, bool) else None),
                          'CODEARTS_CREDITS_FILE': '<tmp>',
                          'CODEARTS_CREDITS_STALE': 24 * 3600})
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False,
                                         encoding='utf-8') as handle:
            path = handle.name
        try:
            scope['CODEARTS_CREDITS_FILE'] = path
            for payload, want_stale, want_remain in cases:
                with open(path, 'w', encoding='utf-8') as handle:
                    json.dump(payload, handle)
                got = scope['_codearts_credits_snapshot']()
                if want_remain is None:
                    self.assertIsNone(got, payload)
                    continue
                self.assertIsNotNone(got, payload)
                self.assertEqual(got['remain'], want_remain, payload)
                self.assertEqual(got['stale'], want_stale, payload)
            # 文件不存在
            os.unlink(path)
            self.assertIsNone(scope['_codearts_credits_snapshot']())
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_codearts_status_uses_healthz_and_admin_overview(self):
        import urllib.request
        seen = {}

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'ok'   # codearts2api /healthz 是纯文本

        def fake_urlopen(req, timeout):
            seen['health_url'] = req.full_url
            return Resp()

        def fake_engine(up, method, path, body=None, auth=None, timeout=None):
            seen['engine'] = (method, path, auth)
            return {"accounts": [{"uid": "u1", "name": "acct", "nickname": "acct",
                                  "disabled": False, "cooling": False,
                                  "token_remaining": "5h0m0s", "credits": 0}]}

        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_codearts', '_shanghai_today',
                          '_known_num', '_num', '_epoch_seconds', '_expiry_row',
                          '_iso_epoch', '_trae_pack_name'},
                         {'urllib': __import__('urllib.request'),
                          'datetime': datetime, 'time': time, 'os': __import__('os'),
                          'json': json,
                          # 本用例只验证端点选择与归一，不涉及余额快照
                          '_codearts_credits_snapshot': lambda: None,
                          'CONFIG': {'upstreams': [{'name': 'codearts',
                                                    'base_url': 'http://127.0.0.1:7866'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': fake_urlopen,
                          'engine_request': fake_engine})
        scope['NORMALIZERS']['codearts'] = scope['normalize_codearts']
        out = scope['platform_status']({'name': 'codearts',
                                        'base_url': 'http://127.0.0.1:7866'})
        self.assertTrue(out['health'])
        self.assertTrue(seen['health_url'].endswith('/healthz'))   # 不是 /health
        self.assertEqual(seen['engine'], ('GET', '/admin/api/overview', 'bearer'))
        self.assertEqual(len(out['accounts']), 1)
        self.assertIsNone(out['accounts'][0]['remain'])
        self.assertEqual(out['expiring'], [])

    def test_codearts_upstream_health_probes_healthz(self):
        import urllib.request
        seen = {}

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'ok'

        def fake_urlopen(req, timeout):
            seen['url'] = req.full_url
            return Resp()

        scope = isolated(ROOT / 'router.py', {'_upstream_health'},
                         {'urllib': __import__('urllib.request'),
                          '_urlopen': fake_urlopen})
        self.assertTrue(scope['_upstream_health'](
            {'name': 'codearts', 'base_url': 'http://127.0.0.1:7866'}))
        self.assertTrue(seen['url'].endswith('/healthz'))

    def test_checkin_timeout_covers_long_claimers(self):
        scope = isolated(ROOT / 'router.py', {'_checkin_timeout'},
                         {'CHECKIN_TIMEOUTS': constant(ROOT / 'router.py', 'CHECKIN_TIMEOUTS')})
        timeout = scope['_checkin_timeout']
        self.assertEqual(timeout('raccoon'), 600)
        self.assertEqual(timeout('codearts'), 600)   # 多活动领取，15s 不够
        self.assertEqual(timeout('trae'), 15)

    def test_automation_daily_cfg_parses_and_falls_back(self):
        scope = isolated(ROOT / 'router.py', {'_automation_daily_cfg'},
                         {'CONFIG': {'automation_daily': {'enabled': True, 'time': '09:30'}}})
        cfg = scope['_automation_daily_cfg']()
        self.assertTrue(cfg['enabled'])
        self.assertEqual(cfg['time'], '09:30')
        self.assertEqual((cfg['hour'], cfg['minute']), (9, 30))
        bad = isolated(ROOT / 'router.py', {'_automation_daily_cfg'},
                       {'CONFIG': {'automation_daily': {'enabled': True, 'time': '25:99'}}})
        self.assertEqual(bad['_automation_daily_cfg']()['time'], '09:00')   # 笔误不影响定时器
        missing = isolated(ROOT / 'router.py', {'_automation_daily_cfg'}, {'CONFIG': {}})
        self.assertFalse(missing['_automation_daily_cfg']()['enabled'])

    def test_automation_daily_due_respects_time_and_last_run(self):
        scope = isolated(ROOT / 'router.py', {'_automation_daily_due'},
                         {'datetime': datetime, 'timedelta': timedelta, 'timezone': timezone})
        due = scope['_automation_daily_due']
        cfg = {'enabled': True, 'hour': 9, 'minute': 0}
        tz = timezone(timedelta(hours=8))
        self.assertFalse(due(dict(cfg, enabled=False), '',
                             datetime(2026, 11, 26, 10, 0, tzinfo=tz)))
        self.assertFalse(due(cfg, '', datetime(2026, 11, 26, 8, 59, tzinfo=tz)))
        self.assertTrue(due(cfg, '', datetime(2026, 11, 26, 9, 0, tzinfo=tz)))
        self.assertTrue(due(cfg, '2026-11-25', datetime(2026, 11, 26, 9, 0, tzinfo=tz)))
        self.assertFalse(due(cfg, '2026-11-26', datetime(2026, 11, 26, 18, 0, tzinfo=tz)))

    def test_automation_daily_steps_only_checkin_and_supported(self):
        plan = {'steps': [
            {'step_id': 'a.checkin', 'kind': 'checkin', 'supported': True},
            {'step_id': 'b.checkin', 'kind': 'checkin', 'supported': False},
            {'step_id': 'a.task', 'kind': 'task', 'supported': True},
        ]}
        scope = isolated(ROOT / 'router.py', {'_automation_daily_steps'})
        steps = scope['_automation_daily_steps']
        self.assertEqual([s['step_id'] for s in steps(plan, [])], ['a.checkin'])
        # 显式列表也过滤掉未启用平台的步骤
        self.assertEqual([s['step_id'] for s in steps(plan, ['a.task', 'b.checkin'])], ['a.task'])

    def test_codearts_checkin_and_automation_specs_registered(self):
        paths = constant(ROOT / 'router.py', 'CHECKIN_PATHS')
        self.assertEqual(paths.get('codearts'), ('/admin/api/checkin', 'bearer'))
        specs = constant(ROOT / 'router.py', '_AUTOMATION_STEP_SPECS')
        ids = [s[0] for s in specs]
        self.assertIn('codearts.checkin', ids)
        step = next(s for s in specs if s[0] == 'codearts.checkin')
        self.assertEqual(step[2], 'checkin')
        self.assertEqual(step[3], 'codearts')

    def test_trae_expiring_lists_packs_with_remaining_credits(self):
        import urllib.request
        class Resp:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self): return b'{"status":"ok"}'
        now = time.time()

        def pack(desc, expire_in, limit, used, hide=False):
            return {"display_desc": desc, "expire_time": int(now + expire_in * 86400),
                    "is_hide": hide,
                    "entitlement_base_info": {"quota": {"credits_limit": limit}},
                    "usage": ({"credits_amount": used} if used is not None else {})}

        account = {"uid": "…4052", "uidHash": "hash", "realm": "cn", "enabled": True,
                   "status": "ready", "account": {"username": "u***@example.com"},
                   "credits": {"remain": 2100, "limit": 2100, "checked_in": True},
                   "entitlement": {"ent_usage": {"user_entitlement_pack_list": [
                       pack("每月登录赠送", 11.4, 500, 128.0048),
                       pack("签到奖励", 30.4, 100, None),
                       pack("已用尽", 5, 100, 100),
                       pack("隐藏包", 3, 100, 0, hide=True),
                       {"display_desc": "无到期", "entitlement_base_info":
                        {"quota": {"credits_limit": 10}}},
                   ]}}}

        scope = isolated(ROOT / 'router.py',
                         {'platform_status', 'normalize_trae', '_known_num',
                          '_epoch_seconds', '_expiry_row', '_trae_pack_name',
                          '_trae_consumed_amount'},
                         {'urllib': __import__('urllib.request'),
                          'time': time,
                          'CONFIG': {'upstreams': [{'name': 'trae',
                                                    'base_url': 'http://127.0.0.1:8791'}]},
                          '_inflight': {}, 'NORMALIZERS': {},
                          'normalize_buddy': lambda a: a,
                          '_urlopen': lambda req, timeout: Resp(),
                          '_append_spend_snapshot': lambda *a, **k: None,
                          '_trae_wallet_account': lambda raw, row: "4052",
                          'os': __import__('os'), 'HERE': str(ROOT),
                          'TRAE_SPEND_HISTORY': '',
                          'TRAE_REFRESH_INTERVAL': 600.0,
                          '_trae_refresh_at': {'ts': time.time()},
                          'engine_request': lambda up, method, path, body=None,
                                                    auth=None, timeout=None: {
                              "accounts": [account]}})
        scope['NORMALIZERS']['trae'] = scope['normalize_trae']
        out = scope['platform_status']({'name': 'trae',
                                        'base_url': 'http://127.0.0.1:8791'})
        names = [r['name'] for r in out['expiring']]
        # 隐藏包、已用尽包、无到期包都不进表
        self.assertEqual(names, ['每月登录赠送', '签到奖励'])
        self.assertEqual(out['expiring'][0]['account'], 'u***@example.com')
        self.assertEqual(out['expiring'][0]['remain'], 372.0)   # 500 - 128.0048
        self.assertEqual(out['expiring'][0]['days_left'], 11.4)
        self.assertEqual(out['expiring'][1]['remain'], 100.0)   # 未消耗 = 全额
        self.assertIsNone(out['expiring'][0]['burn_days'])

    def test_raccoon_points_audit_is_cached_and_backs_off_on_failure(self):
        calls = []
        state = {"fail": False}

        def fake(up, method, path, body=None, auth=None, timeout=None):
            calls.append(path)
            if state["fail"]:
                raise OSError("points down")
            return {"accounts": [{"uid": "u1", "audit": {"remain": 5, "expire": 1795622399}},
                                 {"uid": "u2"},  # 没有 audit 的账号不进映射
                                 "junk"]}

        scope = isolated(ROOT / 'router.py', {'_raccoon_points_audit'}, {
            'engine_request': fake, 'time': time,
            '_raccoon_points_lock': threading.Lock(),
            '_raccoon_points_cache': {'ts': 0.0, 'by_uid': {}, 'fail_ts': 0.0},
            'RACCOON_POINTS_TTL': 300.0, 'RACCOON_POINTS_FAIL_TTL': 60.0})
        audit = scope['_raccoon_points_audit']
        self.assertEqual(list(audit({'name': 'raccoon'})), ['u1'])
        audit({'name': 'raccoon'})
        # 第二次命中缓存：慢端点（400-660ms）不能每轮询都打一次
        self.assertEqual(len(calls), 1)
        # 失败后退避 60 秒：端点挂掉时每轮轮询都等一次超时的代价更高
        state["fail"] = True
        scope['_raccoon_points_cache'].update(ts=0.0, by_uid={}, fail_ts=0.0)
        self.assertEqual(audit({'name': 'raccoon'}), {})
        self.assertEqual(audit({'name': 'raccoon'}), {})
        self.assertEqual(len(calls), 2)
        # 退避窗口过后仍会重试，恢复后重新写入缓存
        scope['_raccoon_points_cache']['fail_ts'] = time.time() - 61
        state["fail"] = False
        self.assertEqual(list(audit({'name': 'raccoon'})), ['u1'])
        self.assertEqual(len(calls), 3)

    def test_platform_toggle_filters_catalog_and_routing(self):
        scope = isolated(ROOT / "router.py", {
            "public_catalog", "platform_enabled", "active_upstreams",
            "_disabled_platforms", "_public_name", "_hidden_set", "_model_hidden",
            "_wb_entry_locked", "_wb_realm_meta", "resolve_model",
            "_model_in_disabled_platform"},
            {"CONFIG": {"upstreams": [{"name": "qoder"}, {"name": "workbuddy"}],
                        "disabled_platforms": ["workbuddy"]},
             "upstream_rows": lambda up: [{"id": "m-" + up["name"]}],
             "_catalog_lock": threading.Lock(),
             "_catalog_cache": {"workbuddy": (time.time(), [{"id": "m-workbuddy"}])},
             "_catalog_pub_lock": threading.Lock(), "_catalog_pub_cache": {},
             "_catalog_pub_generation": 0,
             "_wb_paid_any": lambda: False})
        self.assertEqual([u["name"] for u in scope["active_upstreams"]()], ["qoder"])
        entries = scope["public_catalog"]()
        self.assertEqual([e["platform"] for e in entries], ["qoder"])
        err = scope["resolve_model"]("workbuddy/m-workbuddy")[3]
        self.assertIn("停用", err)
        err = scope["resolve_model"]("m-workbuddy")[3]
        self.assertIn("停用", err)

    def test_launch_spec_validation(self):
        scope = isolated(ROOT / "router.py", {"_launch_spec"},
                         {"os": __import__("os"), "HERE": str(ROOT)})
        fn = scope["_launch_spec"]
        self.assertEqual(fn({"launch": {"command": ["python", "x.py"]}})["command"],
                         ["python", "x.py"])
        shell = (["cmd.exe", "/c", "python"] if __import__("os").name == "nt"
                 else ["/bin/sh", "-c", "python"])
        self.assertEqual(fn({"launch": {"command": "python"}})["command"], shell)
        self.assertIsNone(fn({}))
        self.assertIsNone(fn({"launch": {"command": []}}))
        self.assertIsNone(fn({"launch": {"command": ["python"], "cwd": "Z:/no-such-dir"}}))
        self.assertEqual(fn({"launch": {"command": ["python"], "cwd": "."}})["cwd"],
                         str(ROOT))
        self.assertEqual(fn({"launch": {"command": ["python"], "timeout": 9999}})["timeout"], 180)
        self.assertEqual(fn({"launch": {"command": ["python"], "timeout": 1}})["timeout"], 5)
        self.assertEqual(fn({"launch": {"command": "x", "env": {"QD_NO_PAUSE": "1"}}})["env"],
                         {"QD_NO_PAUSE": "1"})
        self.assertIsNone(fn({"launch": {"command": "x", "env": {"A": 1}}}))

    def test_platform_toggle_persists_to_config(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text('{"upstreams":[],"disabled_platforms":[]}', encoding="utf-8")
            scope = isolated(ROOT / "router.py", {"_set_platform_enabled", "save_config"}, {
                "os": __import__("os"), "CONFIG_PATH": str(path),
                "CONFIG": {"upstreams": [], "disabled_platforms": []},
                "_config_lock": threading.RLock(),
                "_catalog_pub_lock": threading.Lock(), "_catalog_pub_cache": {},
                "_catalog_pub_generation": 0,
                "_estimate_lock": threading.Lock(),
                "_estimate_cache": {"ts": 0.0, "data": None},
                "_updates_lock": threading.Lock(),
                "_updates_cache": {"ts": 0.0, "data": None}})
            scope["_set_platform_enabled"]("qoder", False)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["disabled_platforms"],
                             ["qoder"])
            scope["_set_platform_enabled"]("qoder", True)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["disabled_platforms"],
                             [])

    def test_platforms_endpoint_lists_and_toggles(self):
        self.ns.update(
            CONFIG={"upstreams": [
                {"name": "qoder", "base_url": "http://127.0.0.1:8790"},
                {"name": "workbuddy", "base_url": "http://127.0.0.1:8788"}],
                "disabled_platforms": ["workbuddy"]},
            _platform_process=lambda up: {"managed": False, "running": False,
                                          "pid": None, "launch_configured": False})
        handler = object.__new__(self.ns["Handler"])
        sent = {}
        handler._send_json = lambda code, payload: sent.update(code=code, payload=payload)
        handler._deny = lambda code, message, *args: sent.update(code=code, error=message)
        handler.command = "GET"
        handler._api_platforms()
        self.assertEqual(sent["code"], 200)
        self.assertEqual([p["name"] for p in sent["payload"]["platforms"]],
                         ["qoder", "workbuddy"])
        self.assertFalse(sent["payload"]["platforms"][1]["enabled"])
        self.assertEqual(sent["payload"]["disabled"], ["workbuddy"])

        self.ns["_set_platform_enabled"] = lambda name, enabled: ["workbuddy"]
        self.ns["_stop_platform"] = lambda up, allow_external=False: {
            "ok": True, "stopped": True, "managed": False, "pid": 123}
        handler.command = "POST"
        handler._post_body = {"name": "qoder", "enabled": False}
        handler._api_platforms()
        self.assertEqual(sent["code"], 200)
        self.assertFalse(sent["payload"]["enabled"])
        self.assertTrue(sent["payload"]["process"]["stopped"])

        # 启用时自动启动引擎
        self.ns["_upstream_health"] = lambda up: False
        self.ns["_launch_spec"] = lambda up: {"command": ["python"], "cwd": None,
                                              "timeout": 30, "env": {}, "shell": False}
        self.ns["_start_platform"] = lambda up: {"ok": True, "started": True, "pid": 456}
        handler._post_body = {"name": "qoder", "enabled": True}
        handler._api_platforms()
        self.assertTrue(sent["payload"]["process"]["started"])

        # 保存启动命令（恢复真实校验）
        real_launch_spec = isolated(ROOT / "router.py", {"_launch_spec"},
                                    {"os": __import__("os"), "HERE": str(ROOT)})["_launch_spec"]
        self.ns["_launch_spec"] = real_launch_spec
        saved = {}
        self.ns["save_config"] = lambda: saved.update(done=True)
        handler._post_body = {"name": "qoder", "launch": {
            "command": "cmd /c start.bat", "cwd": str(ROOT)}}
        handler._api_platforms()
        self.assertTrue(sent["payload"]["launch_configured"])
        self.assertEqual(self.ns["CONFIG"]["upstreams"][0]["launch"]["command"],
                         "cmd /c start.bat")
        self.assertTrue(saved.get("done"))

    def test_stop_managed_keeps_record_when_process_survives(self):
        class Proc:
            pid = 778
            def poll(self): return None
            def wait(self, timeout=None): return None
        proc = Proc()
        managed = {"p": proc}
        scope = isolated(ROOT / "router.py", {"_stop_platform", "_stop_platform_locked", "_command_matches_engine"}, {
            "_proc_lock": threading.Lock(), "_managed_procs": managed,
            "_platform_op_lock": lambda name: threading.Lock(),
            "_kill_tree": lambda pid: None,
            "subprocess": __import__("subprocess"),
            "log": lambda *_: None})
        out = scope["_stop_platform"]({"name": "p"}, allow_external=False)
        self.assertFalse(out["ok"])
        self.assertFalse(out["stopped"])
        self.assertIs(managed["p"], proc)

    def test_stop_external_verifies_process_identity(self):
        killed = []
        pids = [4321, 4321, None]
        scope = isolated(ROOT / "router.py", {"_stop_platform", "_stop_platform_locked", "_command_matches_engine"}, {
            "_proc_lock": threading.Lock(), "_managed_procs": {},
            "urlsplit": __import__("urllib.parse", fromlist=["urlsplit"]).urlsplit,
            "os": __import__("os"), "time": time, "log": lambda *_: None,
            "_port_listener_pid": lambda port, host=None: pids.pop(0) if pids else None,
            "_platform_op_lock": lambda name: threading.Lock(),
            "_process_command_line": lambda pid: "python qoder_proxy.py --port 8790",
            "_engine_identity_tokens": lambda up: {"qoder_proxy.py"},
            "subprocess": __import__("subprocess"),
            "os_name": "nt",
            "_kill_tree": lambda pid: killed.append(pid),
            "_platform_op_lock": lambda name: threading.Lock()})
        fn = scope["_stop_platform"]
        out = fn({"name": "qoder", "base_url": "http://127.0.0.1:8790"},
                 allow_external=True)
        self.assertTrue(out["ok"])
        self.assertEqual(killed, [4321])
        killed.clear()
        pids[:] = [4321, 4321]
        scope["_process_command_line"] = lambda pid: "python other.py"
        out = fn({"name": "qoder", "base_url": "http://127.0.0.1:8790"},
                 allow_external=True)
        self.assertFalse(out["ok"])
        self.assertEqual(killed, [])

    def test_missing_result_ok_is_not_success(self):
        status=self.ns['_action_status']
        missing=status({'results':[{'uid':'a'},{'ok':True}]})
        self.assertFalse(missing['ok'])
        self.assertTrue(missing['partial'])
        self.assertEqual(missing['unknown'],1)
        self.assertIn('ok', missing['error'])
        self.assertFalse(status({'results':[{'uid':'a'}]})['ok'])
        self.assertTrue(status({'results':[{'ok':True}]})['ok'])
        self.assertTrue(status({'ok':True,'accounts':[{'error':'snapshot'}]})['ok'])

    def test_checkin_zero_of_n_is_not_ok(self):
        from unittest.mock import Mock
        self.ns.update(CONFIG={'upstreams':[{'name':'qoder'}]},
                       CHECKIN_PATHS={'qoder':('/accounts/checkin','panel')},
                       _checkin_timeout=lambda name: 15)
        for payload, expected_ok, expected_partial in (
                ({'results':[{'uid':'a'},{'ok':False}]}, False, False),
                ({'results':[{'ok':True},{'ok':False}]}, False, True),
                ({'results':[{'ok':True}]}, True, False)):
            handler=object.__new__(self.ns['Handler'])
            handler._send_json=Mock(); handler._deny=Mock()
            self.ns['engine_request']=Mock(return_value=payload)
            handler._api_checkin('qoder')
            body=handler._send_json.call_args.args[1]
            self.assertEqual(body['ok'], expected_ok)
            self.assertEqual(body['partial'], expected_partial)
            self.assertEqual((body['checked'],body['total']),
                             (self.ns['_checkin_tally'](payload)))
            if not expected_ok:
                self.assertFalse(body['ok'] is True)

    def post(self, path, raw=b'{}', lengths=None, transfer=None):
        from email.message import Message
        from unittest.mock import Mock
        obj = object.__new__(self.ns['Handler'])
        obj.path, obj.command, obj.close_connection = path, 'POST', False
        obj.headers = Message()
        for length in lengths if lengths is not None else [str(len(raw))]:
            obj.headers['Content-Length'] = length
        if transfer:
            obj.headers['Transfer-Encoding'] = transfer
        obj.rfile = io.BytesIO(raw)
        obj._authorized = lambda: True
        obj._deny = Mock()
        for name in ('_api_checkin', '_api_qwenwork_reimport', '_api_tasks_post', '_api_login_start'):
            setattr(obj, name, Mock())
        return obj

    def test_multipart_rejection_closes_unread_connection(self):
        from unittest.mock import Mock
        body = b'--boundary\r\nfile content\r\nGET /v1/models HTTP/1.1\r\n\r\n'
        handler = self.post('/v1/images/edits', body)
        handler.headers['Content-Type'] = 'multipart/form-data; boundary=boundary'
        handler._media_post = Mock()
        handler.do_POST()
        handler._deny.assert_called_once()
        self.assertEqual(handler._deny.call_args.args[0], 400)
        self.assertTrue(handler.close_connection)
        self.assertEqual(handler.rfile.tell(), 0)
        handler._media_post.assert_not_called()

    def test_raccoon_billing_link_preserves_title_and_relay(self):
        import contextlib
        from unittest.mock import Mock
        for platform, path, title, stream in (
                ('raccoon', '/v1/chat/completions', None, False),
                ('raccoon', '/v1/chat/completions', None, True),
                ('raccoon', '/v1/chat/completions', 'Client title', True),
                ('raccoon', '/v1/responses', None, False),
                ('qoder', '/v1/chat/completions', None, False)):
            with self.subTest(platform=platform, path=path, title=title, stream=stream):
                request = {'model': platform + '/m', 'messages': [], 'stream': stream}
                if title is not None:
                    request['title'] = title
                forward = Mock(return_value=(200, {'X-Request-Id': 'engine-request'},
                                             iter([b'data: [DONE]\n\n']) if stream else b'{}'))
                record = Mock()
                self.ns.update(resolve_model=lambda _: ({'name': platform}, 'm', platform + '/m', None),
                               forward=forward, _record_raccoon_billing_link=record,
                               _platform_inflight=lambda _: contextlib.nullcontext())
                handler = self.post(path, json.dumps(request).encode())
                handler._relay_json = Mock()
                handler._relay_stream = Mock()
                handler.do_POST()
                forward.assert_called_once()
                sent = json.loads(forward.call_args.args[3])
                self.assertEqual(sent['model'], 'm')
                self.assertEqual(sent['messages'], [])
                if platform == 'raccoon' and path.endswith('/chat/completions') and title is None:
                    self.assertRegex(sent['title'], r'^gw-[a-f0-9]{32}$')
                    record.assert_called_once()
                    self.assertEqual(record.call_args.args[:3], ('engine-request', sent['title'], 'm'))
                else:
                    self.assertEqual(sent.get('title'), title)
                    record.assert_not_called()
                (handler._relay_stream if stream else handler._relay_json).assert_called_once()
                (handler._relay_json if stream else handler._relay_stream).assert_not_called()

    def test_raccoon_session_key_injected_as_x_session_id(self):
        """同一会话必须粘住一个账号：网关派生稳定的 X-Session-Id。

        ZCode 不发会话头，引擎的前缀指纹被 system 里的时间戳打穿（命中率 ~44%），
        同一会话在两个账号间轮番。网关用「第一条 user 消息」派生键——system 每轮
        都变但首问不变；覆盖客户端自带头，保证键由网关统一。
        """
        import contextlib
        from unittest.mock import Mock

        def run_case(first_user, system_text, client_header=None):
            request = {'model': 'raccoon/m',
                       'messages': [{'role': 'system', 'content': system_text},
                                    {'role': 'user', 'content': first_user}]}
            forward = Mock(return_value=(200, {}, b'{}'))
            self.ns.update(resolve_model=lambda _: ({'name': 'raccoon'}, 'm', 'raccoon/m', None),
                           forward=forward, _record_raccoon_billing_link=Mock(),
                           _platform_inflight=lambda _: contextlib.nullcontext())
            handler = self.post('/v1/chat/completions', json.dumps(request).encode())
            if client_header:
                handler.headers['X-Session-Id'] = client_header
            handler._relay_json = Mock()
            handler.do_POST()
            headers = forward.call_args.args[4]
            return headers.get('X-Session-Id') or ''

        # 同一会话：system 每轮都变（时间戳/目录），首问不变 → 键稳定
        sid_a = run_case('帮我写个爬虫', '当前时间 2026-11-22 10:00，cwd=/a')
        sid_b = run_case('帮我写个爬虫', '当前时间 2026-11-22 10:05，cwd=/a')
        self.assertTrue(sid_a.startswith('gw-'))
        self.assertEqual(sid_a, sid_b, 'same conversation must keep one session key')
        # 不同首问 → 新会话新键
        sid_c = run_case('查一下天气', '当前时间 2026-11-22 10:00，cwd=/a')
        self.assertNotEqual(sid_c, sid_a)
        # 客户端自带的 X-Session-Id 被网关派生键覆盖
        overridden = run_case('帮我写个爬虫', 'sys', client_header='client-key')
        self.assertNotEqual(overridden, 'client-key')
        self.assertTrue(overridden.startswith('gw-'))

    def test_invalid_bodies_never_dispatch(self):
        paths = ['/api/checkin/workbuddy', '/api/qwenwork/reimport',
                 '/api/tasks/run/workbuddy', '/api/login/start/qoder']
        cases = [(b'{}', ['9'], None), (b'{}', ['2', '2'], None),
                 (b'{}', ['2'], 'chunked'), (b'{}', ['-1'], None),
                 (b'{}', ['33554433'], None), (b'[]', None, None),
                 (b'false', None, None), (b'null', None, None), (b'{', None, None)]
        for path in paths:
            for raw, lengths, transfer in cases:
                with self.subTest(path=path, raw=raw, lengths=lengths):
                    obj = self.post(path, raw, lengths, transfer)
                    obj.do_POST()
                    obj._deny.assert_called_once()
                    for name in ('_api_checkin', '_api_qwenwork_reimport', '_api_tasks_post', '_api_login_start'):
                        getattr(obj, name).assert_not_called()
                    if lengths is not None:
                        self.assertTrue(obj.close_connection)

    def test_absolute_request_target_is_normalized(self):
        from unittest.mock import Mock
        self.ns['CONFIG'] = {'api_key': 'synthetic-only'}
        handler = object.__new__(self.ns['Handler'])
        handler.headers = {'x-api-key': 'synthetic-only'}
        handler._models = Mock()
        handler._deny = Mock()
        handler.path = 'http://127.0.0.1:8787/v1/models?x=1'
        handler.do_GET()
        handler._models.assert_called_once()
        handler._deny.assert_not_called()

    def test_chunked_json_is_decoded_before_dispatch(self):
        body = b'{}'
        wire = f'{len(body):x}\r\n'.encode() + body + b'\r\n0\r\n\r\n'
        obj = self.post('/api/checkin/workbuddy', wire, lengths=[], transfer='chunked')
        obj.do_POST()
        obj._api_checkin.assert_called_once_with('workbuddy')
        obj._deny.assert_not_called()

    def test_anthropic_auth_error_shape(self):
        from unittest.mock import Mock
        self.ns['CONFIG'] = {'api_key': 'synthetic-only'}
        handler = self.post('/v1/messages', b'{}')
        handler._authorized = lambda: False
        handler._send_json = Mock()
        handler._deny = self.ns['Handler']._deny.__get__(handler)
        handler.do_POST()
        code, payload = handler._send_json.call_args.args[:2]
        self.assertEqual(code, 401)
        self.assertEqual(payload.get('type'), 'error')
        self.assertEqual(payload['error']['type'], 'authentication_error')

    def test_body_timeout_and_early_return(self):
        from unittest.mock import Mock
        obj = self.post('/api/checkin/workbuddy')
        obj.rfile = Mock()
        obj.rfile.read.side_effect = TimeoutError()
        obj.do_POST()
        self.assertTrue(obj.close_connection)
        obj._api_checkin.assert_not_called()
        obj = self.post('/unknown', b'{}')
        obj.do_POST()
        self.assertEqual(obj.rfile.tell(), 2)
        self.assertEqual(obj._deny.call_args.args[0], 404)

    def test_valid_and_empty_body_dispatch_once(self):
        for raw in (b'{}', b''):
            obj = self.post('/api/checkin/workbuddy', raw)
            obj.do_POST()
            obj._api_checkin.assert_called_once_with('workbuddy')
            obj._deny.assert_not_called()

    def test_model_type_rejected_before_resolution(self):
        from unittest.mock import Mock
        self.ns['resolve_model'] = Mock(side_effect=AssertionError('must not resolve'))
        for model in ([], {}, 12, False, None):
            obj = self.post('/v1/chat/completions', json.dumps({'model':model}).encode())
            obj.do_POST()
            self.assertEqual(obj._deny.call_args.args[0], 400)
        self.ns['resolve_model'].assert_not_called()

    def test_wb_lock_only_applies_to_wb(self):
        from unittest.mock import Mock
        scope = isolated(ROOT / 'router.py', {'_wb_entry_locked'},
            {'CONFIG':{'hide_pro_locked':True}, '_wb_realm_meta':Mock(return_value={'shared':{'pro':['cn']}}),
             '_wb_usable_realms':lambda _: [], '_wb_paid_any':lambda:False})
        for platform in ('qoder', 'qwenwork'):
            self.assertFalse(scope['_wb_entry_locked']({'platform':platform, 'upstream':'shared','requires_pro':True}))
        scope['_wb_realm_meta'].assert_not_called()
        self.assertTrue(scope['_wb_entry_locked']({'platform':'workbuddy','upstream':'shared'}))

    def test_redirect_never_reaches_second_origin(self):
        import urllib.request
        import urllib.error
        from email.message import Message
        scope = isolated(ROOT / 'router.py', {'_NoRedirect'}, {'urllib':__import__('urllib')})
        class Transport(urllib.request.HTTPHandler):
            def __init__(self, code):
                super().__init__()
                self.calls, self.code = [], code
            def http_open(self, req):
                self.calls.append(req.full_url)
                headers = Message()
                headers['Location'] = 'http://other.invalid/stolen'
                response = urllib.response.addinfourl(io.BytesIO(b''), headers, req.full_url, self.code)
                response.msg = 'Redirect'
                return response
        for code in (301, 302, 303, 307, 308):
            transport = Transport(code)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), scope['_NoRedirect'](), transport)
            request = urllib.request.Request('http://127.0.0.1/local',
                headers={'Authorization':'synthetic', 'X-Panel-Token':'synthetic'})
            with self.assertRaises(urllib.error.HTTPError):
                from unittest.mock import patch
                with patch('socket.create_connection', side_effect=AssertionError('offline test attempted socket')):
                    opener.open(request)
            self.assertEqual(transport.calls, ['http://127.0.0.1/local'])

    def test_audit_response_contracts(self):
        import urllib.request
        scope = isolated(ROOT / 'audit_platform.py', {'valid_response','run_checks'}, {'urllib':__import__('urllib')})
        valid = scope['valid_response']
        fixtures = {'/health':{'ok':True,'version':'test','platforms':{'fake':True}},
                    '/api/ping':{'ok':True}, '/api/catalog':{'ok':True,'items':[]},
                    '/v1/models':{'object':'list','data':[]},
                    '/api/gwconfig':{'ok':True,'hide_pro_locked':True,'wb_dual_prefer':'auto','wb_realms':{}}}
        for path, data in fixtures.items():
            self.assertTrue(valid(path, 200, 200, data))
            for bad in ({}, [], None, {'error':'failure'}):
                self.assertFalse(valid(path, 200, 200, bad))
            self.assertFalse(valid(path, 503, 200, data))
        self.assertTrue(valid('/api/ping',401,401,{'error':{'message':'denied'}}))
        calls=[]
        class Opener:
            def open(self, request, timeout):
                calls.append(request.full_url)
                response = io.BytesIO(b'{}')
                response.code = 401 if len(calls)==2 else 200
                return response
        from contextlib import redirect_stdout
        with redirect_stdout(io.StringIO()):
            self.assertEqual(scope['run_checks']('http://127.0.0.1', 'synthetic', Opener()), 1)
        self.assertEqual(len(calls),6)
        self.assertTrue(all(url.split('127.0.0.1')[1] in fixtures for url in calls))

    def test_audit_local_opener_bypasses_proxy(self):
        import os
        import urllib.request
        from unittest.mock import patch
        scope = isolated(ROOT / 'audit_platform.py', {'_NoRedirect', 'local_opener'},
                         {'urllib': __import__('urllib')})
        env = {'HTTP_PROXY':'http://proxy.invalid:9', 'HTTPS_PROXY':'http://proxy.invalid:9',
               'http_proxy':'http://proxy.invalid:9', 'https_proxy':'http://proxy.invalid:9'}
        with patch.dict(os.environ, env, clear=False):
            opener = scope['local_opener']()
            default = urllib.request.build_opener()
        proxies = [h for h in opener.handlers if isinstance(h, urllib.request.ProxyHandler)]
        # An empty ProxyHandler replaces the default one, so no proxy route exists at all.
        self.assertEqual(proxies, [])
        self.assertTrue(any(isinstance(h, urllib.request.ProxyHandler) for h in default.handlers))
        self.assertTrue(any(isinstance(h, scope['_NoRedirect']) for h in opener.handlers))

    def test_raw_media_download_does_not_require_sse(self):
        class Response:
            status = 200
            def getheaders(self): return [('content-type', 'video/mp4')]
            def read(self): raise AssertionError('video must not be buffered')
            def read1(self, size):
                if getattr(self, 'sent', False): return b''
                self.sent = True
                return b'video-bytes'
        class Connection:
            def request(self, *args, **kwargs):
                self.headers = kwargs['headers']
            def getresponse(self): return Response()
            def close(self): self.closed = True
        conn = Connection()
        self.ns.update(_connect=lambda _: conn, _upstream_headers=lambda _: {},
                       _upstream_path=lambda _, p:p, HOP_HEADERS=set())
        status, headers, chunks = self.ns['forward']({}, 'GET', '/', None, {}, 'raw')
        self.assertEqual(status, 200)
        self.assertEqual(headers['Content-Type'], 'video/mp4')
        self.assertNotEqual(conn.headers.get('Accept'), 'text/event-stream')
        self.assertEqual(b''.join(chunks), b'video-bytes')
        self.assertTrue(conn.closed)

    def test_media_model_resolution(self):
        scope = isolated(ROOT / "router.py", {"resolve_media_model", "_media_supports_tag"}, {
            "_media_supports_tag": self.ns["_media_supports_tag"],
            "CONFIG": {"upstreams": [{"name": "workbuddy"}]},
            "platform_enabled": lambda name: True,
            "public_media_catalog": lambda force=False: [
                {"public": "kling-video-v3", "id": "kling-video-v3",
                 "tags": ["text-to-video"], "kind": "video", "available": True},
                {"public": "hy-image-3.5", "id": "hy-image-3.5",
                 "tags": ["text-to-image"], "kind": "image", "available": False}],
            "_model_hidden": lambda platform, model: model == "hy-image-3.5"})
        fn = scope["resolve_media_model"]
        self.assertEqual(fn("workbuddy/kling-video-v3")[1], "kling-video-v3")
        self.assertEqual(fn("kling-video-v3")[2], "kling-video-v3")
        self.assertIn("隐藏", fn("workbuddy/hy-image-3.5")[3])
        self.assertIn("未知媒体平台", fn("qoder/kling-video-v3")[3])
        self.assertIn("未知媒体模型", fn("not-a-media-model")[3])
        up, upstream, facing, err = fn("")
        self.assertIs(up, scope["CONFIG"]["upstreams"][0])
        self.assertEqual((upstream, facing, err), ("kling-video-v3", "workbuddy/kling-video-v3", None))
        self.assertIsNone(fn("", "text-to-image")[0])
        scope["_model_hidden"] = lambda platform, model: False
        self.assertIn("当前媒体出口", fn("hy-image-3.5")[3])
        self.assertIn("不支持", fn("kling-video-v3", "text-to-image")[3])
        scope["_model_hidden"] = lambda platform, model: True
        self.assertIsNone(fn("")[0])

    def test_media_routes_raccoon_images(self):
        scope = isolated(ROOT / "router.py", {"resolve_media_model", "_media_supports_tag"}, {
            "_media_supports_tag": self.ns["_media_supports_tag"],
            "CONFIG": {"upstreams": [{"name": "workbuddy"}, {"name": "raccoon"}]},
            "platform_enabled": lambda name: True,
            "public_media_catalog": lambda force=False: [
                {"public": "wb-img", "platform": "workbuddy",
                 "tags": ["text-to-image"], "kind": "image", "available": True},
                {"public": "raccoon-image-gen", "platform": "raccoon",
                 "tags": ["text-to-image", "image-to-image"], "kind": "image",
                 "available": True,
                 "aliases": ["raccoon-image-gen", "gpt-image-1"]}],
            "_model_hidden": lambda platform, model: False})
        fn = scope["resolve_media_model"]
        up, wanted, facing, err = fn("raccoon/gpt-image-1", "text-to-image")
        self.assertEqual((up["name"], wanted, facing, err),
                         ("raccoon", "gpt-image-1", "raccoon/gpt-image-1", None))
        up, wanted, facing, err = fn("raccoon/raccoon-image-gen", "image-to-image")
        self.assertEqual((up["name"], wanted, err), ("raccoon", "raccoon-image-gen", None))
        self.assertIn("不支持", fn("raccoon/raccoon-image-gen", "text-to-video")[3])

    def test_raccoon_image_price_from_bills(self):
        calls = []

        def fake(up, method, path, body=None, auth=None, timeout=None):
            calls.append(path)
            if path.endswith("/overview"):
                return {"accounts": [{"uid": "u1"}]}
            return {"bills": [
                {"created_at": "2026-11-19T01:38:52+08:00", "points": -20,
                 "event_name": "任务消耗 · 图片生成"},
                {"created_at": "2026-11-19T01:30:00+08:00", "points": -4,
                 "event_name": "对话消耗"},
            ]}

        scope = isolated(ROOT / "router.py", {"_raccoon_image_price"}, {
            "engine_request": fake,
            "_raccoon_price_lock": threading.Lock(),
            "_raccoon_price_cache": {"ts": 0.0, "price": None},
            "RACCOON_PRICE_TTL": 300,
            "quote": quote})
        self.assertEqual(scope["_raccoon_image_price"]({"name": "raccoon"}), 20)
        self.assertEqual(len(calls), 2)
        # 缓存命中后不再打引擎
        self.assertEqual(scope["_raccoon_image_price"]({"name": "raccoon"}), 20)
        self.assertEqual(len(calls), 2)
        # 没有图片账单时返回 None，不猜
        scope["_raccoon_price_cache"].update(ts=0.0, price=None)
        scope["engine_request"] = lambda *a, **k: (
            {"accounts": [{"uid": "u1"}]} if str(a[2]).endswith("/overview")
            else {"bills": [{"created_at": "2026-11-19T01:30:00+08:00",
                             "points": -4, "event_name": "对话消耗"}]})
        self.assertIsNone(scope["_raccoon_image_price"]({"name": "raccoon"}))

    def test_raccoon_image_price_skips_failed_account(self):
        calls = []

        def fake(up, method, path, body=None, auth=None, timeout=None):
            calls.append(path)
            if path.endswith("/overview"):
                return {"accounts": [{"uid": "u1"}, {"uid": "u2"}]}
            if "/u1/" in path:
                raise OSError("boom")
            return {"bills": [{"created_at": "2026-11-19T01:38:52+08:00", "points": -20,
                               "event_name": "任务消耗 · 图片生成"}]}

        scope = isolated(ROOT / "router.py", {"_raccoon_image_price"}, {
            "engine_request": fake,
            "_raccoon_price_lock": threading.Lock(),
            "_raccoon_price_cache": {"ts": 0.0, "price": None},
            "RACCOON_PRICE_TTL": 300,
            "quote": quote})
        # 单账号账单失败只跳过该账号：不能中止整轮并把 None 缓存 5 分钟
        self.assertEqual(scope["_raccoon_image_price"]({"name": "raccoon"}), 20)
        self.assertEqual(sum(1 for p in calls if "/u1/" in p or "/u2/" in p), 2)

    def test_raccoon_media_fetch_unreachable_and_aliases(self):
        scope = isolated(ROOT / "router.py", {"_raccoon_media_fetch"}, {
            "CONFIG": {"upstreams": [{"name": "raccoon"}]},
            "platform_enabled": lambda name: True,
            "_upstream_by_name": lambda name: {"name": "raccoon"},
            "upstream_rows": lambda up: None,
            "_raccoon_image_price": lambda up: None,
            "_raccoon_media_lock": threading.Lock(),
            "_raccoon_media_cache": {"ts": 0.0, "models": [], "errors": {}},
            "RACCOON_MEDIA_TTL": 30})
        # 引擎不可达（upstream_rows 返回 None）必须显式报错，而不是静默变空
        models, errors = scope["_raccoon_media_fetch"](force=True)
        self.assertEqual(models, [])
        self.assertIn("不可达", errors.get("active", ""))
        # 目录恢复：规范 id 排最前、其余保持引擎顺序；别名登记在规范行上供 fallback
        scope["upstream_rows"] = lambda up: [
            {"id": "dall-e-2", "capabilities": {"image": True}},
            {"id": "raccoon-image-gen", "capabilities": {"image": True}},
            {"id": "gpt-image-1", "capabilities": {"image": True}}]
        models, errors = scope["_raccoon_media_fetch"](force=True)
        self.assertEqual(errors, {})
        self.assertEqual([m["id"] for m in models],
                         ["raccoon-image-gen", "dall-e-2", "gpt-image-1"])
        self.assertEqual(models[0]["aliases"], ["dall-e-2", "gpt-image-1"])
        self.assertEqual(models[1]["aliases"], [])

    def test_raccoon_image_count_is_not_silently_dropped(self):
        from unittest.mock import Mock

        handler = object.__new__(self.ns['Handler'])
        handler.path = '/v1/images/generations'
        handler.headers = {}
        handler._send_json = Mock()
        handler._relay_media_json = Mock()
        forward = Mock(return_value=(200, {}, b'{"ok":true}'))
        self.ns.update(resolve_media_model=lambda model, tag: (
                           {'name': 'raccoon'}, 'gpt-image-1', model, None),
                       forward=forward, log=lambda *_: None)
        handler._post_body = {'model': 'raccoon/gpt-image-1', 'prompt': 'synthetic', 'n': 2}
        handler._deny = Mock()
        handler._media_post()
        handler._deny.assert_called_once()
        self.assertEqual(handler._deny.call_args.args[0], 400)
        self.assertIn('不支持一次生成多张', handler._deny.call_args.args[1])
        forward.assert_not_called()

        handler._send_json.reset_mock()
        handler._relay_media_json.reset_mock()
        handler._deny.reset_mock()
        handler._post_body = {'model': 'raccoon/gpt-image-1', 'prompt': 'synthetic', 'n': 1}
        handler._media_post()
        body = json.loads(forward.call_args.args[3])
        self.assertNotIn('n', body)
        self.assertEqual(handler._relay_media_json.call_args.args[0], 200)

    def test_media_multipart_rejected(self):
        fn = self.ns["_media_multipart_reject"]
        self.assertTrue(fn("/v1/images/edits", "multipart/form-data; boundary=x"))
        self.assertTrue(fn("/images/edits", "multipart/form-data"))
        self.assertTrue(fn("/v1/videos", "multipart/form-data"))
        self.assertFalse(fn("/v1/images/edits", "application/json"))
        self.assertFalse(fn("/v1/chat/completions", "multipart/form-data"))
        self.assertFalse(fn("/api/checkin/workbuddy", "multipart/form-data"))

    def test_wb_claim_query_business_error_is_not_skipped(self):
        accounts = [{"uid": "u", "realm": "cn", "enabled": True}]
        def fake(up, method, path, body=None, **kwargs):
            if path.startswith('/accounts'):
                return {'accounts': accounts}
            if path == '/tasks?uid=u':
                return {'ok': False, 'error': '任务服务不可用'}
            raise AssertionError(path)
        scope = isolated(ROOT / 'router.py', {'Handler', '_num', '_action_status'},
                         {'engine_request': fake, 'json': json, 'log': lambda *_: None})
        obj = object.__new__(scope['Handler'])
        result = scope['Handler']._wb_claim_all(obj, {}, {})
        self.assertFalse(scope['_action_status'](result)['ok'])
        self.assertEqual(result['accounts'][0]['error'], '任务服务不可用')
        self.assertNotIn('skipped', result['accounts'][0])

    def test_media_capability_stays_realm_specific(self):
        import urllib.request  # ensure package submodule is available to isolated handler
        payloads = {
            "cn": {"models": [
                {"id": "kling-video-v3", "name": "Kling", "tags": ["text-to-video"],
                 "videoGeneration": {"maxSeconds": 15}, "creditPerSecondCeiling": 26.0},
                {"id": "hunyuan-image-alpha", "tags": ["text-to-image"],
                 "creditPerImage": 6.0}]},
            "intl": {"models": [
                {"id": "kling-video-v3", "name": "Kling", "tags": ["image-to-video"]},
                {"id": "hy-image-3.5", "tags": ["text-to-image"],
                 "creditPerImage": 6.0, "creditPerImageMeasured": True}]},
        }

        class Resp:
            def __init__(self, data): self.data = data
            def read(self): return json.dumps(self.data).encode()
            def __enter__(self): return self
            def __exit__(self, *exc): return False

        def fake_urlopen(req, timeout):
            realm = "intl" if "realm=intl" in req.full_url else "cn"
            return Resp(dict(payloads[realm], realm=realm))

        scope = isolated(ROOT / "router.py", {"_wb_media_fetch"}, {
            "CONFIG": {"upstreams": [{"name": "workbuddy",
                                      "base_url": "http://127.0.0.1:8788"}]},
            "WB_MEDIA_REALMS": ("cn", "intl"), "WB_MEDIA_TTL": 30.0,
            "_wb_media_lock": threading.Lock(),
            "_wb_media_cache": {"ts": 0.0, "models": [], "errors": {}},
            "_upstream_headers": lambda up, extra=None: {},
            "_urlopen": fake_urlopen,
            "urllib": __import__("urllib"),
            "_known_num": self.ns["_known_num"]})
        models, errors = scope["_wb_media_fetch"](force=True)
        by_id = {m["id"]: m for m in models}
        self.assertEqual(errors, {})
        self.assertEqual(by_id["kling-video-v3"]["realms"], ["cn", "intl"])
        self.assertEqual(by_id["kling-video-v3"]["kind"], "video")
        self.assertEqual(by_id["kling-video-v3"]["tags_by_realm"], {
            "cn": ["text-to-video"], "intl": ["image-to-video"]})
        self.assertEqual(by_id["hunyuan-image-alpha"]["realms"], ["cn"])
        self.assertEqual(by_id["hy-image-3.5"]["realms"], ["intl"])
        self.assertEqual(by_id["hy-image-3.5"]["kind"], "image")

    def test_media_urls_are_absolute(self):
        self.ns["CONFIG"] = {"listen_port": 8787}
        obj = object.__new__(self.ns["Handler"])
        out = obj._media_rewrite(
            {"model": "kling-video-v3", "url": "/v1/videos/a/content?t=1",
             "tasks": [{"url": "/v1/videos/b/content?t=2"}]},
            "workbuddy/kling-video-v3")
        self.assertEqual(out["model"], "workbuddy/kling-video-v3")
        from urllib.parse import urlsplit
        target = urlsplit(out["url"])
        self.assertEqual(target.netloc, "127.0.0.1:8787")
        self.assertTrue(self.ns["_media_download_allowed"](target.path + '?' + target.query))
        self.assertFalse(self.ns["_media_download_allowed"](
            (target.path + '?' + target.query).replace('/a/content', '/other/content')))
        self.assertFalse(self.ns["_media_download_allowed"]('/v1/videos/a/content?t=1'))
        self.assertIn('gw_sig=', out["tasks"][0]["url"])
        obj.path = target.path + '?' + target.query
        obj._authorized = lambda: False
        calls = []
        obj._media_video_get = lambda: calls.append('download')
        obj.do_GET()
        self.assertEqual(calls, ['download'])

    def test_claim_scans_all_active_accounts(self):
        accounts = [
            {"uid": "a", "nickname": "A", "realm": "cn", "enabled": True, "tasksEnabled": True},
            {"uid": "b", "nickname": "B", "realm": "cn", "enabled": True},
            {"uid": "c", "nickname": "C", "realm": "intl", "enabled": True},
        ]
        tasks = {
            "a": [{"task_code": "daily", "status": "accepted", "current": 1, "target": 1}],
            "b": [{"task_code": "later", "status": "accepted", "current": 0, "target": 1}],
        }
        claims, queried = [], []

        def fake_engine(up, method, path, body=None, **kwargs):
            if path.startswith("/accounts"):
                return {"accounts": accounts}
            if path.startswith("/tasks?uid="):
                uid = path.split("uid=")[-1]
                queried.append(uid)
                return {"tasks": tasks.get(uid, [])}
            if path == "/tasks/claim":
                claims.append(body)
                return {"ok": True, "credit": 5}
            raise AssertionError(path)

        scope = isolated(ROOT / "router.py", {"Handler", "_num"}, {
            "engine_request": fake_engine, "json": json, "log": lambda *_: None})
        obj = object.__new__(scope["Handler"])
        result = scope["Handler"]._wb_claim_all(obj, {"name": "workbuddy"}, {})
        self.assertEqual(queried, ["a", "b"])
        self.assertEqual(claims, [{"uid": "a", "code": "daily"}])
        self.assertEqual(result["claimed"][0]["uid"], "a")
        self.assertTrue(any(row.get("skipped") for row in result["accounts"]))
        self.assertTrue(self.ns["_action_status"](result)["ok"])
        tasks["b"] = None
        original_engine = scope["engine_request"]
        def failing_engine(up, method, path, body=None, **kwargs):
            if path == '/tasks?uid=b':
                raise TimeoutError('task snapshot unavailable')
            return original_engine(up, method, path, body, **kwargs)
        scope["engine_request"] = failing_engine
        result = obj._wb_claim_all({"name": "workbuddy"}, {})
        status = self.ns["_action_status"](result)
        self.assertFalse(status['ok'])
        self.assertTrue(status['partial'])
        self.assertIn('unavailable', status['error'])

    def test_workbuddy_mirror_fallback(self):
        seen = []
        scope = isolated(ROOT / "router.py", {"_wb_mirror_update", "_ver_tuple"}, {
            "CONFIG": {"wb_update_mirror": "custom/mirror"},
            "WB_UPDATE_MIRRORS": ("default/mirror",),
            "_REPO_RE": re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$"),
            "_github_latest": lambda repo: seen.append(repo) or "v2.1.3"})
        row = {"current": "2.1.3", "latest": None, "source": "engine",
               "error": "引擎更新检测失败: HTTP 404"}
        self.assertTrue(scope["_wb_mirror_update"](row))
        self.assertEqual(seen, ["custom/mirror"])
        self.assertEqual(row["latest"], "v2.1.3")
        self.assertFalse(row["has_update"])
        self.assertTrue(row["mirror"])
        self.assertEqual(row["source"], "mirror:custom/mirror")

    def test_tail_log_reads_only_tail(self):
        import os
        import tempfile
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as fh:
            for i in range(5000):
                fh.write(f"line {i} API Key: secret\n")
            path = fh.name
        try:
            scope = isolated(ROOT / "router.py", {"tail_log"},
                             {"LOG_PATH": path, "os": os})
            rows = scope["tail_log"](3)
            self.assertEqual(len(rows), 3)
            self.assertIn("line 4999", rows[-1])
            self.assertIn("[redacted]", rows[-1])
            self.assertNotIn("secret", rows[-1])
        finally:
            os.unlink(path)

    def test_wb_session_key_prefers_explicit_id(self):
        scope = isolated(ROOT / "router.py", {"_wb_session_key", "_wb_header"},
                         {"hashlib": hashlib})
        fn = scope["_wb_session_key"]
        a = fn({"prompt_cache_key": "abc"}, "/v1/chat/completions", {}, "m")
        b = fn({"prompt_cache_key": "abc"}, "/v1/chat/completions", {}, "m")
        c = fn({"prompt_cache_key": "xyz"}, "/v1/chat/completions", {}, "m")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        h1 = fn({"messages": [{"role": "system", "content": "sys"}], "tools": [{"x": 1}]},
                "/v1/chat/completions", {}, "m")
        h2 = fn({"messages": [{"role": "system", "content": "sys"}], "tools": [{"x": 1}]},
                "/v1/chat/completions", {}, "m")
        self.assertEqual(h1, h2)
        self.assertIsNone(fn({"messages": [{"role": "user", "content": "hi"}]},
                             "/v1/chat/completions", {}, "m"))

    def test_wb_plan_prefers_sticky_then_health(self):
        scope = isolated(ROOT / "router.py", {"_wb_plan"}, {
            "_wb_account_realms": lambda _: ["cn", "intl"],
            "_wb_model_realm": lambda _: None,
            "_wb_session_key": lambda *_: "k",
            "_wb_sticky_get": lambda _: "intl",
            "_wb_dual_prefer": lambda: "cn",
            "_wb_health_ok": lambda realm: realm != "intl"})
        plan = scope["_wb_plan"]("m", {}, "/v1/chat/completions", {})
        self.assertEqual(plan["order"], ["cn", "intl"])
        self.assertEqual(plan["reason"], "sticky+health")
        self.assertEqual(plan["sticky"], "intl")
        self.assertEqual(plan["preferred"], "cn")
        scope["_wb_model_realm"] = lambda _: "intl"
        scope["_wb_account_realms"] = lambda _: ["intl"]
        plan = scope["_wb_plan"]("m", {}, "/v1/chat/completions", {})
        self.assertEqual(plan["order"], ["intl"])
        self.assertEqual(plan["reason"], "fixed")
        scope["_wb_account_realms"] = lambda _: []
        self.assertEqual(scope["_wb_plan"]("m", {}, "/v1/chat/completions", {})["order"], [])

    def test_wb_retryable_only_for_upstream_pressure(self):
        scope = isolated(ROOT / "router.py", {"_wb_retryable"}, {
            "WB_RETRY_STATUSES": {408, 425, 429, 500, 502, 503, 504}})
        fn = scope["_wb_retryable"]
        self.assertTrue(fn(429, b"{}"))
        self.assertTrue(fn(500, b"{}"))
        self.assertFalse(fn(400, b"{}"))
        self.assertTrue(fn(403, b'{"code":10605,"isQueued":true}'))
        self.assertFalse(fn(403, b'{"error":"content policy"}'))

    def test_wb_health_cooldown_and_recovery(self):
        scope = isolated(ROOT / "router.py",
                         {"_wb_health_fail", "_wb_health_success", "_wb_health_ok"}, {
                             "WB_HEALTH_BASE_COOLDOWN": 30.0,
                             "WB_HEALTH_MAX_COOLDOWN": 300.0,
                             "_wb_route_lock": threading.Lock(),
                             "_wb_route_health": {},
                             "log": lambda *_: None})
        ok, fail, success = (scope["_wb_health_ok"], scope["_wb_health_fail"],
                             scope["_wb_health_success"])
        self.assertTrue(ok("cn"))
        fail("cn", 429)
        self.assertFalse(ok("cn"))
        self.assertGreaterEqual(scope["_wb_route_health"]["cn"]["cooldown_until"],
                                time.time() + 25)
        success("cn")
        self.assertTrue(ok("cn"))

    def test_wb_sticky_set_get_clear(self):
        scope = isolated(ROOT / "router.py",
                         {"_wb_sticky_get", "_wb_sticky_set", "_wb_sticky_clear"}, {
                             "WB_STICKY_TTL": 1800.0, "WB_STICKY_MAX": 4,
                             "_wb_route_lock": threading.Lock(),
                             "_wb_route_sticky": {}})
        get, put, clear = (scope["_wb_sticky_get"], scope["_wb_sticky_set"],
                           scope["_wb_sticky_clear"])
        put("k", "cn")
        self.assertEqual(get("k"), "cn")
        clear("k")
        self.assertIsNone(get("k"))

    def test_wb_retries_alternate_realm_before_relay(self):
        import contextlib
        from unittest.mock import Mock
        calls = []

        def fake_forward(up, method, path, payload, headers, stream):
            calls.append(dict(headers))
            if len(calls) == 1:
                return 429, {"Content-Type": "application/json"}, b'{"error":"rate limit"}'
            return 200, {"Content-Type": "application/json"}, b'{"ok":true}'

        fail, success = Mock(), Mock()
        sticky = Mock()
        self.ns.update(
            CONFIG={"upstreams": [{"name": "workbuddy", "base_url": "http://127.0.0.1:8788"}]},
            resolve_model=lambda _: (self.ns["CONFIG"]["upstreams"][0], "m", "m", None),
            _wb_plan=lambda *_: {"order": ["cn", "intl"], "available": ["cn", "intl"],
                                 "preferred": "cn", "sticky": None, "key": "k",
                                 "reason": "prefer", "fixed": None},
            forward=fake_forward,
            _platform_inflight=lambda _: contextlib.nullcontext(),
            _wb_health_fail=fail, _wb_health_success=success,
            _wb_sticky_set=sticky, _wb_sticky_clear=Mock(),
            _wb_retryable=lambda status, body: status == 429,
            _wb_retry_after=lambda headers: 30,
            _wb_log_route=Mock())
        obj = self.post("/v1/chat/completions",
                        raw=b'{"model":"workbuddy/m","messages":[]}')
        obj._relay_json = Mock()
        obj.do_POST()
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].get("X-Realm"), "cn")
        self.assertEqual(calls[1].get("X-Realm"), "intl")
        fail.assert_called_once()
        success.assert_called_once_with("intl")
        sticky.assert_called_once_with("k", "intl")
        obj._relay_json.assert_called_once()
        self.assertEqual(obj._relay_json.call_args.args[0], 200)

    def test_wb_stops_after_success_or_nonretryable_response(self):
        import contextlib
        from unittest.mock import Mock

        for path in ('/v1/chat/completions', '/v1/responses', '/v1/messages'):
            for status in (200, 204, 400, 401, 403, 404, 422):
                with self.subTest(path=path, status=status):
                    headers = {'Content-Type': 'application/json'}
                    first, second = b'{"source":"cn"}', b'{"source":"intl"}'
                    forward = Mock(side_effect=[(status, headers, first), (200, headers, second)])
                    fail, success, sticky = Mock(), Mock(), Mock()
                    self.ns.update(
                        resolve_model=lambda _: ({'name': 'workbuddy'}, 'm', 'workbuddy/m', None),
                        _wb_plan=lambda *_: {'order': ['cn', 'intl'], 'key': 'k'},
                        forward=forward, _platform_inflight=lambda _: contextlib.nullcontext(),
                        _wb_health_fail=fail, _wb_health_success=success,
                        _wb_sticky_set=sticky, _wb_sticky_clear=Mock(),
                        _wb_retryable=lambda status, body: status == 429,
                        _wb_retry_after=lambda _: 30, _wb_log_route=Mock())
                    handler = self.post(path, b'{"model":"workbuddy/m","messages":[]}')
                    handler._relay_json = Mock()
                    handler.do_POST()
                    forward.assert_called_once()
                    self.assertEqual(forward.call_args.args[4]['X-Realm'], 'cn')
                    handler._relay_json.assert_called_once_with(status, headers, first, 'workbuddy/m')
                    fail.assert_not_called()
                    if status < 400:
                        success.assert_called_once_with('cn')
                        sticky.assert_called_once_with('k', 'cn')
                    else:
                        success.assert_not_called()
                        sticky.assert_not_called()

    def test_inflight_covers_response_writes_and_stream_cleanup(self):
        import contextlib
        from unittest.mock import Mock

        counts = {}
        scope = isolated(ROOT / 'router.py', {'_platform_inflight'}, {
            'contextlib': contextlib, 'threading': threading,
            '_inflight_lock': threading.Lock(), '_inflight': counts})
        for platform in ('workbuddy', 'qoder'):
            for stream, outcome in ((False, 'complete'), (False, 'disconnect'),
                                    (True, 'complete'), (True, 'disconnect'),
                                    (True, 'upstream-error')):
                with self.subTest(platform=platform, stream=stream, outcome=outcome):
                    closed = []

                    def chunks():
                        try:
                            self.assertEqual(counts[platform], 1)
                            yield b'data: {"model":"m"}\n\n'
                            self.assertEqual(counts[platform], 1)
                            if outcome == 'upstream-error':
                                raise TimeoutError('synthetic stream timeout')
                            yield b'data: [DONE]\n\n'
                        finally:
                            closed.append(counts[platform])

                    def forward(*args):
                        self.assertEqual(counts[platform], 1)
                        return 200, {'Content-Type': 'text/event-stream' if stream else 'application/json'}, (
                            chunks() if stream else b'{"model":"m"}')

                    def write(data):
                        self.assertEqual(counts[platform], 1)
                        if outcome == 'disconnect':
                            raise BrokenPipeError('synthetic client disconnect')
                        return len(data)

                    self.ns.update(
                        resolve_model=lambda _: ({'name': platform}, 'm', platform + '/m', None),
                        _platform_inflight=scope['_platform_inflight'], forward=Mock(side_effect=forward),
                        _wb_plan=lambda *_: {'order': ['cn', 'intl'], 'key': 'k'},
                        _wb_health_success=Mock(), _wb_sticky_set=Mock(),
                        _wb_retryable=lambda *_: False, _wb_log_route=Mock())
                    handler = self.post('/v1/chat/completions', json.dumps({
                        'model': platform + '/m', 'messages': [], 'stream': stream}).encode())
                    handler.send_response = Mock()
                    handler.send_header = Mock()
                    handler.end_headers = Mock()
                    handler.wfile = Mock(write=Mock(side_effect=write))
                    if not stream and outcome == 'disconnect':
                        with self.assertRaises(BrokenPipeError):
                            handler.do_POST()
                    else:
                        handler.do_POST()
                    self.ns['forward'].assert_called_once()
                    self.assertEqual(counts[platform], 0)
                    self.assertEqual(closed, [1] if stream else [])

    def test_console_api_hardening(self):
        from unittest.mock import Mock
        # usage limit：output_limit= 不再影响 limit；非法/越界回退默认并夹紧
        handler = object.__new__(self.ns["Handler"])
        handler._send_json = Mock()
        self.ns["active_upstreams"] = lambda: []
        for path, expected in (("/api/usage?output_limit=30", 60),
                               ("/api/usage?limit=abc", 60),
                               ("/api/usage?limit=9999", 500),
                               ("/api/usage", 60)):
            handler.path = path
            handler._api_usage()
            self.assertEqual(handler._send_json.call_args.args[1]["requested_limit"],
                             expected, path)

        # engine 代理：白名单路径上的非 GET/POST 方法必须 405
        handler._deny = Mock()
        self.ns["CONFIG"] = {"upstreams": [{"name": "workbuddy", "base_url": "http://127.0.0.1:8788"}]}
        self.ns["platform_enabled"] = lambda name: True
        handler._post_body = {"platform": "workbuddy", "path": "accounts/refresh",
                              "method": "DELETE"}
        handler._api_engine()
        self.assertEqual(handler._deny.call_args.args[0], 405)

        # reimport：discover 失败分支不得把 preview_token 带回浏览器
        self.ns["CONFIG"] = {"upstreams": [{"name": "qwenwork", "base_url": "http://127.0.0.1:8792"}]}
        self.ns["engine_request"] = Mock(return_value={"preview_token": "synthetic-token",
                                                       "importable_count": 0})
        handler._post_body = {}
        handler._api_qwenwork_reimport()
        code, body = handler._send_json.call_args.args
        self.assertFalse(body["ok"])
        self.assertNotIn("preview_token", json.dumps(body))
        self.assertEqual(body["discover"]["importable_count"], 0)

    def test_visibility_rejects_unknown_model_on_hide(self):
        from unittest.mock import Mock
        path = ROOT / "router.py"
        scope = isolated(path, {"Handler", "_hidden_set"},
                         {"CONFIG": {"upstreams": [{"name": "workbuddy",
                                                    "base_url": "http://127.0.0.1:8788"}],
                                     "hidden_models": []},
                          "_config_lock": threading.RLock(),
                          "public_catalog": lambda: [{"platform": "workbuddy", "public": "m1"}],
                          "public_media_catalog": lambda: [],
                          "save_config": Mock(),
                          "log": lambda *_: None})
        handler = object.__new__(scope["Handler"])
        handler._send_json = Mock()
        handler._deny = Mock()
        for body, expect_hide_ok in (({"id": "workbuddy/nope", "visible": False}, False),
                                     ({"id": "workbuddy/m1", "visible": False}, True),
                                     ({"id": "workbuddy/nope", "visible": True}, True)):
            handler._post_body = body
            handler._api_visibility()
            if expect_hide_ok:
                handler._deny.assert_not_called()
            else:
                self.assertEqual(handler._deny.call_args.args[0], 400)
            handler._deny.reset_mock()
        self.assertEqual(scope["CONFIG"]["hidden_models"], ["workbuddy/m1"])

    def test_usage_truncated_follows_per_source_limit(self):
        from unittest.mock import Mock
        handler = object.__new__(self.ns["Handler"])
        handler.path = '/api/usage?limit=10'
        handler._send_json = Mock()
        row = {'at': 1000, 'model': 'm', 'account': 'u', 'total_tokens': 5,
               'prompt_tokens': 3, 'completion_tokens': 2, 'status_code': 200}

        def fake(up, method, path_, body=None, auth=None, timeout=None):
            if path_.startswith('/accounts'):
                return {'accounts': []}
            return {'rows': [row] * 10}

        self.ns.update(active_upstreams=lambda: [{'name': 'qoder', 'base_url': 'http://127.0.0.1:8790'}],
                       engine_request=fake, _num=lambda n: float(n or 0), time=time)
        handler._api_usage()
        body = handler._send_json.call_args.args[1]
        self.assertTrue(body["truncated"],
                        "a source returning exactly limit rows counts as truncated")
        # 合计超限但没有任何单源触顶时不算截断
        handler.path = '/api/usage?limit=100'
        handler._send_json.reset_mock()
        handler._api_usage()
        body = handler._send_json.call_args.args[1]
        self.assertEqual(body["returned"], 10)
        self.assertFalse(body["truncated"])

    def test_codearts_usage_rows_from_admin_recent(self):
        from unittest.mock import Mock
        scope = isolated(ROOT / 'router.py', {'_codearts_usage_rows', '_iso_epoch'},
                         {'datetime': datetime})
        self.ns.update(scope)
        handler = object.__new__(self.ns["Handler"])
        handler.path = '/api/usage?limit=10'
        handler._send_json = Mock()

        def fake(up, method, path_, body=None, auth=None, timeout=None):
            self.assertEqual(path_, '/admin/api/recent?limit=10')   # 不是 /usage/recent
            self.assertEqual(auth, 'bearer')
            return {'items': [
                {'time': '2026-11-26T14:26:04+08:00', 'model': 'glm-5.3', 'account': 'u1',
                 'status': 200, 'duration_ms': 8346, 'stream': True},
                {'time': '2026-11-26T14:25:53+08:00', 'model': 'glm-5.3', 'account': 'u1',
                 'status': 503, 'code': 'no_healthy_account', 'kind': 'unknown'},
            ]}

        self.ns.update(active_upstreams=lambda: [
                           {'name': 'codearts', 'base_url': 'http://127.0.0.1:7866'}],
                       engine_request=fake,
                       _num=lambda n: float(n or 0),
                       _known_num=lambda n: (float(n) if isinstance(n, (int, float))
                                             and not isinstance(n, bool) else None))
        handler._api_usage()
        body = handler._send_json.call_args.args[1]
        self.assertEqual(body['returned'], 2)
        self.assertEqual(body['coverage']['codearts']['all'], {'ok': True, 'rows': 2})
        outcomes = sorted(r['outcome'] for r in body['rows'])
        self.assertEqual(outcomes, ['no_healthy_account', 'ok'])   # 用错误分类而非裸状态码
        self.assertTrue(all(r['total'] == 0 for r in body['rows']))  # token 未知不造假

    def test_codearts_keepalive_interval_and_call(self):
        """codearts 保活：间隔解析 + 调用形状 + 停用/失败分支。

        token 只有约 2 小时有效期，且引擎不持久化 expires_at 导致 watchdog
        无法提前刷新；不主动续期就会出现「首次 401 → 10 分钟冷却」的窗口
        （实测 2026-11-26 19:57–20:02 全部 503）。
        """
        from unittest.mock import Mock

        scope = isolated(ROOT / 'router.py',
                         {'_codearts_keepalive_cfg', '_codearts_keepalive_once'},
                         {'CONFIG': {}, 'CODECARTS_KEEPALIVE_MINUTES': 30})
        cfg = scope['_codearts_keepalive_cfg']

        # 缺省 → 30 分钟
        self.assertEqual(cfg(), 1800.0)
        # 显式值
        scope['CONFIG']['codearts_keepalive_minutes'] = 15
        self.assertEqual(cfg(), 900.0)
        # 0 / 负数 → 关闭
        for off in (0, -5):
            scope['CONFIG']['codearts_keepalive_minutes'] = off
            self.assertEqual(cfg(), 0, off)
        # 非法值 → 回退默认
        scope['CONFIG']['codearts_keepalive_minutes'] = "abc"
        self.assertEqual(cfg(), 1800.0)
        # 下限 60 秒，防把上游打爆
        scope['CONFIG']['codearts_keepalive_minutes'] = 0.1
        self.assertEqual(cfg(), 60.0)

        # 调用形状：对 codearts 发 POST /admin/api/keepalive，bearer 鉴权
        calls = []

        def fake_engine(up, method, path, body=None, auth=None, timeout=None):
            calls.append((up.get('name'), method, path, body, auth, timeout))
            return {'ok': True, 'message': '保活完成: 1 成功'}

        scope.update(_upstream_by_name=lambda n: {'name': 'codearts'} if n == 'codearts' else None,
                     platform_enabled=lambda n: True,
                     engine_request=fake_engine)
        out = scope['_codearts_keepalive_once']()
        self.assertTrue(out['ok'])
        self.assertEqual(calls, [('codearts', 'POST', '/admin/api/keepalive', {}, 'bearer', 120)])

        # 平台停用 → 跳过，不打引擎
        calls.clear()
        scope['platform_enabled'] = lambda n: False
        out = scope['_codearts_keepalive_once']()
        self.assertTrue(out['ok'])
        self.assertTrue(out.get('skipped'))
        self.assertEqual(calls, [])

        # 引擎拒绝 → ok False 带原因
        scope['platform_enabled'] = lambda n: True
        scope['engine_request'] = Mock(return_value={'ok': False, 'message': 'token 已失效'})
        out = scope['_codearts_keepalive_once']()
        self.assertFalse(out['ok'])
        self.assertIn('token', out['error'])

        # 传输异常 → ok False，不抛给线程
        def boom(*args, **kwargs):
            raise OSError('connection refused')
        scope['engine_request'] = boom
        out = scope['_codearts_keepalive_once']()
        self.assertFalse(out['ok'])
        self.assertIn('OSError', out['error'])

    def test_codearts_rejects_oversized_function_names(self):
        """codearts 的 function name ≤64 字符限制要在网关侧拦下。

        上游超限报 502（InferHub.001001017.413），引擎会把这次 502 当成账号
        异常、冷却 10 分钟——客户端带着 70+ 字符的 MCP 工具名时，每发一次就
        把唯一账号锁死一次，表现就是「codearts 用不了、反复冷却」。
        """
        from unittest.mock import Mock

        limit = self.ns["CODECARTS_FUNCTION_NAME_LIMIT"]
        self.assertEqual(limit, 64)
        over = "mcp__plugin_android-emulator_android-emulator__android_build_and_run"
        self.assertGreater(len(over), limit)

        names = self.ns["_oversized_function_names"]
        # 三种出现位置都要能抓到
        self.assertEqual(names({"tools": [{"type": "function", "function": {"name": over}}]}), [over])
        self.assertEqual(names({"tools": [{"name": over}]}), [over])          # 扁平形状
        self.assertEqual(names({"messages": [{"role": "assistant",
                                              "tool_calls": [{"function": {"name": over}}]}]}), [over])
        self.assertEqual(names({"messages": [{"role": "assistant",
                                              "function_call": {"name": over}}]}), [over])
        # 恰好 64 通过；65 拒绝；重复只报一次
        exact = "a" * limit
        self.assertEqual(names({"tools": [{"function": {"name": exact}}]}), [])
        dup = "a" * (limit + 1)
        self.assertEqual(names({"tools": [{"function": {"name": dup}},
                                          {"function": {"name": dup}}]}), [dup])
        # 畸形载荷不炸
        for bad in (None, {}, {"tools": "x", "messages": {}}, {"messages": [None, 1]}):
            self.assertEqual(names(bad), [])

        # do_POST 守卫：codearts + 超长名 → 400，且带上名字与限制值
        handler = object.__new__(self.ns["Handler"])
        handler.path = "/v1/chat/completions"
        handler.headers = {}
        handler.close_connection = False
        handler._authorized = lambda: True
        handler._read_post_object = lambda: {
            "model": "codearts/deepseek-v4.1-flash-1017",
            "tools": [{"type": "function", "function": {"name": over}}],
            "messages": [{"role": "user", "content": "hi"}]}
        handler._deny = Mock()
        upstream = {"name": "codearts", "base_url": "http://127.0.0.1:7866"}
        self.ns["resolve_model"] = lambda m: (upstream, m.split("/", 1)[-1], m, "")
        handler.do_POST()
        self.assertEqual(handler._deny.call_args.args[0], 400)
        message = handler._deny.call_args.args[1]
        self.assertIn("64", message)
        self.assertIn(over, message)
        self.assertIn("冷却", message)

    def test_automation_job_concurrent_reuse_and_retry_boundary(self):
        from concurrent.futures import ThreadPoolExecutor
        from types import SimpleNamespace
        from unittest.mock import Mock

        path = ROOT / "router.py"
        creation = threading.Barrier(2)
        worker = Mock()

        def make_id(size):
            creation.wait(timeout=5)
            return secrets.token_urlsafe(size)

        scope = isolated(path, {"Handler", "_automation_plan", "_automation_now",
                                "_automation_public", "_automation_run_step", "_automation_summary",
                                "_automation_safe_retry", "_request_target", "_media_download_allowed"}, {
            "CONFIG": {"upstreams": [{"name": "workbuddy"}, {"name": "trae"}]},
            "platform_enabled": lambda name: name != "trae", "hashlib": hashlib,
            "_AUTOMATION_STEP_SPECS": constant(path, "_AUTOMATION_STEP_SPECS"),
            "_AUTOMATION_JOB_LIMIT": 40, "_automation_jobs": {},
            "_automation_idempotency": {}, "_automation_lock": threading.RLock(),
            "urlsplit": __import__("urllib.parse", fromlist=["urlsplit"]).urlsplit,
            "_media_download_secret": b"offline-test-only",
            "_media_link_secret": b"offline-test-only",
            "secrets": SimpleNamespace(token_urlsafe=make_id),
            "threading": SimpleNamespace(Thread=worker),
        })

        def handler(body=None):
            obj = object.__new__(scope["Handler"])
            obj._post_body = body
            obj._send_json = lambda code, data: (code, data)
            obj._deny = lambda code, message: (code, message)
            return obj

        code, plan = handler()._api_automation_plan()
        self.assertEqual(code, 200)
        planned = {step["step_id"]: step for step in plan["steps"]}
        self.assertFalse(planned["trae.checkin"]["supported"])
        self.assertEqual(planned["qwenwork.auto-credit"]["kind"], "skip")
        selected = ["workbuddy.checkin", "workbuddy.scheduler", "qwenwork.auto-credit"]
        body = {"idempotency_key": "offline", "plan_version": plan["version"], "steps": selected}
        self.assertEqual(handler(dict(body, plan_version="stale"))._api_automation_job_post()[0], 409)
        self.assertEqual(handler(dict(body, steps=[]))._api_automation_job_post()[0], 400)
        self.assertEqual(handler(dict(body, steps=[selected[0], selected[0]]))._api_automation_job_post()[0], 400)
        self.assertEqual(handler(dict(body, steps=[{}]))._api_automation_job_post()[0], 400)
        self.assertEqual(handler(dict(body, steps=[True]))._api_automation_job_post()[0], 400)
        self.assertEqual(handler(dict(body, steps=["unlisted"]))._api_automation_job_post()[0], 400)
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(handler(body)._api_automation_job_post) for _ in range(2)]
            replies = [call.result(timeout=7) for call in calls]
        self.assertEqual(sorted(code for code, _ in replies), [200, 202])
        job_id = replies[0][1]["job"]["id"]
        self.assertEqual(replies[1][1]["job"]["id"], job_id)
        self.assertEqual(list(scope["_automation_jobs"]), [job_id])
        worker.return_value.start.assert_called_once()
        code, reused = handler(body)._api_automation_job_post()
        self.assertEqual((code, reused["reused"], reused["job"]["id"]), (200, True, job_id))
        missing_reader = object.__new__(scope["Handler"])
        missing_reader.path = "/api/automation/jobs"
        missing_reader.headers = {"Idempotency-Key": ""}
        missing_reader._deny = lambda code, message: (code, message)
        self.assertEqual(missing_reader.do_GET()[0], 400)
        class HeaderMap(dict):
            def get(self, name, default=None):
                return super().get(name.lower(), default)
        reader = object.__new__(scope["Handler"])
        reader.path = "/api/automation/jobs"
        reader.headers = HeaderMap({"idempotency-key": "offline"})
        reader._send_json = lambda code, data: (code, data)
        reader._deny = lambda code, message: (code, message)
        response = reader.do_GET()
        self.assertEqual(response[0], 200)
        self.assertEqual(response[1]["job"]["id"], job_id)
        job = scope["_automation_jobs"][job_id]
        job["steps"][0]["result"] = {"private": "synthetic-only"}
        code, public = handler()._api_automation_job_get(job_id)
        self.assertEqual(code, 200)
        self.assertNotIn("idempotency_key", public["job"])
        self.assertNotIn("result", public["job"]["steps"][0])
        self.assertEqual(handler()._api_automation_job_post(job_id, selected[0])[0], 409)
        job.update(status="failed")
        job["steps"][0].update(status="failed", retryable=True)
        self.assertEqual(handler()._api_automation_job_post(job_id, selected[0])[0], 202)
        self.assertEqual(job["steps"][0]["status"], "queued")
        with self.subTest("a failed scheduler submission is not accepted"):
            scope["_automation_call_step"] = lambda step: {"ok": False, "submitted": True}
            scope["_automation_run_step"](job_id, 1)
            self.assertEqual(job["steps"][1]["status"], "failed")
            self.assertFalse(job["steps"][1]["retryable"])
        with self.subTest("a deterministic checkin failure stays retryable"):
            # checkin 幂等（已签会返回已完成）：确定性 failed 可以安全重试；
            # 传输异常路径才是 unknown 且不可重试。
            scope["_automation_call_step"] = lambda step: {"ok": False, "error": "引擎拒绝签到"}
            job["steps"][0].update(status="queued")
            scope["_automation_run_step"](job_id, 0)
            self.assertEqual(job["steps"][0]["status"], "failed")
            self.assertTrue(job["steps"][0]["retryable"])

    def test_dashboard_account_rows_grouped_by_realm(self):
        """账号页按区域分组：组内标题行 + 平台内连续序号 + 单区域不插标题。

        实测引擎返回顺序是 cn,intl,intl,intl,cn（WorkBuddy），不分组时国内号被
        夹在国际号中间，靠「区域」列自己看容易串行。这里用合成 DOM 抽出
        renderPlatforms 跑一遍，钉死分组顺序、序号连续性、合计口径与单区域降级。
        """
        import subprocess
        script = r'''
const fs=require('node:fs'), vm=require('node:vm'), assert=require('node:assert/strict');
const html=fs.readFileSync(process.argv[2],'utf8');

// 按函数名精确截取（大括号配平）：正则非贪婪会在模板字符串里提前截断。
function extractFn(name){
  const start=html.indexOf('function '+name+'(');
  assert.ok(start>=0, name+' not found');
  let i=html.indexOf('{',start), depth=0, end=-1;
  for(;i<html.length;i++){
    const c=html[i];
    if(c==='{') depth++;
    else if(c==='}'){ depth--; if(depth===0){ end=i+1; break; } }
  }
  assert.ok(end>0, name+' unbalanced');
  return html.slice(start,end);
}

const stubs=`
const REALM_LABEL={cn:'国内版',intl:'国际版'};
const PLATFORM_LABEL={workbuddy:'WorkBuddy'};
let ACC_PLATFORMS=[];
function knownNumber(v){return (v===null||v===undefined||v===''||typeof v==='boolean'||!Number.isFinite(Number(v)))?null:Number(v);}
function fmt(n){return Math.round(n).toLocaleString();}
function esc(s){return String(s===null||s===undefined?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function realmPill(a){
  if(a.realm==='cn') return '<span class="rt cn">国内版</span>';
  if(a.realm==='intl') return '<span class="rt intl">国际版</span>';
  return '<span class="rt">-</span>';
}
function intlPunched(a){
  const today=new Date().toISOString().slice(0,10);
  return !!(a.last_daily_chat && String(a.last_daily_chat).slice(0,10)===today);
}
function safeHistoryText(s){return String(s===null||s===undefined?'':s);}
function balanceSummary(accounts){
  const active=accounts.filter(a=>a.enabled);
  const known=active.map(a=>knownNumber(a.remain)).filter(v=>v!==null);
  return {value:known.length?known.reduce((a,b)=>a+b,0):null,known:known.length,total:active.length};
}
var __store={};
const document={
  getElementById:(id)=>({
    value:'',
    get textContent(){return '';},
    set textContent(v){__store['txt:'+id]=v;},
    get innerHTML(){return __store[id]||'';},
    set innerHTML(v){__store[id]=v;},
  }),
  querySelectorAll:()=>[],
};
`;
const ctx={console,Math,Number,String,JSON,Date,Object,Array};
vm.createContext(ctx);
vm.runInContext(stubs+'\n'+extractFn('renderPlatforms'),ctx);
const store=vm.runInContext('__store',ctx);
const render=(platforms)=>{ctx.renderPlatforms(platforms);return store['platforms']||'';};

const today=new Date().toISOString();
// 真实形状：引擎顺序 cn,intl,intl,intl,cn
const mixed=render([{name:'workbuddy',health:true,inflight:0,base_url:'http://127.0.0.1:8788',
  observed_at:'2026-11-26T14:00:00',data_state:'ok',balance_state:'ok',accounts:[
  {name:'493ef753',realm:'cn',remain:1333.84,size:null,enabled:true,checked_in:true,can_checkin:false},
  {name:'384462',realm:'intl',remain:120,size:null,enabled:true,checked_in:null,last_daily_chat:today},
  {name:'linyanhe6@gmail.com',realm:'intl',remain:120,size:null,enabled:true,checked_in:null,last_daily_chat:today},
  {name:'c8509c44',realm:'intl',remain:120,size:null,enabled:true,checked_in:null,last_daily_chat:today},
  {name:'十二月',realm:'cn',remain:203.87,size:null,enabled:true,checked_in:false,can_checkin:true},
]}]);

// 序号在平台内连续（跨组不重排），从 1 开始
const seqs=[...mixed.matchAll(/<td class="num seq-cell">(\d+)<\/td>/g)].map(m=>m[1]);
assert.equal(seqs.join(','),'1,2,3,4,5','序号应平台内连续');
assert.ok(/<th scope="col" class="num seq-cell">#<\/th>/.test(mixed),'表头应有序号列');
// 两个分组标题，且国内在前
assert.equal((mixed.match(/class="realm-group"/g)||[]).length,2,'多区域应有 2 个分组标题');
assert.ok(mixed.indexOf('493ef753')<mixed.indexOf('384462'),'国内组应排在国际组之前');
// 标题行含账号数与合计（合计走 fmt 取整，与平台卡片口径一致）
assert.ok(/国内版[\s\S]{0,120}?2 个账号/.test(mixed),'国内标题应含账号数');
assert.ok(/国际版[\s\S]{0,120}?3 个账号/.test(mixed),'国际标题应含账号数');
assert.ok(/合计 1,538 分/.test(mixed),'国内合计应取整显示 1,538');
assert.ok(/合计 360 分/.test(mixed),'国际合计应为 360');
// 序号不能退化成 0/NaN
assert.ok(!/seq-cell">(0|NaN)/.test(mixed),'序号不应为 0 或 NaN');

// 单区域平台不插分组标题（只有一种区域时那行纯属噪音）
const single=render([{name:'workbuddy',health:true,inflight:0,base_url:'x',observed_at:'',
  data_state:'ok',balance_state:'ok',accounts:[
  {name:'a',realm:'cn',remain:10,size:null,enabled:true,checked_in:true},
  {name:'b',realm:'cn',remain:20,size:null,enabled:true,checked_in:true},
]}]);
assert.equal((single.match(/class="realm-group"/g)||[]).length,0,'单区域不应插分组标题');
assert.equal([...single.matchAll(/seq-cell">(\d+)</g)].map(m=>m[1]).join(','),'1,2','单区域仍要有序号');

console.log('dashboard accounts: realm grouping, continuous seq, single-realm fallback passed');
'''
        result = subprocess.run(['node', '-', str(ROOT / 'dashboard.html')], input=script,
                                text=True, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_dashboard_expiring_grouped_by_platform(self):
        """总览「积分到期」按平台分组：同平台的行不再被别的平台打断。

        全局按到期时间排序会把 WB 与 Trae 的行交错成
        「WB→Trae→WB×3→Trae→…」，同平台信息要跳着读。分组后每平台一块，
        块内仍按到期升序（紧迫度不丢），块顺序按该平台最早到期排。
        """
        import subprocess
        script = r'''
const fs=require('node:fs'), vm=require('node:vm'), assert=require('node:assert/strict');
const html=fs.readFileSync(process.argv[2],'utf8');
function extractFn(name){
  const start=html.indexOf('function '+name+'(');
  assert.ok(start>=0, name+' not found');
  let i=html.indexOf('{',start), depth=0, end=-1;
  for(;i<html.length;i++){
    const c=html[i];
    if(c==='{') depth++;
    else if(c==='}'){ depth--; if(depth===0){ end=i+1; break; } }
  }
  assert.ok(end>0, name+' unbalanced');
  return html.slice(start,end);
}
const stubs=`
const PLATFORM_LABEL={workbuddy:'WorkBuddy',trae:'Trae',raccoon:'小浣熊'};
function knownNumber(v){return (v===null||v===undefined||v===''||typeof v==='boolean'||!Number.isFinite(Number(v)))?null:Number(v);}
function fmt(n){return Math.round(n).toLocaleString();}
function esc(s){return String(s===null||s===undefined?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
var __store={};
const document={getElementById:(id)=>({value:'',get textContent(){return '';},set textContent(v){__store['txt:'+id]=v;},get innerHTML(){return __store[id]||'';},set innerHTML(v){__store[id]=v;}}),querySelectorAll:()=>[]};
`;
const ctx={console,Math,Number,String,JSON,Date,Object,Array};
vm.createContext(ctx);
vm.runInContext(stubs+'\n'+extractFn('renderExpiring'),ctx);
const store=vm.runInContext('__store',ctx);

// 真实交错形状：WB 12-13 → Trae 12-22 → WB 12-23 → Trae 12-23 → WB 12-25 → Trae 12-27
const rows=[
  {platform:'workbuddy',account:'493ef753',name:'CodeBuddy个人版拉新权益包',remain:82.07,expire:'2026-12-13 14:20:07',days_left:17.1},
  {platform:'trae',account:'十***',name:'签到奖励',remain:92.38,expire:'2026-12-22 00:29',days_left:25.5},
  {platform:'workbuddy',account:'384462',name:'Bonus Pack',remain:30,expire:'2026-12-23 01:52:24',days_left:26.6},
  {platform:'trae',account:'十***',name:'签到奖励',remain:100,expire:'2026-12-23 09:03',days_left:26.8},
  {platform:'workbuddy',account:'十二月',name:'裂变包',remain:100,expire:'2026-12-25 21:20:55',days_left:29.4},
  {platform:'trae',account:'十***',name:'签到奖励',remain:100,expire:'2026-12-27 12:44',days_left:31.0},
];
ctx.renderExpiring(rows,[]);
const out=store['expiring-list']||'';

assert.equal((out.match(/class="realm-group"/g)||[]).length,2,'两个平台应各有一个分组标题');
assert.ok(/WorkBuddy[\s\S]{0,60}?3 个临期包/.test(out),'WB 标题应含包数');
assert.ok(/Trae[\s\S]{0,60}?3 个临期包/.test(out),'Trae 标题应含包数');
// 同平台三行连续，中间不能夹别的平台
const wbIdx=[...out.matchAll(/CodeBuddy个人版拉新权益包|Bonus Pack|裂变包/g)].map(m=>m.index);
const trIdx=[...out.matchAll(/签到奖励/g)].map(m=>m.index);
assert.equal(wbIdx.length,3);
assert.ok(Math.max(...wbIdx)<Math.min(...trIdx),'WB 三行应连续，不被 Trae 打断');
// 组内仍按到期升序
const d1=out.indexOf('2026-12-13'), d2=out.indexOf('2026-12-23 01:52'), d3=out.indexOf('2026-12-25');
assert.ok(d1>0&&d1<d2&&d2<d3,'WB 组内应按到期升序');
// 更紧急的平台排在前（WB 最早 12-13 < Trae 最早 12-22）
assert.ok(out.indexOf('WorkBuddy')<out.indexOf('Trae'),'更紧急的平台应排在前面');
assert.ok(/合计 212 分/.test(out),'WB 合计 = 82.07+30+100 → 取整 212');

// 单平台不插分组标题
store['expiring-list']='';
ctx.renderExpiring([rows[0],rows[2]],[]);
const single=store['expiring-list']||'';
assert.equal((single.match(/class="realm-group"/g)||[]).length,0,'单平台不应插分组标题');

console.log('dashboard expiring: platform grouping, in-group expiry order, single-platform fallback passed');
'''
        result = subprocess.run(['node', '-', str(ROOT / 'dashboard.html')], input=script,
                                text=True, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_dashboard_offline(self):
        import subprocess
        # Run the actual inline script with a synthetic DOM, transport and clock.
        script = r'''
const fs=require('node:fs'), vm=require('node:vm'), assert=require('node:assert/strict');
const html=fs.readFileSync(process.argv[2],'utf8');
const source=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].map(m=>m[1]).join('\n');
const elements=new Map();
function element(id=''){
  const classes=new Set(id==='usage-statview'?['hidden']:[]);
  return {id,value:id==='usage-pagesize'?'10':'',textContent:'',innerHTML:'',style:{},dataset:{},options:[{text:'全部'}],
    classList:{contains:k=>classes.has(k),add:k=>classes.add(k),remove:k=>classes.delete(k),toggle(k,on){if(on)classes.add(k);else classes.delete(k);}},
    addEventListener(){},setAttribute(k,v){this.attrs=this.attrs||{};this.attrs[k]=v;this[k]=v;},removeAttribute(k){if(this.attrs)delete this.attrs[k];delete this[k];},focus(){},showModal(){this.open=true;},close(){this.open=false;},open:false,attrs:{},
    prepend(){},querySelector(){return null;},querySelectorAll(){return []}};
}
function el(id){if(!elements.has(id))elements.set(id,element(id));return elements.get(id);}
const navs=['overview','models'].map(v=>{const e=element();e.dataset.view=v;return e;});
const usageTabs=['records','stats'].map((v,i)=>{const e=element();e.dataset.utab=v;e.attrs={'aria-selected':String(i===0)};e.getAttribute=k=>e.attrs[k];return e;});
const views=['overview','models'].map(v=>el('view-'+v));
const copyButtons=[...html.matchAll(/data-media-copy="([^"]+)"/g)].map(m=>{
 const button=element();button.dataset.mediaCopy=m[1];
 const pre=html.match(new RegExp('<pre id="'+m[1]+'">([\\s\\S]*?)</pre>'));
 assert.ok(pre,'copy target must exist');
 el(m[1]).textContent=pre[1].replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');
 return button;
});
let copiedText='';
navs[0].classList.add('active');views[0].classList.add('active');
const store=new Map(), scheduledTimers=[];
let fetchImpl=async()=>({ok:true,status:200,json:async()=>({})});
const context=vm.createContext({console,Map,Set,Date,Promise,JSON,Number,String,Math,Array,Object,Error,AbortController,
 document:{getElementById:el,createElement:()=>element(),querySelectorAll:s=>s==='.nav'?navs:s==='.view'?views:s==='.usage-tab'?usageTabs:s==='[data-media-copy]'?copyButtons:[],querySelector:()=>null,
   documentElement:{attrs:{},setAttribute(k,v){this.attrs[k]=v;}}},
 localStorage:{getItem:k=>store.get(k),setItem:(k,v)=>store.set(k,v),removeItem:k=>store.delete(k)},
 fetch:(...a)=>fetchImpl(...a),setInterval:()=>1,clearInterval(){},setTimeout:(fn,ms)=>{scheduledTimers.push({fn,ms});return scheduledTimers.length;},clearTimeout(){},
 requestAnimationFrame(){},navigator:{clipboard:{writeText:async text=>{copiedText=text;}}},CSS:{escape:x=>x}});
vm.runInContext(source,context);
const run=s=>vm.runInContext(s,context);
const reply=(data,status=200)=>({ok:status>=200&&status<300,status,json:async()=>data});
(async()=>{
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(copyButtons.length,4);
 let copyRequests=0;
 fetchImpl=async()=>{copyRequests++;throw new Error('copy must not submit a request');};
 for(const button of copyButtons){
   assert.equal(typeof button.onclick,'function');
   await button.onclick();await new Promise(resolve=>setImmediate(resolve));
   assert.match(copiedText,/<GATEWAY_KEY>/);
   assert.equal(button.textContent,'已复制 ✓');
 }
 assert.equal(copyRequests,0);
 store.set('gh_key','synthetic');
 fetchImpl=async()=>reply({error:{message:'unavailable'}},503);
 assert.equal(await run("refreshView('models',true)"),false);
 assert.equal(run("VIEW_LOADED.has('models')"),false);
 assert.match(el('load-state-models').innerHTML,/加载失败/);
 fetchImpl=async()=>reply({ok:true,items:[],hidden:[]});
 assert.equal(await run("refreshView('models')"),true);
 assert.equal(run("VIEW_LOADED.has('models')"),true);
 let calls=0,release;
 fetchImpl=()=>{calls++;return new Promise(resolve=>release=resolve);};
 const a=run("api('/api/ping')"), b=run("api('/api/ping')");
 assert.equal(calls,1);release(reply({ok:true}));await Promise.all([a,b]);
 fetchImpl=async()=>reply({ok:true,result:{}});
 run("VIEW_LOADED.set('accounts',123);VIEW_LOADED.set('models',123);VIEW_LOADED.set('settings',123)");
 await run("api('/api/engine',{method:'POST',body:JSON.stringify({method:'GET',path:'scheduler'})})");
 assert.equal(run("VIEW_LOADED.get('models')"),123);
 await run("api('/api/checkin/fake',{method:'POST'})");
 assert.equal(run("VIEW_LOADED.has('accounts')"),false);
 assert.equal(run("VIEW_LOADED.get('models')"),123);
 assert.equal(run("VIEW_LOADED.get('settings')"),123);
 fetchImpl=async()=>reply({error:{message:'wrong key'}},401);
 el('gate-input').value='bad';await run('tryGate()');
 assert.equal(el('gate-err').style.display,'block');assert.match(el('gate-err').textContent,/401/);
 store.set('gh_key','synthetic');
 // Platform toggles: disabled platform renders, toggle posts the new state.
 let platformPosts=[];
 fetchImpl=async (p,opt)=>{
   if(p==='/api/platforms'){
     if(opt&&opt.method==='POST'){platformPosts.push(JSON.parse(opt.body));return reply({ok:true});}
     return reply({ok:true,platforms:[
       {name:'qoder',enabled:false,launch_configured:false,process:{managed:false,running:false,pid:null}},
       {name:'workbuddy',enabled:true,launch_configured:true,process:{managed:true,running:true,pid:123}}]});
   }
   return reply({});
 };
 await run('loadPlatforms()');
 assert.match(el('platform-list').innerHTML,/Qoder/);
 assert.match(el('platform-list').innerHTML,/未运行/);
 assert.match(el('platform-list').innerHTML,/网关托管/);
	 await run("togglePlatform('qoder',true)");
	 assert.equal(platformPosts.length,1);
	 assert.equal(platformPosts[0].name,'qoder');
	 assert.equal(platformPosts[0].enabled,true);
	 const fakeToggle=run("globalThis.__fakeToggle={checked:false,disabled:false,isConnected:true}");
	 run('confirmAction=async()=>false');
	 await run("togglePlatform('qoder',false,__fakeToggle)");
	 assert.equal(platformPosts.length,1,'cancel must not post platform disable');
	 assert.equal(run('__fakeToggle.checked'),true,'cancel restores the route toggle');
	 run('confirmAction=async()=>true');
	 fetchImpl=async (p,opt)=>{
	   if(p==='/api/platforms'&&opt&&opt.method==='POST'){
	     platformPosts.push(JSON.parse(opt.body));
	     return reply({ok:true,process_ok:false,warning:'safe stop unavailable',process:{ok:false,error:'busy'}});
	   }
	   if(p==='/api/platforms') return reply({ok:true,platforms:[
	     {name:'qoder',enabled:false,launch_configured:false,warning:'safe stop unavailable',process:{managed:false,running:false,state:'unknown'}}]});
	   return reply({});
	 };
	 await run("togglePlatform('qoder',false,__fakeToggle)");
	 assert.equal(platformPosts.length,2);
	 assert.equal(platformPosts[1].enabled,false);
	 assert.match(el('platform-list').innerHTML,/safe stop unavailable/,'failed stop is retained inline');
	 // Media catalog shares the authoritative hidden set with chat models.
	 run("confirmAction=async()=>true;MEDIA={items:[{id:'workbuddy/image-one',model:'image-one',name:'Image One',platform:'workbuddy',kind:'image',tags:['text-to-image'],realms:['cn'],hidden:false}],hidden:[],errors:{}};MEDIA_UNDO=[];MEDIA_SELECTED.clear();renderMedia()");
	 assert.match(el('media-models').innerHTML,/workbuddy\/image-one/);
	 fetchImpl=async(p,opt)=>p==='/api/models/visibility'
	   ?reply({ok:true,hidden:['workbuddy/image-one']}):reply({});
	 await run("setMediaVisible('workbuddy/image-one',false,null)");
	 assert.deepEqual(Array.from(run('MEDIA.hidden')),['workbuddy/image-one']);
	 assert.deepEqual(Array.from(run('MEDIA_UNDO')),['workbuddy/image-one']);
	 assert.match(el('media-action-result').textContent,/撤销/);
	 run("document.getElementById('media-visfilter').value='hidden';renderMedia()");
	 assert.match(el('media-models').innerHTML,/已隐藏/);
	 assert.equal(el('btn-media-hidden').classList.contains('hidden'),false);
	 fetchImpl=async(p,opt)=>p==='/api/models/visibility'
	   ?reply({ok:true,hidden:[]}):reply({});
	 await run("document.getElementById('btn-media-undo').onclick()");
	 assert.deepEqual(Array.from(run('MEDIA.hidden')),[]);
	 assert.deepEqual(Array.from(run('MEDIA_UNDO')),[]);
	 assert.match(el('media-action-result').textContent,/已恢复显示/);
	 // CSV quoting covers CR/LF, commas and quotes exactly as CSV requires.

 const cr=run('csvCell(String.fromCharCode(65,13,66))');assert.equal(cr.length,5);assert.equal(cr.charCodeAt(0),34);assert.equal(cr.charCodeAt(2),13);assert.equal(cr.charCodeAt(4),34);
 const lf=run('csvCell(String.fromCharCode(65,10,66))');assert.equal(lf.length,5);assert.equal(lf.charCodeAt(2),10);
 assert.equal(run('csvCell("A,B")'),'"A,B"');
 assert.equal(run('csvCell("A\\\"B")'),'"A""B"');
 let rows=Array.from({length:25},(_,i)=>({model:'m',platform:'fake',total:10,at:i,outcome:'ok',credit:null}));
 fetchImpl=async p=>reply(p.startsWith('/api/usage')?{rows}:{items:[]});
 run('UPAGE=2');await run('loadUsage()');assert.equal(run('UPAGE'),2);
 rows=rows.slice(0,3);await run('loadUsage()');assert.equal(run('UPAGE'),1);
 // Catalog failure must not count as a successful usage refresh.
 run("VIEW_LOADED.delete('usage')");
 fetchImpl=async p=>p.startsWith('/api/usage')?reply({ok:true,rows:[]})
   :reply({error:{message:'catalog down'}},503);
 assert.equal(await run("refreshView('usage',true)"),false);
 assert.equal(run("VIEW_LOADED.has('usage')"),false);
 assert.match(el('load-state-usage').innerHTML,/加载失败/);
 fetchImpl=async p=>reply(p.startsWith('/api/usage')?{rows}:{items:[]});
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:null,known:0,total:1,models:[{model:'qwen4-max',rate_per_mtok:null,rate_source:'unknown',est_tokens:null,note:'余额待测，暂不估算',samples:0}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/待测/);
 assert.match(el('overview-estimate').innerHTML,/0\/1 账号/);
 // 余额为 null 时不能渲染成「0 分」（卡片版把数值与单位分成两个 span）
 assert.doesNotMatch(el('overview-estimate').innerHTML,/<span class="est-bal">0<\/span>/);
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:0,known:1,total:1,models:[{model:'qwen4-max',rate_per_mtok:51.02,rate_source:'anchored',est_tokens:0,note:'短样本相对价 × 钱包实测均价锚定',samples:3,sample_tokens:1000000,sample_hours:24}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/<span class="est-bal">0<\/span>/);
 assert.match(el('overview-estimate').innerHTML,/51\.02/);
 assert.match(el('overview-estimate').innerHTML,/Mtok/);
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:100,known:1,total:1,models:[{model:'qwen4-max',rate_per_mtok:51.02,rate_source:'anchored',est_tokens:1960015,note:'短样本相对价 × 钱包实测均价锚定',samples:3,sample_tokens:1000000,sample_hours:24}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/1\.96M/);
 assert.doesNotMatch(el('overview-estimate').innerHTML,/全未命中下限|乐观/);
 // Manual refresh must force the estimate; the 30s auto refresh must keep the 5-minute cache.
 let estimateCalls=[];
 fetchImpl=async p=>{
   if(p==='/api/status') return reply({ok:true,platforms:[],expiring:[],
     kpi:{platforms_ok:0,platforms_total:0,accounts:0,checked_in:0,checkable:0}});
   if(p.startsWith('/api/estimate')){
     estimateCalls.push(p);
     return reply({ok:true,platforms:[],generated_at:Math.floor(Date.now()/1000),
       unit_note:'单位不可比',sample_basis:'按模型实扣'});
   }
   if(p.startsWith('/api/usage')) return reply({ok:true,rows:[],errors:{}});
   return reply({});
 };
 run("EST_CACHE={ts:0,data:null};VIEW_LOADED.delete('overview')");
 await run("refreshView('overview', true)");
 assert.equal(estimateCalls.length,1);
 assert.match(estimateCalls[0],/force=1/);
 assert.match(el('overview-estimate-note').textContent,/预估生成于/);
 // Auto refresh: no force, estimate served from cache.
 run("VIEW_LOADED.set('overview', Date.now()-20000)");
 await run("refreshView('overview')");
 assert.equal(estimateCalls.length,1);
 run("globalThis.__toasts=[];toast=m=>__toasts.push(m)");
 fetchImpl=async()=>reply({ok:false,platform:'qoder',checked:0,total:2,
   error:'0/2 个账号成功',result:{results:[{uid:'a'},{uid:'b'}]}});
 await run("doCheckin('qoder',null)");
 assert.match(run("__toasts.join('|')"),/签到失败/);
 assert.doesNotMatch(run("__toasts.join('|')"),/签到成功|\[Qoder\]/);
 run("renderUsageConversion([{platform:'fake',model:'m',total:100,credit:10},{platform:'fake',model:'m',total:900,cost_cny:1}])");
 assert.match(el('usage-conv').innerHTML,/100\.0000/);
 assert.match(el('usage-conv').innerHTML,/积分覆盖 1\/2/);
 assert.match(el('usage-conv-note').textContent,/100\.0000/);
 // Usage charts are part of the records view and must render on first load.
 run("USAGE={rows:[{at:100,platform:'workbuddy',prompt:10,completion:4,total:14,cached:0},{at:200,platform:'trae',prompt:20,completion:5,total:25,cached:5}],usd_cny:null};renderUsage();");
 assert.match(el('usage-charts').innerHTML,/Token 使用/);
 assert.match(el('usage-cache').innerHTML,/缓存命中率/);
 assert.equal(el('usage-statview').classList.contains('hidden'),true);
 el('usage-records').classList.add('hidden');el('usage-statview').classList.remove('hidden');
 run('renderUsage()');
 assert.match(el('usage-cache').innerHTML,/缓存命中率/);
 assert.match(el('usage-charts').innerHTML,/Trae/);
 // Unreported cache (cached=null) must read as unknown, not 0% — and such a
 // platform belongs in the "no curve" list instead of a flat 0% line.
 run("USAGE={rows:[{at:100,platform:'workbuddy',prompt:10,completion:4,total:14,cached:0},{at:200,platform:'workbuddy',prompt:20,completion:5,total:25,cached:5},{at:300,platform:'trae',prompt:80000,completion:200,total:80200,cached:null}],usd_cny:null};renderUsage();");
 assert.match(el('usage-cache').innerHTML,/Trae 上游不回传缓存，无曲线/);
 assert.doesNotMatch(el('usage-cache').innerHTML,/— Trae/);
 assert.match(el('usage-stats').innerHTML,/缓存数据覆盖 \d+%/);
 const gwCoverage=run("(()=>{const rows=USAGE.rows;const known=rows.filter(r=>r.cached!=null).reduce((n,r)=>n+(r.prompt||0),0);const prompt=rows.reduce((n,r)=>n+(r.prompt||0),0);return Math.round(known/prompt*100);})()");
 assert.equal(gwCoverage,0);
 run("USAGE={rows:[{at:100,platform:'trae',prompt:80000,completion:200,total:80200,cached:null},{at:200,platform:'trae',prompt:90000,completion:300,total:90300,cached:null}],usd_cny:null};renderUsage();");
 assert.doesNotMatch(el('usage-cache').innerHTML,/缓存命中率 [0-9]/);
 assert.match(el('usage-cache').innerHTML,/无曲线/);
 assert.match(el('usage-body').innerHTML,/缓存未知/);
 assert.doesNotMatch(el('usage-body').innerHTML,/缓存 0%/);
 run("USAGE={rows:[{at:100,platform:'trae',model:'m',prompt:1000,completion:5,total:1005,cached:null}],usd_cny:null};renderUsage();renderUsagePerModel(USAGE.rows)");
 assert.match(el('usage-permodel').innerHTML,/未知/);
 run("globalThis.__toasts=[];toast=m=>__toasts.push(m)");
 run("renderUsageConversion([{platform:'raccoon',model:'sn-qwen4-max',total:1000,credit:null,cost_cny:null}])");
 assert.match(el('usage-conv').innerHTML,/小浣熊/);
 assert.match(el('usage-conv').innerHTML,/上游未提供逐条积分/);
 assert.match(el('usage-conv-note').textContent,/积分未知/);
 assert.equal(run("balanceSummary([{enabled:true,remain:null}]).value"),null);
 assert.equal(run("balanceSummary([{enabled:true,remain:0}]).value"),0);
 run("renderUsageStats([{credit:0}])");assert.match(el('usage-stats').innerHTML,/0\.00/);
 run("pushHistory('partial',{ok:false,partial:true},'detail','');pushHistory('skip','skipped','detail','')");
	 assert.match(el('task-history').innerHTML,/部分失败/);assert.match(el('task-history').innerHTML,/已跳过/);
	 assert.equal(run("localStorage.getItem('gh_task_history_v1')!==null"),true);
	 assert.ok(JSON.parse(run("localStorage.getItem('gh_task_history_v1')")).length<=20);
	 assert.equal(run('TASK_HISTORY.some(h=>Object.hasOwn(h,"detail"))'),false);
	 run("USAGE={rows:[{at:1,platform:'trae',model:'m',prompt:1,completion:1,total:2,cached:null}],generated_at:'2026-11-20T10:00:00+0000',requested_limit:500,returned:500,truncated:true,coverage:{trae:{all:{ok:true,rows:500}}},failure_coverage:{complete:false,note:'Trae 上游仅记录成功请求'},errors:{}};renderUsage()");
	 assert.match(el('usage-coverage').textContent,/最多 500 条/);
	 assert.match(el('usage-coverage').textContent,/已截断/);
	 assert.match(el('usage-coverage').textContent,/成功请求/);
	 assert.equal(el('usage-records').attrs['aria-hidden'],'false');
	 assert.equal(el('usage-statview').attrs['aria-hidden'],'true');
	 run("selectUsageTab(usageTabs[1])");
	 assert.equal(el('usage-records').attrs['aria-hidden'],'true');
	 assert.equal(el('usage-statview').attrs['aria-hidden'],'false');
	 const plan={version:'offline-v1',steps:[{step_id:'workbuddy.checkin',label:'WorkBuddy · 签到',kind:'checkin',platform:'workbuddy',supported:true,dynamic:true},{step_id:'qwenwork.auto-credit',label:'千问办公 · 上游自动发放',kind:'skip',platform:'qwenwork',supported:false,skip_reason:'每日积分由上游自动发放'}]};
	 let automationPosts=[];
	 fetchImpl=async(p,opt)=>{
	   if(p==='/api/automation/plan') return reply({ok:true,...plan});
	   if(p==='/api/automation/jobs'&&opt?.method==='POST'){
	     const body=JSON.parse(opt.body);automationPosts.push(body);
	     return reply({ok:true,job:{id:'job-offline',status:'succeeded',completed:2,counts:{succeeded:1,skipped:1,failed:0,partial:0,unknown:0,submitted:0},steps:[{step_id:'workbuddy.checkin',label:'WorkBuddy · 签到',status:'succeeded',summary:'已执行'},{step_id:'qwenwork.auto-credit',label:'千问办公 · 上游自动发放',status:'skipped',summary:'每日积分由上游自动发放'}]}});
	   }
	   return reply({ok:true});
	 };
	 run('confirmAction=async()=>true;loadAuto=async()=>true;loadStatus=async()=>true;loadTasksStatus=async()=>true');
	 await run('loadAutomationPlan()');
	 assert.equal(run('AUTOMATION_PLAN.version'),'offline-v1');
	 await run('startAutomationJob(null,undefined,false)');
	 assert.equal(automationPosts.length,1);
	 assert.equal(automationPosts[0].plan_version,'offline-v1');
	 assert.ok(automationPosts[0].idempotency_key);
	 assert.deepEqual(automationPosts[0].steps,['workbuddy.checkin','qwenwork.auto-credit']);
	 assert.match(el('automation-job-status').textContent,/已完成/);
	 assert.equal(store.get('gh_automation_job_id_v1'),'job-offline');
	 assert.equal(JSON.parse(store.get('gh_task_history_v1'))[0].jobId,'job-offline');
	 // Cross-reload guard: a restored job has only an id, so start must refresh
	 // its status first instead of submitting a second job.
	 fetchImpl=async p=>p==='/api/automation/jobs/job-offline'
	   ?reply({ok:true,job:{id:'job-offline',status:'succeeded',completed:2,counts:{succeeded:1,skipped:1,failed:0,partial:0,unknown:0,submitted:0},steps:[]}}):reply({ok:true});
	 run("AUTOMATION_JOB={id:'job-offline'};AUTOMATION_IDEMPOTENCY=null");
	 await run('startAutomationJob(null,undefined,false)');
	 assert.equal(automationPosts.length,1,'restore-then-refresh must not double submit');
	 assert.equal(run('AUTOMATION_JOB?.status'),'succeeded');
	 // 404 on the stored job clears the dead key instead of looping forever.
	 fetchImpl=async p=>p==='/api/automation/jobs/job-offline'?reply({ok:false,error:{message:'gone'}},404):reply({ok:true});
	 await run('pollAutomationJob("job-offline",true).catch(()=>{})');
	 assert.equal(run('AUTOMATION_JOB'),null);
	 assert.equal(store.get('gh_automation_job_id_v1'),undefined);
	 // A failed/partial toast must not repeat on every 30s auto refresh.
	 run("globalThis.__toasts=[];toast=m=>__toasts.push(m)");
	 const partialJob={id:'job-p',status:'partial',completed:2,counts:{succeeded:1,skipped:0,failed:1,partial:0,unknown:0,submitted:0},steps:[]};
	 fetchImpl=async p=>reply({ok:true,job:partialJob});
	 await run('pollAutomationJob("job-p")');
	 await run('pollAutomationJob("job-p")');
	 assert.equal(run("__toasts.filter(t=>t.includes('一键任务')).length"),1);
	 // Media URLs are protocol-checked before they reach href/src.
	 assert.equal(run('safeMediaUrl("javascript:alert(1)")'),'');
	 assert.equal(run('safeMediaUrl("http://x/y.mp4")'),'http://x/y.mp4');
	 assert.equal(run('safeMediaUrl("data:image/png;base64,AAAA")'),'data:image/png;base64,AAAA');
	 // Media page renders both kinds and switches the form between image/video.

	 run("document.getElementById('media-visfilter').value='visible';MEDIA={items:[{id:'workbuddy/hunyuan-image-v3.0-art',model:'hunyuan-image-v3.0-art',name:'Hunyuan',kind:'image',tags:['text-to-image'],realms:['cn'],credit_per_image:6,image_measured:false,hidden:false},{id:'workbuddy/kling-video-v3',model:'kling-video-v3',name:'Kling',kind:'video',tags:['text-to-video'],realms:['cn','intl'],credits_per_second:{'720P':10},hidden:false}],errors:{},hidden:[]};renderMedia()");

 assert.match(el('media-models').innerHTML,/hunyuan-image-v3\.0-art/);
 assert.match(el('media-models').innerHTML,/积分\/张/);
 assert.match(el('media-models').innerHTML,/积分\/秒/);
 run("document.getElementById('media-model').value='workbuddy/hunyuan-image-v3.0-art';updateMediaForm()");
 assert.equal(run("currentMediaModel().kind"),'image');
 assert.equal(el('media-video-opts').style.display,'none');
 run("document.getElementById('media-model').value='workbuddy/kling-video-v3';updateMediaForm()");
 assert.equal(el('media-video-opts').style.display,'flex');
 assert.equal(run("DAILY_STEPS.some(s=>s.platform==='workbuddy'&&s.path==='scheduler/trigger')"),true);
 assert.equal(run("mediaPrice({media:'image',credit_per_image:6})"),'6 积分/张（估）');
 // Expiring table keeps "expires in" and "usable at current burn rate" apart.
 run("renderExpiring([{platform:'raccoon',account:'nick',name:'积分',remain:9421,expire:'2026-11-25 23:59',days_left:6.4,burn_days:374.7},{platform:'trae',account:'u',name:'每月登录赠送',remain:372,expire:'2026-11-30 23:59',days_left:11.4,burn_days:null}])");
 assert.match(el('expiring-list').innerHTML,/距过期/);
 assert.match(el('expiring-list').innerHTML,/按当前速度可用/);
 assert.match(el('expiring-list').innerHTML,/7 天/);        // 6.4 → ceil 7，不是 374
 assert.match(el('expiring-list').innerHTML,/375 天/);      // 374.7 → ceil 375，只出现在速度列
 assert.match(el('expiring-list').innerHTML,/12 天/);
	 run("renderExpiring([])");
	 assert.match(el('expiring-list').innerHTML,/暂无已知过期信息/);
	 // Engine error marks surface on the account rows: 6004 cap with the
	 // upstream reset wall clock, plus panel-disabled models.
	 run("renderPlatforms([{name:'workbuddy',health:true,observed_at:'2026-11-22T17:00:00+0800',accounts:[{name:'十二月',uid:'dd31a37f',realm:'cn',enabled:true,remain:938,size:1000,last_error:'stream limit (model throttled)',model_caps:{'deepseek-v4.1-flash':{at:111,reset:222}},disabled_models:[]}]}])");
	 assert.match(el('platforms').innerHTML,/配额耗尽/);
	 assert.match(el('platforms').innerHTML,/deepseek-v4\.1-flash/);
	 assert.match(el('platforms').innerHTML,/stream limit/);
 // Trae platform wiring: label, sidebar link, add-account card, filters, automation.
 assert.equal(run("PLATFORM_LABEL.trae"),'Trae');
 assert.equal(run("PF_COLOR.trae"),'#0dbd8b');
 assert.equal(run("!!AUTO_BUTTONS.trae"),true);
 assert.equal(run("AUTO_BUTTONS.trae[0].items.some(i=>i.k==='checkin/trae')"),true);
 assert.equal(run("DAILY_STEPS.some(s=>s.platform==='trae'&&s.kind==='checkin')"),true);
 assert.equal(html.includes('data-engine-link="trae"'),true);
 assert.equal(html.includes('id="trae-start"'),true);
  assert.equal(html.split('<option value="trae">Trae</option>').length-1,3);

 // Navigation paints immediately even when requests never settle.
 fetchImpl=()=>new Promise(()=>{});navs[1].onclick();
 assert.equal(views[1].classList.contains('active'),true);
 assert.equal(views[0].classList.contains('active'),false);
 // Gateway self-update card: render, confirm gate, preflight → restart → recovery.
 run("globalThis.__toasts=[];toast=m=>__toasts.push(m)");
 const gwSnap=(job,extra)=>Object.assign({ok:true,current:'1.10.0',
   repo:{is_repo:true,clean:true,branch:'main',origin_ok:true},
   latest:{tag:'v1.11.0',version:'1.11.0',commit:'c'.repeat(40)},
   has_update:true,error:'',job:job||{status:'idle'}},extra||{});
 let gwPosts=[];
 const gwFetch=job=>async(p,opt)=>{
   if(p==='/api/gateway-update'&&opt&&opt.method==='POST'){gwPosts.push(JSON.parse(opt.body));return reply({ok:true,status:'preflight'},202);}
   if(p.startsWith('/api/gateway-update')) return reply(gwSnap(job));
   return reply({});
 };
 fetchImpl=gwFetch({status:'idle'});
 await run('loadGatewayUpdate()');
 assert.match(el('gw-update-info').innerHTML,/v1\.10\.0/);
 assert.match(el('gw-update-info').innerHTML,/v1\.11\.0/);
 assert.equal(el('btn-gw-update-deploy').disabled,false);
	 run("confirmAction=async()=>false");
	 await el('btn-gw-update-deploy').onclick();
	 assert.equal(gwPosts.length,0);
	 run("confirmAction=async()=>true");
 fetchImpl=gwFetch({status:'preflight',version:'1.11.0'});
 await el('btn-gw-update-deploy').onclick();
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(gwPosts.length,1);
 assert.deepEqual(gwPosts[0],{confirm:true});
 assert.equal(el('btn-gw-update-deploy').disabled,true);
 assert.match(el('gw-update-info').innerHTML,/预检中/);
 fetchImpl=gwFetch({status:'scheduled',version:'1.11.0'});
 await run('loadGatewayUpdate()');
 assert.equal(run('gwUpdateRestarting'),true);
 fetchImpl=async()=>{throw new Error('connection refused');};
 await run('loadGatewayUpdate()');
 assert.match(el('gw-update-info').textContent,/重启中/);
 fetchImpl=async p=>p.startsWith('/api/gateway-update')
   ?reply(gwSnap({status:'idle'},{current:'1.11.0',has_update:false})):reply({});
 await run('loadGatewayUpdate()');
 assert.equal(run('gwUpdateRestarting'),false);
 assert.match(run("__toasts.join('|')"),/已更新到 v1\.11\.0/);
 fetchImpl=async p=>p.startsWith('/api/gateway-update')
   ?reply(gwSnap({status:'idle'},{repo:{is_repo:true,clean:false,branch:'main',origin_ok:true}})):reply({});
 await run('loadGatewayUpdate()');
 assert.equal(el('btn-gw-update-deploy').disabled,true);
 assert.match(el('gw-update-info').innerHTML,/未提交改动/);
 // Theme: default light without matchMedia; toggle persists an explicit choice.
 assert.equal(run("document.documentElement.attrs['data-theme']"),'light');
 assert.equal(el('theme-icon-moon').style.display,'block');
 assert.equal(el('theme-icon-sun').style.display,'none');
 assert.equal(el('btn-theme').title,'切换到深色模式');
 el('btn-theme').onclick();
 assert.equal(store.get('gh_theme'),'dark');
 assert.equal(run("document.documentElement.attrs['data-theme']"),'dark');
 assert.equal(el('theme-icon-sun').style.display,'block');
 assert.equal(el('theme-icon-moon').style.display,'none');
 assert.equal(el('btn-theme').title,'切换到浅色模式');
 el('btn-theme').onclick();
 assert.equal(store.get('gh_theme'),'light');
 assert.equal(run("document.documentElement.attrs['data-theme']"),'light');
 // Startup must not force a full estimate recomputation on every page load.
 run("globalThis.__refreshCalls=[];refreshView=(view,force)=>{__refreshCalls.push({view,force});return Promise.resolve(true)};loadAll()");
 assert.equal(run('JSON.stringify(__refreshCalls)'),JSON.stringify([{view:'overview',force:false}]));
 // Estimate is an independent panel: slow estimate must not hold status/recent rows loading.
 run("const realApi=api;globalThis.__finishEstimate=null;loadStatus=async()=>true;loadEstimate=()=>new Promise(resolve=>__finishEstimate=resolve);api=async()=>({rows:[],errors:{}})");
 let overviewDone=false;
 const overviewPromise=run('loadOverview(false)').then(()=>{overviewDone=true;});
 await new Promise(resolve=>setImmediate(resolve));
 const overviewReturnedBeforeEstimate=overviewDone;
 run('__finishEstimate()');
 await overviewPromise;
 assert.equal(overviewReturnedBeforeEstimate,true);
 // Read APIs have a finite abort deadline; a stalled fetch must become a retryable error.
 const timersBefore=scheduledTimers.length;
 fetchImpl=(_p,opt)=>new Promise((resolve,reject)=>opt.signal.addEventListener('abort',()=>reject(Object.assign(new Error('Aborted'),{name:'AbortError'}))));
 const slowResult=run("realApi('/api/slow')").then(()=>({ok:true}),e=>({ok:false,error:e.message}));
 await new Promise(resolve=>setImmediate(resolve));
 const timeoutMs=run('API_READ_TIMEOUT_MS');
 const timeoutTimer=scheduledTimers.slice(timersBefore).find(t=>t.ms===timeoutMs);
 assert.ok(timeoutTimer,'read API should schedule its timeout '+JSON.stringify({timeoutMs,timers:scheduledTimers.slice(timersBefore).map(t=>t.ms)}));
 timeoutTimer.fn();
 const timeoutResult=await slowResult;
 assert.equal(timeoutResult.ok,false);
 assert.match(timeoutResult.error,/超时/);
 console.log('dashboard offline: 503/retry, single-flight, invalidation, 401, pagination, cost coverage, unknown, result states, navigation, self-update, theme, load latency passed');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(['node', '-', str(ROOT / 'dashboard.html')], input=script,
                                text=True, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_gateway_update_deploy_requires_confirm_and_update(self):
        import sys
        import types
        from unittest.mock import Mock
        calls = []
        stub = types.ModuleType("gateway_update")
        stub.schedule_update = lambda *a, **k: calls.append(a) or {
            "ok": True, "status": "scheduled", "version": "1.11.0", "commit": "c" * 40}
        shutdowns = []
        server = type("S", (), {"shutdown": lambda self: shutdowns.append(1)})()
        scope = isolated(ROOT / "router.py",
                         {"Handler", "_gateway_update_deploy", "_gateway_update_deploy_thread",
                          "_gateway_update_job_state", "_gateway_update_error"},
                         {"threading": threading, "os": __import__("os"), "HERE": str(ROOT),
                          "_gateway_update_lock": threading.Lock(),
                          "_gateway_update_job": {"status": "idle", "version": "", "commit": "",
                                                  "error": "", "ts": 0.0},
                          "_SERVER": server,
                          "_gateway_update_snapshot": lambda force=False: {
                              "ok": True, "current": "1.10.0", "has_update": True,
                              "latest": {"tag": "v1.11.0", "version": "1.11.0", "commit": "c" * 40}}})
        old = sys.modules.get("gateway_update")
        sys.modules["gateway_update"] = stub
        try:
            handler = scope["Handler"].__new__(scope["Handler"])
            handler._send_json = Mock()
            handler._deny = Mock()
            handler._post_body = {}
            handler._api_gateway_update_deploy()
            handler._deny.assert_called_once()
            self.assertEqual(handler._deny.call_args.args[0], 400)
            self.assertEqual(calls, [])
            handler._post_body = {"confirm": True}
            handler._api_gateway_update_deploy()
            handler._send_json.assert_called_once()
            self.assertEqual(handler._send_json.call_args.args[0], 202)
            deadline = time.time() + 5
            while scope["_gateway_update_job_state"]()["status"] != "scheduled" and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(scope["_gateway_update_job_state"]()["status"], "scheduled")
            self.assertEqual(len(calls), 1)
            deadline = time.time() + 5
            while not shutdowns and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(shutdowns, [1])
            with self.assertRaises(ValueError):
                scope["_gateway_update_deploy"]({"ok": True, "has_update": True,
                                                 "latest": {"version": "1.11.0"}})
        finally:
            if old is None:
                sys.modules.pop("gateway_update", None)
            else:
                sys.modules["gateway_update"] = old

    def test_gateway_update_deploy_preflight_failure_is_retryable(self):
        import sys
        import types
        from unittest.mock import Mock
        attempts = []
        stub = types.ModuleType("gateway_update")
        def schedule(root, snapshot, parent_pid, **kw):
            attempts.append(1)
            if len(attempts) == 1:
                raise ValueError("candidate regression failed")
            return {"ok": True, "status": "scheduled", "version": "1.11.0", "commit": "c" * 40}
        stub.schedule_update = schedule
        shutdowns = []
        server = type("S", (), {"shutdown": lambda self: shutdowns.append(1)})()
        scope = isolated(ROOT / "router.py",
                         {"_gateway_update_deploy", "_gateway_update_deploy_thread",
                          "_gateway_update_job_state", "_gateway_update_error"},
                         {"threading": threading, "os": __import__("os"), "HERE": str(ROOT),
                          "_gateway_update_lock": threading.Lock(),
                          "_gateway_update_job": {"status": "idle", "version": "", "commit": "",
                                                  "error": "", "ts": 0.0},
                          "_SERVER": server})
        snapshot = {"ok": True, "current": "1.10.0", "has_update": True,
                    "latest": {"tag": "v1.11.0", "version": "1.11.0", "commit": "c" * 40}}
        old = sys.modules.get("gateway_update")
        sys.modules["gateway_update"] = stub
        try:
            with self.assertRaises(ValueError):
                scope["_gateway_update_deploy"]({"ok": True, "has_update": False})
            scope["_gateway_update_deploy"](snapshot)
            deadline = time.time() + 5
            while scope["_gateway_update_job_state"]()["status"] != "failed" and time.time() < deadline:
                time.sleep(0.02)
            state = scope["_gateway_update_job_state"]()
            self.assertEqual(state["status"], "failed")
            self.assertIn("candidate regression failed", state["error"])
            self.assertEqual(shutdowns, [])
            scope["_gateway_update_deploy"](snapshot)
            deadline = time.time() + 5
            while scope["_gateway_update_job_state"]()["status"] != "scheduled" and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(scope["_gateway_update_job_state"]()["status"], "scheduled")
            self.assertEqual(len(attempts), 2)
        finally:
            if old is None:
                sys.modules.pop("gateway_update", None)
            else:
                sys.modules["gateway_update"] = old


if __name__ == '__main__':
    unittest.main()
