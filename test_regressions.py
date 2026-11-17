"""Offline regression tests. No credentials, sockets or upstream requests."""
import ast
import io
import json
from pathlib import Path
import re
import secrets
import threading
import time
import unittest
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
            "_estimate_public_model", "_qwen_rate_spec", "estimate_snapshot",
            "_row_multiplier",
            "_media_link_signature", "_media_download_allowed"},
            {"QWEN_MODEL_RATES": {}, "hashlib": __import__('hashlib'),
             "hmac": __import__('hmac'), "_media_link_secret": b'offline-test-only',
             "MEDIA_POST_PATHS": constant(ROOT / "router.py", "MEDIA_POST_PATHS")})

    def test_loopback_bypasses_proxy(self):
        import urllib.request
        from urllib.parse import urlsplit
        from unittest.mock import Mock, patch
        opener = Mock()
        scope = isolated(ROOT / 'router.py', {'_urlopen'},
                         {'urllib': __import__('urllib'), 'urlsplit': urlsplit, '_local_opener': opener})
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
            public_catalog=lambda:[{'platform':'qwenwork','public':'mystery-model','upstream':'mystery-model'}],
            platform_status=lambda _: {'health':True,'accounts':[{'enabled':True,'remain':100}]},
            _estimate_rows=lambda _: [{'created_at':time.time(),'model':'mystery-model','total_tokens':2000}],
            _estimate_at=lambda r:r['created_at'],_model_hidden=lambda *_:False)
        result=self.ns['estimate_snapshot'](True)
        self.assertIsNone(result['platforms'][0]['models'][0]['est_tokens'])

    def estimate(self, accounts, rows=(),
                 catalog=None, rates=None):
        self.ns.update(
            _estimate_lock=threading.Lock(), _estimate_cache={'data':None}, ESTIMATE_TTL=120,
            QWEN_MODEL_RATES=rates or {},
            CONFIG={'upstreams':[{'name':'qwenwork'}]},
            public_catalog=lambda: catalog or [
                {'platform':'qwenwork','public':'flash','upstream':'qwork-auto','row':{}}],
            platform_status=lambda _: {'health':True,'accounts':accounts},
            _estimate_rows=lambda _: list(rows), _estimate_at=lambda r:r['created_at'],
            _model_hidden=lambda *_:False, _wb_usable_realms=lambda _:[])
        return self.ns['estimate_snapshot'](True)['platforms'][0]

    def test_unknown_balance_stays_unknown_with_credit_sample(self):
        rates={'qwork-auto':{'public':'flash','hit_per_ktok':0.001,'miss_per_ktok':0.01,
                             'hit_rate':0.9}}
        rows=[{'created_at':time.time(),'model':'qwork-auto','total_tokens':1000,'credit':1}]
        item=self.estimate([{'enabled':True,'remain':None}], rows, rates=rates)
        self.assertIsNone(item['balance'])
        self.assertEqual((item['known'],item['total']),(0,1))
        model=item['models'][0]
        self.assertIsNone(model['model_balance'])
        self.assertIsNone(model['est_tokens'])
        self.assertIsNone(model['est_tokens_opt'])
        self.assertIsNone(model['est_tokens_floor'])
        self.assertTrue(model['balance_unknown'])
        self.assertIn('待测', model['note'])
        item=self.estimate([{'enabled':True,'remain':0}], rates=rates)
        self.assertEqual(item['balance'],0.0)
        self.assertEqual((item['known'],item['total']),(1,1))
        self.assertEqual(item['models'][0]['model_balance'],0.0)
        self.assertIsNone(item['models'][0]['est_tokens'])
        item=self.estimate([{'enabled':True,'remain':None},{'enabled':True,'remain':5}],
                           rates=rates)
        self.assertEqual(item['balance'],5.0)
        self.assertEqual((item['known'],item['total']),(1,2))

    def test_qwen_estimate_uses_full_price_as_headline(self):
        rates = constant(ROOT / 'router.py', 'QWEN_MODEL_RATES')
        self.assertIn('qwork-auto', rates)
        rates={'qwork-auto':{'public':'flash','hit_per_ktok':0.001,'miss_per_ktok':0.01,
                             'hit_rate':0.9}}
        for upstream in ('qwork-auto', 'flash', 'qwenwork/flash',
                         'qwenwork/qwork-auto', 'qwenwork/qwenwork/flash'):
            with self.subTest(upstream=upstream):
                item=self.estimate([{'enabled':True,'remain':100}],
                                   catalog=[{'platform':'qwenwork','public':'flash',
                                             'upstream':upstream,'row':{}}],
                                   rates=rates)
                model=item['models'][0]
                self.assertEqual(model['rate_source'], 'cache_assumed')
                self.assertTrue(model['cache_assumed'])
                self.assertEqual(model['cache_hit_rate'], 0.9)
                # headline = 全价（零缓存）保守下限；缓存假设只作乐观参考
                self.assertEqual(model['est_tokens'], 10_000_000)
                self.assertIsNone(model['est_tokens_floor'])
                self.assertGreater(model['est_tokens_opt'], model['est_tokens'])

    def test_measured_rate_beats_catalog_and_carries_floor(self):
        rows=[]
        for _ in range(4):
            rows.append({'created_at':time.time(),'model':'ds','total_tokens':10000,
                         'prompt_tokens':9000,'completion_tokens':1000,
                         'cached_tokens':8800,'credit':0.02})
        self.ns.update(
            _estimate_lock=threading.Lock(), _estimate_cache={'data':None}, ESTIMATE_TTL=120,
            CONFIG={'upstreams':[{'name':'workbuddy'}]},
            public_catalog=lambda:[{'platform':'workbuddy','public':'ds',
                                    'upstream':'ds','row':{'credits':'x0.5'}}],
            platform_status=lambda _: {'health':True,'accounts':[{'enabled':True,'remain':100}]},
            _estimate_rows=lambda _: list(rows), _estimate_at=lambda r:r['created_at'],
            _model_hidden=lambda *_:False, _wb_usable_realms=lambda _:['cn'],
            _wb_realm_meta=lambda:{}, WB_INTL_ALIAS={})
        item=self.ns['estimate_snapshot'](True)['platforms'][0]
        model=item['models'][0]
        self.assertEqual(model['rate_source'],'measured')
        # 有效单价 0.08/40000*1000*... = 0.002/ktok → 100 分 ≈ 5000 万 token
        self.assertEqual(model['est_tokens'], 50_000_000)
        # 全价 = 0.08 / (4×1200 billed) = 0.0167/ktok → 下限 600 万
        self.assertEqual(model['est_tokens_floor'], 6_000_000)
        self.assertGreater(model['est_tokens'], model['est_tokens_floor'])
        self.assertIn('扣费样本', model['note'])

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
                       CHECKIN_PATHS={'qoder':('/accounts/checkin','panel')})
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
        scope = isolated(ROOT / "router.py", {"resolve_media_model"}, {
            "CONFIG": {"upstreams": [{"name": "workbuddy"}]},
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
        self.assertIn("只由 workbuddy", fn("qoder/kling-video-v3")[3])
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

    def test_media_catalog_merges_realms(self):
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
    addEventListener(){},setAttribute(){},prepend(){},querySelector(){return null;},querySelectorAll(){return []}};
}
function el(id){if(!elements.has(id))elements.set(id,element(id));return elements.get(id);}
const navs=['overview','models'].map(v=>{const e=element();e.dataset.view=v;return e;});
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
const store=new Map();
let fetchImpl=async()=>({ok:true,status:200,json:async()=>({})});
const context=vm.createContext({console,Map,Set,Date,Promise,JSON,Number,String,Math,Array,Object,Error,
 document:{getElementById:el,createElement:()=>element(),querySelectorAll:s=>s==='.nav'?navs:s==='.view'?views:s==='[data-media-copy]'?copyButtons:[],querySelector:()=>null},
 localStorage:{getItem:k=>store.get(k),setItem:(k,v)=>store.set(k,v),removeItem:k=>store.delete(k)},
 fetch:(...a)=>fetchImpl(...a),setInterval:()=>1,clearInterval(){},setTimeout:()=>1,clearTimeout(){},
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
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:null,known:0,total:1,models:[{model:'flash',balance_scope:'',model_balance:null,est_tokens:null,note:'余额待测，暂不估算',samples:0}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/待测/);
 assert.match(el('overview-estimate').innerHTML,/已知 0\/1/);
 assert.doesNotMatch(el('overview-estimate').innerHTML,/0 分/);
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:0,known:1,total:1,models:[{model:'flash',balance_scope:'',model_balance:0,est_tokens:null,note:'',samples:0}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/0 分/);
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:100,known:1,total:1,models:[{model:'flash',balance_scope:'',model_balance:100,est_tokens:1000,est_tokens_opt:1500,cache_assumed:true,cache_hit_rate:0.9,est_tokens_floor:800,note:'短样本全价下限',samples:2}]}]}};renderEstimate()");
 assert.match(el('overview-estimate').innerHTML,/全未命中下限/);
 assert.match(el('overview-estimate').innerHTML,/800/);
 assert.match(el('overview-estimate').innerHTML,/乐观（90% 缓存假设）/);
 assert.match(el('overview-estimate').innerHTML,/1\.5k/);
 assert.match(el('overview-estimate').innerHTML,/短样本全价下限/);
 run("EST_CACHE={ts:Date.now(),data:{platforms:[{name:'qwenwork',balance:100,known:1,total:1,models:[{model:'flash',est_tokens:1000,note:'',samples:2}]}]}};renderEstimate()");
 assert.doesNotMatch(el('overview-estimate').innerHTML,/全未命中下限/);
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
 assert.equal(run("balanceSummary([{enabled:true,remain:null}]).value"),null);
 assert.equal(run("balanceSummary([{enabled:true,remain:0}]).value"),0);
 run("renderUsageStats([{credit:0}])");assert.match(el('usage-stats').innerHTML,/0\.00/);
 run("pushHistory('partial',{ok:false,partial:true},'detail','');pushHistory('skip','skipped','detail','')");
 assert.match(el('task-history').innerHTML,/部分失败/);assert.match(el('task-history').innerHTML,/已跳过/);
 assert.equal(run('TASK_HISTORY[0].state'),'skipped');assert.equal(run('TASK_HISTORY[1].state'),'partial');
 // Media page renders both kinds and switches the form between image/video.
 run("MEDIA={items:[{id:'workbuddy/hunyuan-image-v3.0-art',model:'hunyuan-image-v3.0-art',name:'Hunyuan',kind:'image',tags:['text-to-image'],realms:['cn'],credit_per_image:6,image_measured:false,hidden:false},{id:'workbuddy/kling-video-v3',model:'kling-video-v3',name:'Kling',kind:'video',tags:['text-to-video'],realms:['cn','intl'],credits_per_second:{'720P':10},hidden:false}],errors:{}};renderMedia()");
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
 // Navigation paints immediately even when requests never settle.
 fetchImpl=()=>new Promise(()=>{});navs[1].onclick();
 assert.equal(views[1].classList.contains('active'),true);
 assert.equal(views[0].classList.contains('active'),false);
 console.log('dashboard offline: 503/retry, single-flight, invalidation, 401, pagination, cost coverage, unknown, result states, navigation passed');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(['node', '-', str(ROOT / 'dashboard.html')], input=script,
                                text=True, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
