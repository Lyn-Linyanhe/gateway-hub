# Gateway Hub · 多引擎聚合控制台

把多个**本地 AI 网关引擎**聚合为一个 OpenAI 兼容入口和统一 Web 看板。仅供本机、个人账号使用；本仓库不包含上游引擎，也不提供第三方账号或额度。

网关负责模型目录、按模型路由、流式响应转发、账号与用量展示、平台启停和管理操作代理。协议转换、账号维护和平台实际计费仍由各上游引擎负责。

| 平台 | 配置名称／模型前缀 | 默认上游地址 |
|---|---|---|
| WorkBuddy | `workbuddy` | `http://127.0.0.1:8788` |
| Qoder | `qoder` | `http://127.0.0.1:8790` |
| 千问办公（Buddy2api） | `qwenwork` | `http://127.0.0.1:8792` |
| 小浣熊（xhx2api） | `raccoon` | `http://127.0.0.1:7867` |
| Trae | `trae` | `http://127.0.0.1:8791` |

```text
本地 WorkBuddy / Qoder / Buddy2api / xhx2api / Trae 引擎
                         │
                  Gateway Hub :8787
                         │
           OpenAI 兼容客户端（ZCode / Codex / …）
```

## 主要功能

- **统一入口**：`/v1/chat/completions`、`/v1/responses`、`/v1/messages`，按 `平台/模型` 路由；SSE 流式转发，非流式响应回显客户端模型名。接口是否支持以及协议转换质量取决于对应引擎。
- **平台开关与进程**：设置页可启用或停用平台；停用后从总览、目录、路由和轮询中排除，并尝试停止引擎；启用后按已配置的 `launch` 自动启动。
- **模型管理**：显示／隐藏、批量操作、收藏、模型规格与倍率展示；裸模型名仅在能唯一解析时兼容。
- **WorkBuddy 双区路由**：依据可用区域、会员资格和临期积分选路；会话区域粘性、失败冷却、首字前换区重试。账号选择仍在引擎内完成。
- **用量与额度**：汇总请求、token、已知缓存、扣费、耗时和错误；按时间／平台／模型筛选与导出。余额、成本或缓存字段缺失时保留未知状态。
- **媒体**：WorkBuddy 图片／视频、小浣熊图片生成与改图；媒体目录与聊天目录分开，避免客户端误选。媒体调用会消耗对应平台额度。
- **管理操作**：按平台白名单代理签到、保活、积分刷新、任务和调度器等操作；执行结果与奖励到账状态分开展示。
- **版本检查**：查询上游引擎版本；社区镜像参照会单独标注。

运行仅需 Python 3.10+ 标准库；看板为单文件 HTML，无构建步骤或 CDN 依赖。开发回归测试另外需要 Node.js。

## 快速开始

### 1. 部署需要的上游引擎

不需要的平台可从配置删除，或在设置页停用。上游参考：

- WorkBuddy：按引擎分发渠道自行部署。
- Qoder：[shuishuipingan/qoder2api-hub](https://github.com/shuishuipingan/qoder2api-hub)。
- Buddy2api：[wicm84266964/Buddy2api](https://github.com/wicm84266964/Buddy2api)，使用 qwenwork 渠道。
- 小浣熊：[rsfree/xhx2api](https://github.com/rsfree/xhx2api)。
- Trae：[shuishuipingan/trae2api-hub](https://github.com/shuishuipingan/trae2api-hub)。

这些是独立项目，许可、安装要求和账号登录方式以各自仓库为准。

### 2. 创建本机配置

```bash
cp config.example.json config.json
```

Windows 也可手动复制。编辑 `config.json` 中的引擎地址、API Key 和面板密码；示例值不能直接用于生产账号。网关入口 `api_key` 留空时在启动时生成，生成后的值保存在本机配置中。

**不要提交 `config.json`、上游账号目录、授权链接、日志或余额历史。** 示例配置不包含可用凭据；仓库忽略规则也不替代提交前检查。

### 3. 启动网关

```bash
python router.py
```

Windows 可双击 `run-router.bat` 使用 `pythonw.exe` 后台启动。浏览器打开 <http://127.0.0.1:8787>，输入本机配置中的网关 Key。启动脚本只提交启动请求，服务是否就绪应以看板或 `/health` 为准。

### 4. 接入客户端

```text
Base URL: http://127.0.0.1:8787/v1
API Key:  config.json 中的 api_key
Model:    平台/模型，例如 raccoon/sn-qwen4-max
```

通过 `/v1/models` 获取当前可见模型，不应把文档示例当作上游始终可用的模型清单。

### 5. 给他人分配用户 Key

在控制台「API Keys」页新建用户 Key（`sk-gw-...`）。每个 Key 可单独设置：

- 每日 token 上限（按上游返回的 usage 统计，兼容 OpenAI / Anthropic 字段）
- 每日请求上限（上游不返回 usage 时兜底）
- 并发上限、每分钟请求上限
- 允许模型（支持 `workbuddy/*`、`codearts/*` 等通配符；留空=全部）

用户 Key 只能调用 `/v1/*`，不能访问 `/api/*` 控制台接口；管理员 Key 仍是 `config.json` 的 `api_key`。完整用户 Key 只在创建/重置时显示一次，网关只保存 SHA-256 哈希。

## 公网访问（Cloudflare Tunnel）

不买服务器也能把本机网关发布到自己的域名：本机继续运行 gateway 和全部引擎，`cloudflared` 把 `http://127.0.0.1:8787` 接到 Cloudflare，再绑定一个子域名。

```bash
cloudflared tunnel login
cloudflared tunnel create gateway-hub
cloudflared tunnel route dns gateway-hub gw.example.com
cloudflared tunnel --config %USERPROFILE%\.cloudflared\config.yml --protocol http2 --edge-ip-version 6 run gateway-hub
```

> 如果运营商阻断了 IPv4 出站 7844（国内常见），必须加 `--edge-ip-version 6 --protocol http2` 走 IPv6；本机已实测 IPv4 7844 超时、IPv6 正常。登录自启可放一个 `.cmd` 到 `shell:startup`，先跑 `start-all.bat`、再跑上面的 cloudflared 命令。

`%USERPROFILE%\.cloudflared\config.yml` 的最小配置：

```yaml
tunnel: gateway-hub
credentials-file: C:\Users\<你>\.cloudflared\<tunnel-uuid>.json
ingress:
  - hostname: gw.example.com
    service: http://127.0.0.1:8787
  - service: http_status:404
```

注意：

- 网关仍只绑定 `127.0.0.1`，不需要开防火墙或端口转发。
- 这台电脑必须保持开机联网；睡眠/关机时公网 URL 不可用。
- Cloudflare 免费代理对 SSE 流式正常；非流式请求超过约 100 秒可能返回 524，长生成建议客户端开启 `stream: true`。
- 公网只暴露 `/v1/*` 的用户 Key 不够，管理员 Key 也要用强随机值；控制台 HTML 本身是公开的，但所有 `/api/*` 数据和操作都需要管理员 Key。

## 平台启停与上游启动

在看板**设置 → 平台开关与进程**中操作：

- **启用**：恢复展示与路由；引擎未健康运行且配置了 `launch` 时自动启动。
- **停用**：停止展示、路由及相关轮询，并尝试停止引擎进程。
- **单独启动／停止**：操作引擎进程，不等同于切换平台启用状态。
- **外部启动的进程**：停用平台时，仅在端口监听者身份可验证时停止；无法确认时返回原因，不任意强杀。

`launch` 示例：

```json
{
  "command": ["pythonw.exe", "D:\\engines\\example\\server.py"],
  "cwd": "D:\\engines\\example",
  "timeout": 30,
  "env": {"EXAMPLE_OPTION": "1"}
}
```

数组形式直接执行；字符串形式通过系统 shell 执行，只应使用自己信任的本机命令。平台启用状态会持久化；重启网关不会自动停止或拉起所有外部引擎。

### 千问办公

Buddy2api 放在网关同级的 `../Buddy2api` 时：

```bat
start-buddy2api.bat
```

其它位置：

```bat
start-buddy2api.bat 8792 D:\engines\Buddy2api
```

脚本使用引擎自己的 `.venv\Scripts\python.exe`，检查 `wasmtime`，并清空启动进程中的 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`。若访问千问必须经过代理，需自行调整脚本；它不会修改系统代理设置。

### 小浣熊

部署 xhx2api 后手动运行示例：

```bat
cd /d D:\engines\xiaohuanxiong2api
xhx2api.exe -config config.json
```

将引擎生成的 Key 配到网关 `name=raccoon` 的 `api_key`，通过 <http://127.0.0.1:7867/panel/> 登录自己的账号。账号凭据留在引擎目录中。网关读取账号概览、请求事件和只读积分账单；签到／保活等操作另走白名单代理。

**思考档位使用顶层 `raccoon_thinking_level`**，不是旧的 `deepThink` 布尔字段。Chat 路径能否透传和其它入口是否丢字段，应按引擎版本区分。配置示例与已知边界见 [小浣熊思考档位](docs/raccoon-thinking.md)。

### Trae

手动启动示例：

```bat
cd /d D:\engines\trae2api-hub
set QODER_DISABLE=1
set TRAE_PROXY_DEFAULT_REALM=cn
python trae_proxy.py --port 8791
```

浏览器打开 <http://127.0.0.1:8791/> 完成账号登录并配置面板密码。将引擎 API Key 和面板密码分别填写到网关的 `api_key` 和 `panel_password`。

- `QODER_DISABLE=1` 用来关闭 Trae 引擎内置的 Qoder 通道，避免与独立 Qoder 引擎重复。
- 上游引擎会读取代理环境变量。若系统残留已停止的本机代理，可在 `launch.env` 中清空三个代理变量并设置合适的 `NO_PROXY`；示例不代表当前机器已完成配置。
- Trae 的 `reasoning_effort_level` 转换由 Trae 引擎按目录处理，网关本身只透传请求。
- 逐请求扣费估值依赖上游明确记录 `credit_reported=true`；引擎未提供该字段时，网关改用 entitlement 累计实扣计数器 `consumed_amount` 的区间增量（每 5 分钟主动刷新一次积分以推进采样），并在样本不足时如实显示待测。上游兼容要求见 [扣费与额度预估](docs/billing.md)。

## 额度预估与计费边界

每个模型按以下公式独立计算：

```text
预计 token = 该模型可用余额 ÷ 有效单价（分/Mtok）× 1,000,000
```

同平台模型通常共享余额，各行是“全部余额用于该模型”的不同情景，**不能相加**。模型倍率不等同于绝对单价；参考货币成本不替代实际积分。样本不足、余额不全或账单无法匹配时显示“待测”。

WorkBuddy/Qoder/小浣熊共用同模型「Σ实扣 ÷ Σtoken」的加权算法和样本门槛；小浣熊先将账单唯一关联到请求，未关联记录不参与计价。千问办公用钱包均价校准短样本相对价，Trae 优先用明确报告的扣费、否则用 entitlement 累计实扣计数器 `consumed_amount` 的区间增量（与 WorkBuddy 同口径）锚定平台均价。当前不会按思考档位分别标价，样本成本不代表固定官方报价。详细来源、样本门槛和限制见 [扣费与额度预估](docs/billing.md)。

## 配置说明

| 字段 | 说明 |
|---|---|
| `listen_port` | 网关端口，默认 8787；只监听本机回环地址 |
| `api_key` | 网关入口 Key；留空时启动生成，勿使用公开占位值 |
| `upstreams[]` | 引擎 `name`、`base_url`、`api_key`、`panel_password`、可选 `repo` 和 `launch` |
| `upstreams[].base_url` | 仅支持回环地址 `127.0.0.1`、`localhost`、`::1`；回环请求禁代理及重定向 |
| `upstreams[].launch` | 可选启动命令、工作目录、等待超时及环境变量；不保存账号凭据 |
| `disabled_platforms` | 停用平台数组，如 `["qoder"]`；设置页维护 |
| `hidden_models` | 隐藏模型的 `平台/模型` 全名 |
| `favorite_models` | 收藏模型的 `平台/模型` 全名 |
| `hide_pro_locked` | WorkBuddy 没有对应会员账号时隐藏会员专属模型，默认 true |
| `wb_dual_prefer` | WorkBuddy 新会话偏好：`auto`／`cn`／`intl`；已有会话优先保持区域粘性 |
| `wb_update_mirror` | 可选 WorkBuddy 社区镜像 release 参照；不保证与官方分发同步 |
| `custom_multipliers` | 手工展示倍率；不能据此假设绝对计费单价 |
| `auto_multipliers` | 历史兼容字段，不再自动标定价格 |
| `model_caps` | 已验证能力补充，例如 `{"qwenwork/qwen4-max":{"vision":true}}` |
| `qwenwork_reset_at` | 可选千问企业配额重置日期；由用户提供，不代表官方接口返回 |

## 常用接口

除健康检查、静态页面与受签名保护的媒体直链外，管理接口和聚合入口均需网关鉴权。

| 接口 | 用途 |
|---|---|
| `GET /health` | 网关及启用平台健康 |
| `GET /v1/models` | 当前可见聊天模型；带平台前缀 |
| `POST /v1/chat/completions`、`/v1/responses`、`/v1/messages` | 转发对应平台接口 |
| `GET /api/status` | 账号池、积分、健康及临期记录 |
| `GET /api/keys`、`POST /api/keys/{id}/update`、`/rotate`、`/delete` | 用户 Key 管理（仅管理员 Key） |
| `GET /api/catalog` | 模型规格、倍率、可见性 |
| `GET /api/usage?limit=500` | 合并用量明细；未知字段保留为空 |
| `GET /api/estimate` | 额度可用量预估与样本依据 |
| `GET /api/platforms`、`POST /api/platforms` | 平台启用状态、启动配置和进程状态；POST 可产生进程或配置变更 |
| `POST /api/models/visibility`、`/api/models/favorite`、`/api/models/rate` | 模型管理 |
| `GET /api/gwconfig`、`POST /api/gwconfig` | 网关选项 |
| `POST /api/checkin/{platform}` | 对应引擎签到；WorkBuddy 可能包含国际活跃打卡 |
| `POST /api/tasks/{action}/{platform}` | 引擎支持的任务操作 |
| `POST /api/engine` | 仅允许平台白名单中的管理路径 |
| `GET /api/updates?force=1` | 检查上游版本 |
| `GET /v1/media/models`、`GET /api/media/models` | 媒体目录；不混入聊天模型 |
| `POST /v1/images/generations`、`/v1/images/edits` | WorkBuddy／小浣熊图片请求，接受 JSON；不支持 multipart |
| `POST /v1/videos`、`GET /v1/videos/{id}` | WorkBuddy 异步视频提交与状态 |
| `POST /v1/videos/{id}/cancel`、`GET /v1/videos/{id}/content` | 视频取消与签名下载 |

模型、媒体别名、输出额度和价格随上游目录变化。小浣熊图片别名可能共用同一通道，不能仅凭别名认定为不同底层模型；生成张数与格式限制以接口错误和上游支持为准。

生图参数的生效范围按平台分开，不能混用：

| 参数 | 小浣熊（`raccoon/*`） | WorkBuddy（`workbuddy/*`） |
|---|---|---|
| `size` | 最长边 ≤ 3840 且总像素 ≤ 8,294,400；方形最多 `2880x2880`；不传则上游自选比例 | 总像素 ≤ 4,194,304（`2560x1600` 是实测边界），超限上游直接拒绝且不降级 |
| `quality` | 生效，`low` / `medium` / `high`，不传默认 `low`；`high` 会让 image tokens 涨 30 倍以上 | 引擎忽略该参数 |
| `image`（改图） | 只收 `data:image/...;base64,...`，传外链 URL 会被上游 400 | 还接受公网 http(s) 图片 URL |
| `n` | 只支持 1；上游收字符串会报 400，网关已剥掉该字段 | 只支持 1 |

媒体页会按当前模型的平台自动切换尺寸档位与 `quality` 是否可用，避免给出必然失败的组合。

## 更新

### 看板一键部署（推荐）

设置页「网关自更新」卡片自动检测本仓库的 GitHub 官方正式版 Release（每 5 分钟缓存，可手动强制检查）。发现新版本后点击「一键部署」并确认，网关会：

1. 预检：拉取该 Release 的 tag 提交、解包候选代码、校验版本与示例配置、在候选目录跑离线回归；
2. 预检通过后拉起独立更新进程，网关本体优雅退出；
3. 更新进程复核工作区仍干净、HEAD 未变，`git merge --ff-only` 到该 tag 提交，重新拉起网关并做健康检查；
4. 任一步失败则 `git reset --keep` 回滚到原提交并恢复旧版网关。

部署边界：只认稳定正式版（拒绝 draft/prerelease/非语义化 tag）；只允许官方 origin 的干净 `main` 检出，且只快进、不合并分叉；`config.json`、`accounts/`、`auths/`、日志与缓存永不覆盖。预检或部署失败会在卡片上显示原因，可重试。

接口：`GET /api/gateway-update`（检测 + 任务状态）、`POST /api/gateway-update`（请求体 `{"confirm": true}`，返回 202 后轮询 job 字段）。

### 手动更新

Windows 仍可用旧版辅助脚本：

```bat
update.bat -CheckOnly
update.bat
update.bat -NoRestart
```

`update.bat` 查询最新 GitHub Release。Git 干净工作区路径会拉取 `origin/main`；源码包路径只复制白名单代码文件并创建 `backup-*`。两者并非严格安装相同 tag，也不保证所有失败均能事务回滚。更新前备份本机配置和自有代码改动；有未提交修改时优先手动检查差异，不要把文件覆盖当作合并。

仅将代码推送 GitHub 不会自动创建 Release，看板自更新与 `update.bat` 都不会因此发现新的 Release 版本。

## 开发与验证

依赖：Python 3.10+；完整离线回归额外需要 Node.js 22+，用于执行真实看板内联脚本的合成 DOM 测试。

```bash
python -I -B -m py_compile router.py gateway_update.py audit_platform.py test_regressions.py test_gateway_update.py
python -I -B test_regressions.py
python -I -B -m unittest discover -p 'test_gateway_update.py'
```

离线测试使用合成请求、假上游及临时目录，不应读取真实账号或调用真实模型。GitHub Actions 执行同一检查。

已有引擎启动且具备本机授权时，可运行：

```bash
python audit_platform.py
```

在线巡检只检查健康、鉴权和目录，不调用模型、签到或导入；它会读取本机网关配置以进行鉴权，不适合含真实配置的公开 CI。

## 仓库结构

```text
router.py                 聚合路由、看板 API、平台管理与额度估值
gateway_update.py         官方正式版检测、预检、快进部署与回滚（独立更新进程入口）
dashboard.html            单文件看板（无构建步骤）
config.example.json       无可用凭据的配置示例
run-router.bat            Windows 网关后台启动
start-buddy2api.bat        千问引擎启动辅助
update.bat / update.ps1   Windows 更新辅助（旧版手动路径）
audit_platform.py         只读在线巡检
test_regressions.py        离线回归与看板合成测试
test_gateway_update.py     自更新决策边界离线测试
docs/                     协议与计费说明
CHANGELOG.md              尚未发布及历史维护记录
```

## 使用边界

- 只使用自己的账号，遵守各平台条款和上游项目许可。
- 不公开部署、不转售或分享网关 Key；控制台含进程和账号管理功能。
- 本项目为非官方个人编排工具，与各平台无隶属关系。

代码许可：[MIT](LICENSE)。
