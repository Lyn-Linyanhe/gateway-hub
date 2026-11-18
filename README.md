# Gateway Hub · 多引擎聚合反代控制台

把多个**本地 AI 网关引擎**（WorkBuddy / Qoder / 千问办公）聚合为**一个 OpenAI 兼容入口 + 一个统一 Web 看板**。

只做编排与展示：模型目录合并、按名字路由、SSE 透传、账号池/积分/用量聚合、自动化操作代理。真正的协议转换与账号维护仍在各引擎里完成。

```
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ workbuddy2api │   │ qoder2api-hub │   │   Buddy2api   │
│    :8788      │   │    :8790      │   │    :8792      │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────┬───┴───────────────────┘
                        ▼
              ┌───────────────────┐
              │   Gateway Hub     │  ← 本仓库（router.py + dashboard.html）
              │       :8787       │
              └─────────┬─────────┘
                        ▼
        OpenAI 兼容客户端（ZCode / Codex / NextChat / …）
        baseUrl: http://127.0.0.1:8787/v1
```

## 特性

- **一个 URL、一个 Key 聚合三平台**：`/v1/chat/completions`、`/v1/responses`、`/v1/messages` 按模型名路由，SSE 流式透传；非流式响应回显客户端请求的模型名。
- **模型可见性管理**：控制哪些模型对外可见——隐藏的模型从 `/v1/models` 消失、调用被拒绝；支持批量勾选、整平台开关、收藏置顶。
- **WorkBuddy 媒体生成**：生图 `/v1/images/generations`、改图 `/v1/images/edits`、异步生视频 `/v1/videos`（提交/状态/取消/产物直链），媒体模型清单 `/v1/media/models`（`workbuddy/hunyuan-image-alpha`、`kling-video-v3` 等）。媒体模型刻意不混入 `/v1/models`，避免聊天客户端误用；可见性与聊天模型共用隐藏表，控制台「媒体」页可直接试生成。
- **统一带平台前缀**：`/v1/models` 只发 `平台/模型`（如 `workbuddy/glm-5.3`），一眼看出归属；裸名调用仍兼容（全网关唯一时）。
- **双区临期调度（WorkBuddy）**：同名模型国内版/国际版都有时，按两区「最临期积分包」的紧迫度自动选择出口（`wb_dual_prefer: auto/cn/intl`）；选中区域对该模型有会员门槛且无付费账号时自动换区。
- **PRO 会员锁定**：上游目录标注的付费专属模型，在账号池没有付费账号时自动从 `/v1/models` 隐藏并给出明确拒绝话术；会员账号入池后自动恢复。
- **用量分析**：三平台请求合并明细（含输入/输出/缓存/推理强度/耗时/速度）；按平台分段的 token 时间曲线；各平台缓存命中率曲线；按模型汇总；**换算统计**（实测积分/千tok vs 目录倍率，WorkBuddy 附引擎参考成本）。
- **自动化操作代理**：签到（WorkBuddy 国内签到+国际打卡合并）、国际网页通道补打卡、成长任务/全账号领奖/猫猫旅行/夜猫子/连登、Token 保活、积分刷新、额度窗口刷新、调度器触发/详情——白名单代理到各引擎。
- **上游版本检测**：Qoder / Buddy2api 直查 GitHub Release tag（HEAD 302，不吃 API 配额），WorkBuddy 走引擎自带更新检查。
- **零依赖**：router.py 仅用 Python 标准库（3.10+），单文件 5k 行以内；dashboard 是单文件静态页（无构建、无 CDN）。

## 快速开始

1. **先自行部署三个上游引擎**（本仓库不含引擎）并记下各自的地址与 API Key / 面板密码：
   - WorkBuddy 引擎（Telegram 分发，默认 `127.0.0.1:8788`）
   - Qoder 引擎 [shuishuipingan/qoder2api-hub](https://github.com/shuishuipingan/qoder2api-hub)（默认 `8790`）
   - Buddy2api [wicm84266964/Buddy2api](https://github.com/wicm84266964/Buddy2api)（qwenwork-only 模式，默认 `8792`）
2. 复制配置并填入你的 Key：

   ```bash
   cp config.example.json config.json
   # 编辑 config.json：api_key / upstreams[].api_key / panel_password
   ```

3. 启动（任选其一）：

   ```bash
   python router.py           # 前台
   run-router.bat             # Windows 静默后台（pythonw）
   ```

4. 浏览器打开 <http://127.0.0.1:8787>，输入 `config.json` 里的 `api_key` 进入控制台。

### 一键更新

朋友/其他机器不需要每次手动打开 GitHub：双击 `update.bat` 即可检查并更新到最新 Release。

- 自动读取 GitHub 最新 release，版本不高于本机时直接退出。
- 优先在干净的 git 仓库里 `git pull --ff-only`；否则下载 release 源码并只覆盖代码文件。
- 不会覆盖 `config.json`、账号、日志和运行态文件。
- 更新后自动做 Python 语法检查并重启 8787；失败会从 `backup-*` 恢复旧文件。
- 只想检查不更新：`update.bat -CheckOnly`；不自动重启：`update.bat -NoRestart`。

首次需要把 `update.bat`、`update.ps1` 放进网关目录（或直接使用 v1.8.1 及以上 Release 的源码包）；之后双击即可。

### QwenWork 上游启动（可选）

Buddy2api 放在网关同级的 `..\Buddy2api` 时，直接双击 `start-buddy2api.bat`；其它位置用：

```bat
start-buddy2api.bat 8792 D:\path\to\Buddy2api
```

它固定用仓库 `.venv\Scripts\python.exe`（含千问聊天必需的 `wasmtime`），并在启动进程里清空 `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY`、只把 `gateway.qwenwork.cn` 放进 `NO_PROXY`。这样即使系统里残留了已停止的 FlClash `127.0.0.1:7890`，也不会再出现 `All connection attempts failed` 的 502。若所在网络必须经代理访问千问，请改脚本显式配置代理。

5. 客户端接入：

   ```
   Base URL : http://127.0.0.1:8787/v1
   API Key  : config.json 的 api_key（agg- 开头）
   Model    : 平台/模型，如 workbuddy/glm-5.3-flash、qoder/Qwen3.8-Flash、qwenwork/qwen4-max
   思考强度 : 按 OpenAI 规范传 reasoning_effort（low/medium/high/max/xhigh，见模型页规格行）
   ```

## 配置说明（config.json）

| 字段 | 说明 |
|---|---|
| `listen_port` | 监听端口，默认 8787（仅本机可访问） |
| `api_key` | 聚合入口 Key，`agg-` 开头；留空启动时自动生成 |
| `upstreams[]` | 上游引擎列表：`name` / `base_url` / `api_key` / `panel_password` / `repo`（更新检测用，可选） |
| `upstreams[].base_url` | **仅支持本机回环地址**（`127.0.0.1` / `localhost` / `::1`）。网关对回环上游禁代理并禁止跟随重定向，避免面板凭证跨源；远程引擎地址未经该保护验证，当前不受支持。 |
| `hidden_models` | 已隐藏模型（`平台/模型` 全名），由控制台维护 |
| `favorite_models` | 收藏模型（置顶展示） |
| `hide_pro_locked` | PRO 专属模型在无付费账号时自动隐藏，默认 `true` |
| `wb_dual_prefer` | 双区模型出口偏好：`auto`（按临期紧迫度）/ `cn` / `intl` |
| `wb_update_mirror` | 可选。WorkBuddy 官方 GitHub 仓库已下线时，用这个 `owner/repo` 镜像的 release tag 做版本参照；默认 `niyouqianma/workbuddy2api-hub`。镜像可能滞后，页面会标注“镜像参照” |
| `custom_multipliers` / `auto_multipliers` | 手工倍率由控制台维护；旧 `auto_multipliers` 仅保留历史，不再自动更新或用作价格 |
| `model_caps` | 实测能力补充表：`{"qwenwork/qwen4-max": {"vision": true}}`。上游目录不声明多模态/工具能力时（如 qwenwork 供应商列表），把实测结论写在这里；`/api/catalog` 与 `/v1/models` 的 `capabilities` 都会带上 |

## 常用接口

| 接口 | 说明 |
|---|---|
| `GET /v1/models` | 聚合模型列表（已隐藏/PRO 锁定除外，均带平台前缀） |
| `POST /v1/chat/completions` 等 | 按模型名路由到对应引擎 |
| `POST /v1/images/generations` · `/v1/images/edits` | 生图/改图（WorkBuddy，同步；`model` 可写 `workbuddy/<id>` 或裸名，省略则用默认图片模型） |
| `POST /v1/videos` | 提交异步生视频任务（返回 202 + 任务 id；支持 `wait=1` 阻塞到完成） |
| `GET /v1/videos/{id}` | 查询视频任务状态（queued / in_progress / completed / failed） |
| `GET /v1/videos/{id}/content` | 下载 MP4 产物（签名直链，不经网关 Key） |
| `POST /v1/videos/{id}/cancel` | 取消仍在排队的视频任务 |
| `GET /v1/media/models` | 媒体模型清单（`?tag=` / `?realm=cn|intl` 过滤；不进 `/v1/models`） |
| `GET /api/media/models` | 控制台媒体目录（含隐藏标记与媒体价格） |
| `GET /api/status` | 三平台健康、账号池、积分、即将过期积分 |
| `GET /api/catalog` | 模型管理页数据（含可见性/倍率/规格/出口/媒体标记） |
| `POST /api/models/visibility` | 隐藏/显示模型（`{id}`、`{ids}`、`{platform}`、`{visible:true}`） |
| `GET /api/usage?limit=500` | 三平台用量合并（WorkBuddy 含国内+国际双区） |
| `POST /api/checkin/{platform}` | 签到（WorkBuddy = 国内签到 + 国际打卡合并） |
| `POST /api/tasks/{action}/{platform}` | 任务：run / claim（按全部国内账号领奖）/ travel / night / streak-bonus / daily-web |
| `POST /api/engine` | 白名单引擎代理（保活/积分刷新/调度器/每日打卡） |
| `GET /api/gwconfig` · `POST` | 网关自身开关（`hide_pro_locked`、`wb_dual_prefer`） |
| `GET /api/updates?force=1` | 上游版本检测 |
| `GET /health` | 健康检查（无鉴权） |

媒体出口沿用 WorkBuddy 引擎的媒体号池和 Key 区域绑定；网关会标注当前出口可用性，不自动改引擎区域或账号归属。控制台生成表单提供文生图、文生视频；图生图通过 JSON `/v1/images/edits`（`image` 为图片 URL 或 data URL）调用，不支持 multipart 文件上传。省略 `model` 时只选当前出口可见且支持目标操作的模型。视频下载直链带独立签名，有效期最多一小时，网关重启后需重新查询任务获取链接。

WorkBuddy 的“运行成长任务”包含上游支持的成长、小程序和运营活动流水线；定时巡检另管国内活跃上报、Buddy 盲盒、签到、旅行、连登、夜猫子和国际打卡。一键补做会在同步步骤之后提交一次补充巡检，提交成功不代表后台任务全部完成。手机 App 对话需在官方 App 完成；夜猫子受时间窗口限制。网关不伪造这些任务的完成状态。

## 回归测试

离线回归不读取凭证、不访问网络，覆盖流式边界、工具结果、鉴权与统计缺失值：

```bash
python test_regressions.py
```

`audit_platform.py` 是只读在线巡检，检查健康、鉴权和目录，不调用模型、不签到、不导入账号或修改配置；失败返回非零退出码：

```bash
python audit_platform.py
```

额度预估以实测扣费为准：优先用「Σ实扣积分 ÷ Σtoken」计算有效单价（天然包含缓存折扣），并给出「全未命中下限」（扣费 ÷ 非缓存计费 token，或目录全价）。没有扣费样本时不编数字——WorkBuddy/Qoder 的目录价只作零缓存下限并标注"未经本机校准"，千问办公用短样本全价作保守下限、缓存假设只作乐观参考。余额未知或单价缺失显示"待测"。国际活跃的执行记录与奖励到账状态分开展示。

## 免责声明

- 本项目是**个人自用的聚合编排工具**，与任何官方产品无关；非官方、非商用。
- 上游三个引擎均为第三方开源项目，请遵守其各自许可；账号与平台使用需遵守对应平台的服务条款。
- **请勿公开部署或分享**：控制台与聚合入口默认只监听本机，配置中含密钥；泄露配置等于泄露账号。
- 使用者对自己的账号、额度与合规性负全部责任，作者不承担任何连带风险。

## License

[MIT](LICENSE)
