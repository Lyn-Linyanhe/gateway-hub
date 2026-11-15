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
- **统一带平台前缀**：`/v1/models` 只发 `平台/模型`（如 `workbuddy/glm-5.3`），一眼看出归属；裸名调用仍兼容（全网关唯一时）。
- **双区临期调度（WorkBuddy）**：同名模型国内版/国际版都有时，按两区「最临期积分包」的紧迫度自动选择出口（`wb_dual_prefer: auto/cn/intl`）；选中区域对该模型有会员门槛且无付费账号时自动换区。
- **PRO 会员锁定**：上游目录标注的付费专属模型，在账号池没有付费账号时自动从 `/v1/models` 隐藏并给出明确拒绝话术；会员账号入池后自动恢复。
- **用量分析**：三平台请求合并明细（含输入/输出/缓存/推理强度/耗时/速度）；按平台分段的 token 时间曲线；各平台缓存命中率曲线；按模型汇总；**换算统计**（实测积分/千tok vs 目录倍率，WorkBuddy 附真实成本锚点）。
- **自动化操作代理**：签到（WorkBuddy 国内签到+国际打卡合并）、成长任务/领奖/猫猫旅行/夜猫子、Token 保活、积分刷新、调度器触发/详情——白名单代理到各引擎。
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

5. 客户端接入：

   ```
   Base URL : http://127.0.0.1:8787/v1
   API Key  : config.json 的 api_key（agg- 开头）
   Model    : 平台/模型，如 workbuddy/glm-5.3-flash、qoder/Qwen3.8-Flash、qwenwork/frontier-1
   思考强度 : 按 OpenAI 规范传 reasoning_effort（low/medium/high/max/xhigh，见模型页规格行）
   ```

## 配置说明（config.json）

| 字段 | 说明 |
|---|---|
| `listen_port` | 监听端口，默认 8787（仅本机可访问） |
| `api_key` | 聚合入口 Key，`agg-` 开头；留空启动时自动生成 |
| `upstreams[]` | 上游引擎列表：`name` / `base_url` / `api_key` / `panel_password` / `repo`（更新检测用，可选） |
| `hidden_models` | 已隐藏模型（`平台/模型` 全名），由控制台维护 |
| `favorite_models` | 收藏模型（置顶展示） |
| `hide_pro_locked` | PRO 专属模型在无付费账号时自动隐藏，默认 `true` |
| `wb_dual_prefer` | 双区模型出口偏好：`auto`（按临期紧迫度）/ `cn` / `intl` |
| `custom_multipliers` / `auto_multipliers` | 手工/实测倍率记录（控制台维护） |
| `model_caps` | 实测能力补充表：`{"qwenwork/frontier-1": {"vision": true}}`。上游目录不声明多模态/工具能力时（如 qwenwork 供应商列表），把实测结论写在这里；`/api/catalog` 与 `/v1/models` 的 `capabilities` 都会带上 |

## 常用接口

| 接口 | 说明 |
|---|---|
| `GET /v1/models` | 聚合模型列表（已隐藏/PRO 锁定除外，均带平台前缀） |
| `POST /v1/chat/completions` 等 | 按模型名路由到对应引擎 |
| `GET /api/status` | 三平台健康、账号池、积分、即将过期积分 |
| `GET /api/catalog` | 模型管理页数据（含可见性/倍率/规格/出口） |
| `POST /api/models/visibility` | 隐藏/显示模型（`{id}`、`{ids}`、`{platform}`、`{visible:true}`） |
| `GET /api/usage?limit=500` | 三平台用量合并（WorkBuddy 含国内+国际双区） |
| `POST /api/checkin/{platform}` | 签到（WorkBuddy = 国内签到 + 国际打卡合并） |
| `POST /api/tasks/{action}/{platform}` | 任务：run / claim / travel / night / streak-bonus |
| `POST /api/engine` | 白名单引擎代理（保活/积分刷新/调度器/每日打卡） |
| `GET /api/gwconfig` · `POST` | 网关自身开关（`hide_pro_locked`、`wb_dual_prefer`） |
| `GET /api/updates?force=1` | 上游版本检测 |
| `GET /health` | 健康检查（无鉴权） |

## 回归测试

`audit_platform.py` 是 48 项断言的全链路回归脚本（路由/可见性/PRO 锁定/分区路由/签到/任务/引擎代理/网关配置），依赖运行中的网关与真实上游：

```bash
python audit_platform.py
```

## 免责声明

- 本项目是**个人自用的聚合编排工具**，与任何官方产品无关；非官方、非商用。
- 上游三个引擎均为第三方开源项目，请遵守其各自许可；账号与平台使用需遵守对应平台的服务条款。
- **请勿公开部署或分享**：控制台与聚合入口默认只监听本机，配置中含密钥；泄露配置等于泄露账号。
- 使用者对自己的账号、额度与合规性负全部责任，作者不承担任何连带风险。

## License

[MIT](LICENSE)
