# 上游引擎本地补丁归档

gateway-hub 依赖 5 个上游引擎。这些引擎由第三方维护，但我们在使用过程中
给每一个都打了本地补丁（修 bug、补缺失的语义）。**引擎仓库的 remote 都是
上游作者的仓库，我们的补丁不能推过去**，所以把补丁本体归档在这里，用途是：

1. 换机器 / 重装时能按这里的 patch 重建；
2. 上游发新版时，用 patch 判断「哪些改动是我们的、会不会被覆盖」；
3. 每周更新任务（`check_engine_updates.py`）报告「需人工合并」时，对照这里。

> 这些 patch 只包含我们自己的改动，不含任何凭据、账号数据或运行时状态。

## 当前状态（2026-11-26 全部升级到最新）

| 引擎 | 版本 | 补丁文件 | 状态 |
|---|---|---|---|
| workbuddy2api-hub | 2.2.5 | `workbuddy-wb_proxy.patch`、`workbuddy-wb_accounts.patch` | 已移植到 2.2.5，计数与 2.2.1 一致 |
| xiaohuanxiong2api | v0.1.36 | `xhx2api.patch`、`xhx2api-account_expiry_test.go.txt` | 已 rebase 到 origin/main |
| trae2api-hub | 1.7.2 | （见下） | 已 rebase；上游吸收了部分同类修复，我们的更细 |
| qoder2api-hub | 1.10.4 | `qoder2api-hub.patch` | 已 rebase，零冲突 |
| Buddy2api | 2.2.25 | `Buddy2api.patch` | 已 rebase，零冲突 |
| codearts2api | v1.0.3 | 无 | 无源码、无补丁 |

## 各补丁做了什么

### workbuddy2api-hub

`workbuddy-wb_proxy.patch`（对应 2.2.1 基线，2.2.4 已按锚点重移植）：

- **`note_stream_limit()`** —— 流内/流尾的限额失败也要记 (账号, 模型) 冷却。
  原生实现只在开流前的 HTTP 429 分支记账；一旦流式响应已开头（200 已发），
  中途被限额走的是收尾/中断路径，那里不记账 → 下一次请求又选中同一账号，
  直到上游用 HTTP 429 硬拒。这是「对话进行到一半被限额却不换号」的根因。
- **`reopen_switch`** —— 刚记了限额账就换号重试（而不是 pin 在失败账号上重连）。
- **`stream_error_text`** —— 捕获流内 `{"error": ...}` 帧正文，供上面分类器识别。

`workbuddy-wb_accounts.patch`：

- **`reserve_activated`** —— reserve 是「停放线」不是硬门槛。原生实现
  `remain <= reserve` 就拒发，导致正在进行的对话在余额跌破门槛的瞬间被切断；
  改为：跨过门槛后可以一路用完，归零才停，充值/签到回到门槛以上再放开。
  门槛值被修改时重置激活标记。

### xiaohuanxiong2api

`xhx2api.patch`：

- **`dailyBatchRemain()`**（替代 `earliestBatchExpiry` / `dailyBatchExpiry`）——
  按 FEFO 扣掉已消耗的每日积分，返回「最早会作废的那批」的**剩余**额度。
  原实现只累加 bills 里的 `daily_grant` 发放额度、从不扣减消耗：一个号领过
  300 分、第二天花光，那条流水和它的 `expire_at` 都还在，于是被报成
  「300 分要作废」。上游 `balance` 的 `daily_points` 才是现存每日积分的权威口径。
- **`PointAudit.ExpirePoints`** —— 把已算好的 `expire_points` 透出给面板，
  否则网关只能拿账号总余额冒充到期金额（虚报约 20 倍）。

`xhx2api-account_expiry_test.go.txt` 是对应的 7 组表驱动测试（含「已花光
不报临期」这个真实场景）。

### trae2api-hub

本地改动是 2 个 git 提交（未导出为 patch，因为上游 1.7.2 已吸收了同类修复，
我们的是超集）：

- **`_solo_usage()`** —— SOLO `token_usage` 帧里的缓存/积分字段
  （`cache_read_input_tokens` / `cached_tokens` / `credits` 等）此前被静默丢弃，
  面板永远显示 0% 缓存命中。改为透传上游实际给出的键，**上游没给的不补**。
- **`trae_usage_to_openai()`** —— 不再无条件写
  `prompt_tokens_details.cached_tokens = 0`：那会把「未上报」降级成
  「已知 0 命中」，`cached_reported` 随之误报。

上游 1.7.2 的 #13/#12 做了「不伪造」，但没有保留缓存字段，所以我们的版本
更完整，保留为 CHANGELOG 的 `[Unreleased]`。

### qoder2api-hub

`qoder2api-hub.patch`：

- `qoder_accounts.py` —— 签到窗口改 10:00 边界（官方规则，错过不补）
- `qoder_proxy.py` —— 账号级并发 `inflight` / `concurrencyLimit`
- `qoder_scheduler.py` —— `status()` 重算 `next_run_time`

### Buddy2api

`Buddy2api.patch`（最大的一份，+385/-127）：

- `providers/qwenwork/chat.py` —— qwenwork 渠道的工具调用支持（+253/-115）
- `store.py` / `token.py` / `database.py` / `server.py` —— 配套改动

## 应用方式

**有 git 的引擎（trae / qoder / Buddy / xhx2api 源码树）**：优先 rebase，
不要用 patch 硬打：

```bash
cd <engine>
git fetch origin --tags
git rebase origin/main      # 冲突时手工解，通常只有 CHANGELOG
```

**workbuddy2api-hub（无上游 git 基线）**：下载官方 release zip，解压到**新建空目录**，
按补丁里的锚点文本精确插入（每步校验锚点唯一），再校验关键字计数与旧版一致。
整目录先备份。

## 升级后必查

1. 版本号（常量 / CHANGELOG / HTTP 端点，各引擎不同，见 `check_engine_updates.py`）
2. 补丁关键字计数与升级前一致
3. `py_compile` / `go build` 通过
4. 引擎自带测试
5. 真实推理一次（HTTP 200 + 正常回复）
