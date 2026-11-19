# 小浣熊思考档位

## 已确认的请求协议

官方桌面客户端 1.2.2 的内置 Box-Agent 经 OpenAI Chat Completions 发送顶层 `raccoon_thinking_level`。

```json
{
  "model": "raccoon/sn-qwen4-max",
  "messages": [{"role": "user", "content": "你好"}],
  "raccoon_thinking_level": "level_3",
  "stream": true
}
```

上述 model 是 Gateway Hub 的公开名称，网关去掉平台前缀后转发。JSON 中无需再包一层 `extra_body`：Python OpenAI SDK 的 `extra_body={...}` 是方法参数，SDK 会把其中字段合并到 HTTP JSON 顶层。

| 原生值 | 通用标签 |
|---|---|
| `off` | Off |
| `level_1` | Minimal |
| `level_2` | Low |
| `level_3` | Medium |
| `level_4` | High |
| `level_5` | Max |

这张表是通用协议枚举，不表示每款模型都提供六个独立选项。应以官方模型目录 `params.raccoon_thinking` 中的 `supported`、`can_disable`、`display_levels` 和 `default_level` 为准。

2026-11-19 留存的目录响应中，`sn-qwen4-max` 与 `sn-deepseek-v4-1-flash-1017` 展示 Minimal／Medium／Max，允许关闭，默认 Off。这个快照不是未来版本的支持承诺。

## 网关与引擎边界

- Gateway Hub 保留显式的 `raccoon_thinking_level`，不会自行把 `reasoning_effort` 翻译为原生等级。
- 网页的“快速／深度思考”布尔选项与桌面版模型档位是不同接口概念；不能用 `deepThink` 或 `deep_think` 替代上述字段。
- 核对过的 xhx2api 提交 `ed4aff22a780b8068d6ae09ebeb34746b49a3ede` 的 Chat Completions 路径保留额外 body 字段；其 Responses 转换会重建请求对象，未复制原生等级。
- 因此在该引擎版本使用 `/v1/responses` 或其它转换路径时，应单独核对字段保留情况，不能把 Chat 路径结论推广到所有入口。

## ZCode 配置示例

使用支持自定义 option map 的 ZCode，在对应的 **Chat Completions** 模型设置中配置：

```json
{
  "values": ["disabled", "minimal", "medium", "max"],
  "map": "{\"raccoon_thinking_level\": reasoningLevel == \"disabled\" ? \"off\" : reasoningLevel == \"minimal\" ? \"level_1\" : reasoningLevel == \"medium\" ? \"level_3\" : \"level_5\", \"reasoning_effort\": null, \"reasoning\": null, \"thinking\": null, \"enable_thinking\": null}"
}
```

这是 `optionSpecs.reasoningLevel` 的配置片段，不是完整供应商配置。`null` 在该版本 option map 的 JSON merge patch 中表示删除旧的通用思考字段；不要将此片段直接作为 HTTP 请求发送。以当前 ZCode 的 schema 为准，不共享个人 provider 配置或 API Key。

## 验证范围

协议依据是官方安装工件中的静态发包实现与 OpenAI SDK 序列化代码，不是 HTTP 200 或少量 token 长度波动。没有依据宣称：

- 每个等级对应固定推理 token 数；
- Max 必然比 Medium 更准确；
- 没有 reasoning 内容或 `reasoning_tokens=0` 就完全没有内部推理；
- 当前账号上的在线效果、延迟或价格已完成分档标定。

若自行验证，应使用正确字段、小量对照和一致的输入及输出上限，遇到限流停止。不要为验证档位批量换号或连续探测。

源码边界参考：

- [xhx2api Chat handler（固定提交）](https://github.com/rsfree/xhx2api/blob/ed4aff22a780b8068d6ae09ebeb34746b49a3ede/internal/server/handler.go)
- [xhx2api Responses 转换（固定提交）](https://github.com/rsfree/xhx2api/blob/ed4aff22a780b8068d6ae09ebeb34746b49a3ede/internal/server/responses.go)
- [xhx2api 出站客户端（固定提交）](https://github.com/rsfree/xhx2api/blob/ed4aff22a780b8068d6ae09ebeb34746b49a3ede/internal/upstream/client.go)
