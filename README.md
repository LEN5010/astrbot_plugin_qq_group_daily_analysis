## AstrBot 群聊日报插件

这是一个面向 AstrBot 的群聊分析插件，保留单群日报链路，并支持跨群联合日报聚合。

## 当前能力

- 单群日报分析
- 增量分析与最终归并
- 本地 JSON 持久化
- 跨群联合日报聚合
- 联合日报固定时间发送
- 联合日报全链路测试命令

## 联合日报说明

联合日报基于已有单群分析结果聚合，不引入额外数据库。

核心流程：

1. 读取 `union_groups_list` 中配置的源群单群 JSON
2. 统计冠军群、聚合话题和金句
3. 调用 LLM 生成跨群 Top 3 金句与全局点评
4. 渲染专属联合日报模板并发送

固定时间模式支持两段式调度：

- 提前 `union_prepare_lead_minutes` 分钟启动源群单群链路
- 在 `union_report_time` 到点后最多等待 `union_wait_timeout_minutes` 分钟

## 关键配置

- `union_report_enabled`
- `union_groups_list`
- `union_target_groups_list`
- `union_report_time`
- `union_prepare_lead_minutes`
- `union_wait_timeout_minutes`

建议在跨平台场景下统一使用完整会话 ID，例如 `onebot:GroupMessage:123456`。

## 常用命令

```text
/群分析 [天数]
/增量状态
/联合日报测试 [YYYY-MM-DD]
```

`/联合日报测试` 会执行联合日报全链路，但最终只发送到当前群，避免误群发。

## 发布前建议

- 修改 `metadata.yaml` 中的插件名称、作者和仓库地址
- 检查插件配置默认值是否符合你的部署环境
- 根据你的仓库策略补充新的 CI、Issue 模板和发布流程
