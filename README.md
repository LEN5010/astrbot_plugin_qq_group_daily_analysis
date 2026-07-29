# AstrBot 增量跨群联合日报插件

这是一个面向 AstrBot 的增量日报插件。当前版本包含两条链路：

```text
源群增量采集 -> 源群最终 JSON -> 跨群聚合 -> 联合日报图片发送
目标群增量采集 -> 目标群最终 JSON -> 单群日报图片发送
```

原项目来自 [SXP-Simon/astrbot_plugin_qq_group_daily_analysis](https://github.com/SXP-Simon/astrbot_plugin_qq_group_daily_analysis)。本仓库基于原项目的跨平台消息获取、LLM 分析、增量批次、HTML 渲染和 AstrBot 插件结构继续改造，保留联合日报与单群日报两种增量链路。

## 当前能力

- 按固定活跃时段对 `union_groups_list` 中的源群执行增量分析
- 在联合日报发送前，将每个源群的增量批次合并为当天 JSON 中间结果
- 严格等待所有源群 JSON 就绪后生成跨群联合日报
- 从候选金句中选择 Top 3，并生成联合日报全局点评
- 复用 AstrBot 人格，为最终展示的每条金句和话题生成 10-30 字点评
- 固定输出联合日报图片，主题色为 `#E799B0`
- 为配置的目标群生成并发送独立的单群日报
- 按日期和接收群归档日报图片，支持普通群成员回看

## 配置要点

基础准入：

- `basic.group_list_mode`
- `basic.group_list`
- `basic.analysis_days`
- `basic.bot_self_ids`

增量源群：

- `union_report.enabled`
- `union_report.union_groups_list`
- `incremental.interval_minutes`
- `incremental.max_daily_analyses`
- `incremental.safe_limit`
- `incremental.min_messages`
- `incremental.topics_per_batch`
- `incremental.quotes_per_batch`
- `incremental.active_start_hour`
- `incremental.active_end_hour`

发送目标：

- `union_report.union_target_groups`
- `union_report.union_report_time`
- `union_report.union_prepare_lead_minutes`
- `union_report.union_wait_timeout_minutes`

提示词：

- `prompts.union_daily_report_prompts.union_daily_report_prompt`
- `prompts.union_daily_report_prompts.persona_comment_prompt`

`union_target_groups` 必填。为空时不会回退发送到源群。

推荐所有群都填写完整会话 ID：

```text
onebot:GroupMessage:123456
telegram:GroupMessage:-1001234567890
```

## 命令

```text
/联合日报测试 [YYYY-MM-DD]
/昨日日报
/日报 -d YYYY-MM-DD
```

`/联合日报测试` 会使用现有增量批次生成指定日期的联合日报，并只发送到当前群，避免误群发。

每天成功生成的联合日报都会在插件数据目录中保存一份图片副本。群内普通成员可使用
`/昨日日报` 查看昨天的联合日报，或使用 `/日报 -d YYYY-MM-DD` 查看指定日期的联合日报。
查询仍会遵守插件的群黑白名单。单群日报不会进入该归档，也不会由这两个命令返回。

## 链路规则

- 源群固定来自 `union_groups_list`，不再存在单独的定时名单或增量名单。
- 所有源群都走增量分析，不再回退到传统全量分析。
- 任一源群缺少最终 JSON，本次联合日报失败。
- 联合总结 JSON、人格点评 JSON 或图片渲染失败时，本次联合日报失败。
- 插件不做旧配置或旧 KV 数据迁移。

## 原项目贡献与协议

本项目基于 [SXP-Simon/astrbot_plugin_qq_group_daily_analysis](https://github.com/SXP-Simon/astrbot_plugin_qq_group_daily_analysis) 改造。原项目提供了群聊日报的主要工程基础，包括平台适配、消息清洗、统计分析、LLM 分析器、报告渲染和插件生命周期结构。

本地 `LICENSE` 为 MIT License，保留原许可证文本与版权声明。继续分发或二次开发时，请遵守 MIT 协议要求并保留许可证与原项目贡献说明。
