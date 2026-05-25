# AstrBot 增量跨群联合日报插件

这是一个面向 AstrBot 的跨群联合日报插件。当前版本只保留一条链路：

```text
源群增量采集 -> 源群最终 JSON -> 跨群聚合 -> 联合日报图片发送
```

原项目来自 [SXP-Simon/astrbot_plugin_qq_group_daily_analysis](https://github.com/SXP-Simon/astrbot_plugin_qq_group_daily_analysis)。本仓库基于原项目的跨平台消息获取、LLM 分析、增量批次、HTML 渲染和 AstrBot 插件结构继续改造，删除了单群日报、模板切换、格式切换、群文件/相册上传等不在当前业务链路中的功能。

## 当前能力

- 按固定活跃时段对 `union_groups_list` 中的源群执行增量分析
- 在联合日报发送前，将每个源群的增量批次合并为当天 JSON 中间结果
- 严格等待所有源群 JSON 就绪后生成跨群联合日报
- 从候选金句中选择 Top 3，并生成联合日报全局点评
- 复用 AstrBot 人格，为最终展示的每条金句和话题生成一句点评
- 固定输出联合日报图片，主题色为 `#E799B0`

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

`union_target_groups` 必填。为空时不会回退发送到源群。

推荐所有群都填写完整会话 ID：

```text
onebot:GroupMessage:123456
telegram:GroupMessage:-1001234567890
```

## 命令

```text
/联合日报测试 [YYYY-MM-DD]
```

该命令会使用现有增量批次生成指定日期的联合日报，并只发送到当前群，避免误群发。

## 链路规则

- 源群固定来自 `union_groups_list`，不再存在单独的定时名单或增量名单。
- 所有源群都走增量分析，不再回退到传统全量分析。
- 任一源群缺少最终 JSON，本次联合日报失败。
- 联合总结 JSON、人格点评 JSON 或图片渲染失败时，本次联合日报失败。
- 插件不做旧配置或旧 KV 数据迁移。

## 原项目贡献与协议

本项目基于 [SXP-Simon/astrbot_plugin_qq_group_daily_analysis](https://github.com/SXP-Simon/astrbot_plugin_qq_group_daily_analysis) 改造。原项目提供了群聊日报的主要工程基础，包括平台适配、消息清洗、统计分析、LLM 分析器、报告渲染和插件生命周期结构。

本地 `LICENSE` 为 MIT License，保留原许可证文本与版权声明。继续分发或二次开发时，请遵守 MIT 协议要求并保留许可证与原项目贡献说明。
