"""
跨群聚合日报应用服务

基于 HistoryRepository 中已落盘的单群 JSON 日报，生成跨群聚合结果。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...infrastructure.analysis.utils.json_utils import parse_json_object_response
from ...infrastructure.analysis.utils.llm_utils import (
    call_provider_with_retry,
    extract_response_text,
    extract_token_usage,
)
from ...infrastructure.analysis.utils.structured_output_schema import (
    JSONObject,
    build_response_format,
)
from ...infrastructure.utils.template_utils import render_template
from ...utils.logger import logger


@dataclass
class UnionGroupSnapshot:
    """单个群在指定日报日期上的聚合快照。"""

    group_ref: str
    platform_id: str
    group_id: str
    group_name: str
    total_messages: int
    participant_count: int
    topics_count: int
    golden_quotes_count: int


@dataclass
class UnionTopic:
    """跨群聚合后的话题条目。"""

    group_ref: str
    platform_id: str
    group_id: str
    group_name: str
    topic: str
    detail: str
    contributors: list[str] = field(default_factory=list)


@dataclass
class UnionQuote:
    """跨群聚合后的金句条目。"""

    group_ref: str
    platform_id: str
    group_id: str
    group_name: str
    content: str
    sender: str
    reason: str


@dataclass
class UnionDailyReport:
    """跨群聚合日报结果。"""

    report_date: str
    champion_group: UnionGroupSnapshot
    group_snapshots: list[UnionGroupSnapshot]
    top_quotes: list[UnionQuote]
    topic_highlights: list[UnionTopic]
    overview: str
    total_messages: int
    total_participants: int
    missing_groups: list[str] = field(default_factory=list)
    llm_token_usage: dict[str, int] = field(default_factory=dict)


class UnionDailyReportService:
    """跨群聚合日报服务。"""

    _DEFAULT_PROMPT = """
你正在为 5 个关系密切的群聊生成一份“跨群聚合日报”。

请基于下面提供的群级统计、候选金句和候选话题，完成两个任务：
1. 从全部候选金句中，严格挑选“今日五群总榜 Top 3 金句”。
2. 生成一段约 200 字的“跨群整体活跃度及大事件点评”。

输出要求：
- 只返回 JSON。
- `top_quotes` 必须是 3 条以内。
- 不要编造未出现在输入中的群号、金句原文或发言人。
- 点评要有整体感，既要提到冠军群，也要概括跨群共性与差异。

统计概览：
${groups_summary_text}

候选金句：
${quotes_text}

候选话题：
${topics_text}
"""

    def __init__(self, config_manager: Any, history_repository: Any, llm_context: Any):
        self.config_manager = config_manager
        self.history_repository = history_repository
        self.llm_context = llm_context

    async def build_union_report(
        self,
        group_refs: list[str],
        report_date: str,
    ) -> UnionDailyReport | None:
        """
        基于指定群列表的单群 JSON 报告，构建跨群聚合日报。
        """
        normalized_group_refs = self._deduplicate_group_refs(group_refs)
        if not normalized_group_refs:
            logger.info("跨群聚合日报跳过：未配置有效的源群列表")
            return None

        group_snapshots: list[UnionGroupSnapshot] = []
        all_quotes: list[UnionQuote] = []
        all_topics: list[UnionTopic] = []
        missing_groups: list[str] = []

        for group_ref in normalized_group_refs:
            analysis_result = self.history_repository.get_analysis_result(
                group_ref, report_date
            )
            if not analysis_result:
                missing_groups.append(group_ref)
                continue

            snapshot = self._build_group_snapshot(group_ref, analysis_result)
            group_snapshots.append(snapshot)
            all_quotes.extend(
                self._extract_quotes(
                    snapshot.group_ref,
                    snapshot.platform_id,
                    snapshot.group_id,
                    snapshot.group_name,
                    analysis_result,
                )
            )
            all_topics.extend(
                self._extract_topics(
                    snapshot.group_ref,
                    snapshot.platform_id,
                    snapshot.group_id,
                    snapshot.group_name,
                    analysis_result,
                )
            )

        if not group_snapshots:
            logger.warning(f"跨群聚合日报跳过：{report_date} 没有任何可用单群 JSON 结果")
            return None

        champion_group = max(
            group_snapshots,
            key=lambda item: (
                item.total_messages,
                item.participant_count,
                item.group_ref,
            ),
        )

        top_quotes, overview, token_usage = await self._generate_llm_summary(
            report_date=report_date,
            champion_group=champion_group,
            group_snapshots=group_snapshots,
            all_quotes=all_quotes,
            all_topics=all_topics,
        )

        total_messages = sum(item.total_messages for item in group_snapshots)
        total_participants = sum(item.participant_count for item in group_snapshots)

        return UnionDailyReport(
            report_date=report_date,
            champion_group=champion_group,
            group_snapshots=group_snapshots,
            top_quotes=top_quotes,
            topic_highlights=all_topics[:6],
            overview=overview,
            total_messages=total_messages,
            total_participants=total_participants,
            missing_groups=missing_groups,
            llm_token_usage=token_usage,
        )

    def _deduplicate_group_refs(self, group_refs: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for group_ref in group_refs:
            text = str(group_ref).strip()
            if not text or text in seen:
                continue
            normalized.append(text)
            seen.add(text)
        return normalized

    def _parse_group_ref(self, group_ref: str) -> tuple[str, str]:
        text = str(group_ref).strip()
        if not text:
            return "", ""
        if ":" not in text:
            return "", text

        parts = text.split(":")
        return parts[0].strip(), parts[-1].strip()

    def _build_group_snapshot(
        self,
        group_ref: str,
        analysis_result: dict[str, Any],
    ) -> UnionGroupSnapshot:
        statistics = analysis_result.get("statistics", {}) or {}
        stored_group_ref = str(analysis_result.get("group_ref", "")).strip() or group_ref
        stored_platform_id = str(analysis_result.get("platform_id", "")).strip()
        stored_group_id = str(analysis_result.get("group_id", "")).strip()
        parsed_platform_id, parsed_group_id = self._parse_group_ref(stored_group_ref)
        platform_id = stored_platform_id or parsed_platform_id
        group_id = stored_group_id or parsed_group_id or group_ref
        group_name = str(
            analysis_result.get("group_name")
            or analysis_result.get("group_title")
            or group_id
        )

        return UnionGroupSnapshot(
            group_ref=stored_group_ref,
            platform_id=platform_id,
            group_id=group_id,
            group_name=group_name,
            total_messages=int(
                analysis_result.get("total_messages")
                or statistics.get("message_count")
                or 0
            ),
            participant_count=int(
                analysis_result.get("participant_count")
                or statistics.get("participant_count")
                or 0
            ),
            topics_count=len(analysis_result.get("topics", []) or []),
            golden_quotes_count=len(
                (statistics.get("golden_quotes") or analysis_result.get("golden_quotes") or [])
            ),
        )

    def _extract_quotes(
        self,
        group_ref: str,
        platform_id: str,
        group_id: str,
        group_name: str,
        analysis_result: dict[str, Any],
    ) -> list[UnionQuote]:
        statistics = analysis_result.get("statistics", {}) or {}
        quote_items = statistics.get("golden_quotes") or analysis_result.get(
            "golden_quotes", []
        )

        quotes: list[UnionQuote] = []
        for item in quote_items or []:
            if not isinstance(item, dict):
                continue

            content = str(item.get("content", "")).strip()
            sender = str(item.get("sender", "")).strip()
            reason = str(item.get("reason", "")).strip()
            if not content or not sender:
                continue

            quotes.append(
                UnionQuote(
                    group_ref=group_ref,
                    platform_id=platform_id,
                    group_id=group_id,
                    group_name=group_name,
                    content=content,
                    sender=sender,
                    reason=reason,
                )
            )

        return quotes

    def _extract_topics(
        self,
        group_ref: str,
        platform_id: str,
        group_id: str,
        group_name: str,
        analysis_result: dict[str, Any],
    ) -> list[UnionTopic]:
        topics: list[UnionTopic] = []
        for item in analysis_result.get("topics", []) or []:
            if not isinstance(item, dict):
                continue

            topic = str(item.get("topic", "")).strip()
            detail = str(item.get("detail", "")).strip()
            if not topic or not detail:
                continue

            contributors_raw = item.get("contributors", [])
            contributors = (
                [str(value).strip() for value in contributors_raw if str(value).strip()]
                if isinstance(contributors_raw, list)
                else []
            )

            topics.append(
                UnionTopic(
                    group_ref=group_ref,
                    platform_id=platform_id,
                    group_id=group_id,
                    group_name=group_name,
                    topic=topic,
                    detail=detail,
                    contributors=contributors,
                )
            )

        return topics

    async def _generate_llm_summary(
        self,
        report_date: str,
        champion_group: UnionGroupSnapshot,
        group_snapshots: list[UnionGroupSnapshot],
        all_quotes: list[UnionQuote],
        all_topics: list[UnionTopic],
    ) -> tuple[list[UnionQuote], str, dict[str, int]]:
        prompt = self._build_prompt(
            report_date=report_date,
            champion_group=champion_group,
            group_snapshots=group_snapshots,
            all_quotes=all_quotes,
            all_topics=all_topics,
        )

        if not prompt.strip():
            return self._fallback_summary(champion_group, group_snapshots, all_quotes)

        response = await call_provider_with_retry(
            context=self.llm_context,
            config_manager=self.config_manager,
            prompt=prompt,
            provider_id_key="union_report_provider_id",
            response_format=build_response_format(
                "union_daily_report",
                self._build_union_report_schema(),
            ),
        )

        if response is None:
            logger.warning("跨群聚合日报 LLM 调用失败，回退到模板摘要")
            return self._fallback_summary(champion_group, group_snapshots, all_quotes)

        result_text = extract_response_text(response)
        token_usage = extract_token_usage(response)
        ok, parsed, error_message = parse_json_object_response(
            result_text, "union_daily_report"
        )
        if not ok or not isinstance(parsed, dict):
            logger.warning(
                f"跨群聚合日报解析失败，回退到模板摘要: {error_message or 'unknown'}"
            )
            return self._fallback_summary(
                champion_group, group_snapshots, all_quotes, token_usage
            )

        top_quotes = self._map_llm_quotes(parsed.get("top_quotes"), all_quotes)
        if not top_quotes:
            top_quotes = all_quotes[:3]

        overview = str(parsed.get("global_commentary", "")).strip()
        if not overview:
            _, overview, _ = self._fallback_summary(
                champion_group, group_snapshots, all_quotes, token_usage
            )

        return top_quotes[:3], overview, token_usage

    def _build_prompt(
        self,
        report_date: str,
        champion_group: UnionGroupSnapshot,
        group_snapshots: list[UnionGroupSnapshot],
        all_quotes: list[UnionQuote],
        all_topics: list[UnionTopic],
    ) -> str:
        groups_summary_lines = [
            (
                f"- 群 {item.group_name} ({item.group_ref})："
                f"消息 {item.total_messages}，参与人数 {item.participant_count}，"
                f"话题 {item.topics_count}，金句 {item.golden_quotes_count}"
            )
            for item in group_snapshots
        ]
        groups_summary_lines.append(
            f"- 今日冠军群：{champion_group.group_name} ({champion_group.group_ref})，"
            f"消息 {champion_group.total_messages}"
        )

        quotes_text = "\n".join(
            [
                (
                    f"{index}. [{quote.group_name}/{quote.group_ref}] "
                    f"{quote.sender}: {quote.content} | 理由: {quote.reason}"
                )
                for index, quote in enumerate(all_quotes[:20], 1)
            ]
        )
        topics_text = "\n".join(
            [
                (
                    f"{index}. [{topic.group_name}/{topic.group_ref}] "
                    f"{topic.topic} | 参与者: {'、'.join(topic.contributors[:4]) or '群友'}"
                    f" | 详情: {topic.detail}"
                )
                for index, topic in enumerate(all_topics[:20], 1)
            ]
        )

        prompt_template = self.config_manager.get_union_daily_analysis_prompt()
        if not prompt_template:
            prompt_template = self._DEFAULT_PROMPT

        return render_template(
            prompt_template,
            report_date=report_date,
            groups_summary_text="\n".join(groups_summary_lines),
            quotes_text=quotes_text or "无可用金句",
            topics_text=topics_text or "无可用话题",
        )

    def _build_union_report_schema(self) -> JSONObject:
        return {
            "type": "object",
            "properties": {
                "top_quotes": {
                    "type": "array",
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": {"type": "string"},
                            "sender": {"type": "string"},
                            "group_ref": {"type": "string"},
                            "reason": {"type": "string"},
                        },
                        "required": ["content", "sender", "group_ref", "reason"],
                        "additionalProperties": False,
                    },
                },
                "global_commentary": {"type": "string"},
            },
            "required": ["top_quotes", "global_commentary"],
            "additionalProperties": False,
        }

    def _map_llm_quotes(
        self,
        raw_quotes: Any,
        source_quotes: list[UnionQuote],
    ) -> list[UnionQuote]:
        if not isinstance(raw_quotes, list):
            return []

        matched_quotes: list[UnionQuote] = []
        used_indexes: set[int] = set()

        for raw_quote in raw_quotes:
            if not isinstance(raw_quote, dict):
                continue

            raw_content = str(raw_quote.get("content", "")).strip()
            raw_group_ref = str(raw_quote.get("group_ref", "")).strip()

            matched_index = None
            for index, source_quote in enumerate(source_quotes):
                if index in used_indexes:
                    continue
                if raw_group_ref and source_quote.group_ref != raw_group_ref:
                    continue
                if raw_content and source_quote.content == raw_content:
                    matched_index = index
                    break

            if matched_index is None and raw_content:
                for index, source_quote in enumerate(source_quotes):
                    if index in used_indexes:
                        continue
                    if raw_content in source_quote.content or source_quote.content in raw_content:
                        matched_index = index
                        break

            if matched_index is None:
                continue

            matched_quotes.append(source_quotes[matched_index])
            used_indexes.add(matched_index)

        return matched_quotes

    def _fallback_summary(
        self,
        champion_group: UnionGroupSnapshot,
        group_snapshots: list[UnionGroupSnapshot],
        all_quotes: list[UnionQuote],
        token_usage: dict[str, int] | None = None,
    ) -> tuple[list[UnionQuote], str, dict[str, int]]:
        top_quotes = all_quotes[:3]
        total_messages = sum(item.total_messages for item in group_snapshots)
        total_participants = sum(item.participant_count for item in group_snapshots)
        overview = (
            f"今日共覆盖 {len(group_snapshots)} 个群，累计消息 {total_messages} 条，"
            f"总参与人数 {total_participants}。活跃冠军群是"
            f"{champion_group.group_name}，贡献了 {champion_group.total_messages} 条消息。"
            "整体讨论既有延续性，也有明显的跨群热点扩散，适合作为后续重点维护与二次互动的素材池。"
        )
        return top_quotes, overview, token_usage or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
