"""
跨群聚合日报应用服务

基于 HistoryRepository 中已落盘的单群 JSON 日报，生成跨群聚合结果。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
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
class UnionWaterKing:
    """全局最活跃用户。"""

    user_id: str
    nickname: str
    message_count: int
    group_ref: str
    group_name: str
    platform_id: str


@dataclass
class UnionDailyReport:
    """跨群聚合日报结果。"""

    report_date: str
    champion_group: UnionGroupSnapshot
    group_snapshots: list[UnionGroupSnapshot]
    top_quotes: list[UnionQuote]
    topic_highlights: list[UnionTopic]
    water_king: UnionWaterKing | None
    overview: str
    total_messages: int
    total_participants: int
    missing_groups: list[str] = field(default_factory=list)
    llm_token_usage: dict[str, int] = field(default_factory=dict)


class UnionDailyReportService:
    """跨群聚合日报服务。"""

    _DEFAULT_PROMPT = """
你正在为“A海岸”生成一份联合日报。

请基于下面提供的群级统计、候选金句和候选话题，完成两个任务：
1. 从全部候选金句中，严格挑选“今日 A海岸 Top 3 金句”。
2. 生成一段约 200 字的“今日 A海岸整体活跃度及大事件点评”。

输出要求：
- 只返回 JSON。
- `top_quotes` 必须是 3 条以内。
- 不要编造未出现在输入中的群号、金句原文或发言人。
- 点评要有整体感，既要提到最活跃群，也要概括 A海岸整体共性与差异。

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
        water_king: UnionWaterKing | None = None
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
            water_king = self._pick_water_king(
                current=water_king,
                candidate=self._extract_water_king(
                    snapshot.group_ref,
                    snapshot.platform_id,
                    snapshot.group_name,
                    analysis_result,
                ),
            )
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
        topic_highlights = self._select_topic_highlights(group_snapshots, all_topics)

        return UnionDailyReport(
            report_date=report_date,
            champion_group=champion_group,
            group_snapshots=group_snapshots,
            top_quotes=top_quotes,
            topic_highlights=topic_highlights,
            water_king=water_king,
            overview=overview,
            total_messages=total_messages,
            total_participants=total_participants,
            missing_groups=missing_groups,
            llm_token_usage=token_usage,
        )

    def get_missing_group_refs_for_date(
        self,
        group_refs: list[str],
        report_date: str,
    ) -> list[str]:
        """
        检查指定日期下哪些源群的单群 JSON 日报尚未就绪。
        """
        missing_group_refs: list[str] = []
        normalized_group_refs = self._deduplicate_group_refs(group_refs)
        for group_ref in normalized_group_refs:
            analysis_result = self.history_repository.get_analysis_result(
                group_ref,
                report_date,
            )
            if analysis_result is None:
                missing_group_refs.append(group_ref)
        return missing_group_refs

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

    def _extract_water_king(
        self,
        group_ref: str,
        platform_id: str,
        group_name: str,
        analysis_result: dict[str, Any],
    ) -> UnionWaterKing | None:
        raw_user_analysis = analysis_result.get("user_analysis", {}) or {}
        if not isinstance(raw_user_analysis, dict):
            return None

        best_user: UnionWaterKing | None = None
        for user_id, raw_stats in raw_user_analysis.items():
            if not isinstance(raw_stats, dict):
                continue

            message_count = int(raw_stats.get("message_count") or 0)
            if message_count <= 0:
                continue

            nickname = str(raw_stats.get("nickname") or user_id).strip() or str(user_id)
            candidate = UnionWaterKing(
                user_id=str(user_id).strip(),
                nickname=nickname,
                message_count=message_count,
                group_ref=group_ref,
                group_name=group_name,
                platform_id=platform_id,
            )
            best_user = self._pick_water_king(best_user, candidate)

        return best_user

    def _pick_water_king(
        self,
        current: UnionWaterKing | None,
        candidate: UnionWaterKing | None,
    ) -> UnionWaterKing | None:
        if candidate is None:
            return current
        if current is None:
            return candidate

        current_key = (
            current.message_count,
            current.group_name,
            current.nickname,
            current.user_id,
        )
        candidate_key = (
            candidate.message_count,
            candidate.group_name,
            candidate.nickname,
            candidate.user_id,
        )
        return candidate if candidate_key > current_key else current

    def _extract_topics(
        self,
        group_ref: str,
        platform_id: str,
        group_id: str,
        group_name: str,
        analysis_result: dict[str, Any],
    ) -> list[UnionTopic]:
        user_display_map = self._build_user_display_map(analysis_result, group_name)
        topics: list[UnionTopic] = []
        for item in analysis_result.get("topics", []) or []:
            if not isinstance(item, dict):
                continue

            topic = str(item.get("topic", "")).strip()
            detail = self._sanitize_topic_detail(
                str(item.get("detail", "")).strip(),
                user_display_map,
            )
            if not topic or not detail:
                continue

            contributors_raw = item.get("contributors", [])
            contributors = self._sanitize_topic_contributors(
                contributors_raw if isinstance(contributors_raw, list) else [],
                user_display_map,
                group_name,
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

    def _build_user_display_map(
        self,
        analysis_result: dict[str, Any],
        group_name: str,
    ) -> dict[str, str]:
        raw_user_analysis = analysis_result.get("user_analysis", {}) or {}
        if not isinstance(raw_user_analysis, dict):
            return {}

        user_display_map: dict[str, str] = {}
        for raw_user_id, raw_stats in raw_user_analysis.items():
            user_id = str(raw_user_id).strip()
            if not user_id or not user_id.isdigit():
                continue

            nickname = user_id
            if isinstance(raw_stats, dict):
                nickname = str(raw_stats.get("nickname") or user_id).strip() or user_id

            if nickname == user_id:
                nickname = "群友"

            user_display_map[user_id] = f"【{nickname}】（来自{group_name}）"

        return user_display_map

    def _sanitize_topic_contributors(
        self,
        contributors: list[str],
        user_display_map: dict[str, str],
        group_name: str,
    ) -> list[str]:
        sanitized: list[str] = []
        seen: set[str] = set()

        for raw_value in contributors:
            text = str(raw_value).strip()
            if not text:
                continue

            if text.isdigit():
                text = user_display_map.get(text, f"【群友】（来自{group_name}）")

            if text in seen:
                continue
            sanitized.append(text)
            seen.add(text)

        return sanitized

    def _sanitize_topic_detail(
        self,
        detail: str,
        user_display_map: dict[str, str],
    ) -> str:
        sanitized = detail
        for user_id in sorted(user_display_map.keys(), key=len, reverse=True):
            replacement = user_display_map[user_id]
            sanitized = re.sub(rf"\[{re.escape(user_id)}\]", replacement, sanitized)
            sanitized = re.sub(
                rf"(?<!\d){re.escape(user_id)}(?!\d)",
                replacement,
                sanitized,
            )
        return sanitized

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
                f"- {item.group_name}："
                f"消息 {item.total_messages}，参与人数 {item.participant_count}，"
                f"话题 {item.topics_count}"
            )
            for item in group_snapshots
        ]
        groups_summary_lines.append(
            f"- 今日 A海岸最活跃群：{champion_group.group_name}，"
            f"消息 {champion_group.total_messages}"
        )

        quotes_text = "\n".join(
            [
                (
                    f"{index}. [{quote.group_name}] "
                    f"{quote.sender}: {quote.content} | 理由: {quote.reason}"
                )
                for index, quote in enumerate(all_quotes[:20], 1)
            ]
        )
        topics_text = "\n".join(
            [
                (
                    f"{index}. [{topic.group_name}] "
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
            f"今日 A海岸累计消息 {total_messages} 条，总参与人数 {total_participants}。"
            f"最活跃的是 {champion_group.group_name}，贡献了 {champion_group.total_messages} 条消息。"
            "整体讨论既有延续性，也有明显的热点扩散，适合作为后续重点维护与二次互动的素材池。"
        )
        return top_quotes, overview, token_usage or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    def _select_topic_highlights(
        self,
        group_snapshots: list[UnionGroupSnapshot],
        all_topics: list[UnionTopic],
        limit: int = 6,
    ) -> list[UnionTopic]:
        if limit <= 0 or not all_topics:
            return []

        selected: list[UnionTopic] = []
        selected_indexes: set[int] = set()

        # 第一轮：每个有话题的群至少保底 1 条，按群快照顺序选首条。
        for snapshot in group_snapshots:
            if len(selected) >= limit:
                break
            for index, topic in enumerate(all_topics):
                if index in selected_indexes:
                    continue
                if topic.group_ref != snapshot.group_ref:
                    continue
                selected.append(topic)
                selected_indexes.add(index)
                break

        # 第二轮：用剩余热点补足名额，仍保持原始排序。
        if len(selected) < limit:
            for index, topic in enumerate(all_topics):
                if index in selected_indexes:
                    continue
                selected.append(topic)
                selected_indexes.add(index)
                if len(selected) >= limit:
                    break

        return selected
