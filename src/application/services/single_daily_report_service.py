"""
单群日报应用服务。

该服务只消费增量最终 JSON，不调用跨群聚合和 LLM。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...utils.logger import logger


@dataclass
class SingleTopic:
    """单群日报话题条目。"""

    topic: str
    detail: str
    contributors: list[str] = field(default_factory=list)


@dataclass
class SingleQuote:
    """单群日报金句条目。"""

    content: str
    sender: str
    reason: str


@dataclass
class SingleWaterKing:
    """单群日报最活跃用户。"""

    user_id: str
    nickname: str
    message_count: int


@dataclass
class SingleDailyReport:
    """单群日报结果。"""

    report_date: str
    group_ref: str
    platform_id: str
    group_id: str
    group_name: str
    total_messages: int
    participant_count: int
    total_characters: int
    most_active_period: str
    topics: list[SingleTopic]
    golden_quotes: list[SingleQuote]
    water_king: SingleWaterKing | None


class SingleDailyReportService:
    """基于已落盘的单群最终 JSON 构建日报展示数据。"""

    def __init__(self, config_manager: Any, history_repository: Any):
        self.config_manager = config_manager
        self.history_repository = history_repository
        self.last_failure_reason: str | None = None

    def get_missing_group_refs_for_date(
        self, group_refs: list[str], report_date: str
    ) -> list[str]:
        """返回指定日期缺少最终 JSON 的群引用。"""
        missing: list[str] = []
        for group_ref in group_refs:
            if not self.history_repository.get_analysis_result(group_ref, report_date):
                missing.append(group_ref)
        return missing

    async def build_single_report(
        self, group_ref: str, report_date: str
    ) -> SingleDailyReport | None:
        """读取单群最终 JSON 并构建日报。"""
        self.last_failure_reason = None
        result = self.history_repository.get_analysis_result(group_ref, report_date)
        if not isinstance(result, dict):
            self.last_failure_reason = "single_report_not_ready"
            logger.warning("单群日报缺少最终 JSON: group_ref=%s date=%s", group_ref, report_date)
            return None

        statistics = result.get("statistics") or {}
        total_messages = self._as_int(
            result.get("total_messages", self._value(statistics, "message_count", 0))
        )
        participant_count = self._as_int(
            result.get(
                "participant_count",
                self._value(statistics, "participant_count", 0),
            )
        )
        if total_messages <= 0:
            self.last_failure_reason = "invalid_single_report_data"
            logger.warning(
                "单群日报最终 JSON 无有效消息统计: group_ref=%s date=%s",
                group_ref,
                report_date,
            )
            return None

        topics = self._build_topics(result.get("topics") or [])
        quotes = self._build_quotes(self._value(statistics, "golden_quotes", []))
        water_king = self._build_water_king(result.get("user_analysis") or {})

        platform_id, group_id = self._parse_group_ref(
            str(result.get("group_ref") or group_ref)
        )
        return SingleDailyReport(
            report_date=report_date,
            group_ref=str(result.get("group_ref") or group_ref),
            platform_id=str(result.get("platform_id") or platform_id or ""),
            group_id=str(result.get("group_id") or group_id),
            group_name=str(result.get("group_name") or group_id),
            total_messages=total_messages,
            participant_count=participant_count,
            total_characters=self._as_int(self._value(statistics, "total_characters", 0)),
            most_active_period=str(
                self._value(statistics, "most_active_period", "") or "暂无"
            ),
            topics=topics,
            golden_quotes=quotes,
            water_king=water_king,
        )

    def _build_topics(self, raw_topics: Any) -> list[SingleTopic]:
        max_topics = max(0, int(self.config_manager.get_max_topics()))
        topics: list[SingleTopic] = []
        if not isinstance(raw_topics, list):
            return topics

        for item in raw_topics[:max_topics]:
            topic = str(self._value(item, "topic", "")).strip()
            detail = str(self._value(item, "detail", "")).strip()
            if not topic and not detail:
                continue
            contributors = self._value(item, "contributors", [])
            if not isinstance(contributors, list):
                contributors = []
            topics.append(
                SingleTopic(
                    topic=topic or "未命名话题",
                    detail=detail,
                    contributors=[str(contributor) for contributor in contributors],
                )
            )
        return topics

    def _build_quotes(self, raw_quotes: Any) -> list[SingleQuote]:
        max_quotes = max(0, int(self.config_manager.get_max_golden_quotes()))
        quotes: list[SingleQuote] = []
        if not isinstance(raw_quotes, list):
            return quotes

        for item in raw_quotes[:max_quotes]:
            content = str(self._value(item, "content", "")).strip()
            if not content:
                continue
            quotes.append(
                SingleQuote(
                    content=content,
                    sender=str(self._value(item, "sender", "")).strip() or "未知",
                    reason=str(self._value(item, "reason", "")).strip(),
                )
            )
        return quotes

    def _build_water_king(self, raw_user_analysis: Any) -> SingleWaterKing | None:
        if not isinstance(raw_user_analysis, dict):
            return None

        best_user_id = ""
        best_stats: Any = None
        best_count = 0
        for user_id, stats in raw_user_analysis.items():
            message_count = self._as_int(self._value(stats, "message_count", 0))
            if message_count > best_count:
                best_user_id = str(user_id)
                best_stats = stats
                best_count = message_count

        if not best_user_id or best_stats is None:
            return None
        return SingleWaterKing(
            user_id=best_user_id,
            nickname=str(self._value(best_stats, "nickname", best_user_id)),
            message_count=best_count,
        )

    @staticmethod
    def _value(source: Any, key: str, default: Any = None) -> Any:
        if isinstance(source, dict):
            return source.get(key, default)
        return getattr(source, key, default)

    @staticmethod
    def _as_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _parse_group_ref(group_ref: str) -> tuple[str, str]:
        if ":" not in group_ref:
            return "", group_ref
        parts = group_ref.split(":")
        return parts[0].strip(), parts[-1].strip()
