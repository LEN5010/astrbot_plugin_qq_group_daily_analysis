"""
LLM 分析入口。

当前链路只需要源群增量提取中的话题与金句分析。
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from ...domain.models.data_models import GoldenQuote, SummaryTopic, TokenUsage
from ...domain.repositories.analysis_repository import IAnalysisProvider
from ...utils.logger import logger
from .analyzers.golden_quote_analyzer import GoldenQuoteAnalyzer
from .analyzers.topic_analyzer import TopicAnalyzer


class LLMAnalyzer(IAnalysisProvider):
    """协调话题与金句分析器。"""

    def __init__(self, context, config_manager):
        self.context = context
        self.config_manager = config_manager
        self.topic_analyzer = TopicAnalyzer(context, config_manager)
        self.golden_quote_analyzer = GoldenQuoteAnalyzer(context, config_manager)

    async def analyze_topics(
        self,
        messages: list[dict],
        umo: str | None = None,
        session_id: str | None = None,
    ) -> tuple[list[SummaryTopic], TokenUsage]:
        session_id = session_id or self._build_session_id("topic", umo)
        return await self.topic_analyzer.analyze_topics(messages, umo, session_id)

    async def analyze_golden_quotes(
        self,
        messages: list[dict],
        umo: str | None = None,
        session_id: str | None = None,
    ) -> tuple[list[GoldenQuote], TokenUsage]:
        session_id = session_id or self._build_session_id("quote", umo)
        return await self.golden_quote_analyzer.analyze_golden_quotes(
            messages,
            umo,
            session_id,
        )

    async def analyze_incremental_concurrent(
        self,
        messages: list[dict],
        umo: str | None = None,
        topics_per_batch: int = 2,
        quotes_per_batch: int = 1,
        topic_enabled: bool = True,
        golden_quote_enabled: bool = True,
    ) -> tuple[list[SummaryTopic], list[GoldenQuote], TokenUsage, None]:
        """增量批次只提取话题与金句。"""
        session_id = self._build_session_id("incr", umo)
        if self.config_manager.get_debug_mode():
            self._save_debug_messages(messages, session_id)

        self.topic_analyzer._incremental_max_count = topics_per_batch
        self.golden_quote_analyzer._incremental_max_count = quotes_per_batch
        try:
            tasks = []
            task_names = []
            if topic_enabled:
                tasks.append(
                    self.topic_analyzer.analyze_topics(messages, umo, session_id)
                )
                task_names.append("topic")
            if golden_quote_enabled:
                tasks.append(
                    self.golden_quote_analyzer.analyze_golden_quotes(
                        messages,
                        umo,
                        session_id,
                    )
                )
                task_names.append("golden_quote")

            if not tasks:
                return [], [], TokenUsage(), None

            results = await asyncio.gather(*tasks, return_exceptions=True)
            topics: list[SummaryTopic] = []
            golden_quotes: list[GoldenQuote] = []
            topic_usage = TokenUsage()
            quote_usage = TokenUsage()

            for index, result in enumerate(results):
                name = task_names[index]
                if isinstance(result, Exception):
                    logger.error("增量%s分析失败: %s", name, result)
                    raise RuntimeError(f"增量{name}分析失败") from result
                if name == "topic" and isinstance(result, tuple):
                    topics, topic_usage = result
                elif name == "golden_quote" and isinstance(result, tuple):
                    golden_quotes, quote_usage = result

            total_usage = TokenUsage(
                prompt_tokens=topic_usage.prompt_tokens + quote_usage.prompt_tokens,
                completion_tokens=topic_usage.completion_tokens
                + quote_usage.completion_tokens,
                total_tokens=topic_usage.total_tokens + quote_usage.total_tokens,
            )
            return topics, golden_quotes, total_usage, None
        finally:
            self.topic_analyzer._incremental_max_count = None
            self.golden_quote_analyzer._incremental_max_count = None

    @staticmethod
    def _build_session_id(prefix: str, umo: str | None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not umo:
            return f"{prefix}_{timestamp}"
        safe_umo = umo.replace(":", "_")
        return f"{prefix}_{timestamp}_{safe_umo}"

    def _save_debug_messages(self, messages: list[dict], session_id: str):
        try:
            import json

            from astrbot.api.star import StarTools

            debug_dir = StarTools.get_data_dir() / "debug_data"
            debug_dir.mkdir(parents=True, exist_ok=True)
            debug_file = debug_dir / f"messages_{session_id}.json"
            with open(debug_file, "w", encoding="utf-8") as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            logger.info("已保存调试消息数据: %s", debug_file)
        except Exception as e:
            logger.warning("保存调试消息数据失败: %s", e)
