"""
内容治理服务

对话题与金句做配置化的展示级治理：
- 敏感词命中的话题直接过滤
- 性/低俗内容按惩罚分降权，但不直接删除
"""

from __future__ import annotations

from typing import Any


class ContentModerationService:
    """展示级内容治理服务。"""

    def __init__(self, config_manager: Any):
        self.config_manager = config_manager

    def moderate_topics(self, topics: list[Any]) -> list[Any]:
        if not self.config_manager.get_content_moderation_enabled():
            return topics

        sensitive_words = self._normalize_words(
            self.config_manager.get_sensitive_topic_blocklist()
        )
        sexual_words = self._normalize_words(
            self.config_manager.get_sexual_vulgar_words()
        )
        penalty_weight = max(0, self.config_manager.get_topic_sexual_penalty_weight())

        scored_topics: list[tuple[int, int, Any]] = []
        for index, topic in enumerate(topics):
            text = self._build_topic_text(topic)
            if sensitive_words and self._contains_any(text, sensitive_words):
                continue

            penalty = self._match_count(text, sexual_words) * penalty_weight
            scored_topics.append((penalty, index, topic))

        scored_topics.sort(key=lambda item: (item[0], item[1]))
        return [topic for _, _, topic in scored_topics]

    def moderate_quotes(self, quotes: list[Any]) -> list[Any]:
        if not self.config_manager.get_content_moderation_enabled():
            return quotes

        sexual_words = self._normalize_words(
            self.config_manager.get_sexual_vulgar_words()
        )
        penalty_weight = max(0, self.config_manager.get_quote_sexual_penalty_weight())

        scored_quotes: list[tuple[int, int, Any]] = []
        for index, quote in enumerate(quotes):
            text = self._build_quote_text(quote)
            penalty = self._match_count(text, sexual_words) * penalty_weight
            scored_quotes.append((penalty, index, quote))

        scored_quotes.sort(key=lambda item: (item[0], item[1]))
        return [quote for _, _, quote in scored_quotes]

    @staticmethod
    def _normalize_words(raw_words: list[str]) -> list[str]:
        words: list[str] = []
        for item in raw_words:
            text = str(item).strip().lower()
            if not text:
                continue
            words.append(text)
        return words

    @staticmethod
    def _contains_any(text: str, words: list[str]) -> bool:
        return any(word in text for word in words)

    @staticmethod
    def _match_count(text: str, words: list[str]) -> int:
        return sum(1 for word in words if word in text)

    @staticmethod
    def _build_topic_text(topic: Any) -> str:
        parts = [
            str(getattr(topic, "topic", "")),
            str(getattr(topic, "detail", "")),
        ]
        contributors = getattr(topic, "contributors", [])
        if isinstance(contributors, list):
            parts.extend(str(item) for item in contributors)
        return " ".join(parts).lower()

    @staticmethod
    def _build_quote_text(quote: Any) -> str:
        return " ".join(
            [
                str(getattr(quote, "content", "")),
                str(getattr(quote, "reason", "")),
                str(getattr(quote, "sender", "")),
            ]
        ).lower()
