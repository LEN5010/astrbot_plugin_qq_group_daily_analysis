from __future__ import annotations

import asyncio
import json
import logging
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


def _install_runtime_stubs() -> None:
    astrbot_module = sys.modules.setdefault("astrbot", types.ModuleType("astrbot"))
    api_module = sys.modules.setdefault("astrbot.api", types.ModuleType("astrbot.api"))
    api_module.logger = logging.getLogger("astrbot-test")
    astrbot_module.api = api_module

    if "apscheduler.triggers.cron" not in sys.modules:
        apscheduler_module = sys.modules.setdefault(
            "apscheduler", types.ModuleType("apscheduler")
        )
        triggers_module = sys.modules.setdefault(
            "apscheduler.triggers", types.ModuleType("apscheduler.triggers")
        )
        cron_module = types.ModuleType("apscheduler.triggers.cron")

        class CronTrigger:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        cron_module.CronTrigger = CronTrigger
        sys.modules["apscheduler.triggers.cron"] = cron_module
        apscheduler_module.triggers = triggers_module
        triggers_module.cron = cron_module

    if "jinja2" not in sys.modules:
        jinja2_module = types.ModuleType("jinja2")

        class Environment:
            def __init__(self, *args, **kwargs):
                self.filters = {}

            def get_template(self, *args, **kwargs):
                return SimpleNamespace(render=lambda **_kwargs: "")

        class FileSystemLoader:
            def __init__(self, *args, **kwargs):
                pass

        def select_autoescape(*args, **kwargs):
            return False

        jinja2_module.Environment = Environment
        jinja2_module.FileSystemLoader = FileSystemLoader
        jinja2_module.select_autoescape = select_autoescape
        sys.modules["jinja2"] = jinja2_module

    if "markupsafe" not in sys.modules:
        markupsafe_module = types.ModuleType("markupsafe")

        class Markup(str):
            pass

        markupsafe_module.Markup = Markup
        sys.modules["markupsafe"] = markupsafe_module

    if "aiohttp" not in sys.modules:
        aiohttp_module = types.ModuleType("aiohttp")

        class ClientTimeout:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        aiohttp_module.ClientTimeout = ClientTimeout
        aiohttp_module.ClientSession = object
        sys.modules["aiohttp"] = aiohttp_module


_install_runtime_stubs()

from src.application.services import union_daily_report_service as union_module
from src.application.services.union_daily_report_service import UnionDailyReportService
from src.infrastructure.analysis.analyzers import base_analyzer as base_analyzer_module
from src.infrastructure.analysis.analyzers.base_analyzer import LLMResponseParseError
from src.infrastructure.analysis.analyzers.topic_analyzer import TopicAnalyzer
from src.infrastructure.platform.adapters.onebot_adapter import OneBotAdapter
from src.infrastructure.persistence.history_repository import HistoryRepository
from src.infrastructure.scheduler.auto_scheduler import AutoScheduler


class FakeConfig:
    def __init__(
        self,
        *,
        source_groups: list[str] | None = None,
        target_groups: list[str] | None = None,
        union_prompt: str = "",
        persona_comment_prompt: str = "",
    ):
        self.source_groups = source_groups or []
        self.target_groups = target_groups or []
        self.union_prompt = union_prompt
        self.persona_comment_prompt = persona_comment_prompt

    def get_union_daily_analysis_prompt(self) -> str:
        return self.union_prompt

    def get_persona_comment_prompt(self) -> str:
        return self.persona_comment_prompt

    def get_use_plugin_specific_persona(self) -> bool:
        return False

    def get_plugin_specific_persona_id(self) -> str:
        return ""

    def get_keep_original_persona(self) -> bool:
        return False

    def get_union_report_enabled(self) -> bool:
        return True

    def get_union_groups_list(self) -> list[str]:
        return self.source_groups

    def get_union_target_groups(self) -> list[str]:
        return self.target_groups

    def get_union_wait_timeout_minutes(self) -> int:
        return 0

    def get_debug_mode(self) -> bool:
        return False

    def get_bot_self_ids(self) -> list[str]:
        return []

    def get_max_topics(self) -> int:
        return 3

    def get_topic_analysis_prompt(self) -> str:
        return (
            "从消息中提取最多 ${max_topics} 个话题，只返回 JSON 数组。\n"
            "${messages_text}"
        )


class FakeHistory:
    def __init__(self, data: dict[tuple[str, str], dict]):
        self.data = data

    def get_analysis_result(self, group_ref: str, report_date: str):
        return self.data.get((group_ref, report_date))


class UnionChainTests(unittest.IsolatedAsyncioTestCase):
    def _analysis_result(
        self,
        group_ref: str,
        *,
        group_name: str = "源群",
        quotes: list[dict] | None = None,
        topics: list[dict] | None = None,
    ) -> dict:
        platform_id = group_ref.split(":", 1)[0]
        group_id = group_ref.rsplit(":", 1)[-1]
        return {
            "group_ref": group_ref,
            "platform_id": platform_id,
            "group_id": group_id,
            "group_name": group_name,
            "statistics": {
                "message_count": 100,
                "participant_count": 8,
                "golden_quotes": quotes or [],
            },
            "topics": topics or [],
            "user_analysis": {
                "u1": {"nickname": "甲", "message_count": 15},
                "u2": {"nickname": "乙", "message_count": 9},
            },
        }

    def test_history_repository_uses_full_group_ref_as_storage_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = HistoryRepository(temp_dir)
            date = "2026-05-26"
            qq_ref = "qq:GroupMessage:100"
            tg_ref = "telegram:GroupMessage:100"

            self.assertTrue(
                repo.save_analysis_result(qq_ref, {"group_name": "QQ群"}, date)
            )
            self.assertTrue(
                repo.save_analysis_result(tg_ref, {"group_name": "TG群"}, date)
            )

            self.assertEqual(
                repo.get_analysis_result(qq_ref, date)["group_name"],
                "QQ群",
            )
            self.assertEqual(
                repo.get_analysis_result(tg_ref, date)["group_name"],
                "TG群",
            )
            self.assertIsNone(repo.get_analysis_result("100", date))

            history_files = list((Path(temp_dir) / "history").glob("group_*.json"))
            self.assertEqual(len(history_files), 2)

    async def test_missing_source_json_fails_before_llm(self):
        date = "2026-05-26"
        service = UnionDailyReportService(
            FakeConfig(),
            FakeHistory(
                {
                    ("qq:GroupMessage:100", date): self._analysis_result(
                        "qq:GroupMessage:100"
                    )
                }
            ),
            SimpleNamespace(),
        )

        original_call = union_module.call_provider_with_retry

        async def fail_if_called(*args, **kwargs):
            raise AssertionError("LLM should not be called when a source JSON is missing")

        union_module.call_provider_with_retry = fail_if_called
        try:
            report = await service.build_union_report(
                ["qq:GroupMessage:100", "telegram:GroupMessage:100"],
                date,
            )
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNone(report)

    async def test_persona_comments_are_generated_only_for_display_items(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        quotes = [
            {"content": "入选金句", "sender": "甲", "reason": "有代表性"},
            {"content": "未入选金句", "sender": "乙", "reason": "候选但不展示"},
        ]
        topics = [
            {"topic": f"话题{i}", "detail": f"详情{i}", "contributors": ["甲"]}
            for i in range(1, 8)
        ]
        service = UnionDailyReportService(
            FakeConfig(),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=quotes,
                        topics=topics,
                    )
                }
            ),
            SimpleNamespace(),
        )

        persona_prompt = None
        responses = [
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "top_quotes": [
                            {
                                "quote_id": 1,
                                "reason": "联合日报入选理由",
                            }
                        ],
                        "global_commentary": "整体活跃，讨论集中。",
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "quote_comments": {"q1": "这句够锋利还挺会整活"},
                        "topic_comments": {
                            f"t{i}": f"话题{i}这波越聊越抽象" for i in range(1, 7)
                        },
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
        ]

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            nonlocal persona_prompt
            prompt = kwargs.get("prompt", "")
            if "最终展示的条目" in prompt:
                persona_prompt = prompt
            return responses.pop(0)

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNotNone(report)
        self.assertEqual(len(report.top_quotes), 1)
        self.assertEqual(report.top_quotes[0].content, "入选金句")
        self.assertEqual(report.top_quotes[0].reason, "联合日报入选理由")
        self.assertEqual(report.top_quotes[0].persona_comment, "这句够锋利还挺会整活")
        self.assertEqual(len(report.topic_highlights), 6)
        self.assertTrue(all(item.persona_comment for item in report.topic_highlights))
        self.assertIsNotNone(persona_prompt)
        self.assertNotIn("未入选金句", persona_prompt)
        self.assertNotIn("话题7", persona_prompt)
        self.assertIn("键集合必须严格等于：q1", persona_prompt)
        self.assertIn("键集合必须严格等于：t1, t2, t3, t4, t5, t6", persona_prompt)
        self.assertIn("10 到 30 个中文字符", persona_prompt)
        self.assertIn("嘲讽、搞耍", persona_prompt)

    async def test_persona_comment_prompt_is_loaded_from_config(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        custom_prompt = (
            "自定义爱驼吐槽口径：保持阴阳怪气但别编事实。\n"
            "金句输入：\n${quote_items_text}\n"
            "话题输入：\n${topic_items_text}\n"
            "长度：${comment_min_length}-${comment_max_length}"
        )
        service = UnionDailyReportService(
            FakeConfig(persona_comment_prompt=custom_prompt),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=[
                            {"content": "原始金句", "sender": "甲", "reason": "原始理由"}
                        ],
                        topics=[
                            {"topic": "话题1", "detail": "详情1", "contributors": ["甲"]}
                        ],
                    )
                }
            ),
            SimpleNamespace(),
        )

        captured_prompt = ""
        responses = [
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "top_quotes": [{"quote_id": 1, "reason": "编号理由"}],
                        "global_commentary": "整体活跃。",
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "quote_comments": {"q1": "这句金句多少有点离谱"},
                        "topic_comments": {"t1": "这个话题越聊越像整活"},
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
        ]

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            nonlocal captured_prompt
            prompt = kwargs.get("prompt", "")
            if "自定义爱驼吐槽口径" in prompt:
                captured_prompt = prompt
            return responses.pop(0)

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNotNone(report)
        self.assertIn("自定义爱驼吐槽口径", captured_prompt)
        self.assertIn("q1. 群：源群；发言人：甲", captured_prompt)
        self.assertIn("长度：10-30", captured_prompt)
        self.assertIn("最终输出合同", captured_prompt)
        self.assertIn("键集合必须严格等于：q1", captured_prompt)

    async def test_union_quotes_are_selected_by_quote_id_not_rewritten_text(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        service = UnionDailyReportService(
            FakeConfig(),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=[
                            {"content": "原始金句", "sender": "甲", "reason": "原始理由"}
                        ],
                        topics=[
                            {"topic": "话题1", "detail": "详情1", "contributors": ["甲"]}
                        ],
                    )
                }
            ),
            SimpleNamespace(),
        )

        responses = [
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "top_quotes": [
                            {"quote_id": 1, "reason": "编号选中的理由"}
                        ],
                        "global_commentary": "整体活跃。",
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "quote_comments": {"q1": "够有代表性但也挺能整"},
                        "topic_comments": {"t1": "这个话题集中得有点好笑"},
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
        ]

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            return responses.pop(0)

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNotNone(report)
        self.assertEqual(report.top_quotes[0].content, "原始金句")
        self.assertEqual(report.top_quotes[0].sender, "甲")
        self.assertEqual(report.top_quotes[0].reason, "编号选中的理由")

    async def test_final_quote_id_contract_overrides_stale_config_prompt(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        stale_prompt = (
            "旧格式要求：top_quotes 返回 content、sender、group_ref、reason。\n"
            "${groups_summary_text}\n${quotes_text}\n${topics_text}"
        )
        service = UnionDailyReportService(
            FakeConfig(union_prompt=stale_prompt),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=[
                            {"content": "原始金句", "sender": "甲", "reason": "原始理由"}
                        ],
                        topics=[
                            {"topic": "话题1", "detail": "详情1", "contributors": ["甲"]}
                        ],
                    )
                }
            ),
            SimpleNamespace(),
        )

        captured_prompt = ""
        responses = [
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "top_quotes": [{"quote_id": 1, "reason": "编号理由"}],
                        "global_commentary": "整体活跃。",
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "quote_comments": {"q1": "这句金句多少有点离谱"},
                        "topic_comments": {"t1": "这个话题越聊越像整活"},
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
        ]

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            nonlocal captured_prompt
            prompt = kwargs.get("prompt", "")
            if "不允许返回 content、sender、group_ref" in prompt:
                captured_prompt = prompt
            return responses.pop(0)

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNotNone(report)
        self.assertIn("最终输出合同", captured_prompt)
        self.assertIn("优先级高于上方所有提示词", captured_prompt)
        self.assertIn("不允许返回 content、sender、group_ref", captured_prompt)
        self.assertEqual(report.top_quotes[0].content, "原始金句")

    async def test_persona_comments_reject_legacy_index_array(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        service = UnionDailyReportService(
            FakeConfig(),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=[
                            {"content": "原始金句", "sender": "甲", "reason": "原始理由"}
                        ],
                        topics=[
                            {"topic": "话题1", "detail": "详情1", "contributors": ["甲"]}
                        ],
                    )
                }
            ),
            SimpleNamespace(),
        )
        responses = [
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "top_quotes": [{"quote_id": 1, "reason": "编号理由"}],
                        "global_commentary": "整体活跃。",
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
            SimpleNamespace(
                completion_text=json.dumps(
                    {
                        "quote_comments": [{"index": 1, "comment": "旧格式点评。"}],
                        "topic_comments": [{"index": 1, "comment": "旧格式话题。"}],
                    },
                    ensure_ascii=False,
                ),
                usage={},
            ),
        ]

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            return responses.pop(0)

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNone(report)
        self.assertEqual(service.last_failure_reason, "persona_comment_failed")

    async def test_invalid_union_llm_json_fails(self):
        date = "2026-05-26"
        group_ref = "qq:GroupMessage:100"
        service = UnionDailyReportService(
            FakeConfig(),
            FakeHistory(
                {
                    (group_ref, date): self._analysis_result(
                        group_ref,
                        quotes=[
                            {
                                "content": "金句",
                                "sender": "甲",
                                "reason": "理由",
                            }
                        ],
                    )
                }
            ),
            SimpleNamespace(),
        )

        original_call = union_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            return SimpleNamespace(completion_text="不是 JSON", usage={})

        union_module.call_provider_with_retry = fake_call
        try:
            report = await service.build_union_report([group_ref], date)
        finally:
            union_module.call_provider_with_retry = original_call

        self.assertIsNone(report)


class SourceAnalyzerTests(unittest.IsolatedAsyncioTestCase):
    async def test_schema_less_source_llm_output_fails(self):
        analyzer = TopicAnalyzer(SimpleNamespace(), FakeConfig())
        messages = [
            {
                "time": 1779724800,
                "sender": {"user_id": "u1", "nickname": "甲"},
                "message": [{"type": "text", "data": {"text": "今天讨论部署"}}],
            }
        ]

        original_call = base_analyzer_module.call_provider_with_retry

        async def fake_call(*args, **kwargs):
            return SimpleNamespace(
                completion_text=json.dumps({"topics": []}, ensure_ascii=False),
                usage={},
            )

        base_analyzer_module.call_provider_with_retry = fake_call
        try:
            with self.assertRaises(LLMResponseParseError):
                await analyzer.analyze_topics(messages, umo="qq:GroupMessage:100")
        finally:
            base_analyzer_module.call_provider_with_retry = original_call


class SchedulerChainTests(unittest.IsolatedAsyncioTestCase):
    def _scheduler(
        self,
        *,
        source_groups: list[str] | None = None,
        target_groups: list[str] | None = None,
        union_service=None,
        report_generator=None,
    ) -> AutoScheduler:
        return AutoScheduler(
            config_manager=FakeConfig(
                source_groups=source_groups,
                target_groups=target_groups,
            ),
            analysis_service=SimpleNamespace(),
            bot_manager=SimpleNamespace(),
            report_generator=report_generator or SimpleNamespace(),
            html_render_func=lambda *args, **kwargs: b"",
            union_daily_report_service=union_service or SimpleNamespace(),
        )

    async def test_union_targets_are_required_without_source_fallback(self):
        scheduler = self._scheduler(
            source_groups=["qq:GroupMessage:100"],
            target_groups=[],
        )

        result = await scheduler._run_union_report_core(
            "2026-05-26",
            target_groups_override=None,
            skip_enabled_check=True,
            bypass_daily_guard=True,
        )

        self.assertEqual(result["success"], False)
        self.assertEqual(result["reason"], "no_targets")

    async def test_empty_image_render_fails_without_text_fallback(self):
        report = SimpleNamespace(
            report_date="2026-05-26",
            group_snapshots=[],
            champion_group=SimpleNamespace(group_ref=""),
            top_quotes=[],
            topic_highlights=[],
            water_king=None,
            runner_up_users=[],
        )
        union_service = SimpleNamespace(
            build_union_report=lambda *args, **kwargs: asyncio.sleep(
                0, result=report
            )
        )

        class EmptyImageReportGenerator:
            async def render_html_content_to_image(self, *args, **kwargs):
                return None

        scheduler = self._scheduler(
            source_groups=["qq:GroupMessage:100"],
            target_groups=["qq:GroupMessage:200"],
            union_service=union_service,
            report_generator=EmptyImageReportGenerator(),
        )
        scheduler.union_report_renderer = SimpleNamespace(
            render_html=lambda _report: "<html></html>",
        )
        scheduler.message_sender = SimpleNamespace(
            send_image_smart=lambda *args, **kwargs: asyncio.sleep(0, result=True)
        )

        result = await scheduler._run_union_report_core(
            "2026-05-26",
            target_groups_override=[("200", "qq")],
            skip_enabled_check=True,
            bypass_daily_guard=True,
        )

        self.assertEqual(result["success"], False)
        self.assertEqual(result["reason"], "dispatch_failed")


class OneBotAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_image_is_sent_as_base64_not_file_path(self):
        class FakeOneBot:
            def __init__(self):
                self.calls = []

            async def call_action(self, action, **kwargs):
                self.calls.append((action, kwargs))
                return {"status": "ok"}

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir) / "union.png"
            image_path.write_bytes(b"\x89PNG\r\n\x1a\nfake-image")

            bot = FakeOneBot()
            adapter = OneBotAdapter(bot, {})

            ok = await adapter.send_image("123456", str(image_path), "日报")

        self.assertTrue(ok)
        self.assertEqual(len(bot.calls), 1)
        action, kwargs = bot.calls[0]
        self.assertEqual(action, "send_group_msg")
        self.assertEqual(kwargs["group_id"], 123456)
        message = kwargs["message"]
        self.assertEqual(message[0], {"type": "text", "data": {"text": "日报"}})
        self.assertTrue(message[1]["data"]["file"].startswith("base64://"))
        self.assertNotIn("file://", message[1]["data"]["file"])


class TemplateStaticTests(unittest.TestCase):
    def test_persona_comment_label_is_aituo(self):
        template_path = (
            Path(__file__).resolve().parents[1]
            / "src/infrastructure/reporting/templates/union/union_template.html"
        )
        html = template_path.read_text(encoding="utf-8")

        self.assertIn("爱驼点评:", html)
        self.assertIn("persona-label", html)


if __name__ == "__main__":
    unittest.main()
