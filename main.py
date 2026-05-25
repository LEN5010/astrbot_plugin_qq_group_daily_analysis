"""
增量跨群联合日报插件

唯一业务链路：
源群增量采集 -> 源群 JSON 中间结果 -> 跨群聚合 -> 联合日报图片发送。
"""

import asyncio
from datetime import datetime
from pathlib import Path

from astrbot.api import AstrBotConfig
from astrbot.api import logger as astrbot_logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event.filter import PermissionType
from astrbot.api.star import Context, Star, StarTools, register

from .src.application.services.analysis_application_service import (
    AnalysisApplicationService,
)
from .src.application.services.message_processing_service import (
    MessageProcessingService,
)
from .src.application.services.union_daily_report_service import (
    UnionDailyReportService,
)
from .src.domain.services.analysis_domain_service import AnalysisDomainService
from .src.domain.services.incremental_merge_service import IncrementalMergeService
from .src.domain.services.statistics_service import StatisticsService
from .src.infrastructure.analysis.llm_analyzer import LLMAnalyzer
from .src.infrastructure.config.config_manager import ConfigManager
from .src.infrastructure.persistence.history_manager import HistoryManager
from .src.infrastructure.persistence.history_repository import HistoryRepository
from .src.infrastructure.persistence.incremental_store import IncrementalStore
from .src.infrastructure.persistence.telegram_group_registry import (
    TelegramGroupRegistry,
)
from .src.infrastructure.platform.bot_manager import BotManager
from .src.infrastructure.reporting.generators import ReportGenerator
from .src.infrastructure.scheduler.auto_scheduler import AutoScheduler
from .src.shared.trace_context import TraceLogFilter
from .src.utils.logger import logger
from .src.utils.resilience import GlobalRateLimiter


@register(
    "astrbot_plugin_qq_group_daily_analysis",
    "LEN5010",
    "基于增量分析的跨群联合日报插件",
    "5.0.0",
)
class GroupDailyAnalysis(Star):
    """增量跨群联合日报插件主类。"""

    config: AstrBotConfig
    config_manager: ConfigManager
    bot_manager: BotManager
    history_manager: HistoryManager
    history_repository: HistoryRepository
    report_generator: ReportGenerator
    telegram_group_registry: TelegramGroupRegistry
    statistics_service: StatisticsService
    analysis_domain_service: AnalysisDomainService
    llm_analyzer: LLMAnalyzer
    union_daily_report_service: UnionDailyReportService
    incremental_store: IncrementalStore
    incremental_merge_service: IncrementalMergeService
    analysis_service: AnalysisApplicationService
    message_processing_service: MessageProcessingService
    auto_scheduler: AutoScheduler

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        self.config_manager = ConfigManager(config)
        self.bot_manager = BotManager(self.config_manager)
        self.bot_manager.set_context(context)
        self.bot_manager.set_plugin_instance(self)
        self.history_manager = HistoryManager(self)

        try:
            plugin_data_dir = StarTools.get_data_dir()
        except Exception:
            plugin_data_dir = (
                Path(get_astrbot_data_path())
                / "plugin_data"
                / "astrbot_plugin_qq_group_daily_analysis"
            )

        self.report_generator = ReportGenerator(self.config_manager, plugin_data_dir)
        self.history_repository = HistoryRepository(str(plugin_data_dir))
        self.telegram_group_registry = TelegramGroupRegistry(self)

        self.statistics_service = StatisticsService()
        self.analysis_domain_service = AnalysisDomainService()
        self.llm_analyzer = LLMAnalyzer(context, self.config_manager)
        self.union_daily_report_service = UnionDailyReportService(
            self.config_manager,
            self.history_repository,
            context,
        )

        self.incremental_store = IncrementalStore(self)
        self.incremental_merge_service = IncrementalMergeService()
        self.analysis_service = AnalysisApplicationService(
            self.config_manager,
            self.bot_manager,
            self.history_manager,
            self.history_repository,
            self.report_generator,
            self.llm_analyzer,
            self.statistics_service,
            self.analysis_domain_service,
            incremental_store=self.incremental_store,
            incremental_merge_service=self.incremental_merge_service,
        )
        self.message_processing_service = MessageProcessingService(
            context, self.telegram_group_registry
        )
        self.auto_scheduler = AutoScheduler(
            self.config_manager,
            self.analysis_service,
            self.bot_manager,
            self.report_generator,
            self.html_render,
            plugin_instance=self,
            union_daily_report_service=self.union_daily_report_service,
        )

        GlobalRateLimiter.get_instance(self.config_manager.get_llm_max_concurrent())

        self._initialized = False
        self._terminating = False
        self._init_lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()

        try:
            loop = asyncio.get_running_loop()
            self._init_task = loop.create_task(
                self._run_initialization("Plugin Reload/Init")
            )
            self._background_tasks.add(self._init_task)
            self._init_task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            self._init_task = None

    @filter.on_platform_loaded()
    async def on_platform_loaded(self):
        """平台加载完成后发现 Bot 并注册调度任务。"""
        await self._run_initialization("Platform Loaded")

    async def _run_initialization(self, source: str):
        """统一初始化逻辑。"""
        async with self._init_lock:
            if (
                self._initialized
                and self.bot_manager
                and self.bot_manager.get_platform_count() > 0
                and source != "Platform Loaded"
            ):
                return

            await asyncio.sleep(5)
            if not self.bot_manager:
                return

            try:
                trace_filter = TraceLogFilter()
                if not any(
                    isinstance(item, TraceLogFilter) for item in astrbot_logger.filters
                ):
                    astrbot_logger.addFilter(trace_filter)
                    astrbot_logger.info("[Trace] TraceID 日志追踪已启用")

                logger.info(f"正在执行插件初始化 (来源: {source})...")

                await self.bot_manager.initialize_from_config()
                self.auto_scheduler.schedule_jobs(self.context)

                self._initialized = True
                logger.info(f"插件任务注册完成 (来源: {source})")
            except Exception as e:
                logger.error(f"插件初始化失败: {e}", exc_info=True)

    async def terminate(self):
        """插件被卸载/停用时调用，清理后台任务与渲染资源。"""
        if self._terminating:
            return
        self._terminating = True

        try:
            logger.info("开始清理跨群联合日报插件资源...")

            if self._background_tasks:
                logger.info(f"正在取消 {len(self._background_tasks)} 个运行中的任务...")
                for task in self._background_tasks:
                    if not task.done():
                        task.cancel()
                try:
                    await asyncio.wait(list(self._background_tasks), timeout=3.0)
                except Exception:
                    pass
                self._background_tasks.clear()

            if self.auto_scheduler:
                logger.debug("正在停止自动调度器...")
                self.auto_scheduler.unschedule_jobs(self.context)

            if self.report_generator:
                await self.report_generator.close()

            logger.info("跨群联合日报插件资源清理完成")
        except Exception as e:
            logger.error(f"插件资源清理失败: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.TELEGRAM)
    async def intercept_telegram_messages(self, event: AstrMessageEvent):
        """记录 Telegram 群消息，供后续增量分析读取。"""
        try:
            await self.message_processing_service.process_message(event)
        except (ValueError, RuntimeError) as e:
            logger.warning(f"[Telegram] 消息存储失败: {e}")
        except Exception as e:
            logger.error(f"[Telegram] 消息存储异常: {e}", exc_info=True)

    async def get_telegram_seen_group_ids(
        self, platform_id: str | None = None
    ) -> list[str]:
        """读取 Telegram 已见群/话题列表。"""
        return await self.telegram_group_registry.get_all_group_ids(platform_id)

    def _get_group_id_from_event(self, event: AstrMessageEvent) -> str | None:
        try:
            group_id = event.get_group_id()
            return group_id if group_id else None
        except Exception:
            return None

    def _get_platform_id_from_event(self, event: AstrMessageEvent) -> str:
        try:
            return event.get_platform_id()
        except Exception:
            if (
                hasattr(event, "platform_meta")
                and event.platform_meta
                and hasattr(event.platform_meta, "id")
            ):
                return event.platform_meta.id
            return "default"

    @filter.command("联合日报测试", alias={"union_report_test"})
    @filter.permission_type(PermissionType.ADMIN)
    async def union_report_test(
        self,
        event: AstrMessageEvent,
        report_date: str = "",
    ):
        """
        手动测试跨群聚合日报。
        用法: /联合日报测试 [YYYY-MM-DD]

        测试结果只发送到当前群，避免误群发。
        """
        if self._terminating:
            return

        event.should_call_llm(True)
        current_task = asyncio.current_task()
        if current_task:
            self._background_tasks.add(current_task)

        try:
            group_id = self._get_group_id_from_event(event)
            platform_id = self._get_platform_id_from_event(event)
            if not group_id:
                yield event.plain_result("请在群聊中使用此命令")
                return

            target_date = report_date.strip() if report_date else ""
            if target_date:
                try:
                    datetime.strptime(target_date, "%Y-%m-%d")
                except ValueError:
                    yield event.plain_result(
                        "日期格式错误，请使用 YYYY-MM-DD，例如 /联合日报测试 2026-04-08"
                    )
                    return
            else:
                target_date = datetime.now().strftime("%Y-%m-%d")

            self.bot_manager.update_from_event(event)
            yield event.plain_result(
                f"开始执行联合日报全链路测试，日期: {target_date}。"
                "本次只发送到当前群。"
            )

            result = await self.auto_scheduler.run_union_report_manual(
                report_date=target_date,
                target_groups_override=[(group_id, platform_id)],
            )
            if result.get("success"):
                yield event.plain_result("跨群联合日报测试完成，已发送到当前群")
                return

            reason = result.get("reason", "unknown")
            reason_map = {
                "no_targets": "没有配置联合日报发送目标",
                "not_initialized": "联合日报组件未初始化完成",
                "no_source_groups": "未配置 union_groups_list",
                "no_report_data": "指定日期没有完整可聚合的源群 JSON",
                "dispatch_failed": "联合日报图片发送失败",
                "disabled": "跨群日报功能未启用",
                "source_reports_not_ready": "源群日报在等待窗口内未全部就绪",
                "prepare_failed": "源群增量最终 JSON 准备失败",
                "already_sent": "今天的联合日报已经发送过",
                "already_running": "当前日期的联合日报正在执行中",
            }
            yield event.plain_result(
                f"跨群联合日报测试失败: {reason_map.get(reason, reason)}"
            )
        except Exception as e:
            logger.error(f"跨群联合日报测试失败: {e}", exc_info=True)
            yield event.plain_result(f"跨群联合日报测试失败: {e}")
        finally:
            if current_task:
                self._background_tasks.discard(current_task)
