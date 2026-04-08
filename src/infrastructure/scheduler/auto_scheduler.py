"""
自动调度器模块
负责定时任务和自动分析功能，支持传统单次分析与增量多次分析两种调度模式。
"""

import asyncio
import time as time_mod
from datetime import datetime
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from ...application.services.analysis_application_service import DuplicateGroupTaskError
from ...shared.trace_context import TraceContext
from ...utils.logger import logger
from ..messaging.message_sender import MessageSender
from ..platform.factory import PlatformAdapterFactory
from ..reporting.dispatcher import ReportDispatcher
from ..reporting.union_report_renderer import UnionReportRenderer


class AutoScheduler:
    """自动调度器，支持传统模式和增量模式"""

    def __init__(
        self,
        config_manager,
        analysis_service,
        bot_manager,
        report_generator=None,
        html_render_func=None,
        plugin_instance: Any | None = None,
        union_daily_report_service: Any | None = None,
    ):
        self.config_manager = config_manager
        self.analysis_service = analysis_service
        self.bot_manager = bot_manager
        self.report_generator = report_generator
        self.html_render_func = html_render_func
        self.plugin_instance = plugin_instance
        self.union_daily_report_service = union_daily_report_service

        # 初始化核心组件
        self.message_sender = MessageSender(bot_manager, config_manager)
        self.report_dispatcher = ReportDispatcher(
            config_manager, report_generator, self.message_sender
        )
        if html_render_func:
            self.report_dispatcher.set_html_render(html_render_func)
        self.union_report_renderer = (
            UnionReportRenderer(report_generator) if report_generator else None
        )

        self.scheduler_job_ids = []  # 存储已注册的定时任务 ID
        self.last_executed_target = None  # 记录上次执行的具体时间点，防止重复执行
        self._last_union_report_date: str | None = None
        self._union_report_guard = asyncio.Lock()
        self._union_report_dates_in_progress: set[str] = set()
        self._union_prepare_results: dict[str, dict[str, Any]] = {}

        # Cache: platform_id:group_id -> group_name (populated lazily)
        self._group_name_cache: dict[str, str] = {}
        self._terminating = False  # 终止标志位

    def set_bot_instance(self, bot_instance):
        """设置bot实例（保持向后兼容）"""
        self.bot_manager.set_bot_instance(bot_instance)

    def set_bot_self_ids(self, bot_self_ids):
        """设置bot ID（支持单个ID或ID列表）"""
        # 确保传入的是列表，保持统一处理
        if isinstance(bot_self_ids, list):
            self.bot_manager.set_bot_self_ids(bot_self_ids)
        elif bot_self_ids:
            self.bot_manager.set_bot_self_ids([bot_self_ids])

    def set_bot_qq_ids(self, bot_qq_ids):
        """设置bot QQ号（已弃用，使用 set_bot_self_ids）"""
        self.set_bot_self_ids(bot_qq_ids)

    async def get_platform_id_for_group(self, group_id):
        """根据群ID获取对应的平台ID"""
        try:
            # 首先检查已注册的bot实例
            if (
                hasattr(self.bot_manager, "_bot_instances")
                and self.bot_manager._bot_instances
            ):
                # 如果只有一个实例，直接返回
                if self.bot_manager.get_platform_count() == 1:
                    platform_id = self.bot_manager.get_platform_ids()[0]
                    logger.debug(f"只有一个适配器，使用平台: {platform_id}")
                    return platform_id

                # 如果有多个实例，尝试通过适配器检查群属于哪个平台
                logger.info(f"检测到多个适配器，正在验证群 {group_id} 属于哪个平台...")
                matched_platform_ids: list[str] = []
                for platform_id in self.bot_manager.get_platform_ids():
                    try:
                        adapter = self.bot_manager.get_adapter(platform_id)
                        if adapter:
                            # 通过统一接口尝试获取群信息，如果能获取到则说明属于该平台
                            info = await adapter.get_group_info(str(group_id))
                            if info:
                                matched_platform_ids.append(platform_id)
                                logger.debug(
                                    f"群 {group_id} 在平台 {platform_id} 上命中"
                                )
                            else:
                                logger.debug(
                                    f"平台 {platform_id} 无法获取群 {group_id} 信息"
                                )
                    except Exception as e:
                        logger.debug(f"平台 {platform_id} 验证群 {group_id} 失败: {e}")
                        continue

                if len(matched_platform_ids) == 1:
                    platform_id = matched_platform_ids[0]
                    logger.info(f"✅ 群 {group_id} 唯一匹配平台 {platform_id}")
                    return platform_id

                if len(matched_platform_ids) > 1:
                    logger.error(
                        "❌ 群 %s 在多个平台同时命中，无法唯一确定平台: %s",
                        group_id,
                        matched_platform_ids,
                    )
                    return None

                # 如果所有适配器都尝试失败，记录错误并返回 None
                logger.error(
                    f"❌ 无法确定群 {group_id} 属于哪个平台 (已尝试: {list(self.bot_manager._bot_instances.keys())})"
                )
                return None

            # 没有任何bot实例，返回None
            logger.error("❌ 没有注册的bot实例")
            return None
        except Exception as e:
            logger.error(f"❌ 获取平台ID失败: {e}")
            return None

    async def _get_group_name_safe(
        self, group_id: str, platform_id: str | None = None
    ) -> str:
        """
        为 TraceID 生成解析可读的群名。
        使用内存缓存以避免重复的 API 调用。
        若名称不可用，则回退到 group_id。
        """
        cache_key = f"{platform_id or 'auto'}:{group_id}"
        if cache_key in self._group_name_cache:
            return self._group_name_cache[cache_key]

        try:
            pid = platform_id or await self.get_platform_id_for_group(group_id)
            if pid:
                adapter = self.bot_manager.get_adapter(pid)
                if adapter:
                    info = await adapter.get_group_info(group_id)
                    if info and info.group_name:
                        self._group_name_cache[cache_key] = info.group_name
                        return info.group_name
        except Exception:
            pass

        return group_id

    # ================================================================
    # 任务注册与取消
    # ================================================================

    def schedule_jobs(self, context):
        """根据分层名单配置注册定时任务。"""
        # 首先清理之前的任务
        self.unschedule_jobs(context)

        # unschedule_jobs 会将 _terminating 设为 True (用于关闭场景),
        # 但 schedule_jobs 意味着插件仍在运行；因此需要重置此标志位
        self._terminating = False

        auto_enabled = self.config_manager.is_auto_analysis_enabled()
        union_fixed_time = (
            self.config_manager.get_union_report_enabled()
            and bool(self.config_manager.get_union_report_time())
        )

        if not auto_enabled and not union_fixed_time:
            logger.info("未启用自动分析，且未配置固定 union 时间，不注册定时任务。")
            return

        scheduler = context.cron_manager.scheduler

        if auto_enabled:
            # 1. 注册核心报告生成任务（涵盖全量分析与增量总结报告）
            # 每个配置的时间点都会触发一次解析
            logger.info("注册定时分析报告任务...")
            self._schedule_report_time_jobs(scheduler)

            # 2. 只有在增量功能总开关开启时，才注册全天候的增量提取任务
            if self.config_manager.get_incremental_enabled():
                logger.info("增量分析功能已开启，正在注册全天增量提取任务...")
                self._schedule_incremental_cron_jobs(scheduler)
            else:
                logger.info("增量分析总开关未启用，仅执行传统定时全量分析。")
        else:
            logger.info("自动分析未启用，跳过单群定时任务注册。")

        if union_fixed_time:
            logger.info("注册跨群聚合日报固定时间任务...")
            self._schedule_union_report_time_job(scheduler)

    def _schedule_report_time_jobs(self, scheduler):
        """在配置的时间点注册报告生成任务。

        这些任务根据运行时解析出的生效模式，决定执行传统的全量分析还是增量汇报。
        """
        time_config = self.config_manager.get_auto_analysis_time()
        if isinstance(time_config, str):
            time_config = [time_config]

        for i, t_str in enumerate(time_config):
            try:
                t_str = str(t_str).replace("：", ":").strip()
                hour, minute = t_str.split(":")

                trigger = CronTrigger(hour=int(hour), minute=int(minute))
                job_id = f"astrbot_plugin_qq_group_daily_analysis_trigger_{i}"

                scheduler.add_job(
                    self._run_scheduled_report,
                    trigger=trigger,
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=60,
                )
                self.scheduler_job_ids.append(job_id)
                logger.info(f"已注册定时报告任务: {t_str} (Job ID: {job_id})")

            except Exception as e:
                logger.error(f"注册定时任务失败 ({t_str}): {e}")

    def _schedule_incremental_cron_jobs(self, scheduler):
        """
        在活跃时段注册增量分析定时任务。

        这类任务仅执行增量数据的提取；而报告生成阶段在配置的每日分析时间点进行。
        """
        active_start_hour = self.config_manager.get_incremental_active_start_hour()
        active_end_hour = self.config_manager.get_incremental_active_end_hour()
        interval_minutes = self.config_manager.get_incremental_interval_minutes()
        max_daily = self.config_manager.get_incremental_max_daily_analyses()

        # 计算活跃时段内的触发时间点
        trigger_times = []
        current_minutes = active_start_hour * 60
        end_minutes = active_end_hour * 60

        while current_minutes < end_minutes and len(trigger_times) < max_daily:
            hour = current_minutes // 60
            minute = current_minutes % 60
            trigger_times.append((hour, minute))
            current_minutes += interval_minutes

        # 注册增量分析任务
        for hour, minute in trigger_times:
            try:
                trigger = CronTrigger(hour=hour, minute=minute)
                job_id = f"incremental_analysis_{hour:02d}{minute:02d}"

                scheduler.add_job(
                    self._run_incremental_analysis,
                    trigger=trigger,
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=60,
                )
                self.scheduler_job_ids.append(job_id)
                logger.info(
                    f"已注册增量分析任务: {hour:02d}:{minute:02d} (Job ID: {job_id})"
                )
            except Exception as e:
                logger.error(f"注册增量分析任务失败 ({hour:02d}:{minute:02d}): {e}")

        logger.info(f"增量调度注册完成: {len(trigger_times)} 个增量分析任务")

    def _schedule_union_report_time_job(self, scheduler):
        """在固定时间注册跨群聚合日报任务和提前准备任务。"""
        time_str = self.config_manager.get_union_report_time()
        if not time_str:
            return

        try:
            normalized = str(time_str).replace("：", ":").strip()
            hour, minute = normalized.split(":")
            publish_hour = int(hour)
            publish_minute = int(minute)
            trigger = CronTrigger(hour=publish_hour, minute=publish_minute)
            job_id = "astrbot_plugin_union_daily_report_trigger"

            scheduler.add_job(
                self._run_union_report_on_schedule,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            self.scheduler_job_ids.append(job_id)
            logger.info(f"已注册跨群聚合日报固定任务: {normalized} (Job ID: {job_id})")

            lead_minutes = max(0, self.config_manager.get_union_prepare_lead_minutes())
            prepare_hour, prepare_minute = self._shift_clock_time(
                publish_hour,
                publish_minute,
                -lead_minutes,
            )
            prepare_trigger = CronTrigger(hour=prepare_hour, minute=prepare_minute)
            prepare_job_id = "astrbot_plugin_union_daily_report_prepare_trigger"
            scheduler.add_job(
                self._run_union_prepare_on_schedule,
                trigger=prepare_trigger,
                id=prepare_job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            self.scheduler_job_ids.append(prepare_job_id)
            logger.info(
                "已注册跨群聚合日报准备任务: %02d:%02d (提前 %d 分钟, Job ID: %s)",
                prepare_hour,
                prepare_minute,
                lead_minutes,
                prepare_job_id,
            )
        except Exception as e:
            logger.error(f"注册跨群聚合日报固定任务失败 ({time_str}): {e}")

    @staticmethod
    def _shift_clock_time(hour: int, minute: int, delta_minutes: int) -> tuple[int, int]:
        """对 HH:MM 做分钟偏移，结果按 24 小时制回绕。"""
        total_minutes = (hour * 60 + minute + delta_minutes) % (24 * 60)
        return total_minutes // 60, total_minutes % 60

    def unschedule_jobs(self, context):
        """取消定时任务"""
        self._terminating = True
        if (
            not context
            or not hasattr(context, "cron_manager")
            or not context.cron_manager
        ):
            return

        scheduler = context.cron_manager.scheduler
        if not scheduler:
            return

        for job_id in self.scheduler_job_ids:
            try:
                if scheduler.get_job(job_id):
                    scheduler.remove_job(job_id)
                    logger.debug(f"已移除定时任务: {job_id}")
            except Exception as e:
                logger.warning(f"移除定时任务失败 ({job_id}): {e}")
        self.scheduler_job_ids.clear()

    # ================================================================
    # 共享辅助方法：解析定时分析目标
    # ================================================================

    async def _get_scheduled_targets(
        self, mode_filter: str | None = None
    ) -> list[tuple[str, str, str]]:
        """
        根据分层过滤逻辑判定所有应参与计划分析的目标群组及其分析策略。

        判定过程：
        1. 准入层：群组必须在基础设置的允许名单内。
        2. 定时层：群组需通过定时分析名单的过滤。
        3. 模式层：如果群组在增量名单内，则使用增量模式，否则使用默认策略。

        参数：
            mode_filter: 如果提供，则只返回匹配指定模式的目标 (traditional 或 incremental)。
        """
        # 获取基础信息
        all_groups = await self._get_all_groups()

        # 预加载所有配置名单和模式
        sched_list = self.config_manager.get_scheduled_group_list()
        sched_list_mode = self.config_manager.get_scheduled_group_list_mode()

        incr_list = self.config_manager.get_incremental_group_list()
        incr_list_mode = self.config_manager.get_incremental_group_list_mode()

        result = []

        # 遍历所有平台上的群组
        for platform_id, group_id_orig in all_groups:
            group_id = str(group_id_orig)
            umo = f"{platform_id}:GroupMessage:{group_id}"

            # 1. 准入层判定 (基础黑白名单)
            if not self.config_manager.is_group_allowed(umo):
                continue

            # 2. 定时层判定 (定时分析黑白名单)
            if not self.config_manager.is_group_in_filtered_list(
                umo, sched_list_mode, sched_list
            ):
                continue

            # 3. 模式层判定 (增量黑白名单)
            # 3. 模式层判定 (增量黑白名单)
            if self.config_manager.is_group_in_filtered_list(
                umo, incr_list_mode, incr_list
            ):
                # 如果在增量名单内，则执行增量模式
                effective_mode = "incremental"
            else:
                # 不在增量名单内，则执行普通模式
                effective_mode = "traditional"

            # 4. 模式过滤 (如果函数调用者要求过滤)
            if mode_filter and effective_mode != mode_filter:
                continue

            result.append((group_id, platform_id, effective_mode))

        logger.info(
            f"分层调度解析完成：符合条件的群组共 {len(result)} 个"
            + (f" (模式过滤: {mode_filter})" if mode_filter else "")
        )
        return result

    # ================================================================
    # 统一报告调度入口
    # ================================================================

    async def _run_scheduled_report(self):
        """统一的定时分析入口。

        在配置的时间点触发，解析所有目标群并根据其分析模式分发任务：
        - traditional: 执行全量拉取分析并发送报告
        - incremental: 执行增量最终报告阶段（合并并汇报）
        """
        if self._terminating:
            return
        try:
            report_date = datetime.now().strftime("%Y-%m-%d")
            logger.info("定时报告触发 — 开始解析调度目标")

            all_targets = await self._get_scheduled_targets()

            if not all_targets:
                logger.info("没有配置的群聊需要定时分析")
                return

            max_concurrent = self.config_manager.get_max_concurrent_tasks()
            sem = asyncio.Semaphore(max_concurrent)
            logger.info(
                f"定时报告: {len(all_targets)} 个目标 (并发限制: {max_concurrent})"
            )

            async def dispatch_group(gid, pid, mode):
                async with sem:
                    if mode == "incremental":
                        return await self._perform_incremental_final_report_for_group_with_timeout(
                            gid, pid, report_date
                        )
                    else:
                        return await self._perform_auto_analysis_for_group_with_timeout(
                            gid, pid, report_date
                        )

            tasks = []
            stagger = self.config_manager.get_stagger_seconds() or 2
            # 针对定时大任务加入交错等待，减少瞬间峰值延迟
            for idx, (gid, pid, mode) in enumerate(all_targets):
                if self._terminating:
                    logger.info("检测到插件正在停止，取消后续任务创建")
                    break

                # 为前几个任务添加微小的启动间隔，均匀分散 API 压力
                if idx > 0 and stagger > 0:
                    await asyncio.sleep(stagger)

                task = asyncio.create_task(
                    dispatch_group(gid, pid, mode),
                    name=f"report_{mode}_{gid}",
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 统计结果
            success_count = 0
            skip_count = 0
            error_count = 0

            for i, result in enumerate(results):
                gid, _, _ = all_targets[i]
                if isinstance(result, DuplicateGroupTaskError):
                    skip_count += 1
                elif isinstance(result, Exception):
                    logger.error(f"群 {gid} 定时报告任务异常: {result}")
                    error_count += 1
                elif isinstance(result, dict) and not result.get("success", True):
                    skip_count += 1
                else:
                    success_count += 1

            logger.info(
                f"定时报告完成 — 成功: {success_count}, 跳过: {skip_count}, "
                f"失败: {error_count}, 总计: {len(all_targets)}"
            )
            await self._run_union_report_after_reports(report_date)

        except Exception as e:
            logger.error(f"定时报告执行失败: {e}", exc_info=True)

    async def _perform_auto_analysis_for_group_with_timeout(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
        dispatch_report: bool = True,
    ):
        """为指定群执行自动分析（带超时控制）"""
        try:
            # 为每个群聊设置独立的超时时间，适当放宽到 30 分钟以支持大型批次
            result = await asyncio.wait_for(
                self._perform_auto_analysis_for_group(
                    group_id,
                    target_platform_id,
                    archive_date,
                    dispatch_report=dispatch_report,
                ),
                timeout=1800,
            )
            return result
        except asyncio.TimeoutError:
            logger.error(f"群 {group_id} 分析超时（30分钟），跳过该群分析")
            return {"success": False, "reason": "timeout"}
        except Exception as e:
            logger.error(f"群 {group_id} 分析任务执行失败: {e}")
            return {"success": False, "reason": str(e)}

    async def _perform_auto_analysis_for_group(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
        dispatch_report: bool = True,
    ):
        """为指定群执行自动分析（业务逻辑委派给 AnalysisApplicationService）"""
        try:
            # 解析可读群名以生成语义化的 TraceID
            group_name = await self._get_group_name_safe(group_id, target_platform_id)
            trace_id = TraceContext.generate(prefix="group", group_name=group_name)
            TraceContext.set(trace_id)

            if self._terminating:
                return {"success": False, "reason": "terminating"}

            logger.info(
                f"开始为群 {group_id} 执行自动分析 (Platform: {target_platform_id or 'Auto'})"
            )

            # 检查平台状态 (BotManager 为基础设施层，用于获取平台就绪状态)
            if not self.bot_manager.is_ready_for_auto_analysis():
                logger.warning(f"群 {group_id} 自动分析跳过：bot管理器未就绪")
                return {"success": False, "reason": "bot_not_ready"}

            # 委派给应用层服务执行核心用例
            # AnalysisApplicationService 内部已处理群锁 (group_lock)
            result = await self.analysis_service.execute_daily_analysis(
                group_id=group_id,
                platform_id=target_platform_id,
                manual=False,
                archive_date=archive_date,
            )

            if not result.get("success"):
                reason = result.get("reason")
                logger.info(f"群 {group_id} 自动分析跳过: {reason}")
                return result

            if dispatch_report:
                # 获取分析结果及适配器
                analysis_result = result["analysis_result"]
                adapter = result["adapter"]

                # 调度导出并发送报告
                await self.report_dispatcher.dispatch(
                    group_id,
                    analysis_result,
                    adapter.platform_id
                    if hasattr(adapter, "platform_id")
                    else target_platform_id,
                )
            else:
                logger.info("群 %s 自动分析已完成，仅做联合日报准备，不发送单群日报", group_id)

            logger.info(f"群 {group_id} 自动分析任务执行成功")
            return result

        except DuplicateGroupTaskError:
            # group_lock 抛出的 DuplicateGroupTaskError 表示任务正在运行，优雅跳过
            logger.debug(f"群 {group_id} 任务因并发锁冲突而跳过（已在运行）")
            raise  # 重新抛出，让上层知道任务并没真正执行而是跳过了
        except Exception as e:
            logger.error(f"群 {group_id} 自动分析执行失败: {e}", exc_info=True)
            return {"success": False, "reason": str(e)}
        finally:
            logger.debug(f"群 {group_id} 自动分析流程结束")

    # ================================================================
    # 增量模式：增量分析
    # ================================================================

    async def _run_incremental_analysis(self):
        """为所有目标模式设定为 incremental 的群执行增量分析任务。"""
        if self._terminating:
            return
        try:
            logger.info("开始执行自动增量分析（并发模式）")

            # 仅选取模式为 incremental 的目标群
            incr_targets = await self._get_scheduled_targets(mode_filter="incremental")

            if not incr_targets:
                logger.info("没有配置为增量模式的群聊需要增量分析")
                return

            target_list = incr_targets
            stagger = self.config_manager.get_incremental_stagger_seconds()
            max_concurrent = self.config_manager.get_max_concurrent_tasks()

            logger.info(
                f"将为 {len(target_list)} 个群聊执行增量分析 "
                f"(并发限制: {max_concurrent}, 交错间隔: {stagger}秒)"
            )

            sem = asyncio.Semaphore(max_concurrent)

            async def staggered_incremental(idx, gid, pid):
                if idx > 0 and stagger > 0:
                    await asyncio.sleep(stagger * idx)

                async with sem:
                    result = (
                        await self._perform_incremental_analysis_for_group_with_timeout(
                            gid, pid
                        )
                    )

                    # 为调试提供的立即上报选项
                    if self.config_manager.get_incremental_report_immediately():
                        if isinstance(result, dict) and result.get("success"):
                            logger.info(
                                f"增量分析立即报告模式生效，正在为群 {gid} 生成报告..."
                            )
                            await self._perform_incremental_final_report_for_group_with_timeout(
                                gid, pid
                            )

                    return result

            analysis_tasks = []
            for idx, (gid, pid, _mode) in enumerate(target_list):
                if self._terminating:
                    logger.info("检测到插件正在停止，取消后续增量分析任务创建")
                    break
                task = asyncio.create_task(
                    staggered_incremental(idx, gid, pid),
                    name=f"incremental_group_{gid}",
                )
                analysis_tasks.append(task)

            results = await asyncio.gather(*analysis_tasks, return_exceptions=True)

            success_count = 0
            skip_count = 0
            error_count = 0

            for i, result in enumerate(results):
                gid, _, _ = target_list[i]
                if isinstance(result, DuplicateGroupTaskError):
                    skip_count += 1
                elif isinstance(result, Exception):
                    logger.error(f"群 {gid} 增量分析任务异常: {result}")
                    error_count += 1
                elif isinstance(result, dict) and not result.get("success", True):
                    skip_count += 1
                else:
                    success_count += 1

            logger.info(
                f"增量分析完成 - 成功: {success_count}, 跳过: {skip_count}, "
                f"失败: {error_count}, 总计: {len(target_list)}"
            )

        except Exception as e:
            logger.error(f"增量分析执行失败: {e}", exc_info=True)

    async def _perform_incremental_analysis_for_group_with_timeout(
        self, group_id: str, target_platform_id: str | None = None
    ):
        """为指定群执行增量分析（带超时控制，10分钟）"""
        try:
            result = await asyncio.wait_for(
                self._perform_incremental_analysis_for_group(
                    group_id, target_platform_id
                ),
                timeout=600,
            )
            return result
        except asyncio.TimeoutError:
            logger.error(f"群 {group_id} 增量分析超时（10分钟），跳过")
            return {"success": False, "reason": "timeout"}
        except Exception as e:
            logger.error(f"群 {group_id} 增量分析任务执行失败: {e}")
            return {"success": False, "reason": str(e)}

    async def _perform_incremental_analysis_for_group(
        self, group_id: str, target_platform_id: str | None = None
    ):
        """为指定群执行增量分析（业务逻辑委派给 AnalysisApplicationService）"""
        try:
            # 解析可读群名以生成语义化的 TraceID
            group_name = await self._get_group_name_safe(group_id, target_platform_id)
            trace_id = TraceContext.generate(prefix="incr", group_name=group_name)
            TraceContext.set(trace_id)

            if self._terminating:
                return

            logger.info(
                f"开始为群 {group_id} 执行增量分析 "
                f"(Platform: {target_platform_id or 'Auto'})"
            )

            # 检查平台状态
            if not self.bot_manager.is_ready_for_auto_analysis():
                logger.warning(f"群 {group_id} 增量分析跳过：bot管理器未就绪")
                return {"success": False, "reason": "bot_not_ready"}

            # 委派给应用层服务执行增量分析用例
            # AnalysisApplicationService 内部已处理群锁 (group_lock)
            result = await self.analysis_service.execute_incremental_analysis(
                group_id=group_id, platform_id=target_platform_id
            )

            if not result.get("success"):
                reason = result.get("reason", "unknown")
                logger.info(f"群 {group_id} 增量分析跳过: {reason}")
                return result

            # 增量分析只累积数据，不发送报告
            batch_summary = result.get("batch_summary", {})
            logger.info(
                f"群 {group_id} 增量分析完成: "
                f"消息数={result.get('messages_count', 0)}, "
                f"话题={batch_summary.get('topics_count', 0)}, "
                f"金句={batch_summary.get('quotes_count', 0)}"
            )
            return result

        except DuplicateGroupTaskError:
            # group_lock 抛出的 DuplicateGroupTaskError 表示任务正在运行，优雅跳过
            logger.debug(f"群 {group_id} 增量分析因并发锁冲突而跳过（已在运行）")
            return {"success": False, "reason": "already_running"}
        except Exception as e:
            logger.error(f"群 {group_id} 增量分析执行失败: {e}", exc_info=True)
            return {"success": False, "reason": str(e)}
        finally:
            logger.debug(f"群 {group_id} 增量分析流程结束")

    # ================================================================
    # 增量最终报告（单群）与回退逻辑
    # ================================================================

    async def _perform_incremental_final_report_for_group_with_timeout(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
        dispatch_report: bool = True,
    ):
        """带超时及回退机制的增量最终报告生成。

        若增量汇报失败（非 '消息不足' 或 '正在运行' 导致的），
        且启用了自动回退，则将该群转由传统模式执行全量分析。
        """
        try:
            result = await asyncio.wait_for(
                self._perform_incremental_final_report_for_group(
                    group_id,
                    target_platform_id,
                    archive_date,
                    dispatch_report=dispatch_report,
                ),
                timeout=1800,
            )

            # 判定是否需要触发回退 (例如：无增量数据等)
            if isinstance(result, dict) and not result.get("success"):
                reason = result.get("reason", "")
                if reason in ("below_threshold", "already_running"):
                    return result  # 正常跳过，无需回退
                if self.config_manager.get_incremental_fallback_enabled():
                    logger.warning(
                        f"群 {group_id} 增量最终报告失败 (reason={reason})，"
                        f"正在回退到传统全量分析..."
                    )
                    return await self._fallback_to_traditional(
                        group_id,
                        target_platform_id,
                        archive_date,
                        dispatch_report=dispatch_report,
                    )

            return result

        except asyncio.TimeoutError:
            logger.error(f"群 {group_id} 最终报告超时（30分钟）")
            if self.config_manager.get_incremental_fallback_enabled():
                logger.warning(f"群 {group_id} 增量报告超时，正在回退到传统全量分析...")
                return await self._fallback_to_traditional(
                    group_id,
                    target_platform_id,
                    archive_date,
                    dispatch_report=dispatch_report,
                )
            return {"success": False, "reason": "timeout"}

        except Exception as e:
            logger.error(f"群 {group_id} 最终报告任务执行失败: {e}")
            if self.config_manager.get_incremental_fallback_enabled():
                logger.warning(f"群 {group_id} 增量报告异常，正在回退到传统全量分析...")
                return await self._fallback_to_traditional(
                    group_id,
                    target_platform_id,
                    archive_date,
                    dispatch_report=dispatch_report,
                )
            return {"success": False, "reason": str(e)}

    async def _fallback_to_traditional(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
        dispatch_report: bool = True,
    ):
        """回退操作：在增量报告失败时，执行传统的全量拉取分析。"""
        try:
            logger.info(
                f"⬆️ 群 {group_id} 回退到传统全量分析 "
                f"(Platform: {target_platform_id or 'Auto'})"
            )
            result = await self._perform_auto_analysis_for_group_with_timeout(
                group_id,
                target_platform_id,
                archive_date,
                dispatch_report=dispatch_report,
            )
            if isinstance(result, dict) and not result.get("success", False):
                return result
            return {"success": True, "fallback": True}
        except Exception as fallback_err:
            logger.error(
                f"群 {group_id} 回退传统分析也失败: {fallback_err}",
                exc_info=True,
            )
            return {"success": False, "reason": f"fallback_failed: {fallback_err}"}

    async def _perform_incremental_final_report_for_group(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
        dispatch_report: bool = True,
    ):
        """为指定群生成增量最终报告（业务逻辑委派给 AnalysisApplicationService）"""
        try:
            # 解析可读群名以生成语义化的 TraceID
            group_name = await self._get_group_name_safe(group_id, target_platform_id)
            trace_id = TraceContext.generate(prefix="report", group_name=group_name)
            TraceContext.set(trace_id)

            if self._terminating:
                return {"success": False, "reason": "terminating"}

            logger.info(
                f"开始为群 {group_id} 生成增量最终报告 "
                f"(Platform: {target_platform_id or 'Auto'})"
            )

            # 检查平台状态
            if not self.bot_manager.is_ready_for_auto_analysis():
                logger.warning(f"群 {group_id} 最终报告跳过：bot管理器未就绪")
                return {"success": False, "reason": "bot_not_ready"}

            # 委派给应用层服务执行最终报告用例
            # AnalysisApplicationService 内部已处理群锁 (group_lock)
            result = await self.analysis_service.execute_incremental_final_report(
                group_id=group_id,
                platform_id=target_platform_id,
                archive_date=archive_date,
            )

            if not result.get("success"):
                reason = result.get("reason", "unknown")
                logger.info(f"群 {group_id} 最终报告跳过: {reason}")
                return result

            if dispatch_report:
                # 获取分析结果及适配器，分发报告
                analysis_result = result["analysis_result"]
                adapter = result["adapter"]

                await self.report_dispatcher.dispatch(
                    group_id,
                    analysis_result,
                    adapter.platform_id
                    if hasattr(adapter, "platform_id")
                    else target_platform_id,
                )

                # 清理过期批次（保留 2 倍窗口范围的数据作为缓冲）
                try:
                    analysis_days = self.config_manager.get_analysis_days()
                    before_ts = time_mod.time() - (analysis_days * 2 * 24 * 3600)
                    incremental_store = self.analysis_service.incremental_store
                    if incremental_store:
                        cleaned = await incremental_store.cleanup_old_batches(
                            group_id, before_ts
                        )
                        if cleaned > 0:
                            logger.info(
                                f"群 {group_id} 报告发送后清理了 {cleaned} 个过期批次"
                            )
                except Exception as cleanup_err:
                    logger.warning(
                        f"群 {group_id} 过期批次清理失败（不影响报告）: {cleanup_err}"
                    )
            else:
                logger.info("群 %s 增量最终报告已完成，仅做联合日报准备，不发送单群日报", group_id)

            if dispatch_report:
                logger.info(f"群 {group_id} 增量最终报告发送成功")
            else:
                logger.info(f"群 {group_id} 增量最终报告准备成功")
            return result

        except DuplicateGroupTaskError:
            # group_lock 抛出的 DuplicateGroupTaskError 表示任务正在运行，优雅跳过
            logger.debug(f"群 {group_id} 最终报告因并发锁冲突而跳过（已在运行）")
            return {"success": False, "reason": "already_running"}
        except Exception as e:
            logger.error(f"群 {group_id} 最终报告执行失败: {e}", exc_info=True)
            return {"success": False, "reason": str(e)}
        finally:
            logger.debug(f"群 {group_id} 最终报告流程结束")

    async def _run_union_report_after_reports(self, report_date: str) -> None:
        """
        在单群日报完成后，延迟执行跨群聚合日报。
        """
        if not self.config_manager.get_union_report_enabled():
            return
        if self.config_manager.get_union_report_time():
            logger.info("已配置跨群日报固定时间，跳过延迟触发模式")
            return
        await self._run_union_report_core(
            report_date=report_date,
            apply_delay=True,
            target_groups_override=None,
            require_all_groups_ready=False,
        )

    async def _run_union_report_on_schedule(self) -> None:
        """固定时间触发跨群聚合日报。"""
        if self._terminating or not self.config_manager.get_union_report_enabled():
            return
        report_date = datetime.now().strftime("%Y-%m-%d")
        await self._run_union_report_core(
            report_date=report_date,
            apply_delay=False,
            target_groups_override=None,
            require_all_groups_ready=True,
        )

    async def _run_union_prepare_on_schedule(self) -> None:
        """固定时间模式下，提前触发 union 源群的单群链路。"""
        if self._terminating or not self.config_manager.get_union_report_enabled():
            return

        report_date = datetime.now().strftime("%Y-%m-%d")
        result = await self._run_union_prepare_core(report_date)
        if not result.get("success"):
            logger.warning(
                "跨群日报准备任务失败: date=%s reason=%s",
                report_date,
                result.get("reason", "unknown"),
            )

    async def run_union_prepare_manual(self, report_date: str) -> dict[str, Any]:
        """手动触发 union 源群的单群链路准备。"""
        return await self._run_union_prepare_core(report_date)

    async def run_union_report_manual(
        self,
        report_date: str,
        target_groups_override: list[tuple[str, str | None]],
    ) -> dict[str, Any]:
        """手动触发跨群聚合日报全链路，用于命令测试。"""
        if not target_groups_override:
            return {"success": False, "reason": "no_targets"}

        prepare_result = await self.run_union_prepare_manual(report_date)
        if not prepare_result.get("success"):
            return prepare_result

        return await self._run_union_report_core(
            report_date=report_date,
            apply_delay=False,
            target_groups_override=target_groups_override,
            skip_enabled_check=True,
            bypass_daily_guard=True,
            require_all_groups_ready=True,
        )

    async def _run_union_prepare_core(self, report_date: str) -> dict[str, Any]:
        """执行 union 源群单群链路，供固定时间和手动测试共用。"""
        target_list = await self._get_union_source_targets()
        if not target_list:
            logger.info("跨群日报准备任务跳过：没有可用的 union 源群")
            result = {"success": False, "reason": "no_source_groups"}
            self._union_prepare_results[report_date] = result
            return result

        max_concurrent = self.config_manager.get_max_concurrent_tasks()
        sem = asyncio.Semaphore(max_concurrent)
        logger.info(
            "跨群日报准备任务开始：date=%s 目标群=%d 并发限制=%d",
            report_date,
            len(target_list),
            max_concurrent,
        )

        async def dispatch_group(
            gid: str, pid: str | None, mode: str
        ) -> dict[str, Any] | None:
            async with sem:
                if mode == "incremental":
                    return await self._perform_incremental_final_report_for_group_with_timeout(
                        gid,
                        pid,
                        report_date,
                        dispatch_report=False,
                    )
                return await self._perform_auto_analysis_for_group_with_timeout(
                    gid,
                    pid,
                    report_date,
                    dispatch_report=False,
                )

        tasks = [
            asyncio.create_task(
                dispatch_group(gid, pid, mode),
                name=f"union_prepare_{mode}_{gid}",
            )
            for gid, pid, mode, _group_ref in target_list
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = 0
        skip_count = 0
        error_count = 0

        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            elif isinstance(result, dict) and not result.get("success", True):
                skip_count += 1
            else:
                success_count += 1

        result_summary: dict[str, Any] = {
            "success": success_count > 0,
            "prepared_count": len(target_list),
            "success_count": success_count,
            "skip_count": skip_count,
            "error_count": error_count,
        }
        if success_count == 0:
            result_summary["reason"] = "prepare_failed"

        logger.info(
            "跨群日报准备任务完成 — date=%s 成功=%d 跳过=%d 失败=%d 总计=%d",
            report_date,
            success_count,
            skip_count,
            error_count,
            len(target_list),
        )
        self._union_prepare_results[report_date] = result_summary
        return result_summary

    async def _run_union_report_core(
        self,
        report_date: str,
        apply_delay: bool,
        target_groups_override: list[tuple[str, str | None]] | None,
        skip_enabled_check: bool = False,
        bypass_daily_guard: bool = False,
        require_all_groups_ready: bool = False,
    ) -> dict[str, Any]:
        """
        执行跨群聚合日报的核心流程。
        """
        if not skip_enabled_check and not self.config_manager.get_union_report_enabled():
            return {"success": False, "reason": "disabled"}
        if (
            not self.union_daily_report_service
            or not self.report_generator
            or not self.union_report_renderer
        ):
            logger.warning("跨群聚合日报未初始化完全，跳过执行")
            return {"success": False, "reason": "not_initialized"}

        if not bypass_daily_guard:
            async with self._union_report_guard:
                if self._last_union_report_date == report_date:
                    logger.info(f"跨群聚合日报已在 {report_date} 生成过，本次跳过")
                    return {"success": False, "reason": "already_sent"}
                if report_date in self._union_report_dates_in_progress:
                    logger.info(f"跨群聚合日报 {report_date} 已在执行中，本次跳过")
                    return {"success": False, "reason": "already_running"}
                self._union_report_dates_in_progress.add(report_date)

        try:
            source_group_refs = await self._normalize_union_group_refs(
                self.config_manager.get_union_groups_list()
            )
            if not source_group_refs:
                logger.info("跨群聚合日报跳过：未配置 union_groups_list")
                return {"success": False, "reason": "no_source_groups"}

            prepare_result = self._union_prepare_results.get(report_date)
            if require_all_groups_ready and prepare_result and not prepare_result.get(
                "success", False
            ):
                logger.warning(
                    "跨群聚合日报跳过：源群准备阶段已失败。date=%s prepare=%s",
                    report_date,
                    prepare_result,
                )
                return {
                    "success": False,
                    "reason": prepare_result.get("reason", "prepare_failed"),
                    "prepare_result": prepare_result,
                }

            if require_all_groups_ready:
                wait_timeout_minutes = max(
                    0,
                    self.config_manager.get_union_wait_timeout_minutes(),
                )
                missing_group_refs = await self._wait_for_union_source_reports_ready(
                    source_group_refs,
                    report_date,
                    wait_timeout_minutes,
                )
                if missing_group_refs:
                    logger.warning(
                        "跨群聚合日报跳过：固定时间触发后等待超时，仍有源群日报未就绪。date=%s missing=%s",
                        report_date,
                        missing_group_refs,
                    )
                    return {
                        "success": False,
                        "reason": "source_reports_not_ready",
                        "missing_groups": missing_group_refs,
                    }

            delay_minutes = max(0, self.config_manager.get_union_report_delay_minutes())
            if apply_delay and delay_minutes > 0:
                logger.info(f"跨群聚合日报将在 {delay_minutes} 分钟后执行")
                await asyncio.sleep(delay_minutes * 60)

            if self._terminating:
                return {"success": False, "reason": "terminating"}

            trace_id = TraceContext.generate(
                prefix="union", group_name=f"union-{report_date}"
            )
            TraceContext.set(trace_id)

            report = await self.union_daily_report_service.build_union_report(
                source_group_refs,
                report_date,
            )
            if report is None:
                logger.info(f"跨群聚合日报跳过：{report_date} 无可用聚合结果")
                return {"success": False, "reason": "no_report_data"}

            await self._enrich_union_report_names(report)
            sent_count = await self._dispatch_union_report(
                report,
                target_groups_override=target_groups_override,
            )
            if sent_count > 0:
                if not bypass_daily_guard:
                    async with self._union_report_guard:
                        self._last_union_report_date = report_date
                logger.info(f"跨群聚合日报发送完成，成功目标数: {sent_count}")
                return {"success": True, "sent_count": sent_count, "report": report}

            logger.warning("跨群聚合日报未成功发送到任何目标群")
            return {"success": False, "reason": "dispatch_failed", "report": report}
        finally:
            if not bypass_daily_guard:
                async with self._union_report_guard:
                    self._union_report_dates_in_progress.discard(report_date)

    async def _dispatch_union_report(
        self,
        report,
        target_groups_override: list[tuple[str, str | None]] | None = None,
    ) -> int:
        target_groups = target_groups_override or self._normalize_union_send_targets(
            self.config_manager.get_union_target_groups()
            or self.config_manager.get_union_groups_list()
        )
        if not target_groups:
            logger.info("跨群聚合日报跳过：未配置发送目标")
            return 0

        output_format = self.config_manager.get_output_format()
        caption = f"📊 五群联合日报 ({report.report_date})"
        html_content = self.union_report_renderer.render_html(report)
        text_content = self.union_report_renderer.render_text(report)
        sent_count = 0

        if output_format == "image" and self.html_render_func:
            image_url = await self.report_generator.render_html_content_to_image(
                html_content,
                f"union_{report.report_date}",
                self.html_render_func,
            )
            if image_url:
                for group_id, platform_id in target_groups:
                    resolved_platform_id = platform_id or await self.get_platform_id_for_group(
                        group_id
                    )
                    if await self.message_sender.send_image_smart(
                        group_id,
                        image_url,
                        caption,
                        resolved_platform_id,
                    ):
                        sent_count += 1
                if sent_count > 0:
                    return sent_count
            logger.warning("跨群聚合日报图片发送失败，回退到文本/HTML发送")

        if output_format == "html":
            html_path = await self.report_generator.save_rendered_html_report(
                html_content,
                f"union_{report.report_date}",
            )
            if html_path:
                html_caption = self.report_generator.build_html_caption(html_path)
                for group_id, platform_id in target_groups:
                    resolved_platform_id = platform_id or await self.get_platform_id_for_group(
                        group_id
                    )
                    if await self.message_sender.send_file(
                        group_id,
                        html_path,
                        caption=html_caption,
                        platform_id=resolved_platform_id,
                    ):
                        sent_count += 1
                if sent_count > 0:
                    return sent_count
            logger.warning("跨群聚合日报 HTML 发送失败，回退到文本发送")

        for group_id, platform_id in target_groups:
            resolved_platform_id = platform_id or await self.get_platform_id_for_group(
                group_id
            )
            if await self.message_sender.send_text(
                group_id,
                text_content,
                resolved_platform_id,
            ):
                sent_count += 1

        return sent_count

    async def _enrich_union_report_names(self, report) -> None:
        name_map: dict[str, str] = {}

        for snapshot in report.group_snapshots:
            resolved_name = await self._get_group_name_safe(
                snapshot.group_id,
                snapshot.platform_id or None,
            )
            if resolved_name:
                snapshot.group_name = resolved_name
                name_map[snapshot.group_ref] = resolved_name

        champion_name = name_map.get(report.champion_group.group_ref)
        if champion_name:
            report.champion_group.group_name = champion_name

        for quote in report.top_quotes:
            if quote.group_ref in name_map:
                quote.group_name = name_map[quote.group_ref]

        for topic in report.topic_highlights:
            if topic.group_ref in name_map:
                topic.group_name = name_map[topic.group_ref]

    @staticmethod
    def _parse_union_group_ref(group_ref: str) -> tuple[str | None, str]:
        text = str(group_ref).strip()
        if not text:
            return None, ""
        if ":" not in text:
            return None, text

        parts = text.split(":")
        platform_id = parts[0].strip() or None
        group_id = parts[-1].strip()
        return platform_id, group_id

    async def _normalize_union_group_refs(self, group_refs: list[str]) -> list[str]:
        normalized_refs: list[str] = []
        seen: set[str] = set()
        for group_ref in group_refs:
            platform_id, group_id = self._parse_union_group_ref(group_ref)
            if group_id and platform_id is None:
                platform_id = await self.get_platform_id_for_group(group_id)
                if platform_id is None:
                    logger.warning(
                        f"跨群聚合源群 {group_id} 未显式配置平台且无法自动解析，已跳过。"
                    )
                    continue
            normalized_ref = self._build_group_ref(group_id, platform_id)
            if not group_id or normalized_ref in seen:
                continue
            normalized_refs.append(normalized_ref)
            seen.add(normalized_ref)
        return normalized_refs

    def _normalize_union_send_targets(
        self,
        group_refs: list[str],
    ) -> list[tuple[str, str | None]]:
        targets: list[tuple[str, str | None]] = []
        seen: set[tuple[str, str]] = set()
        for group_ref in group_refs:
            platform_id, group_id = self._parse_union_group_ref(group_ref)
            if not group_id:
                continue
            key = (platform_id or "", group_id)
            if key in seen:
                continue
            targets.append((group_id, platform_id))
            seen.add(key)
        return targets

    @staticmethod
    def _build_group_ref(group_id: str, platform_id: str | None) -> str:
        if platform_id:
            return f"{platform_id}:GroupMessage:{group_id}"
        return group_id

    async def _get_union_source_targets(
        self,
    ) -> list[tuple[str, str | None, str, str]]:
        """解析 union 源群及其单群执行模式。"""
        source_group_refs = await self._normalize_union_group_refs(
            self.config_manager.get_union_groups_list()
        )
        if not source_group_refs:
            return []

        incr_list = self.config_manager.get_incremental_group_list()
        incr_list_mode = self.config_manager.get_incremental_group_list_mode()
        targets: list[tuple[str, str | None, str, str]] = []

        for group_ref in source_group_refs:
            platform_id, group_id = self._parse_union_group_ref(group_ref)
            if not group_id:
                continue
            mode = (
                "incremental"
                if self.config_manager.is_group_in_filtered_list(
                    group_ref,
                    incr_list_mode,
                    incr_list,
                )
                else "traditional"
            )
            targets.append((group_id, platform_id, mode, group_ref))

        return targets

    async def _wait_for_union_source_reports_ready(
        self,
        source_group_refs: list[str],
        report_date: str,
        wait_timeout_minutes: int,
    ) -> list[str]:
        """等待 union 源群日报就绪，直到全部完成或超时。"""
        missing_group_refs = self.union_daily_report_service.get_missing_group_refs_for_date(
            source_group_refs,
            report_date,
        )
        if not missing_group_refs:
            return []

        timeout_seconds = wait_timeout_minutes * 60
        if timeout_seconds <= 0:
            return missing_group_refs

        poll_interval_seconds = 30
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        logger.info(
            "跨群聚合日报等待源群就绪，最多 %d 分钟。当前缺失: %s",
            wait_timeout_minutes,
            missing_group_refs,
        )

        while missing_group_refs and not self._terminating:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break

            await asyncio.sleep(min(poll_interval_seconds, remaining))
            missing_group_refs = (
                self.union_daily_report_service.get_missing_group_refs_for_date(
                    source_group_refs,
                    report_date,
                )
            )

        return missing_group_refs

    # ================================================================
    # 群列表获取（基础设施层）
    # ================================================================

    async def _get_all_groups(self) -> list[tuple[str, str]]:
        """
        获取所有bot实例所在的群列表（使用 PlatformAdapter）

        Returns:
            list[tuple[str, str]]: [(platform_id, group_id), ...]
        """
        all_groups = set()

        # 1. [韧性增强] 进入扫描前，尝试最后一次实时发现机器人
        # 这确保了即使冷启动初始化失败，定时任务触发时仍能刷新状态
        if hasattr(self.bot_manager, "auto_discover_bot_instances"):
            try:
                await self.bot_manager.auto_discover_bot_instances()
            except Exception as e:
                logger.warning(f"[AutoScheduler] 周期性扫描中的平台发现失败: {e}")

        bot_ids = list(self.bot_manager._bot_instances.keys())

        if not bot_ids:
            logger.warning(
                "[AutoScheduler] 分析周期开启，但全局未发现任何在线 Bot。任务将跳过。"
            )
            return []

        logger.info(f"[AutoScheduler] 正在扫描 {len(bot_ids)} 个平台的群聊资源...")

        for platform_id, bot_instance in self.bot_manager._bot_instances.items():
            # 检查该平台是否启用了此插件
            if not self.bot_manager.is_plugin_enabled(
                platform_id, "astrbot_plugin_qq_group_daily_analysis"
            ):
                logger.debug(f"平台 {platform_id} 未启用此插件，跳过获取群列表")
                continue

            try:
                # 1. 优先从 BotManager 获取已创建的适配器
                adapter = self.bot_manager.get_adapter(platform_id)

                # 2. 如果没有，尝试临时创建（降级方案）
                platform_name = None
                if not adapter:
                    platform_name = self.bot_manager._detect_platform_name(bot_instance)
                    if platform_name:
                        adapter = PlatformAdapterFactory.create(
                            platform_name,
                            bot_instance,
                            config={
                                "bot_self_ids": self.config_manager.get_bot_self_ids(),
                                "platform_id": str(platform_id),
                            },
                        )

                # 3. 使用适配器获取群列表
                if adapter:
                    try:
                        groups = await adapter.get_group_list()
                        groups = [
                            str(group_id).strip()
                            for group_id in groups
                            if str(group_id).strip()
                        ]

                        # 获取平台名称（仅用于日志）
                        p_name = None
                        if hasattr(adapter, "get_platform_name"):
                            try:
                                p_name = adapter.get_platform_name()
                            except Exception:
                                p_name = None

                        for group_id in groups:
                            all_groups.add((platform_id, str(group_id)))

                        logger.info(
                            f"平台 {platform_id} ({p_name or 'unknown'}) 成功获取 {len(groups)} 个群组"
                        )
                        continue

                    except Exception as e:
                        logger.warning(f"适配器 {platform_id} 获取群列表失败: {e}")

                # 4. 降级：无法通过适配器获取
                logger.debug(f"平台 {platform_id} 无法通过适配器获取群列表")

            except Exception as e:
                logger.error(f"平台 {platform_id} 获取群列表异常: {e}")

        return list(all_groups)
