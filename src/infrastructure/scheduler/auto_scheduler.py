"""
跨群联合日报调度器。

只保留增量链路：
1. 活跃时段按固定间隔对 union 源群执行增量提取。
2. 联合日报发送前执行源群增量最终 JSON 准备。
3. 到点聚合所有源群 JSON 并发送联合日报图片。
"""

from __future__ import annotations

import asyncio
import time as time_mod
from datetime import datetime
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from ...application.services.analysis_application_service import DuplicateGroupTaskError
from ...shared.trace_context import TraceContext
from ...utils.logger import logger
from ..messaging.message_sender import MessageSender
from ..reporting.union_report_renderer import UnionReportRenderer


class AutoScheduler:
    """增量跨群联合日报调度器。"""

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

        self.message_sender = MessageSender(bot_manager, config_manager)
        self.union_report_renderer = (
            UnionReportRenderer(report_generator) if report_generator else None
        )

        self.scheduler_job_ids: list[str] = []
        self._last_union_report_date: str | None = None
        self._union_report_guard = asyncio.Lock()
        self._union_report_dates_in_progress: set[str] = set()
        self._union_prepare_results: dict[str, dict[str, Any]] = {}
        self._group_name_cache: dict[str, str] = {}
        self._terminating = False

    def set_bot_instance(self, bot_instance):
        self.bot_manager.set_bot_instance(bot_instance)

    def set_bot_self_ids(self, bot_self_ids):
        if isinstance(bot_self_ids, list):
            self.bot_manager.set_bot_self_ids(bot_self_ids)
        elif bot_self_ids:
            self.bot_manager.set_bot_self_ids([bot_self_ids])

    def set_bot_qq_ids(self, bot_qq_ids):
        self.set_bot_self_ids(bot_qq_ids)

    async def get_platform_id_for_group(self, group_id: str) -> str | None:
        """根据群 ID 在已发现的平台中解析唯一 platform_id。"""
        try:
            if not getattr(self.bot_manager, "_bot_instances", None):
                logger.error("无法解析群 %s 的平台：未发现任何 Bot 实例", group_id)
                return None

            if self.bot_manager.get_platform_count() == 1:
                return self.bot_manager.get_platform_ids()[0]

            matched_platform_ids: list[str] = []
            for platform_id in self.bot_manager.get_platform_ids():
                try:
                    adapter = self.bot_manager.get_adapter(platform_id)
                    if not adapter:
                        continue
                    info = await adapter.get_group_info(str(group_id))
                    if info:
                        matched_platform_ids.append(platform_id)
                except Exception as e:
                    logger.debug("平台 %s 验证群 %s 失败: %s", platform_id, group_id, e)

            if len(matched_platform_ids) == 1:
                return matched_platform_ids[0]

            if len(matched_platform_ids) > 1:
                logger.error(
                    "群 %s 在多个平台同时命中，无法唯一确定平台: %s",
                    group_id,
                    matched_platform_ids,
                )
                return None

            logger.error("无法确定群 %s 属于哪个平台", group_id)
            return None
        except Exception as e:
            logger.error("获取群 %s 的平台 ID 失败: %s", group_id, e)
            return None

    async def _get_group_name_safe(
        self, group_id: str, platform_id: str | None = None
    ) -> str:
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

    def schedule_jobs(self, context) -> None:
        """注册增量采集、联合日报准备和联合日报发送任务。"""
        self.unschedule_jobs(context)
        self._terminating = False

        if not self.config_manager.get_union_report_enabled():
            logger.info("跨群联合日报未启用，不注册定时任务。")
            return

        if not self.config_manager.get_union_groups_list():
            logger.info("未配置 union_groups_list，不注册定时任务。")
            return

        if not self.config_manager.get_union_report_time():
            logger.info("未配置 union_report_time，不注册定时任务。")
            return

        if (
            not context
            or not hasattr(context, "cron_manager")
            or not context.cron_manager
            or not context.cron_manager.scheduler
        ):
            logger.warning("Cron 管理器不可用，无法注册跨群联合日报任务。")
            return

        scheduler = context.cron_manager.scheduler
        self._schedule_incremental_cron_jobs(scheduler)
        self._schedule_union_report_time_job(scheduler)

    def _schedule_incremental_cron_jobs(self, scheduler) -> None:
        active_start_hour = self.config_manager.get_incremental_active_start_hour()
        active_end_hour = self.config_manager.get_incremental_active_end_hour()
        interval_minutes = self.config_manager.get_incremental_interval_minutes()
        max_daily = self.config_manager.get_incremental_max_daily_analyses()

        trigger_times: list[tuple[int, int]] = []
        current_minutes = active_start_hour * 60
        end_minutes = active_end_hour * 60
        while current_minutes < end_minutes and len(trigger_times) < max_daily:
            trigger_times.append((current_minutes // 60, current_minutes % 60))
            current_minutes += interval_minutes

        for hour, minute in trigger_times:
            try:
                job_id = f"union_incremental_analysis_{hour:02d}{minute:02d}"
                scheduler.add_job(
                    self._run_incremental_analysis,
                    trigger=CronTrigger(hour=hour, minute=minute),
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=60,
                )
                self.scheduler_job_ids.append(job_id)
                logger.info("已注册源群增量分析任务: %02d:%02d", hour, minute)
            except Exception as e:
                logger.error("注册源群增量分析任务失败 (%02d:%02d): %s", hour, minute, e)

        logger.info("源群增量调度注册完成: %d 个任务", len(trigger_times))

    def _schedule_union_report_time_job(self, scheduler) -> None:
        time_str = self.config_manager.get_union_report_time()
        try:
            normalized = str(time_str).replace("：", ":").strip()
            hour, minute = normalized.split(":")
            publish_hour = int(hour)
            publish_minute = int(minute)

            publish_job_id = "astrbot_plugin_union_daily_report_trigger"
            scheduler.add_job(
                self._run_union_report_on_schedule,
                trigger=CronTrigger(hour=publish_hour, minute=publish_minute),
                id=publish_job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            self.scheduler_job_ids.append(publish_job_id)
            logger.info("已注册跨群联合日报发送任务: %s", normalized)

            lead_minutes = max(0, self.config_manager.get_union_prepare_lead_minutes())
            prepare_hour, prepare_minute = self._shift_clock_time(
                publish_hour,
                publish_minute,
                -lead_minutes,
            )
            prepare_job_id = "astrbot_plugin_union_daily_report_prepare_trigger"
            scheduler.add_job(
                self._run_union_prepare_on_schedule,
                trigger=CronTrigger(hour=prepare_hour, minute=prepare_minute),
                id=prepare_job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            self.scheduler_job_ids.append(prepare_job_id)
            logger.info(
                "已注册跨群联合日报准备任务: %02d:%02d (提前 %d 分钟)",
                prepare_hour,
                prepare_minute,
                lead_minutes,
            )
        except Exception as e:
            logger.error("注册跨群联合日报固定任务失败 (%s): %s", time_str, e)

    @staticmethod
    def _shift_clock_time(hour: int, minute: int, delta_minutes: int) -> tuple[int, int]:
        total_minutes = (hour * 60 + minute + delta_minutes) % (24 * 60)
        return total_minutes // 60, total_minutes % 60

    def unschedule_jobs(self, context) -> None:
        self._terminating = True
        if (
            not context
            or not hasattr(context, "cron_manager")
            or not context.cron_manager
            or not context.cron_manager.scheduler
        ):
            return

        scheduler = context.cron_manager.scheduler
        for job_id in self.scheduler_job_ids:
            try:
                if scheduler.get_job(job_id):
                    scheduler.remove_job(job_id)
            except Exception as e:
                logger.warning("移除定时任务失败 (%s): %s", job_id, e)
        self.scheduler_job_ids.clear()

    async def _run_incremental_analysis(self) -> None:
        """对所有 union 源群执行一次增量提取。"""
        if self._terminating:
            return

        target_list = await self._get_incremental_runtime_targets()
        if not target_list:
            logger.info("没有可执行增量提取的 union 源群")
            return

        max_concurrent = self.config_manager.get_max_concurrent_tasks()
        sem = asyncio.Semaphore(max_concurrent)
        stagger = self.config_manager.get_incremental_stagger_seconds()

        async def dispatch_group(gid: str, pid: str | None) -> dict[str, Any] | None:
            async with sem:
                return await self._perform_incremental_analysis_for_group_with_timeout(
                    gid,
                    pid,
                )

        tasks = []
        for index, (group_id, platform_id, _group_ref) in enumerate(target_list):
            if self._terminating:
                break
            if index > 0 and stagger > 0:
                await asyncio.sleep(stagger)
            tasks.append(
                asyncio.create_task(
                    dispatch_group(group_id, platform_id),
                    name=f"union_incremental_{group_id}",
                )
            )

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

        logger.info(
            "源群增量提取完成 - 成功: %d, 跳过: %d, 失败: %d, 总计: %d",
            success_count,
            skip_count,
            error_count,
            len(target_list),
        )

    async def _perform_incremental_analysis_for_group_with_timeout(
        self,
        group_id: str,
        target_platform_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            return await asyncio.wait_for(
                self._perform_incremental_analysis_for_group(
                    group_id,
                    target_platform_id,
                ),
                timeout=600,
            )
        except asyncio.TimeoutError:
            logger.error("群 %s 增量分析超时", group_id)
            return {"success": False, "reason": "timeout"}
        except Exception as e:
            logger.error("群 %s 增量分析任务执行失败: %s", group_id, e)
            return {"success": False, "reason": str(e)}

    async def _perform_incremental_analysis_for_group(
        self,
        group_id: str,
        target_platform_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            group_name = await self._get_group_name_safe(group_id, target_platform_id)
            TraceContext.set(TraceContext.generate(prefix="incr", group_name=group_name))

            if self._terminating:
                return {"success": False, "reason": "terminating"}

            if not self.bot_manager.is_ready_for_union_tasks():
                return {"success": False, "reason": "bot_not_ready"}

            result = await self.analysis_service.execute_incremental_analysis(
                group_id=group_id,
                platform_id=target_platform_id,
            )
            if not result.get("success"):
                logger.info("群 %s 增量分析跳过: %s", group_id, result.get("reason"))
                return result

            batch_summary = result.get("batch_summary", {})
            logger.info(
                "群 %s 增量分析完成: 消息=%s 话题=%s 金句=%s",
                group_id,
                result.get("messages_count", 0),
                batch_summary.get("topics_count", 0),
                batch_summary.get("quotes_count", 0),
            )
            return result
        except DuplicateGroupTaskError:
            return {"success": False, "reason": "already_running"}
        except Exception as e:
            logger.error("群 %s 增量分析执行失败: %s", group_id, e, exc_info=True)
            return {"success": False, "reason": str(e)}

    async def _perform_incremental_final_report_for_group_with_timeout(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
    ) -> dict[str, Any]:
        """生成源群增量最终 JSON；失败直接返回。"""
        try:
            return await asyncio.wait_for(
                self._perform_incremental_final_report_for_group(
                    group_id,
                    target_platform_id,
                    archive_date,
                ),
                timeout=1800,
            )
        except asyncio.TimeoutError:
            logger.error("群 %s 增量最终 JSON 生成超时", group_id)
            return {"success": False, "reason": "timeout"}
        except Exception as e:
            logger.error("群 %s 增量最终 JSON 任务失败: %s", group_id, e)
            return {"success": False, "reason": str(e)}

    async def _perform_incremental_final_report_for_group(
        self,
        group_id: str,
        target_platform_id: str | None = None,
        archive_date: str | None = None,
    ) -> dict[str, Any]:
        try:
            group_name = await self._get_group_name_safe(group_id, target_platform_id)
            TraceContext.set(
                TraceContext.generate(prefix="report", group_name=group_name)
            )

            if self._terminating:
                return {"success": False, "reason": "terminating"}
            if not self.bot_manager.is_ready_for_union_tasks():
                return {"success": False, "reason": "bot_not_ready"}

            result = await self.analysis_service.execute_incremental_final_report(
                group_id=group_id,
                platform_id=target_platform_id,
                archive_date=archive_date,
            )
            if not result.get("success"):
                logger.info("群 %s 增量最终 JSON 跳过: %s", group_id, result.get("reason"))
                return result

            try:
                analysis_days = self.config_manager.get_analysis_days()
                before_ts = time_mod.time() - (analysis_days * 2 * 24 * 3600)
                incremental_store = self.analysis_service.incremental_store
                if incremental_store:
                    cleaned = await incremental_store.cleanup_old_batches(
                        self._build_group_ref(group_id, result.get("platform_id")),
                        before_ts,
                    )
                    if cleaned > 0:
                        logger.info("群 %s 清理了 %d 个过期增量批次", group_id, cleaned)
            except Exception as cleanup_err:
                logger.warning("群 %s 过期批次清理失败: %s", group_id, cleanup_err)

            return result
        except DuplicateGroupTaskError:
            return {"success": False, "reason": "already_running"}
        except Exception as e:
            logger.error("群 %s 增量最终 JSON 生成失败: %s", group_id, e, exc_info=True)
            return {"success": False, "reason": str(e)}

    async def _run_union_prepare_on_schedule(self) -> None:
        if self._terminating or not self.config_manager.get_union_report_enabled():
            return

        report_date = datetime.now().strftime("%Y-%m-%d")
        result = await self._run_union_prepare_core(report_date)
        if not result.get("success"):
            logger.warning(
                "跨群联合日报准备失败: date=%s reason=%s",
                report_date,
                result.get("reason", "unknown"),
            )

    async def _run_union_report_on_schedule(self) -> None:
        if self._terminating or not self.config_manager.get_union_report_enabled():
            return

        report_date = datetime.now().strftime("%Y-%m-%d")
        await self._run_union_report_core(
            report_date=report_date,
            target_groups_override=None,
            require_all_groups_ready=True,
        )

    async def run_union_prepare_manual(self, report_date: str) -> dict[str, Any]:
        return await self._run_union_prepare_core(report_date)

    async def run_union_report_manual(
        self,
        report_date: str,
        target_groups_override: list[tuple[str, str | None]],
    ) -> dict[str, Any]:
        if not target_groups_override:
            return {"success": False, "reason": "no_targets"}

        prepare_result = await self.run_union_prepare_manual(report_date)
        if not prepare_result.get("success"):
            return prepare_result

        return await self._run_union_report_core(
            report_date=report_date,
            target_groups_override=target_groups_override,
            skip_enabled_check=True,
            bypass_daily_guard=True,
            require_all_groups_ready=True,
        )

    async def _run_union_prepare_core(self, report_date: str) -> dict[str, Any]:
        target_list = await self._get_union_source_targets()
        if not target_list:
            result = {"success": False, "reason": "no_source_groups"}
            self._union_prepare_results[report_date] = result
            return result

        max_concurrent = self.config_manager.get_max_concurrent_tasks()
        sem = asyncio.Semaphore(max_concurrent)

        async def dispatch_group(
            gid: str, pid: str | None
        ) -> dict[str, Any] | None:
            async with sem:
                return await self._perform_incremental_final_report_for_group_with_timeout(
                    gid,
                    pid,
                    report_date,
                )

        tasks = [
            asyncio.create_task(
                dispatch_group(gid, pid),
                name=f"union_prepare_incremental_{gid}",
            )
            for gid, pid, _group_ref in target_list
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        skip_count = 0
        error_count = 0
        group_results: dict[str, dict[str, Any]] = {}
        for index, result in enumerate(results):
            _gid, _pid, group_ref = target_list[index]
            if isinstance(result, Exception):
                error_count += 1
                group_results[group_ref] = {
                    "success": False,
                    "reason": str(result),
                    "status": "error",
                }
            elif isinstance(result, dict) and not result.get("success", True):
                skip_count += 1
                group_results[group_ref] = {
                    "success": False,
                    "reason": result.get("reason", "unknown"),
                    "status": "skipped",
                }
            else:
                success_count += 1
                group_results[group_ref] = {"success": True, "status": "success"}

        terminal_failed_group_refs = sorted(
            [
                group_ref
                for group_ref, item in group_results.items()
                if not item.get("success")
            ]
        )
        ready_group_refs = sorted(
            [
                group_ref
                for group_ref, item in group_results.items()
                if item.get("success")
            ]
        )
        success = success_count == len(target_list)
        result_summary: dict[str, Any] = {
            "success": success,
            "prepared_count": len(target_list),
            "success_count": success_count,
            "skip_count": skip_count,
            "error_count": error_count,
            "group_results": group_results,
            "ready_group_refs": ready_group_refs,
            "terminal_failed_group_refs": terminal_failed_group_refs,
        }
        if not success:
            result_summary["reason"] = "prepare_failed"

        logger.info(
            "跨群联合日报准备完成 - date=%s 成功=%d 跳过=%d 失败=%d 总计=%d",
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
        target_groups_override: list[tuple[str, str | None]] | None,
        skip_enabled_check: bool = False,
        bypass_daily_guard: bool = False,
        require_all_groups_ready: bool = False,
    ) -> dict[str, Any]:
        if not skip_enabled_check and not self.config_manager.get_union_report_enabled():
            return {"success": False, "reason": "disabled"}
        if (
            not self.union_daily_report_service
            or not self.report_generator
            or not self.union_report_renderer
            or not self.html_render_func
        ):
            return {"success": False, "reason": "not_initialized"}

        source_group_refs = await self._normalize_union_group_refs(
            self.config_manager.get_union_groups_list()
        )
        if not source_group_refs:
            return {"success": False, "reason": "no_source_groups"}

        target_groups = target_groups_override or self._normalize_union_send_targets(
            self.config_manager.get_union_target_groups()
        )
        if not target_groups:
            return {"success": False, "reason": "no_targets"}

        if not bypass_daily_guard:
            async with self._union_report_guard:
                if self._last_union_report_date == report_date:
                    return {"success": False, "reason": "already_sent"}
                if report_date in self._union_report_dates_in_progress:
                    return {"success": False, "reason": "already_running"}
                self._union_report_dates_in_progress.add(report_date)

        try:
            prepare_result = self._union_prepare_results.get(report_date)
            if require_all_groups_ready and prepare_result and not prepare_result.get(
                "success", False
            ):
                return {
                    "success": False,
                    "reason": prepare_result.get("reason", "prepare_failed"),
                    "prepare_result": prepare_result,
                }

            if require_all_groups_ready:
                missing_group_refs = await self._wait_for_union_source_reports_ready(
                    source_group_refs,
                    report_date,
                    max(0, self.config_manager.get_union_wait_timeout_minutes()),
                )
                if missing_group_refs:
                    return {
                        "success": False,
                        "reason": "source_reports_not_ready",
                        "missing_groups": missing_group_refs,
                    }

            if self._terminating:
                return {"success": False, "reason": "terminating"}

            TraceContext.set(
                TraceContext.generate(prefix="union", group_name=f"union-{report_date}")
            )
            report = await self.union_daily_report_service.build_union_report(
                source_group_refs,
                report_date,
            )
            if report is None:
                reason = getattr(
                    self.union_daily_report_service,
                    "last_failure_reason",
                    None,
                )
                return {"success": False, "reason": reason or "no_report_data"}

            await self._enrich_union_report_names(report)
            sent_count = await self._dispatch_union_report(report, target_groups)
            if sent_count > 0:
                if not bypass_daily_guard:
                    async with self._union_report_guard:
                        self._last_union_report_date = report_date
                return {"success": True, "sent_count": sent_count, "report": report}

            return {"success": False, "reason": "dispatch_failed", "report": report}
        finally:
            if not bypass_daily_guard:
                async with self._union_report_guard:
                    self._union_report_dates_in_progress.discard(report_date)

    async def _dispatch_union_report(
        self,
        report,
        target_groups: list[tuple[str, str | None]],
    ) -> int:
        """固定以图片形式发送联合日报；发送失败则本次任务失败。"""
        if not target_groups or not self.html_render_func:
            return 0

        html_content = self.union_report_renderer.render_html(report)
        image_url = await self.report_generator.render_html_content_to_image(
            html_content,
            f"union_{report.report_date}",
            self.html_render_func,
        )
        if not image_url:
            logger.warning("跨群联合日报图片渲染失败")
            return 0

        caption = f"📊 A海岸联合日报 ({report.report_date})"
        sent_count = 0
        for group_id, platform_id in target_groups:
            resolved_platform_id = platform_id or await self.get_platform_id_for_group(
                group_id
            )
            if not resolved_platform_id:
                logger.warning("目标群 %s 无法解析平台，跳过发送", group_id)
                continue
            if await self.message_sender.send_image_smart(
                group_id,
                image_url,
                caption,
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

        if report.champion_group.group_ref in name_map:
            report.champion_group.group_name = name_map[report.champion_group.group_ref]

        for quote in report.top_quotes:
            if quote.group_ref in name_map:
                quote.group_name = name_map[quote.group_ref]
        for topic in report.topic_highlights:
            if topic.group_ref in name_map:
                topic.group_name = name_map[topic.group_ref]
        if report.water_king and report.water_king.group_ref in name_map:
            report.water_king.group_name = name_map[report.water_king.group_ref]
        for user in report.runner_up_users:
            if user.group_ref in name_map:
                user.group_name = name_map[user.group_ref]

    @staticmethod
    def _parse_union_group_ref(group_ref: str) -> tuple[str | None, str]:
        text = str(group_ref).strip()
        if not text:
            return None, ""
        if ":" not in text:
            return None, text
        parts = text.split(":")
        return parts[0].strip() or None, parts[-1].strip()

    async def _normalize_union_group_refs(self, group_refs: list[str]) -> list[str]:
        normalized_refs: list[str] = []
        seen: set[str] = set()
        for group_ref in group_refs:
            platform_id, group_id = self._parse_union_group_ref(group_ref)
            if group_id and platform_id is None:
                platform_id = await self.get_platform_id_for_group(group_id)
                if platform_id is None:
                    logger.warning("源群 %s 未显式配置平台且无法自动解析，已跳过", group_id)
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
    ) -> list[tuple[str, str | None, str]]:
        source_group_refs = await self._normalize_union_group_refs(
            self.config_manager.get_union_groups_list()
        )
        targets: list[tuple[str, str | None, str]] = []
        for group_ref in source_group_refs:
            if not self.config_manager.is_group_allowed(group_ref):
                logger.info("源群 %s 未通过基础群准入，跳过", group_ref)
                continue
            platform_id, group_id = self._parse_union_group_ref(group_ref)
            if group_id:
                targets.append((group_id, platform_id, group_ref))
        return targets

    async def _get_incremental_runtime_targets(
        self,
    ) -> list[tuple[str, str | None, str]]:
        targets = await self._get_union_source_targets()
        logger.info("后台增量提取目标解析完成：共 %d 个群", len(targets))
        return targets

    async def _wait_for_union_source_reports_ready(
        self,
        source_group_refs: list[str],
        report_date: str,
        wait_timeout_minutes: int,
    ) -> list[str]:
        missing_group_refs = (
            self.union_daily_report_service.get_missing_group_refs_for_date(
                source_group_refs,
                report_date,
            )
        )
        if not missing_group_refs:
            return []

        timeout_seconds = wait_timeout_minutes * 60
        if timeout_seconds <= 0:
            return missing_group_refs

        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while missing_group_refs and not self._terminating:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(30, remaining))
            missing_group_refs = (
                self.union_daily_report_service.get_missing_group_refs_for_date(
                    source_group_refs,
                    report_date,
                )
            )
        return missing_group_refs
