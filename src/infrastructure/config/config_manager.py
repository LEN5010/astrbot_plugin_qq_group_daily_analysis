"""
配置管理模块 - 基础设施层
负责处理插件配置
"""

from astrbot.api import AstrBotConfig


class ConfigManager:
    """配置管理器

    配置结构采用分组嵌套方式，顶层分为以下分组：
    - basic: 基础设置
    - llm: LLM 设置
    - analysis_features: 分析与人格设置
    - incremental: 增量分析设置
    - union_report: 跨群联合日报设置
    - prompts: 提示词模板
    - performance: 并发限流设置
    """

    def __init__(self, config: AstrBotConfig):
        self.config = config

    def _get_group(self, group: str) -> dict:
        """获取指定分组的配置字典，不存在时返回空字典"""
        return self.config.get(group, {})

    def get_group_list_mode(self) -> str:
        """获取群组列表模式 (whitelist/blacklist/none)"""
        return self._get_group("basic").get("group_list_mode", "none")

    def get_group_list(self) -> list[str]:
        """获取群组列表（用于黑白名单）"""
        return self._get_group("basic").get("group_list", [])

    def is_group_allowed(self, group_id_or_umo: str) -> bool:
        """
        根据配置的白/黑名单判断是否允许在该群聊中使用
        支持传入 simple group_id 或 UMO (Unified Message Origin)
        """
        mode = self.get_group_list_mode().lower()
        if mode not in ("whitelist", "blacklist", "none"):
            mode = "none"

        if mode == "none":
            return True

        glist = [str(g).strip() for g in self.get_group_list()]
        target = str(group_id_or_umo).strip()

        is_in_list = any(self._is_group_match(target, item) for item in glist)

        if mode == "whitelist":
            return is_in_list
        if mode == "blacklist":
            return not is_in_list

        return True

    def _is_group_match(self, target: str, item: str) -> bool:
        """
        核心匹配逻辑：判断名单中的 item 是否匹配目标的 target (Unified Message Origin, UMO 或 纯 ID)。
        支持处理 Telegram 话题 (#) 和 独立隔离会话 (_) 的双向穿透匹配。
        """
        if item == target:
            return True

        # 分解目标 UMO 的前缀和 ID 部分 (如 default:GroupMessage:ID)
        if ":" in target:
            target_prefix, target_id = target.rsplit(":", 1)
        else:
            target_prefix, target_id = "", target

        # 生成目标 ID 的所有“穿透”候选 (处理隔离模式和话题)
        candidates = {target_id}
        if "#" in target_id:
            candidates.add(target_id.split("#", 1)[0])
        if "_" in target_id:
            for part in target_id.split("_"):
                candidates.add(part)

        # 检查名单项 (item) 的格式
        if ":" in item:
            i_prefix, i_id = item.rsplit(":", 1)
            # 名单项带前缀时，前缀必须匹配 (如果 target 本身没前缀，则允许作为跨平台通用 ID 匹配)
            if target_prefix and i_prefix != target_prefix:
                return False
        else:
            i_id = item

        # [修复] 名单项 ID 也可能包含复合形式 (如 UserId_GroupId)，需要拆解匹配
        item_variants = {i_id}
        if "#" in i_id:
            item_variants.add(i_id.split("#", 1)[0])
        if "_" in i_id:
            for part in i_id.split("_"):
                item_variants.add(part)

        # 只要两边的 ID “核心部分”存在交集，即视为匹配成功
        return not item_variants.isdisjoint(candidates)

    def get_analysis_days(self) -> int:
        """获取分析天数"""
        return self._get_group("basic").get("analysis_days", 1)

    def get_topic_analysis_enabled(self) -> bool:
        """增量链路固定启用话题分析。"""
        return True

    def get_golden_quote_analysis_enabled(self) -> bool:
        """增量链路固定启用金句分析。"""
        return True

    def get_max_topics(self) -> int:
        """获取最大话题数量"""
        return self._get_group("analysis_features").get("max_topics", 5)

    def get_max_golden_quotes(self) -> int:
        """获取最大金句数量"""
        return self._get_group("analysis_features").get("max_golden_quotes", 5)

    def get_llm_retries(self) -> int:
        """获取LLM请求重试次数"""
        return self._get_group("llm").get("llm_retries", 2)

    def get_llm_backoff(self) -> int:
        """获取LLM请求重试退避基值（秒），实际退避会乘以尝试次数"""
        return self._get_group("llm").get("llm_backoff", 2)

    def get_debug_mode(self) -> bool:
        """获取是否启用调试模式"""
        return self._get_group("basic").get("debug_mode", False)

    def get_llm_provider_id(self) -> str:
        """获取主 LLM Provider ID"""
        return self._get_group("llm").get("llm_provider_id", "")

    def get_topic_provider_id(self) -> str:
        """获取话题分析专用 Provider ID"""
        return self._get_group("llm").get("topic_provider_id", "")

    def get_golden_quote_provider_id(self) -> str:
        """获取金句分析专用 Provider ID"""
        return self._get_group("llm").get("golden_quote_provider_id", "")

    def get_union_report_provider_id(self) -> str:
        """获取跨群聚合日报专用 Provider ID"""
        return self._get_group("llm").get("union_report_provider_id", "")

    def get_keep_original_persona(self) -> bool:
        """获取是否继承会话原始人格设定"""
        return self._get_group("analysis_features").get("keep_original_persona", True)

    def get_use_plugin_specific_persona(self) -> bool:
        """获取是否强制使用插件指定的人格设定"""
        return self._get_group("analysis_features").get(
            "use_plugin_specific_persona", False
        )

    def get_plugin_specific_persona_id(self) -> str:
        """获取插件指定的全局人格 ID (通过 select_persona 接口选择)"""
        return self._get_group("analysis_features").get(
            "plugin_specific_persona_id", ""
        )

    def get_bot_self_ids(self) -> list:
        """获取机器人自身的 ID 列表。"""
        return self._get_group("basic").get("bot_self_ids", [])

    def get_topic_analysis_prompt(self, style: str = "topic_prompt") -> str:
        """获取话题分析提示词模板"""
        prompts_config = self._get_group("prompts").get("topic_analysis_prompts", {})
        prompt = prompts_config.get(style, "")
        if prompt:
            return prompt
        return ""

    def get_golden_quote_analysis_prompt(
        self, style: str = "golden_quote_v2_prompt"
    ) -> str:
        """获取金句分析提示词模板"""
        prompts_config = self._get_group("prompts").get(
            "golden_quote_analysis_prompts", {}
        )
        prompt = prompts_config.get(style, "")
        if prompt:
            return prompt
        return ""

    def get_union_daily_analysis_prompt(
        self, style: str = "union_daily_report_prompt"
    ) -> str:
        """获取跨群聚合日报提示词模板"""
        prompts_config = self._get_group("prompts").get(
            "union_daily_report_prompts", {}
        )
        prompt = prompts_config.get(style, "")
        if prompt:
            return prompt
        return ""

    def get_persona_comment_prompt(self, style: str = "persona_comment_prompt") -> str:
        """获取联合日报人格点评提示词模板"""
        prompts_config = self._get_group("prompts").get(
            "union_daily_report_prompts", {}
        )
        prompt = prompts_config.get(style, "")
        if prompt:
            return prompt
        return ""

    def get_max_concurrent_tasks(self) -> int:
        """获取自动分析最大并发群数"""
        return self._get_group("performance").get("max_concurrent_groups", 1)

    def get_llm_max_concurrent(self) -> int:
        """获取全局 LLM 最大并发请求数"""
        return self._get_group("performance").get("max_concurrent_llm", 1)

    def get_t2i_max_concurrent(self) -> int:
        """获取全局图片渲染（T2I）最大并发数"""
        return self._get_group("performance").get("max_concurrent_t2i", 1)

    # ========== 增量分析配置 ==========

    def get_incremental_interval_minutes(self) -> int:
        """获取增量分析间隔（分钟）"""
        return int(self._get_group("incremental").get("interval_minutes", 120))

    def get_incremental_max_daily_analyses(self) -> int:
        """获取每天最大增量分析次数"""
        return int(self._get_group("incremental").get("max_daily_analyses", 8))

    def get_incremental_safe_limit(self) -> int:
        """获取单次增量分析的安全分析/同步上限 (Safe Count)"""
        return int(self._get_group("incremental").get("safe_limit", 2000))

    def get_incremental_min_messages(self) -> int:
        """获取触发增量分析的最小消息数阈值"""
        return int(self._get_group("incremental").get("min_messages", 300))

    def get_incremental_topics_per_batch(self) -> int:
        """获取单次增量分析提取的最大话题数"""
        return int(self._get_group("incremental").get("topics_per_batch", 2))

    def get_incremental_quotes_per_batch(self) -> int:
        """获取单次增量分析提取的最大金句数"""
        return int(self._get_group("incremental").get("quotes_per_batch", 2))

    def get_incremental_active_start_hour(self) -> int:
        """获取增量分析活跃时段起始小时（24小时制）"""
        return int(self._get_group("incremental").get("active_start_hour", 8))

    def get_incremental_active_end_hour(self) -> int:
        """获取增量分析活跃时段结束小时（24小时制）"""
        return int(self._get_group("incremental").get("active_end_hour", 23))

    def get_incremental_stagger_seconds(self) -> int:
        """获取多群增量分析的交错间隔（秒），避免 API 压力"""
        return int(self._get_group("incremental").get("stagger_seconds", 30))

    # ========== 跨群聚合日报配置 ==========

    def get_union_report_enabled(self) -> bool:
        """获取是否启用跨群聚合日报。"""
        return bool(self._get_group("union_report").get("enabled", False))

    def get_union_groups_list(self) -> list[str]:
        """获取跨群聚合的源群列表。"""
        return self._get_group("union_report").get("union_groups_list", [])

    def get_union_target_groups(self) -> list[str]:
        """获取跨群日报的发送目标列表。为空时不发送。"""
        return self._get_group("union_report").get("union_target_groups", [])

    def get_union_report_time(self) -> str:
        """获取跨群日报固定发送时间。"""
        return str(self._get_group("union_report").get("union_report_time", "")).strip()

    def get_union_prepare_lead_minutes(self) -> int:
        """获取固定时间模式下，提前生成源群最终 JSON 的分钟数。"""
        return int(
            self._get_group("union_report").get("union_prepare_lead_minutes", 20)
        )

    def get_union_wait_timeout_minutes(self) -> int:
        """获取固定时间模式下，等待所有源群日报就绪的超时时间。"""
        return int(
            self._get_group("union_report").get("union_wait_timeout_minutes", 20)
        )
