"""
基础分析器抽象类。

当前插件只保留增量跨群日报链路。源群话题/金句分析必须依赖结构化输出；
Provider 调用失败、空响应、无效 JSON 或字段校验失败都视为本批次失败。
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Sized
from typing import Generic, TypeVar

from ....domain.models.data_models import TokenUsage
from ....utils.logger import logger
from ..utils.llm_utils import (
    call_provider_with_retry,
    extract_response_text,
    extract_token_usage,
)
from ..utils.structured_output_schema import JSONObject, build_response_format

TDataObject = TypeVar("TDataObject")
TInputData = TypeVar("TInputData")


class LLMAnalysisError(RuntimeError):
    """LLM 分析链路失败。"""


class LLMResponseParseError(LLMAnalysisError):
    """LLM 响应不是预期的严格 JSON 结构。"""


class BaseAnalyzer(ABC, Generic[TDataObject, TInputData]):
    """话题/金句分析器的公共流程。"""

    def __init__(self, context, config_manager):
        self.context = context
        self.config_manager = config_manager
        self._incremental_max_count: int | None = None

    def get_provider_id_key(self) -> str | None:
        return None

    @abstractmethod
    def get_data_type(self) -> str:
        pass

    @abstractmethod
    def get_max_count(self) -> int:
        pass

    @abstractmethod
    def build_prompt(self, data: TInputData) -> str:
        pass

    @abstractmethod
    def create_data_objects(self, data_list: list[dict]) -> list[TDataObject]:
        pass

    def get_response_schema_name(self) -> str:
        return f"{self.get_data_type()}_output"

    def get_response_schema(self) -> JSONObject | None:
        return None

    def get_response_format(self) -> JSONObject | None:
        schema = self.get_response_schema()
        if not schema:
            return None
        return build_response_format(self.get_response_schema_name(), schema)

    def validate_parsed_data(
        self, data_list: list[dict]
    ) -> tuple[bool, list[dict] | None, str | None]:
        return True, data_list, None

    def _parse_strict_json_array(self, result_text: str) -> list[dict]:
        try:
            parsed = json.loads(result_text)
        except json.JSONDecodeError as e:
            raise LLMResponseParseError(f"{self.get_data_type()}响应不是有效 JSON: {e}") from e

        if not isinstance(parsed, list):
            raise LLMResponseParseError(
                f"{self.get_data_type()}响应必须是 JSON 数组，实际为 {type(parsed).__name__}"
            )

        if any(not isinstance(item, dict) for item in parsed):
            raise LLMResponseParseError(f"{self.get_data_type()}响应数组元素必须全部为对象")

        success, validated_data, error_msg = self.validate_parsed_data(parsed)
        if not success or validated_data is None:
            raise LLMResponseParseError(
                f"{self.get_data_type()}响应字段校验失败: {error_msg or 'unknown_error'}"
            )

        return validated_data

    def _save_debug_data(self, prompt: str, session_id: str):
        try:
            from pathlib import Path

            from astrbot.api.star import StarTools
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path

            try:
                data_path = StarTools.get_data_dir() / "debug_data"
            except Exception:
                data_path = (
                    Path(get_astrbot_data_path())
                    / "plugin_data"
                    / "astrbot_plugin_qq_group_daily_analysis"
                    / "debug_data"
                )

            data_path.mkdir(parents=True, exist_ok=True)
            file_path = data_path / f"{session_id}_{self.get_data_type()}.txt"

            logger.info(f"正在保存调试数据到: {file_path}")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(prompt)
            logger.info(f"已保存 {self.get_data_type()} 分析 Prompt 到 {file_path}")
        except Exception as e:
            logger.error(f"保存调试数据失败: {e}", exc_info=True)

    def _apply_persona_reinforcement(
        self, prompt: str, system_prompt: str | None
    ) -> str:
        if not system_prompt or not system_prompt.strip():
            return prompt

        persona_content = system_prompt.strip()
        logger.info(f"[{self.get_data_type()}分析] 已启用人格设定")
        return (
            "【SYSTEM_CORE_IDENTITY_FIXED】\n"
            f"你现在的身份已由系统初始化为：\n{persona_content}\n\n"
            "--- MISSION_DIRECTIVE_START ---\n"
            f"请以该人格的观察方式处理以下“{self.get_data_type()}”分析任务。\n"
            "最终输出必须严格遵守任务要求的纯 JSON 格式，不得添加 Markdown 或解释。\n\n"
            f"{prompt}\n"
            "--- MISSION_DIRECTIVE_END ---"
        )

    async def analyze(
        self, data: TInputData, umo: str | None = None, session_id: str | None = None
    ) -> tuple[list[TDataObject], TokenUsage]:
        data_length = len(data) if isinstance(data, Sized) else "N/A"
        logger.debug(f"{self.get_data_type()}分析输入数据长度: {data_length}")

        prompt = self.build_prompt(data)
        if not prompt or not prompt.strip():
            logger.info(f"{self.get_data_type()}分析无有效输入，返回空结果")
            return [], TokenUsage()

        debug_mode = self.config_manager.get_debug_mode()
        if debug_mode and session_id:
            self._save_debug_data(prompt, session_id)
        elif debug_mode:
            logger.warning("[Debug] Debug mode enabled but no session_id provided")

        provider_id_key = self.get_provider_id_key()
        system_prompt = await self._build_system_prompt(umo)
        prompt = self._apply_persona_reinforcement(prompt, system_prompt)

        response = await call_provider_with_retry(
            self.context,
            self.config_manager,
            prompt=prompt,
            umo=umo,
            provider_id_key=provider_id_key,
            system_prompt=system_prompt,
            response_format=self.get_response_format(),
        )
        if response is None:
            raise LLMAnalysisError(f"{self.get_data_type()}分析调用 LLM 失败")

        token_usage_dict = extract_token_usage(response)
        token_usage = TokenUsage(
            prompt_tokens=token_usage_dict["prompt_tokens"],
            completion_tokens=token_usage_dict["completion_tokens"],
            total_tokens=token_usage_dict["total_tokens"],
        )

        result_text = extract_response_text(response)
        if not result_text or not result_text.strip():
            raise LLMResponseParseError(f"{self.get_data_type()}分析返回空响应")

        parsed_data = self._parse_strict_json_array(result_text.strip())
        data_objects = self.create_data_objects(parsed_data)
        logger.info(f"{self.get_data_type()}分析成功，解析到 {len(data_objects)} 条数据")
        return data_objects, token_usage

    async def _build_system_prompt(self, umo: str | None) -> str | None:
        """
        构建带有会话人格的系统提示词，优先级：
        1. 插件指定人格
        2. 源群会话人格
        3. 源群默认人格
        """
        use_specific = self.config_manager.get_use_plugin_specific_persona()
        specific_id = self.config_manager.get_plugin_specific_persona_id()
        keep_original = self.config_manager.get_keep_original_persona()

        persona_mgr = getattr(self.context, "persona_manager", None)
        if persona_mgr is None:
            return None

        persona_prompt = None

        if use_specific and specific_id:
            try:
                persona_obj = await persona_mgr.get_persona(specific_id)
                persona_prompt = getattr(persona_obj, "system_prompt", None)
                if persona_prompt:
                    logger.debug(f"已应用插件指定人格: {specific_id}")
            except Exception as e:
                logger.warning(f"获取插件指定人格失败 (ID: {specific_id}): {e}")

        if not persona_prompt and keep_original and umo:
            try:
                from astrbot.api import sp

                session_service_config = await sp.get_async(
                    scope="umo",
                    scope_id=str(umo),
                    key="session_service_config",
                    default={},
                )
                persona_id = (
                    session_service_config.get("persona_id")
                    if session_service_config
                    else None
                )
                if persona_id and persona_id != "[%None]":
                    persona_obj = await persona_mgr.get_persona(persona_id)
                    persona_prompt = getattr(persona_obj, "system_prompt", None)
                    if persona_prompt:
                        logger.debug(f"继承到会话选定人格: {persona_id}")

                if not persona_prompt:
                    conv_mgr = getattr(self.context, "conversation_manager", None)
                    if conv_mgr:
                        curr_conv_id = await conv_mgr.get_curr_conversation_id(umo)
                        if curr_conv_id:
                            conv_obj = await conv_mgr.get_conversation(
                                umo, curr_conv_id
                            )
                            if (
                                conv_obj
                                and getattr(conv_obj, "persona_id", None)
                                and conv_obj.persona_id != "[%None]"
                            ):
                                persona_obj = await persona_mgr.get_persona(
                                    conv_obj.persona_id
                                )
                                persona_prompt = getattr(
                                    persona_obj, "system_prompt", None
                                )
                                if persona_prompt:
                                    logger.debug(
                                        f"继承到对话设定人格: {conv_obj.persona_id}"
                                    )

                if not persona_prompt:
                    personality = await persona_mgr.get_default_persona_v3(umo)
                    if isinstance(personality, dict):
                        persona_prompt = personality.get("prompt")
                    else:
                        persona_prompt = getattr(personality, "prompt", None)
                    if persona_prompt:
                        logger.debug("继承到 UMO 默认人格设定")
            except Exception as e:
                logger.warning(f"分析人格识别失败 (umo: {umo}): {e}")

        if not isinstance(persona_prompt, str) or not persona_prompt.strip():
            return None
        return persona_prompt.strip()
