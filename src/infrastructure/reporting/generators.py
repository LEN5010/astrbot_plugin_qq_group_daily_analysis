"""
联合日报图片渲染器。

当前插件只保留跨群联合日报链路，因此这里只负责将联合日报 HTML
渲染为图片，不再生成其它报告格式或处理群文件上传。
"""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from typing import Any

from ...utils.logger import logger


class ReportGenerator:
    """将已经渲染好的 HTML 转换为图片。"""

    def __init__(self, config_manager: Any, data_dir: str | Path):
        self.config_manager = config_manager
        self.data_dir = Path(data_dir)
        max_concurrent = self.config_manager.get_t2i_max_concurrent()
        self._render_semaphore = asyncio.Semaphore(max(1, int(max_concurrent)))

    async def render_html_content_to_image(
        self,
        html_content: str,
        group_id: str,
        html_render_func,
    ) -> str | None:
        """将联合日报 HTML 渲染为图片；渲染失败直接返回 None。"""
        if not html_content or not html_content.strip():
            logger.error("render_html_content_to_image 收到空 HTML 内容")
            return None

        image_options = {
            "full_page": True,
            "type": "png",
            "scale": "device",
            "device_scale_factor_level": "ultra",
        }

        async with self._render_semaphore:
            try:
                image_data = await html_render_func(
                    html_content,
                    {},
                    False,
                    image_options,
                )
            except Exception as e:
                logger.error("联合日报图片渲染调用失败: %s", e)
                return None

        if not image_data:
            logger.error("联合日报图片渲染返回空结果")
            return None

        if isinstance(image_data, bytes):
            if not self._is_valid_image_head(image_data[:10]):
                logger.error("联合日报图片渲染返回了无效图片字节")
                return None
            b64 = base64.b64encode(image_data).decode("utf-8")
            return f"base64://{b64}"

        if isinstance(image_data, str):
            image_path = Path(image_data)
            if not image_path.exists():
                logger.error("联合日报图片渲染返回了不存在的图片路径: %s", image_data)
                return None

            try:
                data_head = image_path.read_bytes()[:10]
            except Exception as e:
                logger.error("读取联合日报图片临时文件失败: %s", e)
                return None

            if not self._is_valid_image_head(data_head):
                logger.error("联合日报图片临时文件不是有效图片: %s", image_data)
                return None
            return image_data

        logger.error("联合日报图片渲染返回了不支持的数据类型: %s", type(image_data))
        return None

    @staticmethod
    def _is_valid_image_head(data_head: bytes) -> bool:
        return bool(
            data_head
            and (
                data_head.startswith(b"\xff\xd8")
                or data_head.startswith(b"\x89PNG")
            )
        )

    async def close(self):
        """保留生命周期钩子，当前无资源需要释放。"""
        return None
