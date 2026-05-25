"""
报告生成接口 - 领域层
定义分析报告生成的抽象契约
"""

from abc import ABC, abstractmethod
from typing import Any


class IReportGenerator(ABC):
    """
    联合日报图片渲染接口
    """

    @abstractmethod
    async def render_html_content_to_image(
        self,
        html_content: str,
        group_id: str,
        html_render_func: Any,
    ) -> str | None:
        """将联合日报 HTML 渲染为图片"""
        pass

    @abstractmethod
    async def close(self):
        """释放资源"""
        pass
