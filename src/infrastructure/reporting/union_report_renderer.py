"""
跨群聚合日报渲染器
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup


class UnionReportRenderer:
    """负责渲染跨群聚合日报 HTML。"""

    def __init__(self, report_generator: Any):
        self.report_generator = report_generator
        self.template_dir = Path(__file__).resolve().parent / "templates" / "union"
        self._env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._env.filters["render_union_markdown"] = self._render_union_markdown

    def render_html(self, report: Any) -> str:
        template = self._env.get_template("union_template.html")
        return template.render(report=report)

    @staticmethod
    def _render_union_markdown(text: Any) -> Markup:
        raw = str(text or "")
        escaped = html.escape(raw, quote=False)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)
        escaped = escaped.replace("\n", "<br>")
        return Markup(escaped)
