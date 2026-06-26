"""
单群日报渲染器。
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup


class SingleReportRenderer:
    """负责渲染单群日报 HTML。"""

    def __init__(self, report_generator: Any):
        self.report_generator = report_generator
        template_root = Path(__file__).resolve().parent / "templates"
        self.template_dir = template_root / "single"
        self.theme_path = template_root / "shared" / "report_theme.css"
        self._env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._env.filters["render_single_markdown"] = self._render_single_markdown

    def render_html(self, report: Any) -> str:
        template = self._env.get_template("single_template.html")
        return template.render(report=report, theme_css=self._load_theme_css())

    def _load_theme_css(self) -> str:
        return self.theme_path.read_text(encoding="utf-8")

    @staticmethod
    def _render_single_markdown(text: Any) -> Markup:
        raw = str(text or "")
        escaped = html.escape(raw, quote=False)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)
        escaped = escaped.replace("\n", "<br>")
        return Markup(escaped)
