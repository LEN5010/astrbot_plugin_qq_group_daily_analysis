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
    """负责渲染跨群聚合日报 HTML / 文本。"""

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

    def render_text(self, report: Any) -> str:
        champion = report.champion_group
        lines = [
            "📊 A海岸联合日报",
            f"📅 {report.report_date}",
            "",
            f"🏆 今日 A海岸最活跃群：{champion.group_name} · {champion.total_messages} 条消息",
            f"📈 今日 A海岸累计消息：{report.total_messages}",
            f"👥 今日 A海岸累计参与人数：{report.total_participants}",
        ]

        if report.water_king:
            lines.extend(
                [
                    (
                        f"💦 全A海岸最能水：{report.water_king.nickname}"
                        f"（ID: {report.water_king.user_id}）"
                        f" · {report.water_king.message_count} 条"
                        f" · 来自 {report.water_king.group_name}"
                    ),
                ]
            )

        lines.extend(
            [
            "",
            "💬 今日 A海岸 Top 3 金句",
        ])

        for index, quote in enumerate(report.top_quotes, 1):
            lines.append(
                f"{index}. [{quote.group_name}] "
                f"{quote.sender}: {quote.content}"
            )

        lines.extend(["", "📝 今日 A海岸点评", report.overview])
        return "\n".join(lines)

    @staticmethod
    def _render_union_markdown(text: Any) -> Markup:
        raw = str(text or "")
        escaped = html.escape(raw, quote=False)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)
        escaped = escaped.replace("\n", "<br>")
        return Markup(escaped)
