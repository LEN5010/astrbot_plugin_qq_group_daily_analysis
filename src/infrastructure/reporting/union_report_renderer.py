"""
跨群聚合日报渲染器
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


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

    def render_html(self, report: Any) -> str:
        template = self._env.get_template("union_template.html")
        return template.render(report=report)

    def render_text(self, report: Any) -> str:
        champion = report.champion_group
        lines = [
            "📊 五群联合日报",
            f"📅 {report.report_date}",
            "",
            (
                f"🏆 今日冠军群：{champion.group_name} ({champion.group_id})"
                f"{' @ ' + champion.platform_id if champion.platform_id else ''}"
                f" · {champion.total_messages} 条消息"
            ),
            f"📈 累计消息：{report.total_messages}",
            f"👥 累计参与人数：{report.total_participants}",
            "",
            "💬 跨群 Top 3 金句",
        ]

        for index, quote in enumerate(report.top_quotes, 1):
            lines.append(
                f"{index}. [{quote.group_name}"
                f"{' @ ' + quote.platform_id if quote.platform_id else ''}] "
                f"{quote.sender}: {quote.content}"
            )

        lines.extend(["", "📝 全局点评", report.overview])
        return "\n".join(lines)
