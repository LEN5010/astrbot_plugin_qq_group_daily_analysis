"""联合日报图片归档，供群成员按日期回看。"""

from __future__ import annotations

import asyncio
import base64
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from ...utils.logger import logger


class ReportArchive:
    """持久化并查询已经生成的联合日报图片。"""

    def __init__(self, data_dir: str | Path):
        self.archive_dir = Path(data_dir) / "report_archive"

    async def save(
        self,
        image_source: str,
        report_date: str,
    ) -> Path | None:
        """保存联合日报图片副本；失败时记录日志并返回 ``None``。"""
        try:
            destination = self._path_for(report_date)
            await asyncio.to_thread(
                self._save_sync,
                image_source,
                destination,
            )
            return destination
        except Exception as exc:
            logger.error(
                "保存联合日报图片副本失败: date=%s error=%s",
                report_date,
                exc,
            )
            return None

    async def find(self, report_date: str) -> Path | None:
        """查找指定日期的联合日报。"""
        try:
            candidate = self._path_for(report_date)
        except ValueError:
            return None
        exists = await asyncio.to_thread(candidate.is_file)
        return candidate if exists else None

    def _path_for(self, report_date: str) -> Path:
        normalized_date = datetime.strptime(report_date, "%Y-%m-%d").strftime(
            "%Y-%m-%d"
        )
        return self.archive_dir / f"{normalized_date}.png"

    @staticmethod
    def _save_sync(image_source: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.tmp"
        )

        try:
            if image_source.startswith("base64://"):
                image_data = base64.b64decode(
                    image_source.removeprefix("base64://"),
                    validate=True,
                )
                temp_path.write_bytes(image_data)
            else:
                source_path = Path(image_source.removeprefix("file://"))
                if not source_path.is_file():
                    raise FileNotFoundError(f"日报图片不存在: {source_path}")
                shutil.copyfile(source_path, temp_path)

            image_head = temp_path.read_bytes()[:10]
            if not (
                image_head.startswith(b"\x89PNG")
                or image_head.startswith(b"\xff\xd8")
            ):
                raise ValueError("日报图片格式无效")

            temp_path.replace(destination)
        finally:
            if temp_path.exists():
                temp_path.unlink()
