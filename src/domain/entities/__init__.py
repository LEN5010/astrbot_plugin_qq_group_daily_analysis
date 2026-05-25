"""
领域实体

当前链路只主动使用增量批次与增量聚合视图。
"""

from .incremental_state import IncrementalBatch, IncrementalState

__all__ = [
    "IncrementalBatch",
    "IncrementalState",
]
