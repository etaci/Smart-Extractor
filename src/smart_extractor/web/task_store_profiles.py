"""兼容层：保留旧导出，内部已拆分为模板与监控模块。"""

from smart_extractor.web.task_store_monitors import (
    fetch_monitor,
    fetch_monitors,
    persist_monitor_notification,
    persist_monitor_result,
    upsert_monitor,
)
from smart_extractor.web.task_store_templates import (
    fetch_template,
    fetch_templates,
    touch_template,
    upsert_template,
)

__all__ = [
    "fetch_monitor",
    "fetch_monitors",
    "fetch_template",
    "fetch_templates",
    "persist_monitor_notification",
    "persist_monitor_result",
    "touch_template",
    "upsert_monitor",
    "upsert_template",
]
