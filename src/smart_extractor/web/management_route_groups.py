"""兼容层：保留旧导入路径，转发到拆分后的管理路由模块。"""

from smart_extractor.web.management_config_routes import register_config_routes
from smart_extractor.web.management_learned_profile_routes import (
    register_learned_profile_routes,
)
from smart_extractor.web.management_monitor_routes import register_monitor_routes
from smart_extractor.web.management_task_routes import register_task_routes
from smart_extractor.web.management_template_routes import register_template_routes

__all__ = [
    "register_config_routes",
    "register_learned_profile_routes",
    "register_monitor_routes",
    "register_task_routes",
    "register_template_routes",
]
