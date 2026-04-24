"""
日志系统模块

基于 Loguru 实现结构化彩色日志，支持文件轮转和自动清理。
"""

import sys
from pathlib import Path

from loguru import logger

from smart_extractor.config import LogConfig, PROJECT_ROOT


def setup_logger(config: LogConfig | None = None) -> None:
    """
    初始化日志系统。

    Args:
        config: 日志配置。如果为 None，使用默认配置。
    """
    if config is None:
        config = LogConfig()

    # 移除默认的 handler
    logger.remove()
    logger.configure(extra={"request_id": "-", "task_id": "-"})

    # 添加控制台输出（彩色）
    logger.add(
        sys.stderr,
        level=config.level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
               "req=<yellow>{extra[request_id]}</yellow> | "
               "task=<magenta>{extra[task_id]}</magenta> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        colorize=True,
    )

    # 添加文件输出（按大小轮转）
    log_dir = PROJECT_ROOT / config.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_dir / "smart_extractor_{time:YYYY-MM-DD}.log"),
        level=config.level,
        format=config.format,
        rotation=config.rotation,
        retention=config.retention,
        encoding="utf-8",
        enqueue=True,  # 线程安全
    )

    # 添加错误日志单独文件
    logger.add(
        str(log_dir / "errors_{time:YYYY-MM-DD}.log"),
        level="ERROR",
        format=config.format,
        rotation=config.rotation,
        retention=config.retention,
        encoding="utf-8",
        enqueue=True,
    )

    logger.info("日志系统初始化完成，日志目录: {}", log_dir)


def get_logger(name: str = "smart_extractor"):
    """获取带模块名的 logger 实例"""
    return logger.bind(name=name)
