"""
重试策略模块

封装 Tenacity 重试策略，针对不同错误类型定义差异化重试行为。
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_random,
    retry_if_exception_type,
    before_sleep_log,
    after_log,
)
from loguru import logger
import logging

# 创建标准 logging 适配器（Tenacity 需要标准 logging）
_std_logger = logging.getLogger("smart_extractor.retry")


def create_api_retry(max_retries: int = 3):
    """
    创建 LLM API 调用的重试装饰器。

    策略：指数退避 + 随机抖动，适用于 API 限流/网络超时等临时错误。

    Args:
        max_retries: 最大重试次数
    """
    return retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=30) + wait_random(0, 2),
        before_sleep=before_sleep_log(_std_logger, logging.WARNING),
        reraise=True,
    )


def create_fetch_retry(max_retries: int = 3):
    """
    创建网页抓取的重试装饰器。

    策略：固定间隔 + 随机抖动，避免高频请求触发反爬。

    Args:
        max_retries: 最大重试次数
    """
    return retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=2, min=3, max=60) + wait_random(1, 5),
        before_sleep=before_sleep_log(_std_logger, logging.WARNING),
        reraise=True,
    )


def retry_with_fallback(func, fallback_func, *args, **kwargs):
    """
    带降级回退的重试：主函数失败后尝试fallback函数。

    Args:
        func: 主函数
        fallback_func: 降级回退函数
        *args, **kwargs: 传给两个函数的参数
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.warning("主函数 {} 执行失败: {}，尝试降级回退", func.__name__, e)
        try:
            return fallback_func(*args, **kwargs)
        except Exception as fallback_error:
            logger.error("降级函数 {} 也执行失败: {}", fallback_func.__name__, fallback_error)
            raise fallback_error from e
