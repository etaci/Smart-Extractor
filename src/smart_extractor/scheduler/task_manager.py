"""
批量任务调度管理器

管理多个 URL 的批量提取任务，支持并发控制、
失败重试、进度跟踪。
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable

from loguru import logger

from smart_extractor.config import SchedulerConfig
from smart_extractor.models.base import BaseExtractModel
from smart_extractor.utils.anti_detect import random_delay


class TaskStatus(str, Enum):
    """任务状态"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Task:
    """单个提取任务"""

    url: str
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[BaseExtractModel] = None
    error: Optional[str] = None
    retries: int = 0
    elapsed_ms: float = 0.0

    @property
    def is_done(self) -> bool:
        return self.status in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.SKIPPED,
        )


@dataclass
class BatchResult:
    """批量处理结果统计"""

    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    total_time_ms: float = 0.0
    tasks: list[Task] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.success / max(self.total, 1)

    def summary(self) -> str:
        return (
            f"批量处理完成: 总计={self.total}, "
            f"成功={self.success}, 失败={self.failed}, 跳过={self.skipped}, "
            f"成功率={self.success_rate:.1%}, 总耗时={self.total_time_ms:.0f}ms"
        )


class TaskManager:
    """
    批量任务管理器。

    功能：
    - 从 URL 列表创建任务队列
    - 串行 / 并发执行控制
    - 自动重试失败任务
    - 实时进度回调
    """

    def __init__(self, config: Optional[SchedulerConfig] = None):
        self._config = config or SchedulerConfig()
        self._tasks: list[Task] = []

    def add_urls(self, urls: list[str]) -> int:
        """
        添加 URL 列表到任务队列。

        Args:
            urls: URL 列表

        Returns:
            添加的任务数量
        """
        count = 0
        for url in urls:
            url = url.strip()
            if url:
                self._tasks.append(Task(url=url))
                count += 1
        logger.info("添加了 {} 个任务到队列", count)
        return count

    def add_urls_from_file(self, filepath: str) -> int:
        """
        从文件中读取 URL 列表（每行一个 URL）。

        Args:
            filepath: URL 列表文件路径

        Returns:
            添加的任务数量
        """
        with open(filepath, "r", encoding="utf-8") as f:
            urls = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]
        return self.add_urls(urls)

    def run_sync(
        self,
        extract_fn: Callable[[str], BaseExtractModel],
        progress_callback: Optional[Callable[[int, int, Task], None]] = None,
    ) -> BatchResult:
        """
        串行执行所有任务（保留兼容性，内部复用并发接口 max_workers=1）。

        Args:
            extract_fn: 提取函数，签名为 (url: str) -> BaseExtractModel
            progress_callback: 进度回调，签名为 (current, total, task) -> None

        Returns:
            BatchResult 批量处理结果
        """
        return self.run_concurrent(
            extract_fn,
            max_workers=1,
            progress_callback=progress_callback,
        )

    def run_concurrent(
        self,
        extract_fn: Callable[[str], BaseExtractModel],
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, Task], None]] = None,
    ) -> BatchResult:
        """
        并发执行所有任务（ThreadPoolExecutor）。

        Args:
            extract_fn: 提取函数，签名为 (url: str) -> BaseExtractModel
            max_workers: 最大并发数，默认使用 config.max_concurrency
            progress_callback: 进度回调，签名为 (current, total, task) -> None

        Returns:
            BatchResult 批量处理结果
        """
        workers = max_workers or self._config.max_concurrency
        start_time = time.time()
        total = len(self._tasks)
        result = BatchResult(total=total, tasks=self._tasks)
        completed_count = 0

        logger.info("开始并发执行 {} 个任务 (max_workers={})", total, workers)

        # 构建 future -> task 映射
        future_to_task: dict = {}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for task in self._tasks:
                task.status = TaskStatus.RUNNING
                future = executor.submit(self._execute_single, task, extract_fn)
                future_to_task[future] = task

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                completed_count += 1

                try:
                    future.result()  # 异常会在此重新抛出（已在 _execute_single 内捕获）
                except Exception as e:
                    # 保险：理论上 _execute_single 内部已处理，不应到达此处
                    task.status = TaskStatus.FAILED
                    task.error = str(e)

                if task.status == TaskStatus.SUCCESS:
                    result.success += 1
                    logger.info(
                        "[{}/{}] [PASS] 成功: {}", completed_count, total, task.url[:60]
                    )
                else:
                    result.failed += 1
                    logger.error(
                        "[{}/{}] [FAIL] 失败: {} -- {}",
                        completed_count,
                        total,
                        task.url[:60],
                        task.error,
                    )

                if progress_callback:
                    progress_callback(completed_count, total, task)

        result.total_time_ms = (time.time() - start_time) * 1000
        logger.info(result.summary())
        return result

    def _execute_single(
        self,
        task: Task,
        extract_fn: Callable[[str], BaseExtractModel],
    ) -> None:
        """
        执行单个任务（在线程池内运行）。
        结果直接写回 task 对象。
        """
        task_start = time.time()
        # 随机延迟，降低被反爬检测的概率
        random_delay(
            self._config.request_delay_min,
            self._config.request_delay_max,
        )
        try:
            task.result = extract_fn(task.url)
            task.status = TaskStatus.SUCCESS
        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
        finally:
            task.elapsed_ms = (time.time() - task_start) * 1000

    def retry_failed(
        self,
        extract_fn: Callable[[str], BaseExtractModel],
        progress_callback: Optional[Callable[[int, int, Task], None]] = None,
    ) -> BatchResult:
        """
        重试所有失败的任务（并发）。

        Args:
            extract_fn: 提取函数
            progress_callback: 进度回调

        Returns:
            重试结果
        """
        failed_tasks = [t for t in self._tasks if t.status == TaskStatus.FAILED]

        if not failed_tasks:
            logger.info("没有失败的任务需要重试")
            return BatchResult()

        logger.info("重试 {} 个失败任务", len(failed_tasks))

        start_time = time.time()
        total = len(failed_tasks)
        result = BatchResult(total=total, tasks=failed_tasks)
        completed_count = 0

        future_to_task: dict = {}

        with ThreadPoolExecutor(max_workers=self._config.max_concurrency) as executor:
            for task in failed_tasks:
                if task.retries >= self._config.max_retries:
                    task.status = TaskStatus.SKIPPED
                    result.skipped += 1
                    logger.warning("已达最大重试次数，跳过: {}", task.url[:60])
                    continue
                task.retries += 1
                task.status = TaskStatus.RUNNING
                future = executor.submit(self._execute_single, task, extract_fn)
                future_to_task[future] = task

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                completed_count += 1

                try:
                    future.result()
                except Exception as e:
                    task.status = TaskStatus.FAILED
                    task.error = str(e)

                if task.status == TaskStatus.SUCCESS:
                    result.success += 1
                    task.error = None
                    logger.info(
                        "[重试 {}/{}] [PASS] 成功: {}",
                        completed_count,
                        total,
                        task.url[:60],
                    )
                else:
                    result.failed += 1
                    logger.error(
                        "[重试 {}/{}] [FAIL] 仍然失败: {}",
                        completed_count,
                        total,
                        task.url[:60],
                    )

                if progress_callback:
                    progress_callback(completed_count, total, task)

        result.total_time_ms = (time.time() - start_time) * 1000
        logger.info("重试 " + result.summary())
        return result

    def get_tasks(self) -> list[Task]:
        """获取所有任务"""
        return self._tasks

    def clear(self) -> None:
        """清空任务队列"""
        self._tasks.clear()
