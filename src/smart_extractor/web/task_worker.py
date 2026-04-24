"""Web 队列任务 worker。"""

from __future__ import annotations

import threading
import time
from typing import Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.task_dispatcher import ExtractionTaskSpec


class SQLiteTaskWorker:
    """从 SQLite 队列领取任务并执行。"""

    def __init__(
        self,
        *,
        task_store,
        runner: Callable[..., None],
        worker_id: str = "",
        stale_after_seconds: float = 300.0,
    ) -> None:
        self._task_store = task_store
        self._runner = runner
        self.worker_id = str(worker_id or f"worker-{uuid4().hex[:8]}")
        self.stale_after_seconds = max(float(stale_after_seconds), 0.0)

    def run_once(self) -> bool:
        claimed = self._task_store.claim_next_queued_task(
            worker_id=self.worker_id,
            stale_after_seconds=self.stale_after_seconds,
        )
        if not claimed:
            return False

        task, payload = claimed
        spec = ExtractionTaskSpec.from_queue_payload(
            task_id=task.task_id,
            payload=payload,
        )
        try:
            self._runner(*spec.to_runner_args(), **spec.to_runner_kwargs())
            self._task_store.mark_queue_done(task.task_id)
        except Exception as exc:
            self._task_store.mark_queue_failed(
                task.task_id,
                f"{type(exc).__name__}: {exc}",
            )
            logger.exception("队列 worker 执行任务崩溃: {}", task.task_id)
        return True

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 2.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds), 0.2)
        external_stop_event = stop_event or threading.Event()
        logger.info("队列 worker 启动: {}", self.worker_id)
        try:
            while not external_stop_event.is_set():
                processed = self.run_once()
                if processed:
                    continue
                external_stop_event.wait(interval)
        finally:
            logger.info("队列 worker 停止: {}", self.worker_id)


class ManagedTaskWorkerThread:
    """用于应用内启动/停止内置 worker 线程。"""

    def __init__(
        self,
        *,
        worker: SQLiteTaskWorker,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._worker = worker
        self._poll_interval_seconds = max(float(poll_interval_seconds), 0.2)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._worker.run_forever,
            kwargs={
                "poll_interval_seconds": self._poll_interval_seconds,
                "stop_event": self._stop_event,
            },
            name=f"smart-extractor-{self._worker.worker_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(float(timeout_seconds), 0.1))

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()
