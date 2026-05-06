"""Web queue worker support."""

from __future__ import annotations

import threading
from inspect import Parameter, signature
from typing import Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.task_dispatcher import ExtractionTaskSpec


class SQLiteTaskWorker:
    """Consume queued extraction tasks from the shared task store."""

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

    def _heartbeat(self, *, status: str, current_load: int, last_error: str = "") -> None:
        self._task_store.heartbeat_worker_node(
            worker_id=self.worker_id,
            display_name=self.worker_id,
            status=status,
            queue_scope="*",
            current_load=max(int(current_load or 0), 0),
            capabilities=["extract", "queue"],
            metadata={"runner": "sqlite-task-worker"},
            last_error=last_error,
            tenant_id="default",
        )

    def run_once(self) -> bool:
        self._heartbeat(status="idle", current_load=0)
        claimed = self._task_store.claim_next_queued_task(
            worker_id=self.worker_id,
            stale_after_seconds=self.stale_after_seconds,
            tenant_id="*",
        )
        if not claimed:
            return False

        task, payload = claimed
        spec = ExtractionTaskSpec.from_queue_payload(
            task_id=task.task_id,
            payload=payload,
        )
        try:
            self._heartbeat(status="busy", current_load=1)
            runner_kwargs = dict(spec.to_runner_kwargs())
            runner_signature = signature(self._runner)
            supports_var_kwargs = any(
                parameter.kind == Parameter.VAR_KEYWORD
                for parameter in runner_signature.parameters.values()
            )
            if supports_var_kwargs or "worker_id" in runner_signature.parameters:
                runner_kwargs["worker_id"] = self.worker_id
            self._runner(*spec.to_runner_args(), **runner_kwargs)
            self._task_store.mark_queue_done(task.task_id, tenant_id=task.tenant_id)
        except Exception as exc:
            self._task_store.mark_queue_failed(
                task.task_id,
                f"{type(exc).__name__}: {exc}",
                tenant_id=task.tenant_id,
            )
            self._heartbeat(
                status="degraded",
                current_load=0,
                last_error=f"{type(exc).__name__}: {exc}",
            )
            logger.exception("Queue worker crashed on task: {}", task.task_id)
        return True

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 2.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds), 0.2)
        external_stop_event = stop_event or threading.Event()
        logger.info("Queue worker started: {}", self.worker_id)
        self._heartbeat(status="starting", current_load=0)
        try:
            while not external_stop_event.is_set():
                processed = self.run_once()
                if processed:
                    continue
                external_stop_event.wait(interval)
        finally:
            self._heartbeat(status="offline", current_load=0)
            logger.info("Queue worker stopped: {}", self.worker_id)


class ManagedTaskWorkerThread:
    """Lifecycle wrapper for the built-in task worker thread."""

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
