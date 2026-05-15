"""Distributed worker support for SQLite queue and Redis queue backends."""

from __future__ import annotations

import threading
from inspect import Parameter, signature
from typing import Any, Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.redis_queue import RedisTaskQueue
from smart_extractor.web.task_dispatcher import ExtractionTaskSpec


class _RedisLeaseHeartbeatThread:
    def __init__(
        self,
        *,
        queue: RedisTaskQueue,
        envelope: dict[str, Any],
        worker_id: str,
        visibility_timeout_seconds: float,
    ) -> None:
        self._queue = queue
        self._envelope = dict(envelope or {})
        self._worker_id = str(worker_id or "").strip()
        self._visibility_timeout_seconds = max(float(visibility_timeout_seconds or 0.0), 1.0)
        self._stop_event = threading.Event()
        interval = max(min(self._visibility_timeout_seconds / 3.0, 30.0), 1.0)
        self._thread = threading.Thread(
            target=self._run,
            args=(interval,),
            name=f"smart-extractor-redis-lease-{self._worker_id}",
            daemon=True,
        )

    def _run(self, interval: float) -> None:
        while not self._stop_event.wait(interval):
            self._queue.heartbeat(
                self._envelope,
                worker_id=self._worker_id,
                visibility_timeout_seconds=self._visibility_timeout_seconds,
            )

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_seconds: float = 2.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(float(timeout_seconds), 0.1))


class DistributedTaskWorker:
    """Consume queued extraction tasks from SQLite queue or Redis queue."""

    def __init__(
        self,
        *,
        task_store,
        runner: Callable[..., None],
        worker_id: str = "",
        queue_scope: str = "*",
        stale_after_seconds: float = 300.0,
        backend: str = "sqlite",
        redis_queue: RedisTaskQueue | None = None,
        visibility_timeout_seconds: float = 300.0,
    ) -> None:
        self._task_store = task_store
        self._runner = runner
        self.worker_id = str(worker_id or f"worker-{uuid4().hex[:8]}")
        self.queue_scope = str(queue_scope or "*").strip() or "*"
        self.stale_after_seconds = max(float(stale_after_seconds), 0.0)
        self.backend = str(backend or "sqlite").strip().lower() or "sqlite"
        self._redis_queue = redis_queue
        self._visibility_timeout_seconds = max(float(visibility_timeout_seconds or 0.0), 1.0)

    def _runtime_metadata(
        self,
        *,
        current_task_id: str = "",
        isolation_key: str = "",
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "runner": "distributed-task-worker",
            "dispatch_backend": self.backend,
            "queue_scope": self.queue_scope,
        }
        if current_task_id:
            metadata["current_task_id"] = current_task_id
        if isolation_key:
            metadata["isolation_key"] = isolation_key
        return metadata

    def _heartbeat(self, *, status: str, current_load: int, last_error: str = "", metadata: dict[str, Any] | None = None) -> None:
        self._task_store.heartbeat_worker_node(
            worker_id=self.worker_id,
            display_name=self.worker_id,
            status=status,
            queue_scope=self.queue_scope,
            current_load=max(int(current_load or 0), 0),
            capabilities=["extract", "queue", self.backend],
            metadata=metadata or self._runtime_metadata(),
            last_error=last_error,
            tenant_id="default",
        )

    def _select_runner_kwargs(self, spec: ExtractionTaskSpec) -> dict[str, Any]:
        runner_kwargs = dict(spec.to_runner_kwargs())
        runner_signature = signature(self._runner)
        supports_var_kwargs = any(
            parameter.kind == Parameter.VAR_KEYWORD
            for parameter in runner_signature.parameters.values()
        )
        if supports_var_kwargs or "worker_id" in runner_signature.parameters:
            runner_kwargs["worker_id"] = self.worker_id
        if supports_var_kwargs:
            return runner_kwargs
        return {
            key: value
            for key, value in runner_kwargs.items()
            if key in runner_signature.parameters
        }

    def _claim_from_sqlite(self) -> tuple[Any, dict[str, Any], dict[str, Any] | None] | None:
        claimed = self._task_store.claim_next_queued_task(
            worker_id=self.worker_id,
            stale_after_seconds=self.stale_after_seconds,
            queue_scope=self.queue_scope,
            tenant_id="*",
        )
        if not claimed:
            return None
        task, payload = claimed
        return task, dict(payload or {}), None

    def _claim_from_redis(self) -> tuple[Any, dict[str, Any], dict[str, Any] | None] | None:
        if self._redis_queue is None:
            raise RuntimeError("Redis worker requires redis_queue")
        envelope = self._redis_queue.claim_next(
            worker_id=self.worker_id,
            queue_scope=self.queue_scope,
            visibility_timeout_seconds=self._visibility_timeout_seconds,
        )
        if not envelope:
            return None
        tenant_id = str(envelope.get("tenant_id") or "default").strip() or "default"
        task_id = str(envelope.get("task_id") or "").strip()
        if not task_id:
            self._redis_queue.ack(envelope)
            return None
        task = self._task_store.get(task_id, tenant_id=tenant_id)
        if task is None:
            self._redis_queue.ack(envelope)
            return None
        payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
        return task, dict(payload or {}), envelope

    def _claim_next(self) -> tuple[Any, dict[str, Any], dict[str, Any] | None] | None:
        if self.backend == "redis":
            return self._claim_from_redis()
        return self._claim_from_sqlite()

    def _complete_dispatch(self, *, task_id: str, tenant_id: str, envelope: dict[str, Any] | None) -> None:
        if self.backend == "redis":
            if self._redis_queue is not None:
                self._redis_queue.ack(envelope)
            return
        self._task_store.mark_queue_done(task_id, tenant_id=tenant_id)

    def _fail_dispatch(self, *, task, envelope: dict[str, Any] | None, error: str) -> None:
        if self.backend == "redis":
            latest_task = self._task_store.get(task.task_id, tenant_id=task.tenant_id)
            if latest_task is None or latest_task.status not in {"failed", "success"}:
                self._task_store.mark_failed(
                    task.task_id,
                    elapsed_ms=0.0,
                    error=error,
                    tenant_id=task.tenant_id,
                )
            if self._redis_queue is not None:
                self._redis_queue.ack(envelope)
            return
        self._task_store.mark_queue_failed(task.task_id, error, tenant_id=task.tenant_id)

    def run_once(self) -> bool:
        self._heartbeat(status="idle", current_load=0)
        claimed = self._claim_next()
        if not claimed:
            return False

        task, payload, envelope = claimed
        spec = ExtractionTaskSpec.from_queue_payload(
            task_id=task.task_id,
            payload=payload,
        )
        spec.dispatch_backend = spec.dispatch_backend or self.backend
        lease_thread = None
        if self.backend == "redis" and envelope is not None and self._redis_queue is not None:
            lease_thread = _RedisLeaseHeartbeatThread(
                queue=self._redis_queue,
                envelope=envelope,
                worker_id=self.worker_id,
                visibility_timeout_seconds=self._visibility_timeout_seconds,
            )
            lease_thread.start()

        try:
            self._heartbeat(
                status="busy",
                current_load=1,
                metadata=self._runtime_metadata(
                    current_task_id=task.task_id,
                    isolation_key=spec.isolation_key,
                ),
            )
            self._runner(*spec.to_runner_args(), **self._select_runner_kwargs(spec))
            self._complete_dispatch(
                task_id=task.task_id,
                tenant_id=task.tenant_id,
                envelope=envelope,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self._fail_dispatch(task=task, envelope=envelope, error=error)
            self._heartbeat(
                status="degraded",
                current_load=0,
                last_error=error,
                metadata=self._runtime_metadata(
                    current_task_id=task.task_id,
                    isolation_key=spec.isolation_key,
                ),
            )
            logger.exception("Queue worker crashed on task: {}", task.task_id)
        finally:
            if lease_thread is not None:
                lease_thread.stop()
        return True

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 2.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds), 0.2)
        external_stop_event = stop_event or threading.Event()
        logger.info(
            "Queue worker started: worker_id={} backend={} queue_scope={}",
            self.worker_id,
            self.backend,
            self.queue_scope,
        )
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


class SQLiteTaskWorker(DistributedTaskWorker):
    def __init__(self, **kwargs) -> None:
        super().__init__(backend="sqlite", **kwargs)


class RedisTaskWorker(DistributedTaskWorker):
    def __init__(self, **kwargs) -> None:
        super().__init__(backend="redis", **kwargs)


class ManagedTaskWorkerThread:
    """Lifecycle wrapper for the built-in task worker thread."""

    def __init__(
        self,
        *,
        worker: DistributedTaskWorker,
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
