"""Web-side execution dispatch abstraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from inspect import Parameter, signature
from typing import Any, Callable

from fastapi import BackgroundTasks

from smart_extractor.web.redis_queue import RedisTaskQueue


@dataclass(slots=True)
class ExtractionTaskSpec:
    """Describe one extraction task across inline / SQLite queue / Redis queue backends."""

    task_id: str
    tenant_id: str = ""
    schema_name: str = "auto"
    use_static: bool = False
    selected_fields: list[str] = field(default_factory=list)
    monitor_id: str = ""
    force_strategy: str = ""
    queue_scope: str = "*"
    isolation_key: str = ""
    site_domain: str = ""
    dispatch_backend: str = ""

    def to_runner_args(self) -> list[object]:
        return [self.task_id]

    def to_queue_payload(self) -> dict[str, object]:
        return {
            "tenant_id": self.tenant_id,
            "schema_name": self.schema_name,
            "use_static": self.use_static,
            "selected_fields": list(self.selected_fields or []),
            "monitor_id": self.monitor_id,
            "force_strategy": self.force_strategy,
            "queue_scope": self.queue_scope,
            "isolation_key": self.isolation_key,
            "site_domain": self.site_domain,
            "dispatch_backend": self.dispatch_backend,
        }

    @classmethod
    def from_queue_payload(
        cls,
        *,
        task_id: str,
        payload: dict[str, object] | None,
    ) -> "ExtractionTaskSpec":
        data = dict(payload or {})
        return cls(
            task_id=task_id,
            tenant_id=str(data.get("tenant_id") or "").strip(),
            schema_name=str(data.get("schema_name") or "auto").strip() or "auto",
            use_static=bool(data.get("use_static", False)),
            selected_fields=[
                str(item).strip()
                for item in data.get("selected_fields", [])
                if str(item).strip()
            ]
            if isinstance(data.get("selected_fields"), list)
            else [],
            monitor_id=str(data.get("monitor_id") or "").strip(),
            force_strategy=str(data.get("force_strategy") or "").strip(),
            queue_scope=str(data.get("queue_scope") or "*").strip() or "*",
            isolation_key=str(data.get("isolation_key") or "").strip(),
            site_domain=str(data.get("site_domain") or "").strip().lower(),
            dispatch_backend=str(data.get("dispatch_backend") or "").strip(),
        )

    def to_runner_kwargs(self) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "schema_name": self.schema_name,
            "use_static": self.use_static,
        }
        if self.tenant_id:
            kwargs["tenant_id"] = self.tenant_id
        if self.selected_fields:
            kwargs["selected_fields"] = list(self.selected_fields)
        if self.monitor_id:
            kwargs["monitor_id"] = self.monitor_id
        if self.force_strategy:
            kwargs["force_strategy"] = self.force_strategy
        if self.queue_scope and self.queue_scope != "*":
            kwargs["queue_scope"] = self.queue_scope
        if self.isolation_key:
            kwargs["isolation_key"] = self.isolation_key
        if self.site_domain:
            kwargs["site_domain"] = self.site_domain
        if self.dispatch_backend:
            kwargs["dispatch_backend"] = self.dispatch_backend
        return kwargs


class InlineBackgroundTaskDispatcher:
    """Default dispatcher: still use FastAPI BackgroundTasks inside current process."""

    @staticmethod
    def _select_runner_kwargs(
        spec: ExtractionTaskSpec,
        runner: Callable[..., None],
    ) -> dict[str, object]:
        runner_signature = signature(runner)
        supports_var_kwargs = any(
            parameter.kind == Parameter.VAR_KEYWORD
            for parameter in runner_signature.parameters.values()
        )
        if supports_var_kwargs:
            return spec.to_runner_kwargs()
        return {
            name: value
            for name, value in spec.to_runner_kwargs().items()
            if name in runner_signature.parameters
        }

    def enqueue(
        self,
        *,
        background_tasks: BackgroundTasks,
        spec: ExtractionTaskSpec,
        runner: Callable[..., None],
    ) -> None:
        background_tasks.add_task(
            runner,
            *spec.to_runner_args(),
            **self._select_runner_kwargs(spec, runner),
        )


class QueuedTaskDispatcher:
    """SQLite-backed queue dispatcher kept for local single-node deployments."""

    def __init__(self, task_store) -> None:
        self._task_store = task_store

    def enqueue(
        self,
        *,
        background_tasks: BackgroundTasks,
        spec: ExtractionTaskSpec,
        runner: Callable[..., None],
    ) -> None:
        del background_tasks
        del runner
        if spec.tenant_id:
            self._task_store.enqueue_task_spec(spec, tenant_id=spec.tenant_id)
            return
        self._task_store.enqueue_task_spec(spec)


class RedisQueuedTaskDispatcher:
    """Redis-backed queue dispatcher for multi-node workers."""

    def __init__(self, task_store, queue: RedisTaskQueue) -> None:
        self._task_store = task_store
        self._queue = queue

    def enqueue(
        self,
        *,
        background_tasks: BackgroundTasks,
        spec: ExtractionTaskSpec,
        runner: Callable[..., None],
    ) -> None:
        del background_tasks
        del runner
        self._queue.enqueue(
            task_id=spec.task_id,
            tenant_id=spec.tenant_id,
            payload=spec.to_queue_payload(),
            queue_scope=spec.queue_scope,
            isolation_key=spec.isolation_key,
        )
        if spec.tenant_id:
            self._task_store.mark_queued(spec.task_id, tenant_id=spec.tenant_id)
            return
        self._task_store.mark_queued(spec.task_id)


def build_task_dispatcher(
    *,
    task_store,
    dispatch_mode: str,
    redis_queue: RedisTaskQueue | None = None,
) -> Any:
    normalized_mode = str(dispatch_mode or "inline").strip().lower()
    if normalized_mode == "redis":
        if redis_queue is None:
            raise RuntimeError("dispatch_mode=redis 时必须提供 redis_queue")
        return RedisQueuedTaskDispatcher(task_store, redis_queue)
    if normalized_mode == "queue":
        return QueuedTaskDispatcher(task_store)
    return InlineBackgroundTaskDispatcher()
