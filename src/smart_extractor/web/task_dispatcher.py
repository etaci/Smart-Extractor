"""Web 侧任务分发抽象。"""

from __future__ import annotations

from dataclasses import dataclass, field
from inspect import Parameter, signature
from typing import Callable

from fastapi import BackgroundTasks


@dataclass(slots=True)
class ExtractionTaskSpec:
    """描述一次网页提取任务的执行参数。"""

    task_id: str
    tenant_id: str = ""
    schema_name: str = "auto"
    use_static: bool = False
    selected_fields: list[str] = field(default_factory=list)
    monitor_id: str = ""
    force_strategy: str = ""

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
        return kwargs


class InlineBackgroundTaskDispatcher:
    """默认分发器：继续使用 FastAPI BackgroundTasks 进程内执行。"""

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
    """队列分发器：写入 SQLite 队列，交给独立 worker 处理。"""

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
        self._task_store.enqueue_task_spec(spec, tenant_id=spec.tenant_id)


def build_task_dispatcher(*, task_store, dispatch_mode: str):
    normalized_mode = str(dispatch_mode or "inline").strip().lower()
    if normalized_mode == "queue":
        return QueuedTaskDispatcher(task_store)
    return InlineBackgroundTaskDispatcher()
