from fastapi import BackgroundTasks

from smart_extractor.web.task_dispatcher import (
    ExtractionTaskSpec,
    InlineBackgroundTaskDispatcher,
    QueuedTaskDispatcher,
    RedisQueuedTaskDispatcher,
    build_task_dispatcher,
)


def test_extraction_task_spec_splits_args_and_kwargs():
    spec = ExtractionTaskSpec(
        task_id="task-000001",
        schema_name="auto",
        use_static=True,
        selected_fields=["title", "price"],
        monitor_id="mon-000001",
        force_strategy="llm",
    )

    assert spec.to_runner_args() == ["task-000001"]
    assert spec.to_runner_kwargs() == {
        "schema_name": "auto",
        "use_static": True,
        "selected_fields": ["title", "price"],
        "monitor_id": "mon-000001",
        "force_strategy": "llm",
    }


def test_inline_background_task_dispatcher_enqueues_runner_call():
    dispatcher = InlineBackgroundTaskDispatcher()
    background_tasks = BackgroundTasks()
    spec = ExtractionTaskSpec(task_id="task-000001", schema_name="news")

    def fake_runner(task_id: str, schema_name: str = "auto", use_static: bool = False):
        return None

    dispatcher.enqueue(
        background_tasks=background_tasks,
        spec=spec,
        runner=fake_runner,
    )

    assert len(background_tasks.tasks) == 1
    task = background_tasks.tasks[0]
    assert task.args == ("task-000001",)
    assert task.kwargs == {"schema_name": "news", "use_static": False}


def test_inline_background_task_dispatcher_filters_unsupported_kwargs():
    dispatcher = InlineBackgroundTaskDispatcher()
    background_tasks = BackgroundTasks()
    spec = ExtractionTaskSpec(
        task_id="task-000001",
        schema_name="news",
        use_static=True,
        selected_fields=["title"],
        monitor_id="mon-000001",
        force_strategy="llm",
    )

    def fake_runner(task_id: str, use_static: bool = False, selected_fields=None):
        return None

    dispatcher.enqueue(
        background_tasks=background_tasks,
        spec=spec,
        runner=fake_runner,
    )

    assert len(background_tasks.tasks) == 1
    task = background_tasks.tasks[0]
    assert task.args == ("task-000001",)
    assert task.kwargs == {
        "use_static": True,
        "selected_fields": ["title"],
    }


def test_queued_task_dispatcher_persists_spec_into_queue():
    recorded_specs = []

    class DummyTaskStore:
        def enqueue_task_spec(self, spec):
            recorded_specs.append(spec)

    dispatcher = QueuedTaskDispatcher(DummyTaskStore())

    dispatcher.enqueue(
        background_tasks=BackgroundTasks(),
        spec=ExtractionTaskSpec(task_id="task-000001", schema_name="news"),
        runner=lambda *args, **kwargs: None,
    )

    assert len(recorded_specs) == 1
    assert recorded_specs[0].task_id == "task-000001"


def test_build_task_dispatcher_returns_queue_dispatcher_when_enabled():
    dispatcher = build_task_dispatcher(task_store=object(), dispatch_mode="queue")

    assert isinstance(dispatcher, QueuedTaskDispatcher)


def test_redis_task_dispatcher_enqueues_scope_and_marks_queued():
    recorded = {}

    class DummyTaskStore:
        def mark_queued(self, task_id, tenant_id=""):
            recorded["task_id"] = task_id
            recorded["tenant_id"] = tenant_id

    class DummyRedisQueue:
        def enqueue(self, **kwargs):
            recorded["payload"] = kwargs

    dispatcher = RedisQueuedTaskDispatcher(DummyTaskStore(), DummyRedisQueue())
    spec = ExtractionTaskSpec(
        task_id="task-redis-1",
        tenant_id="tenant-a",
        schema_name="auto",
        queue_scope="group-a",
        isolation_key="tenant:tenant-a:domain:example.com:monitor:-",
        site_domain="example.com",
        dispatch_backend="redis",
    )

    dispatcher.enqueue(
        background_tasks=BackgroundTasks(),
        spec=spec,
        runner=lambda *args, **kwargs: None,
    )

    assert recorded["task_id"] == "task-redis-1"
    assert recorded["tenant_id"] == "tenant-a"
    assert recorded["payload"]["queue_scope"] == "group-a"
    assert recorded["payload"]["isolation_key"].startswith("tenant:tenant-a")


def test_build_task_dispatcher_returns_redis_dispatcher_when_enabled():
    dispatcher = build_task_dispatcher(
        task_store=object(),
        dispatch_mode="redis",
        redis_queue=object(),
    )

    assert isinstance(dispatcher, RedisQueuedTaskDispatcher)
