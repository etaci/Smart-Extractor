from smart_extractor.web.task_dispatcher import ExtractionTaskSpec
from smart_extractor.web.redis_queue import RedisTaskQueue
from smart_extractor.web.task_store import SQLiteTaskStore
from smart_extractor.web.task_worker import RedisTaskWorker, SQLiteTaskWorker


class _FakeRedis:
    def __init__(self):
        self.sets = {}
        self.lists = {}
        self.hashes = {}

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def smembers(self, key):
        return self.sets.get(key, set())

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def rpoplpush(self, source, dest):
        source_list = self.lists.setdefault(source, [])
        if not source_list:
            return None
        item = source_list.pop()
        self.lists.setdefault(dest, []).insert(0, item)
        return item

    def lrange(self, key, start, end):
        data = list(self.lists.get(key, []))
        if end == -1:
            return data[start:]
        return data[start : end + 1]

    def lrem(self, key, count, value):
        data = self.lists.setdefault(key, [])
        removed = 0
        remaining = []
        for item in data:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            remaining.append(item)
        self.lists[key] = remaining
        return removed

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    def hdel(self, key, field):
        bucket = self.hashes.get(key, {})
        if field in bucket:
            del bucket[field]

    def llen(self, key):
        return len(self.lists.get(key, []))


def test_sqlite_task_worker_claims_and_executes_queued_task(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    task = store.create(
        url="https://example.com/worker",
        schema_name="news",
        storage_format="json",
    )
    store.enqueue_task_spec(
        ExtractionTaskSpec(
            task_id=task.task_id,
            schema_name="news",
            use_static=True,
            selected_fields=["title"],
            force_strategy="llm",
        )
    )

    captured = {}

    def fake_runner(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        captured["task_id"] = task_id
        captured["schema_name"] = schema_name
        captured["use_static"] = use_static
        captured["selected_fields"] = selected_fields or []
        captured["force_strategy"] = force_strategy
        store.mark_success(
            task_id,
            elapsed_ms=12.0,
            quality_score=0.9,
            data={"title": "队列任务"},
        )

    worker = SQLiteTaskWorker(
        task_store=store,
        runner=fake_runner,
        worker_id="worker-test",
        stale_after_seconds=30.0,
    )

    assert worker.run_once() is True
    assert worker.run_once() is False

    updated = store.get(task.task_id)
    assert updated is not None
    assert updated.status == "success"
    assert captured == {
        "task_id": task.task_id,
        "schema_name": "news",
        "use_static": True,
        "selected_fields": ["title"],
        "force_strategy": "llm",
    }


def test_sqlite_task_worker_can_retry_requeued_failed_task(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    task = store.create(
        url="https://example.com/retry",
        schema_name="news",
        storage_format="json",
    )
    spec = ExtractionTaskSpec(
        task_id=task.task_id,
        schema_name="news",
        use_static=True,
        selected_fields=["title"],
    )
    store.enqueue_task_spec(spec)

    call_count = {"value": 0}

    def flaky_runner(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        call_count["value"] += 1
        if call_count["value"] == 1:
            store.mark_failed(
                task_id,
                elapsed_ms=5.0,
                error="first failure",
            )
            raise RuntimeError("first failure")
        store.mark_success(
            task_id,
            elapsed_ms=8.0,
            quality_score=0.91,
            data={"title": "retry success"},
        )

    worker = SQLiteTaskWorker(
        task_store=store,
        runner=flaky_runner,
        worker_id="worker-retry",
        stale_after_seconds=30.0,
    )

    assert worker.run_once() is True
    failed_task = store.get(task.task_id)
    assert failed_task is not None
    assert failed_task.status == "failed"

    store.enqueue_task_spec(spec)
    assert worker.run_once() is True

    retried_task = store.get(task.task_id)
    assert retried_task is not None
    assert retried_task.status == "success"
    assert call_count["value"] == 2


def test_sqlite_task_worker_can_take_over_stale_running_queue_item(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    task = store.create(
        url="https://example.com/stale",
        schema_name="news",
        storage_format="json",
    )
    store.enqueue_task_spec(
        ExtractionTaskSpec(
            task_id=task.task_id,
            schema_name="news",
            use_static=False,
        )
    )

    claimed = store.claim_next_queued_task(worker_id="worker-a", stale_after_seconds=30.0)
    assert claimed is not None

    with store._connect() as conn:
        conn.execute(
            """
            UPDATE web_task_dispatch_queue
            SET status='running', claimed_at='2000-01-01 00:00:00', worker_id='worker-a'
            WHERE task_id=?
            """,
            (task.task_id,),
        )
        conn.commit()

    executed = {}

    def takeover_runner(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        executed["task_id"] = task_id
        store.mark_success(
            task_id,
            elapsed_ms=6.0,
            quality_score=0.88,
            data={"title": "stale recovered"},
        )

    worker = SQLiteTaskWorker(
        task_store=store,
        runner=takeover_runner,
        worker_id="worker-b",
        stale_after_seconds=1.0,
    )

    assert worker.run_once() is True
    updated = store.get(task.task_id)
    assert updated is not None
    assert updated.status == "success"
    assert executed["task_id"] == task.task_id


def test_redis_task_worker_claims_and_executes_queued_task(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    queue = RedisTaskQueue(
        redis_url="redis://unused",
        queue_name="test-queue",
        client=_FakeRedis(),
    )
    task = store.create(
        url="https://example.com/redis-worker",
        schema_name="news",
        storage_format="json",
    )
    queue.enqueue(
        task_id=task.task_id,
        tenant_id="default",
        payload=ExtractionTaskSpec(
            task_id=task.task_id,
            tenant_id="default",
            schema_name="news",
            queue_scope="group-a",
            dispatch_backend="redis",
        ).to_queue_payload(),
        queue_scope="group-a",
        isolation_key="tenant:default:domain:example.com:monitor:-",
    )
    store.mark_queued(task.task_id, tenant_id="default")

    captured = {}

    def fake_runner(task_id: str, schema_name: str = "auto", **kwargs):
        captured["task_id"] = task_id
        captured["schema_name"] = schema_name
        captured["queue_scope"] = kwargs.get("queue_scope")
        store.mark_success(
            task_id,
            elapsed_ms=16.0,
            quality_score=0.95,
            data={"redis": True},
            tenant_id="default",
        )

    worker = RedisTaskWorker(
        task_store=store,
        runner=fake_runner,
        worker_id="redis-worker-a",
        queue_scope="group-a",
        redis_queue=queue,
        visibility_timeout_seconds=30.0,
    )

    assert worker.run_once() is True
    assert worker.run_once() is False

    updated = store.get(task.task_id, tenant_id="default")
    assert updated is not None
    assert updated.status == "success"
    assert captured["task_id"] == task.task_id
    assert captured["queue_scope"] == "group-a"
