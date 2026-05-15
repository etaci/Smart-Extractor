import time

from smart_extractor.web.redis_queue import RedisTaskQueue


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


def test_redis_queue_enqueue_claim_and_ack():
    queue = RedisTaskQueue(
        redis_url="redis://unused",
        queue_name="test-queue",
        client=_FakeRedis(),
    )

    queue.enqueue(
        task_id="task-1",
        tenant_id="default",
        payload={"schema_name": "news"},
        queue_scope="group-a",
        isolation_key="iso-1",
    )

    envelope = queue.claim_next(worker_id="worker-a", queue_scope="group-a")
    assert envelope is not None
    assert envelope["task_id"] == "task-1"
    assert envelope["queue_scope"] == "group-a"
    assert queue.pending_size("group-a") == 0
    assert queue.processing_size("group-a") == 1

    queue.ack(envelope)
    assert queue.processing_size("group-a") == 0


def test_redis_queue_nack_requeues_item():
    queue = RedisTaskQueue(
        redis_url="redis://unused",
        queue_name="test-queue",
        client=_FakeRedis(),
    )
    queue.enqueue(
        task_id="task-2",
        tenant_id="default",
        payload={},
        queue_scope="group-b",
    )

    envelope = queue.claim_next(worker_id="worker-b", queue_scope="group-b")
    assert envelope is not None
    queue.nack(envelope, requeue=True)

    assert queue.pending_size("group-b") == 1
    assert queue.processing_size("group-b") == 0


def test_redis_queue_requeues_expired_lease():
    client = _FakeRedis()
    queue = RedisTaskQueue(
        redis_url="redis://unused",
        queue_name="test-queue",
        client=client,
        visibility_timeout_seconds=1.0,
    )
    queue.enqueue(
        task_id="task-3",
        tenant_id="default",
        payload={},
        queue_scope="group-c",
    )

    envelope = queue.claim_next(worker_id="worker-c", queue_scope="group-c")
    assert envelope is not None
    lease_key = "test-queue:leases:group-c"
    dispatch_id = envelope["dispatch_id"]
    client.hashes[lease_key][dispatch_id] = '{"worker_id":"worker-c","lease_until":0}'

    recovered = queue.claim_next(worker_id="worker-d", queue_scope="group-c")
    assert recovered is not None
    assert recovered["task_id"] == "task-3"


def test_redis_queue_heartbeat_extends_lease():
    client = _FakeRedis()
    queue = RedisTaskQueue(
        redis_url="redis://unused",
        queue_name="test-queue",
        client=client,
        visibility_timeout_seconds=1.0,
    )
    queue.enqueue(
        task_id="task-4",
        tenant_id="default",
        payload={},
        queue_scope="group-d",
    )

    envelope = queue.claim_next(worker_id="worker-d", queue_scope="group-d")
    assert envelope is not None
    lease_key = "test-queue:leases:group-d"
    before = client.hashes[lease_key][envelope["dispatch_id"]]
    time.sleep(0.01)
    queue.heartbeat(envelope, worker_id="worker-d", visibility_timeout_seconds=5.0)
    after = client.hashes[lease_key][envelope["dispatch_id"]]
    assert before != after
