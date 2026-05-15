"""Redis-backed dispatch queue for multi-node workers."""

from __future__ import annotations

import json
import time
from typing import Any
from uuid import uuid4


class RedisTaskQueue:
    """A small Redis queue with scope isolation, visibility lease, and worker ACK/NACK."""

    def __init__(
        self,
        *,
        redis_url: str,
        queue_name: str,
        visibility_timeout_seconds: float = 300.0,
        client: Any | None = None,
    ) -> None:
        self._redis_url = str(redis_url or "").strip()
        self._queue_name = str(queue_name or "smart-extractor:dispatch").strip()
        self._visibility_timeout_seconds = max(
            float(visibility_timeout_seconds or 0.0),
            1.0,
        )
        self._client = client

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        if not self._redis_url:
            raise RuntimeError("Redis 队列未配置 redis_url")
        try:
            from redis import Redis
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("当前环境未安装 redis 依赖，无法启用 Redis 队列") from exc
        self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    @staticmethod
    def _normalize_scope(queue_scope: str) -> str:
        return str(queue_scope or "").strip() or "*"

    def _groups_key(self) -> str:
        return f"{self._queue_name}:groups"

    def _pending_key(self, queue_scope: str) -> str:
        return f"{self._queue_name}:pending:{self._normalize_scope(queue_scope)}"

    def _processing_key(self, queue_scope: str) -> str:
        return f"{self._queue_name}:processing:{self._normalize_scope(queue_scope)}"

    def _lease_key(self, queue_scope: str) -> str:
        return f"{self._queue_name}:leases:{self._normalize_scope(queue_scope)}"

    def enqueue(
        self,
        *,
        task_id: str,
        tenant_id: str,
        payload: dict[str, Any],
        queue_scope: str = "*",
        isolation_key: str = "",
    ) -> None:
        client = self._ensure_client()
        normalized_scope = self._normalize_scope(queue_scope)
        envelope = {
            "dispatch_id": f"rqd-{uuid4().hex[:16]}",
            "task_id": str(task_id or "").strip(),
            "tenant_id": str(tenant_id or "default").strip() or "default",
            "queue_scope": normalized_scope,
            "isolation_key": str(isolation_key or "").strip(),
            "payload": dict(payload or {}),
            "enqueued_at": time.time(),
        }
        serialized = json.dumps(envelope, ensure_ascii=False, sort_keys=True)
        client.sadd(self._groups_key(), normalized_scope)
        client.rpush(self._pending_key(normalized_scope), serialized)

    def _matching_scopes(self, worker_scope: str) -> list[str]:
        normalized_scope = self._normalize_scope(worker_scope)
        client = self._ensure_client()
        groups = sorted(
            str(item or "").strip()
            for item in (client.smembers(self._groups_key()) or set())
            if str(item or "").strip()
        )
        if normalized_scope == "*":
            ordered = ["*"]
            ordered.extend(item for item in groups if item != "*")
            return ordered
        scopes = [normalized_scope]
        if "*" not in scopes:
            scopes.append("*")
        return scopes

    def _lease_payload(
        self,
        *,
        worker_id: str,
        visibility_timeout_seconds: float,
    ) -> str:
        return json.dumps(
            {
                "worker_id": str(worker_id or "").strip(),
                "lease_until": time.time() + max(float(visibility_timeout_seconds or 0.0), 1.0),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def _requeue_expired(self, queue_scope: str) -> None:
        client = self._ensure_client()
        processing_key = self._processing_key(queue_scope)
        lease_key = self._lease_key(queue_scope)
        now = time.time()
        for item in list(client.lrange(processing_key, 0, -1) or []):
            try:
                envelope = json.loads(item)
            except json.JSONDecodeError:
                client.lrem(processing_key, 1, item)
                continue
            dispatch_id = str(envelope.get("dispatch_id") or "").strip()
            if not dispatch_id:
                client.lrem(processing_key, 1, item)
                continue
            lease_raw = client.hget(lease_key, dispatch_id)
            if not lease_raw:
                continue
            try:
                lease_data = json.loads(lease_raw)
            except json.JSONDecodeError:
                lease_data = {}
            if float(lease_data.get("lease_until", 0.0) or 0.0) > now:
                continue
            if client.lrem(processing_key, 1, item):
                client.rpush(self._pending_key(queue_scope), item)
                client.hdel(lease_key, dispatch_id)

    def claim_next(
        self,
        *,
        worker_id: str,
        queue_scope: str = "*",
        visibility_timeout_seconds: float | None = None,
    ) -> dict[str, Any] | None:
        client = self._ensure_client()
        lease_seconds = max(
            float(
                visibility_timeout_seconds
                if visibility_timeout_seconds is not None
                else self._visibility_timeout_seconds
            ),
            1.0,
        )
        for scope in self._matching_scopes(queue_scope):
            self._requeue_expired(scope)
            item = client.rpoplpush(
                self._pending_key(scope),
                self._processing_key(scope),
            )
            if not item:
                continue
            try:
                envelope = json.loads(item)
            except json.JSONDecodeError:
                client.lrem(self._processing_key(scope), 1, item)
                continue
            dispatch_id = str(envelope.get("dispatch_id") or "").strip()
            if not dispatch_id:
                client.lrem(self._processing_key(scope), 1, item)
                continue
            client.hset(
                self._lease_key(scope),
                dispatch_id,
                self._lease_payload(
                    worker_id=worker_id,
                    visibility_timeout_seconds=lease_seconds,
                ),
            )
            envelope["_redis_scope"] = scope
            envelope["_redis_item"] = item
            return envelope
        return None

    def heartbeat(
        self,
        envelope: dict[str, Any] | None,
        *,
        worker_id: str,
        visibility_timeout_seconds: float | None = None,
    ) -> None:
        if not envelope:
            return
        client = self._ensure_client()
        scope = self._normalize_scope(
            str(envelope.get("_redis_scope") or envelope.get("queue_scope") or "*")
        )
        dispatch_id = str(envelope.get("dispatch_id") or "").strip()
        if not dispatch_id:
            return
        lease_seconds = max(
            float(
                visibility_timeout_seconds
                if visibility_timeout_seconds is not None
                else self._visibility_timeout_seconds
            ),
            1.0,
        )
        client.hset(
            self._lease_key(scope),
            dispatch_id,
            self._lease_payload(
                worker_id=worker_id,
                visibility_timeout_seconds=lease_seconds,
            ),
        )

    def ack(self, envelope: dict[str, Any] | None) -> None:
        if not envelope:
            return
        client = self._ensure_client()
        scope = self._normalize_scope(
            str(envelope.get("_redis_scope") or envelope.get("queue_scope") or "*")
        )
        item = str(envelope.get("_redis_item") or "")
        dispatch_id = str(envelope.get("dispatch_id") or "").strip()
        if item:
            client.lrem(self._processing_key(scope), 1, item)
        if dispatch_id:
            client.hdel(self._lease_key(scope), dispatch_id)

    def nack(
        self,
        envelope: dict[str, Any] | None,
        *,
        requeue: bool = True,
    ) -> None:
        if not envelope:
            return
        client = self._ensure_client()
        scope = self._normalize_scope(
            str(envelope.get("_redis_scope") or envelope.get("queue_scope") or "*")
        )
        item = str(envelope.get("_redis_item") or "")
        dispatch_id = str(envelope.get("dispatch_id") or "").strip()
        removed = False
        if item:
            removed = bool(client.lrem(self._processing_key(scope), 1, item))
        if requeue and removed and item:
            client.rpush(self._pending_key(scope), item)
        if dispatch_id:
            client.hdel(self._lease_key(scope), dispatch_id)

    def pending_size(self, queue_scope: str = "*") -> int:
        client = self._ensure_client()
        scopes = self._matching_scopes(queue_scope)
        return sum(int(client.llen(self._pending_key(scope)) or 0) for scope in scopes)

    def processing_size(self, queue_scope: str = "*") -> int:
        client = self._ensure_client()
        scopes = self._matching_scopes(queue_scope)
        return sum(int(client.llen(self._processing_key(scope)) or 0) for scope in scopes)
