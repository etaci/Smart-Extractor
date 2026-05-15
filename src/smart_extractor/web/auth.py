"""Login, role, and tenant-aware access control."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException, Request

ConnectionFactory = Callable[[], object]

DEFAULT_SESSION_SECRET = "smart-extractor-local-default-session-secret"

ROLE_PERMISSIONS = {
    "admin": {
        "dashboard:read",
        "task:create",
        "task:read",
        "task:export",
        "task:review",
        "monitor:manage",
        "template:manage",
        "config:manage",
        "audit:read",
        "notification:manage",
        "user:manage",
    },
    "operator": {
        "dashboard:read",
        "task:create",
        "task:read",
        "task:export",
        "task:review",
        "monitor:manage",
        "template:manage",
        "notification:manage",
    },
    "viewer": {
        "dashboard:read",
        "task:read",
    },
}


def _now() -> datetime:
    return datetime.now()


def _now_text() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def hash_password(password: str, *, salt: str | None = None) -> str:
    normalized_salt = salt or secrets.token_hex(16)
    derived = hashlib.scrypt(
        str(password or "").encode("utf-8"),
        salt=normalized_salt.encode("utf-8"),
        n=2**14,
        r=8,
        p=1,
    )
    return f"scrypt${normalized_salt}${derived.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, salt, digest = str(password_hash or "").split("$", 2)
    except ValueError:
        return False
    if algorithm != "scrypt":
        return False
    expected = hash_password(password, salt=salt)
    return hmac.compare_digest(expected, password_hash)


@dataclass
class UserIdentity:
    user_id: str
    username: str
    role: str
    tenant_id: str
    display_name: str
    auth_mode: str

    @property
    def permissions(self) -> set[str]:
        return set(ROLE_PERMISSIONS.get(self.role, set()))

    def require(self, permission: str) -> None:
        if permission in self.permissions:
            return
        raise HTTPException(status_code=403, detail="当前账号没有对应权限")


class AuthService:
    def __init__(self, *, connect: ConnectionFactory, lock: Any, config):
        self._connect = connect
        self._lock = lock
        self._config = config

    @property
    def enabled(self) -> bool:
        return True

    def _session_secret(self) -> str:
        return str(self._config.security.auth_secret_key or "").strip() or DEFAULT_SESSION_SECRET

    def ensure_bootstrap_admin(self) -> None:
        if not self.enabled:
            return
        bootstrap_password = str(self._config.security.bootstrap_admin_password or "").strip()
        if not bootstrap_password:
            return
        tenant_id = str(self._config.security.default_tenant_id or "default").strip() or "default"
        username = str(self._config.security.bootstrap_admin_username or "admin").strip() or "admin"
        display_name = str(
            self._config.security.bootstrap_admin_display_name or "System Admin"
        ).strip() or "System Admin"
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT user_id FROM web_users WHERE tenant_id=? AND username=?",
                    (tenant_id, username),
                ).fetchone()
                if existing is None:
                    user_id = f"usr-{uuid4().hex[:12]}"
                    conn.execute(
                        """
                        INSERT INTO web_users (
                            user_id, tenant_id, username, password_hash, role,
                            display_name, is_active, created_at, updated_at, last_login_at
                        ) VALUES (?, ?, ?, ?, 'admin', ?, 1, ?, ?, '')
                        """,
                        (
                            user_id,
                            tenant_id,
                            username,
                            hash_password(bootstrap_password),
                            display_name,
                            _now_text(),
                            _now_text(),
                        ),
                    )
                    conn.commit()

    def _load_user(self, *, tenant_id: str, username: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM web_users
                WHERE tenant_id=? AND username=? AND is_active=1
                """,
                (tenant_id, username),
            ).fetchone()
        return dict(row) if row is not None else None

    def _load_user_by_id(self, *, tenant_id: str, user_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM web_users
                WHERE tenant_id=? AND user_id=? AND is_active=1
                """,
                (tenant_id, user_id),
            ).fetchone()
        return dict(row) if row is not None else None

    def _create_session_token(self, payload: dict[str, Any]) -> str:
        payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        encoded_payload = _b64url(payload_bytes)
        signature = hmac.new(
            self._session_secret().encode("utf-8"),
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return f"se1.{encoded_payload}.{_b64url(signature)}"

    def _decode_session_token(self, token: str) -> dict[str, Any]:
        try:
            version, encoded_payload, encoded_sig = str(token or "").split(".", 2)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail="登录态无效") from exc
        if version != "se1":
            raise HTTPException(status_code=401, detail="登录态版本不支持")
        expected_sig = hmac.new(
            self._session_secret().encode("utf-8"),
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(_b64url(expected_sig), encoded_sig):
            raise HTTPException(status_code=401, detail="登录态签名无效")
        payload = json.loads(_b64url_decode(encoded_payload).decode("utf-8"))
        if float(payload.get("exp", 0) or 0) < _now().timestamp():
            raise HTTPException(status_code=401, detail="登录态已过期")
        return payload if isinstance(payload, dict) else {}

    def login(self, *, username: str, password: str, tenant_id: str = "") -> dict[str, Any]:
        if not self.enabled:
            raise HTTPException(status_code=400, detail="当前环境未启用账号登录")
        normalized_tenant_id = str(
            tenant_id or self._config.security.default_tenant_id or "default"
        ).strip() or "default"
        user = self._load_user(tenant_id=normalized_tenant_id, username=username)
        if user is None or not verify_password(password, str(user.get("password_hash") or "")):
            raise HTTPException(status_code=401, detail="账号或密码错误")

        session_id = f"ses-{uuid4().hex[:16]}"
        issued_at = _now()
        expires_at = issued_at + timedelta(hours=max(int(self._config.security.session_ttl_hours or 24), 1))
        payload = {
            "sid": session_id,
            "uid": user["user_id"],
            "tenant": normalized_tenant_id,
            "role": user["role"],
            "username": user["username"],
            "display_name": user.get("display_name", ""),
            "iat": issued_at.timestamp(),
            "exp": expires_at.timestamp(),
        }
        token = self._create_session_token(payload)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO web_sessions (
                        session_id, user_id, tenant_id, issued_at, expires_at, revoked_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, '', ?)
                    """,
                    (
                        session_id,
                        user["user_id"],
                        normalized_tenant_id,
                        issued_at.strftime("%Y-%m-%d %H:%M:%S"),
                        expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                        issued_at.strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                conn.execute(
                    """
                    UPDATE web_users
                    SET last_login_at=?, updated_at=?
                    WHERE tenant_id=? AND user_id=?
                    """,
                    (
                        issued_at.strftime("%Y-%m-%d %H:%M:%S"),
                        issued_at.strftime("%Y-%m-%d %H:%M:%S"),
                        normalized_tenant_id,
                        user["user_id"],
                    ),
                )
                conn.commit()
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": {
                "user_id": user["user_id"],
                "username": user["username"],
                "role": user["role"],
                "tenant_id": normalized_tenant_id,
                "display_name": user.get("display_name", ""),
            },
        }

    def register(
        self,
        *,
        username: str,
        password: str,
        tenant_id: str = "",
        display_name: str = "",
    ) -> dict[str, Any]:
        normalized_username = str(username or "").strip()
        normalized_password = str(password or "")
        if not normalized_username:
            raise HTTPException(status_code=400, detail="账号不能为空")
        if len(normalized_password) < 6:
            raise HTTPException(status_code=400, detail="密码至少 6 位")
        normalized_tenant_id = str(
            tenant_id or self._config.security.default_tenant_id or "default"
        ).strip() or "default"
        normalized_display_name = str(display_name or "").strip() or normalized_username
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT user_id FROM web_users WHERE tenant_id=? AND username=?",
                    (normalized_tenant_id, normalized_username),
                ).fetchone()
                if existing is not None:
                    raise HTTPException(status_code=409, detail="账号已存在")
                user_id = f"usr-{uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO web_users (
                        user_id, tenant_id, username, password_hash, role,
                        display_name, is_active, created_at, updated_at, last_login_at
                    ) VALUES (?, ?, ?, ?, 'admin', ?, 1, ?, ?, '')
                    """,
                    (
                        user_id,
                        normalized_tenant_id,
                        normalized_username,
                        hash_password(normalized_password),
                        normalized_display_name,
                        _now_text(),
                        _now_text(),
                    ),
                )
                conn.commit()
        return self.login(
            username=normalized_username,
            password=normalized_password,
            tenant_id=normalized_tenant_id,
        )

    def authenticate_bearer(self, token: str) -> UserIdentity:
        if not self.enabled:
            raise HTTPException(status_code=401, detail="当前环境未启用账号登录")
        payload = self._decode_session_token(token)
        tenant_id = str(payload.get("tenant") or "").strip() or "default"
        user_id = str(payload.get("uid") or "").strip()
        session_id = str(payload.get("sid") or "").strip()
        with self._connect() as conn:
            session = conn.execute(
                """
                SELECT *
                FROM web_sessions
                WHERE tenant_id=? AND session_id=? AND user_id=? AND revoked_at=''
                """,
                (tenant_id, session_id, user_id),
            ).fetchone()
        if session is None:
            raise HTTPException(status_code=401, detail="登录态已失效")
        user = self._load_user_by_id(tenant_id=tenant_id, user_id=user_id)
        if user is None:
            raise HTTPException(status_code=401, detail="当前账号不可用")
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE web_sessions
                    SET last_seen_at=?
                    WHERE tenant_id=? AND session_id=?
                    """,
                    (_now_text(), tenant_id, session_id),
                )
                conn.commit()
        return UserIdentity(
            user_id=user["user_id"],
            username=user["username"],
            role=user["role"],
            tenant_id=tenant_id,
            display_name=str(user.get("display_name") or ""),
            auth_mode="session",
        )

    def authenticate_request(self, request: Request, *, expected_api_token: str) -> UserIdentity:
        auth_header = str(request.headers.get("authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            identity = self.authenticate_bearer(auth_header[7:].strip())
            request.state.identity = identity
            return identity

        provided_token = str(request.headers.get("x-api-token") or "").strip()
        if expected_api_token and provided_token and secrets.compare_digest(
            provided_token,
            expected_api_token,
        ):
            identity = UserIdentity(
                user_id="token-admin",
                username="token-admin",
                role="admin",
                tenant_id=str(self._config.security.default_tenant_id or "default").strip()
                or "default",
                display_name="Token Admin",
                auth_mode="token",
            )
            request.state.identity = identity
            return identity

        raise HTTPException(status_code=401, detail="鉴权失败")
