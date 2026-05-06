"""Secret encryption helpers used by configuration and API key storage."""

from __future__ import annotations

from base64 import urlsafe_b64encode
from hashlib import sha256

try:  # pragma: no cover - dependency availability is environment-specific
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover
    Fernet = None
    InvalidToken = Exception


ENCRYPTED_PREFIX = "enc:v1:"


def _build_fernet(secret_key: str) -> Fernet:
    if Fernet is None:  # pragma: no cover
        raise RuntimeError(
            "缺少 cryptography 依赖，无法加密敏感配置。请安装项目依赖后重试。"
        )
    normalized = str(secret_key or "").strip()
    if not normalized:
        raise ValueError("config secret key is empty")
    digest = sha256(normalized.encode("utf-8")).digest()
    return Fernet(urlsafe_b64encode(digest))


def is_encrypted_secret(value: str) -> bool:
    return str(value or "").startswith(ENCRYPTED_PREFIX)


def encrypt_secret(secret_key: str, plaintext: str) -> str:
    normalized_text = str(plaintext or "")
    if not normalized_text:
        return ""
    token = _build_fernet(secret_key).encrypt(normalized_text.encode("utf-8"))
    return ENCRYPTED_PREFIX + token.decode("utf-8")


def decrypt_secret(secret_key: str, ciphertext: str) -> str:
    normalized_value = str(ciphertext or "").strip()
    if not normalized_value:
        return ""
    if not is_encrypted_secret(normalized_value):
        return normalized_value
    payload = normalized_value[len(ENCRYPTED_PREFIX) :]
    try:
        return _build_fernet(secret_key).decrypt(payload.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:  # pragma: no cover - depends on runtime secret mismatch
        raise RuntimeError("配置中的密文无法解密，请检查 SMART_EXTRACTOR_CONFIG_SECRET_KEY") from exc
