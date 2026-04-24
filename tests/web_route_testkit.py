from __future__ import annotations

import importlib
from pathlib import Path

import yaml
from fastapi.testclient import TestClient


def _write_test_config(
    config_path: Path,
    output_dir: Path,
    rate_limit: int,
    dispatch_mode: str = "inline",
    start_builtin_worker: bool = False,
    start_builtin_monitor_scheduler: bool = True,
    start_builtin_notification_retry: bool = True,
    start_builtin_notification_digest: bool = True,
    allowed_hosts: list[str] | None = None,
    trusted_proxy_ips: list[str] | None = None,
    request_max_body_bytes: int = 1048576,
) -> None:
    config_data = {
        "llm": {
            "api_key": "",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "max_retries": 1,
            "timeout": 5,
        },
        "storage": {
            "output_dir": str(output_dir),
            "default_format": "json",
            "sqlite_db_name": "test.db",
        },
        "web": {
            "api_token": "test-token",
            "rate_limit_per_minute": int(rate_limit),
            "allowed_hosts": list(allowed_hosts or []),
            "trusted_proxy_ips": list(trusted_proxy_ips or []),
            "request_max_body_bytes": int(request_max_body_bytes),
            "security_headers_enabled": True,
            "task_dispatch_mode": str(dispatch_mode),
            "start_builtin_worker": bool(start_builtin_worker),
            "worker_poll_interval_seconds": 0.2,
            "worker_stale_after_seconds": 5.0,
            "monitor_scheduler_poll_interval_seconds": 0.2,
            "monitor_scheduler_batch_size": 5,
            "start_builtin_monitor_scheduler": bool(start_builtin_monitor_scheduler),
            "start_builtin_notification_retry": bool(
                start_builtin_notification_retry
            ),
            "notification_retry_poll_interval_seconds": 0.2,
            "notification_retry_batch_size": 10,
            "start_builtin_notification_digest": bool(
                start_builtin_notification_digest
            ),
            "notification_digest_poll_interval_seconds": 0.2,
            "notification_digest_batch_size": 10,
            "startup_check_enabled": True,
            "startup_check_verify_model": False,
            "startup_check_timeout": 5,
        },
        "log": {
            "level": "INFO",
            "log_dir": str(output_dir / "logs"),
            "rotation": "10 MB",
            "retention": "1 day",
        },
    }
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as config_file:
        yaml.safe_dump(config_data, config_file, allow_unicode=True, sort_keys=False)


def _build_test_client(
    monkeypatch,
    tmp_path: Path,
    rate_limit: int = 120,
    dispatch_mode: str = "inline",
    start_builtin_worker: bool = False,
    start_builtin_monitor_scheduler: bool = True,
    start_builtin_notification_retry: bool = True,
    start_builtin_notification_digest: bool = True,
    allowed_hosts: list[str] | None = None,
    trusted_proxy_ips: list[str] | None = None,
    request_max_body_bytes: int = 1048576,
):
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = tmp_path / "config" / "default.yaml"
    _write_test_config(
        config_path,
        output_dir,
        rate_limit=rate_limit,
        dispatch_mode=dispatch_mode,
        start_builtin_worker=start_builtin_worker,
        start_builtin_monitor_scheduler=start_builtin_monitor_scheduler,
        start_builtin_notification_retry=start_builtin_notification_retry,
        start_builtin_notification_digest=start_builtin_notification_digest,
        allowed_hosts=allowed_hosts,
        trusted_proxy_ips=trusted_proxy_ips,
        request_max_body_bytes=request_max_body_bytes,
    )

    import smart_extractor.config as config_module

    monkeypatch.setattr(config_module, "DEFAULT_CONFIG_PATH", config_path)
    monkeypatch.setenv("SMART_EXTRACTOR_API_KEY", "test-api-key")
    monkeypatch.setenv("SMART_EXTRACTOR_WEB_API_TOKEN", "test-token")
    monkeypatch.setenv("SMART_EXTRACTOR_STARTUP_CHECK_ENABLED", "true")
    monkeypatch.setenv("SMART_EXTRACTOR_STARTUP_CHECK_VERIFY_MODEL", "false")
    monkeypatch.setenv("SMART_EXTRACTOR_WEB_RATE_LIMIT_PER_MINUTE", str(rate_limit))
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_ALLOWED_HOSTS",
        ",".join(allowed_hosts or []),
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_TRUSTED_PROXY_IPS",
        ",".join(trusted_proxy_ips or []),
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_REQUEST_MAX_BODY_BYTES",
        str(request_max_body_bytes),
    )
    monkeypatch.setenv("SMART_EXTRACTOR_WEB_TASK_DISPATCH_MODE", str(dispatch_mode))
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_START_BUILTIN_WORKER",
        "true" if start_builtin_worker else "false",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_MONITOR_SCHEDULER_POLL_INTERVAL_SECONDS",
        "0.2",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_MONITOR_SCHEDULER_BATCH_SIZE",
        "5",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_START_BUILTIN_MONITOR_SCHEDULER",
        "true" if start_builtin_monitor_scheduler else "false",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_START_BUILTIN_NOTIFICATION_RETRY",
        "true" if start_builtin_notification_retry else "false",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_NOTIFICATION_RETRY_POLL_INTERVAL_SECONDS",
        "0.2",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_NOTIFICATION_RETRY_BATCH_SIZE",
        "10",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_START_BUILTIN_NOTIFICATION_DIGEST",
        "true" if start_builtin_notification_digest else "false",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_NOTIFICATION_DIGEST_POLL_INTERVAL_SECONDS",
        "0.2",
    )
    monkeypatch.setenv(
        "SMART_EXTRACTOR_WEB_NOTIFICATION_DIGEST_BATCH_SIZE",
        "10",
    )

    import smart_extractor.web.routes as routes_module
    import smart_extractor.web.app as app_module

    routes_module = importlib.reload(routes_module)
    app_module = importlib.reload(app_module)

    return TestClient(app_module.app), routes_module
