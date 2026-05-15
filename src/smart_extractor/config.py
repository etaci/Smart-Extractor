"""
配置管理模块。

使用 pydantic-settings 统一管理配置，支持 YAML 与环境变量覆盖。
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings

from smart_extractor.security.crypto import decrypt_secret, encrypt_secret


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "default.yaml"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_SCHEMA_DIR = PROJECT_ROOT / "config" / "schemas"


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def load_raw_yaml_config(config_path: str | Path | None = None) -> dict[str, Any]:
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    path = Path(config_path)
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def resolve_local_config_path(config_path: str | Path | None = None) -> Path:
    base_path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_PATH
    return base_path.with_name("local.yaml")


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def save_raw_yaml_config(
    config_data: dict[str, Any], config_path: str | Path | None = None
) -> Path:
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        yaml.safe_dump(config_data, file, allow_unicode=True, sort_keys=False)
    return path


def update_llm_basic_config(
    api_key: str,
    base_url: str,
    model: str,
    temperature: float,
    config_path: str | Path | None = None,
    config_secret_key: str = "",
) -> Path:
    if config_path is None:
        config_path = resolve_local_config_path()

    config_data = load_raw_yaml_config(config_path)
    llm_data = dict(config_data.get("llm", {}))
    normalized_api_key = str(api_key)
    if config_secret_key and normalized_api_key:
        llm_data["api_key_encrypted"] = encrypt_secret(
            config_secret_key,
            normalized_api_key,
        )
        llm_data["api_key"] = ""
    else:
        llm_data["api_key"] = normalized_api_key
        llm_data.pop("api_key_encrypted", None)
    llm_data.update(
        {
            "base_url": str(base_url),
            "model": str(model),
            "temperature": float(temperature),
        }
    )
    config_data["llm"] = llm_data
    if config_secret_key:
        security_data = dict(config_data.get("security", {}))
        security_data["config_secret_key"] = str(config_secret_key)
        config_data["security"] = security_data
    return save_raw_yaml_config(config_data, config_path)


class LLMConfig(BaseSettings):
    api_key: str = Field(default="", description="LLM API 密钥")
    base_url: str = Field(
        default="https://api.openai.com/v1", description="API 基础地址"
    )
    model: str = Field(default="gpt-4o-mini", description="模型名称")
    temperature: float = Field(default=0.0, description="生成温度")
    max_tokens: int = Field(default=4096, description="最大输出 token 数")
    max_retries: int = Field(default=3, description="API 最大重试次数")
    timeout: int = Field(default=60, description="API 超时时间，单位秒")
    rule_precheck_enabled: bool = Field(
        default=True, description="是否在 LLM 之前先尝试低成本规则抽取"
    )
    rule_precheck_min_completeness: float = Field(
        default=0.85, description="规则预检命中 LLM 前的最低完整度"
    )
    rule_precheck_min_fields: int = Field(
        default=2, description="规则预检认为可用时的最少非空字段数"
    )


class FetcherConfig(BaseSettings):
    headless: bool = Field(default=True, description="是否使用无头浏览器")
    timeout: int = Field(default=30000, description="页面加载超时，单位毫秒")
    wait_after_load: int = Field(
        default=2000, description="页面加载后额外等待时间，单位毫秒"
    )
    viewport_width: int = Field(default=1920, description="浏览器视口宽度")
    viewport_height: int = Field(default=1080, description="浏览器视口高度")
    locale: str = Field(default="zh-CN", description="浏览器语言区域")
    timezone_id: str = Field(default="Asia/Shanghai", description="浏览器时区 ID")
    verify_ssl: bool = Field(default=True, description="是否校验 HTTPS 证书")
    screenshot: bool = Field(default=False, description="是否保存页面截图")
    screenshot_dir: str = Field(default="screenshots", description="截图保存目录")
    user_agent: Optional[str] = Field(default=None, description="自定义 User-Agent")
    proxy_url: Optional[str] = Field(default=None, description="抓取阶段使用的代理 URL")
    proxy_urls: list[str] = Field(
        default_factory=list,
        description="抓取阶段可轮换使用的代理 URL 列表",
    )
    storage_state_path: Optional[str] = Field(
        default=None, description="Playwright storage_state 文件路径"
    )
    persistent_context_dir: Optional[str] = Field(
        default=None, description="Playwright 持久化浏览器 Profile 目录"
    )
    challenge_retry_attempts: int = Field(
        default=2, description="遇到反爬/挑战页时的额外尝试次数"
    )
    challenge_retry_backoff_ms: int = Field(
        default=1500, description="反爬/挑战页再次尝试前的等待时间，毫秒"
    )
    browser_session_pool_size: int = Field(
        default=2,
        description="动态抓取浏览器会话池大小",
    )
    persistent_profile_pool_size: int = Field(
        default=1,
        description="持久化浏览器 Profile 池大小",
    )
    proxy_rotation_enabled: bool = Field(
        default=True,
        description="是否在多代理场景下自动轮换代理",
    )
    fetch_max_attempts: int = Field(
        default=3,
        description="抓取器跨代理/跨会话的最大总尝试次数",
    )
    challenge_fallback_to_static: bool = Field(
        default=True,
        description="动态抓取疑似被拦截时是否自动回退到静态抓取",
    )


class CleanerConfig(BaseSettings):
    remove_tags: list[str] = Field(
        default=[
            "script",
            "style",
            "nav",
            "footer",
            "header",
            "aside",
            "iframe",
            "noscript",
        ],
        description="需要移除的 HTML 标签列表",
    )
    max_text_length: int = Field(default=8000, description="清洗后最大文本长度")
    keep_structure: bool = Field(default=True, description="是否保留标题、列表等结构")


class StorageConfig(BaseSettings):
    output_dir: str = Field(default=str(DEFAULT_OUTPUT_DIR), description="输出目录")
    default_format: str = Field(default="json", description="默认输出格式")
    sqlite_db_name: str = Field(
        default="extracted_data.db", description="SQLite 数据库文件名"
    )
    database_url: str = Field(
        default="",
        description="主数据库 URL，支持 sqlite:/// 与 postgresql://",
    )
    task_store_database_url: str = Field(
        default="",
        description="任务元数据数据库 URL，未设置时回退到 database_url",
    )
    backup_dir: str = Field(
        default=str(DEFAULT_OUTPUT_DIR / "backups"),
        description="应用级数据库备份目录",
    )
    csv_encoding: str = Field(default="utf-8-sig", description="CSV 编码")
    sqlite_busy_timeout_ms: int = Field(
        default=5000,
        description="SQLite busy timeout，单位毫秒",
    )
    sqlite_enable_wal: bool = Field(
        default=True,
        description="是否启用 SQLite WAL 模式以提升并发读写能力",
    )
    sqlite_synchronous: str = Field(
        default="NORMAL",
        description="SQLite synchronous 模式",
    )


class SchedulerConfig(BaseSettings):
    max_concurrency: int = Field(default=3, description="最大并发任务数")
    request_delay_min: float = Field(default=1.0, description="请求最小间隔，单位秒")
    request_delay_max: float = Field(default=3.0, description="请求最大间隔，单位秒")
    max_retries: int = Field(default=2, description="失败任务最大重试次数")


class LogConfig(BaseSettings):
    level: str = Field(default="INFO", description="日志级别")
    log_dir: str = Field(default="logs", description="日志目录")
    rotation: str = Field(default="10 MB", description="日志轮转大小")
    retention: str = Field(default="30 days", description="日志保留时间")
    format: str = Field(
        default=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "req={extra[request_id]} | task={extra[task_id]} | "
            "{name}:{function}:{line} | {message}"
        ),
        description="日志格式",
    )


class WebConfig(BaseSettings):
    api_token: str = Field(default="", description="Web API 鉴权 Token")
    rate_limit_per_minute: int = Field(default=120, description="每分钟最大请求数")
    allowed_hosts: list[str] = Field(
        default_factory=list,
        description="允许访问的 Host 列表；为空表示不限制",
    )
    trusted_proxy_ips: list[str] = Field(
        default_factory=list,
        description="可信反向代理 IP 列表；仅来自这些代理时才信任 X-Forwarded-For",
    )
    request_max_body_bytes: int = Field(
        default=1048576,
        description="允许的最大请求体大小，单位字节",
    )
    security_headers_enabled: bool = Field(
        default=True,
        description="是否自动附加基础安全响应头",
    )
    task_dispatch_mode: str = Field(
        default="inline",
        description="Web 任务分发模式：inline、queue 或 redis",
    )
    redis_url: str = Field(
        default="",
        description="Redis 队列连接地址，task_dispatch_mode=redis 时使用",
    )
    redis_queue_name: str = Field(
        default="smart-extractor:dispatch",
        description="Redis 队列名称前缀",
    )
    redis_visibility_timeout_seconds: float = Field(
        default=300.0,
        description="Redis 队列任务可见性超时，单位秒",
    )
    start_builtin_worker: bool = Field(
        default=False,
        description="队列模式下是否在 Web 进程内启动内置 worker",
    )
    worker_queue_scope: str = Field(
        default="*",
        description="Worker 可消费的队列范围；* 表示全部，其他值表示指定 worker group",
    )
    worker_poll_interval_seconds: float = Field(
        default=2.0,
        description="队列 worker 轮询间隔，单位秒",
    )
    worker_stale_after_seconds: float = Field(
        default=300.0,
        description="运行中队列任务的超时接管阈值，单位秒",
    )
    monitor_scheduler_poll_interval_seconds: float = Field(
        default=15.0,
        description="监控调度轮询间隔，单位秒",
    )
    monitor_scheduler_batch_size: int = Field(
        default=5,
        description="每轮最多触发的自动巡检数",
    )
    monitor_scheduler_lease_seconds: float = Field(
        default=120.0,
        description="监控调度抢占租约时长，单位秒",
    )
    start_builtin_monitor_scheduler: bool = Field(
        default=True,
        description="是否在 Web 进程内启动内置监控调度器",
    )
    start_builtin_notification_retry: bool = Field(
        default=True,
        description="是否在 Web 进程内启动内置通知自动重试服务",
    )
    notification_retry_poll_interval_seconds: float = Field(
        default=20.0,
        description="通知自动重试轮询间隔，单位秒",
    )
    notification_retry_batch_size: int = Field(
        default=10,
        description="通知自动重试每轮最多处理的事件数",
    )
    start_builtin_notification_digest: bool = Field(
        default=True,
        description="是否在 Web 进程内启动内置日报 Digest 自动发送服务",
    )
    notification_digest_poll_interval_seconds: float = Field(
        default=60.0,
        description="每日 Digest 自动发送轮询间隔，单位秒",
    )
    notification_digest_batch_size: int = Field(
        default=10,
        description="每轮最多处理的 Digest 目标数",
    )
    startup_check_enabled: bool = Field(default=True, description="是否启用启动自检")
    startup_check_verify_model: bool = Field(
        default=True, description="是否校验模型可用性"
    )
    startup_check_timeout: int = Field(
        default=15, description="启动自检超时时间，单位秒"
    )
    csrf_protection_enabled: bool = Field(
        default=True,
        description="是否对浏览器发起的状态变更请求启用 CSRF / Origin 校验",
    )
    csrf_allowed_origins: list[str] = Field(
        default_factory=list,
        description="允许的跨源 Origin 列表（除当前 Host 外）。留空只允许同源",
    )


class SecurityConfig(BaseSettings):
    config_secret_key: str = Field(
        default="",
        description="用于加密 local.yaml 敏感配置的主密钥",
    )
    auth_secret_key: str = Field(
        default="",
        description="用于签发登录会话 token 的密钥",
    )
    bootstrap_admin_username: str = Field(
        default="admin",
        description="首个管理员账号名",
    )
    bootstrap_admin_password: str = Field(
        default="",
        description="首个管理员密码",
    )
    bootstrap_admin_display_name: str = Field(
        default="System Admin",
        description="首个管理员显示名",
    )
    default_tenant_id: str = Field(
        default="default",
        description="默认租户 ID",
    )
    session_ttl_hours: int = Field(
        default=24,
        description="登录会话有效期，单位小时",
    )
    export_allowed_roles: list[str] = Field(
        default_factory=lambda: ["admin", "operator"],
        description="允许导出任务结果的角色列表",
    )
    audit_log_enabled: bool = Field(
        default=True,
        description="是否启用审计日志",
    )


class AppConfig(BaseSettings):
    llm: LLMConfig = Field(default_factory=LLMConfig)
    fetcher: FetcherConfig = Field(default_factory=FetcherConfig)
    cleaner: CleanerConfig = Field(default_factory=CleanerConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    log: LogConfig = Field(default_factory=LogConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)

    @classmethod
    def from_yaml(
        cls,
        config_path: str | Path | None = None,
        local_config_path: str | Path | None = None,
    ) -> "AppConfig":
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH
        if local_config_path is None:
            local_config_path = resolve_local_config_path(config_path)

        config_path = Path(config_path)
        config_data = load_raw_yaml_config(config_path)
        local_config_data = load_raw_yaml_config(local_config_path)
        merged_config = _merge_dicts(config_data, local_config_data)

        env_api_key = os.environ.get("SMART_EXTRACTOR_API_KEY", "")
        env_base_url = os.environ.get("SMART_EXTRACTOR_BASE_URL", "")
        env_model = os.environ.get("SMART_EXTRACTOR_MODEL", "")

        env_fetcher_verify_ssl = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_VERIFY_SSL", ""
        )
        env_fetcher_locale = os.environ.get("SMART_EXTRACTOR_FETCHER_LOCALE", "")
        env_fetcher_timezone = os.environ.get("SMART_EXTRACTOR_FETCHER_TIMEZONE_ID", "")
        env_fetcher_proxy_url = os.environ.get("SMART_EXTRACTOR_FETCHER_PROXY_URL", "")
        env_fetcher_proxy_urls = os.environ.get("SMART_EXTRACTOR_FETCHER_PROXY_URLS", "")
        env_fetcher_storage_state = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_STORAGE_STATE_PATH", ""
        )
        env_fetcher_persistent_context = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_PERSISTENT_CONTEXT_DIR", ""
        )
        env_fetcher_browser_session_pool_size = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_BROWSER_SESSION_POOL_SIZE", ""
        )
        env_fetcher_persistent_profile_pool_size = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_PERSISTENT_PROFILE_POOL_SIZE", ""
        )
        env_fetcher_proxy_rotation_enabled = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_PROXY_ROTATION_ENABLED", ""
        )
        env_fetcher_fetch_max_attempts = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_FETCH_MAX_ATTEMPTS", ""
        )
        env_fetcher_challenge_fallback_to_static = os.environ.get(
            "SMART_EXTRACTOR_FETCHER_CHALLENGE_FALLBACK_TO_STATIC", ""
        )
        env_storage_sqlite_busy_timeout_ms = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_SQLITE_BUSY_TIMEOUT_MS", ""
        )
        env_storage_sqlite_enable_wal = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_SQLITE_ENABLE_WAL", ""
        )
        env_storage_sqlite_synchronous = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_SQLITE_SYNCHRONOUS", ""
        )
        env_storage_database_url = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_DATABASE_URL", ""
        )
        env_storage_task_store_database_url = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_TASK_STORE_DATABASE_URL", ""
        )
        env_storage_backup_dir = os.environ.get(
            "SMART_EXTRACTOR_STORAGE_BACKUP_DIR", ""
        )

        env_web_token = os.environ.get("SMART_EXTRACTOR_WEB_API_TOKEN", "")
        env_web_rate_limit = os.environ.get(
            "SMART_EXTRACTOR_WEB_RATE_LIMIT_PER_MINUTE", ""
        )
        env_web_allowed_hosts = os.environ.get(
            "SMART_EXTRACTOR_WEB_ALLOWED_HOSTS", ""
        )
        env_web_trusted_proxy_ips = os.environ.get(
            "SMART_EXTRACTOR_WEB_TRUSTED_PROXY_IPS", ""
        )
        env_web_request_max_body_bytes = os.environ.get(
            "SMART_EXTRACTOR_WEB_REQUEST_MAX_BODY_BYTES", ""
        )
        env_web_security_headers_enabled = os.environ.get(
            "SMART_EXTRACTOR_WEB_SECURITY_HEADERS_ENABLED", ""
        )
        env_web_dispatch_mode = os.environ.get(
            "SMART_EXTRACTOR_WEB_TASK_DISPATCH_MODE", ""
        )
        env_web_redis_url = os.environ.get("SMART_EXTRACTOR_WEB_REDIS_URL", "")
        env_web_redis_queue_name = os.environ.get(
            "SMART_EXTRACTOR_WEB_REDIS_QUEUE_NAME", ""
        )
        env_web_redis_visibility_timeout = os.environ.get(
            "SMART_EXTRACTOR_WEB_REDIS_VISIBILITY_TIMEOUT_SECONDS", ""
        )
        env_web_start_builtin_worker = os.environ.get(
            "SMART_EXTRACTOR_WEB_START_BUILTIN_WORKER", ""
        )
        env_web_worker_queue_scope = os.environ.get(
            "SMART_EXTRACTOR_WEB_WORKER_QUEUE_SCOPE", ""
        )
        env_web_worker_poll = os.environ.get(
            "SMART_EXTRACTOR_WEB_WORKER_POLL_INTERVAL_SECONDS", ""
        )
        env_web_worker_stale_after = os.environ.get(
            "SMART_EXTRACTOR_WEB_WORKER_STALE_AFTER_SECONDS", ""
        )
        env_monitor_scheduler_poll = os.environ.get(
            "SMART_EXTRACTOR_WEB_MONITOR_SCHEDULER_POLL_INTERVAL_SECONDS", ""
        )
        env_monitor_scheduler_batch_size = os.environ.get(
            "SMART_EXTRACTOR_WEB_MONITOR_SCHEDULER_BATCH_SIZE", ""
        )
        env_monitor_scheduler_lease_seconds = os.environ.get(
            "SMART_EXTRACTOR_WEB_MONITOR_SCHEDULER_LEASE_SECONDS", ""
        )
        env_web_start_builtin_monitor_scheduler = os.environ.get(
            "SMART_EXTRACTOR_WEB_START_BUILTIN_MONITOR_SCHEDULER", ""
        )
        env_web_start_builtin_notification_retry = os.environ.get(
            "SMART_EXTRACTOR_WEB_START_BUILTIN_NOTIFICATION_RETRY", ""
        )
        env_notification_retry_poll = os.environ.get(
            "SMART_EXTRACTOR_WEB_NOTIFICATION_RETRY_POLL_INTERVAL_SECONDS", ""
        )
        env_notification_retry_batch_size = os.environ.get(
            "SMART_EXTRACTOR_WEB_NOTIFICATION_RETRY_BATCH_SIZE", ""
        )
        env_web_start_builtin_notification_digest = os.environ.get(
            "SMART_EXTRACTOR_WEB_START_BUILTIN_NOTIFICATION_DIGEST", ""
        )
        env_notification_digest_poll = os.environ.get(
            "SMART_EXTRACTOR_WEB_NOTIFICATION_DIGEST_POLL_INTERVAL_SECONDS", ""
        )
        env_notification_digest_batch_size = os.environ.get(
            "SMART_EXTRACTOR_WEB_NOTIFICATION_DIGEST_BATCH_SIZE", ""
        )
        env_startup_check_enabled = os.environ.get(
            "SMART_EXTRACTOR_STARTUP_CHECK_ENABLED", ""
        )
        env_startup_check_verify_model = os.environ.get(
            "SMART_EXTRACTOR_STARTUP_CHECK_VERIFY_MODEL", ""
        )
        env_startup_check_timeout = os.environ.get(
            "SMART_EXTRACTOR_STARTUP_CHECK_TIMEOUT", ""
        )
        env_security_config_secret_key = os.environ.get(
            "SMART_EXTRACTOR_CONFIG_SECRET_KEY", ""
        )
        env_security_auth_secret_key = os.environ.get(
            "SMART_EXTRACTOR_AUTH_SECRET_KEY", ""
        )
        env_security_bootstrap_admin_username = os.environ.get(
            "SMART_EXTRACTOR_BOOTSTRAP_ADMIN_USERNAME", ""
        )
        env_security_bootstrap_admin_password = os.environ.get(
            "SMART_EXTRACTOR_BOOTSTRAP_ADMIN_PASSWORD", ""
        )
        env_security_bootstrap_admin_display_name = os.environ.get(
            "SMART_EXTRACTOR_BOOTSTRAP_ADMIN_DISPLAY_NAME", ""
        )
        env_security_default_tenant_id = os.environ.get(
            "SMART_EXTRACTOR_DEFAULT_TENANT_ID", ""
        )
        env_security_session_ttl_hours = os.environ.get(
            "SMART_EXTRACTOR_SESSION_TTL_HOURS", ""
        )
        env_security_export_allowed_roles = os.environ.get(
            "SMART_EXTRACTOR_EXPORT_ALLOWED_ROLES", ""
        )
        env_security_audit_log_enabled = os.environ.get(
            "SMART_EXTRACTOR_AUDIT_LOG_ENABLED", ""
        )

        llm_data = merged_config.get("llm", {})
        if env_api_key:
            llm_data["api_key"] = env_api_key
        if env_base_url:
            llm_data["base_url"] = env_base_url
        if env_model:
            llm_data["model"] = env_model
        config_secret_key = (
            env_security_config_secret_key
            or str(
                merged_config.get("security", {}).get("config_secret_key", "")
                if isinstance(merged_config.get("security"), dict)
                else ""
            )
        )
        encrypted_api_key = str(llm_data.get("api_key_encrypted", "") or "").strip()
        if not env_api_key and encrypted_api_key and config_secret_key:
            llm_data["api_key"] = decrypt_secret(config_secret_key, encrypted_api_key)
        llm_data.pop("api_key_encrypted", None)

        fetcher_data = merged_config.get("fetcher", {})
        if env_fetcher_verify_ssl:
            fetcher_data["verify_ssl"] = _parse_bool(env_fetcher_verify_ssl)
        if env_fetcher_locale:
            fetcher_data["locale"] = env_fetcher_locale
        if env_fetcher_timezone:
            fetcher_data["timezone_id"] = env_fetcher_timezone
        if env_fetcher_proxy_url:
            fetcher_data["proxy_url"] = env_fetcher_proxy_url
        if env_fetcher_proxy_urls:
            fetcher_data["proxy_urls"] = _parse_csv_list(env_fetcher_proxy_urls)
        if env_fetcher_storage_state:
            fetcher_data["storage_state_path"] = env_fetcher_storage_state
        if env_fetcher_persistent_context:
            fetcher_data["persistent_context_dir"] = env_fetcher_persistent_context
        if env_fetcher_browser_session_pool_size:
            try:
                fetcher_data["browser_session_pool_size"] = int(
                    env_fetcher_browser_session_pool_size
                )
            except ValueError:
                pass
        if env_fetcher_persistent_profile_pool_size:
            try:
                fetcher_data["persistent_profile_pool_size"] = int(
                    env_fetcher_persistent_profile_pool_size
                )
            except ValueError:
                pass
        if env_fetcher_proxy_rotation_enabled:
            fetcher_data["proxy_rotation_enabled"] = _parse_bool(
                env_fetcher_proxy_rotation_enabled
            )
        if env_fetcher_fetch_max_attempts:
            try:
                fetcher_data["fetch_max_attempts"] = int(
                    env_fetcher_fetch_max_attempts
                )
            except ValueError:
                pass
        if env_fetcher_challenge_fallback_to_static:
            fetcher_data["challenge_fallback_to_static"] = _parse_bool(
                env_fetcher_challenge_fallback_to_static
            )

        storage_data = merged_config.get("storage", {})
        if env_storage_sqlite_busy_timeout_ms:
            try:
                storage_data["sqlite_busy_timeout_ms"] = int(
                    env_storage_sqlite_busy_timeout_ms
                )
            except ValueError:
                pass
        if env_storage_sqlite_enable_wal:
            storage_data["sqlite_enable_wal"] = _parse_bool(
                env_storage_sqlite_enable_wal
            )
        if env_storage_sqlite_synchronous:
            storage_data["sqlite_synchronous"] = env_storage_sqlite_synchronous
        if env_storage_database_url:
            storage_data["database_url"] = env_storage_database_url
        if env_storage_task_store_database_url:
            storage_data["task_store_database_url"] = env_storage_task_store_database_url
        if env_storage_backup_dir:
            storage_data["backup_dir"] = env_storage_backup_dir

        web_data = merged_config.get("web", {})
        if env_web_token:
            web_data["api_token"] = env_web_token
        if env_web_rate_limit:
            try:
                web_data["rate_limit_per_minute"] = int(env_web_rate_limit)
            except ValueError:
                pass
        if env_web_allowed_hosts:
            web_data["allowed_hosts"] = _parse_csv_list(env_web_allowed_hosts)
        if env_web_trusted_proxy_ips:
            web_data["trusted_proxy_ips"] = _parse_csv_list(
                env_web_trusted_proxy_ips
            )
        if env_web_request_max_body_bytes:
            try:
                web_data["request_max_body_bytes"] = int(
                    env_web_request_max_body_bytes
                )
            except ValueError:
                pass
        if env_web_security_headers_enabled:
            web_data["security_headers_enabled"] = _parse_bool(
                env_web_security_headers_enabled
            )
        if env_web_dispatch_mode:
            web_data["task_dispatch_mode"] = env_web_dispatch_mode
        if env_web_redis_url:
            web_data["redis_url"] = env_web_redis_url
        if env_web_redis_queue_name:
            web_data["redis_queue_name"] = env_web_redis_queue_name
        if env_web_redis_visibility_timeout:
            try:
                web_data["redis_visibility_timeout_seconds"] = float(
                    env_web_redis_visibility_timeout
                )
            except ValueError:
                pass
        if env_web_start_builtin_worker:
            web_data["start_builtin_worker"] = _parse_bool(
                env_web_start_builtin_worker
            )
        if env_web_worker_queue_scope:
            web_data["worker_queue_scope"] = env_web_worker_queue_scope
        if env_web_worker_poll:
            try:
                web_data["worker_poll_interval_seconds"] = float(env_web_worker_poll)
            except ValueError:
                pass
        if env_web_worker_stale_after:
            try:
                web_data["worker_stale_after_seconds"] = float(
                    env_web_worker_stale_after
                )
            except ValueError:
                pass
        if env_monitor_scheduler_poll:
            try:
                web_data["monitor_scheduler_poll_interval_seconds"] = float(
                    env_monitor_scheduler_poll
                )
            except ValueError:
                pass
        if env_monitor_scheduler_batch_size:
            try:
                web_data["monitor_scheduler_batch_size"] = int(
                    env_monitor_scheduler_batch_size
                )
            except ValueError:
                pass
        if env_monitor_scheduler_lease_seconds:
            try:
                web_data["monitor_scheduler_lease_seconds"] = float(
                    env_monitor_scheduler_lease_seconds
                )
            except ValueError:
                pass
        if env_web_start_builtin_monitor_scheduler:
            web_data["start_builtin_monitor_scheduler"] = _parse_bool(
                env_web_start_builtin_monitor_scheduler
            )
        if env_web_start_builtin_notification_retry:
            web_data["start_builtin_notification_retry"] = _parse_bool(
                env_web_start_builtin_notification_retry
            )
        if env_notification_retry_poll:
            try:
                web_data["notification_retry_poll_interval_seconds"] = float(
                    env_notification_retry_poll
                )
            except ValueError:
                pass
        if env_notification_retry_batch_size:
            try:
                web_data["notification_retry_batch_size"] = int(
                    env_notification_retry_batch_size
                )
            except ValueError:
                pass
        if env_web_start_builtin_notification_digest:
            web_data["start_builtin_notification_digest"] = _parse_bool(
                env_web_start_builtin_notification_digest
            )
        if env_notification_digest_poll:
            try:
                web_data["notification_digest_poll_interval_seconds"] = float(
                    env_notification_digest_poll
                )
            except ValueError:
                pass
        if env_notification_digest_batch_size:
            try:
                web_data["notification_digest_batch_size"] = int(
                    env_notification_digest_batch_size
                )
            except ValueError:
                pass
        if env_startup_check_enabled:
            web_data["startup_check_enabled"] = _parse_bool(env_startup_check_enabled)
        if env_startup_check_verify_model:
            web_data["startup_check_verify_model"] = _parse_bool(
                env_startup_check_verify_model
            )
        if env_startup_check_timeout:
            try:
                web_data["startup_check_timeout"] = int(env_startup_check_timeout)
            except ValueError:
                pass

        security_data = merged_config.get("security", {})
        if env_security_config_secret_key:
            security_data["config_secret_key"] = env_security_config_secret_key
        if env_security_auth_secret_key:
            security_data["auth_secret_key"] = env_security_auth_secret_key
        if env_security_bootstrap_admin_username:
            security_data["bootstrap_admin_username"] = (
                env_security_bootstrap_admin_username
            )
        if env_security_bootstrap_admin_password:
            security_data["bootstrap_admin_password"] = (
                env_security_bootstrap_admin_password
            )
        if env_security_bootstrap_admin_display_name:
            security_data["bootstrap_admin_display_name"] = (
                env_security_bootstrap_admin_display_name
            )
        if env_security_default_tenant_id:
            security_data["default_tenant_id"] = env_security_default_tenant_id
        if env_security_session_ttl_hours:
            try:
                security_data["session_ttl_hours"] = int(
                    env_security_session_ttl_hours
                )
            except ValueError:
                pass
        if env_security_export_allowed_roles:
            security_data["export_allowed_roles"] = _parse_csv_list(
                env_security_export_allowed_roles
            )
        if env_security_audit_log_enabled:
            security_data["audit_log_enabled"] = _parse_bool(
                env_security_audit_log_enabled
            )

        return cls(
            llm=LLMConfig(**llm_data) if llm_data else LLMConfig(),
            fetcher=FetcherConfig(**fetcher_data),
            cleaner=CleanerConfig(**merged_config.get("cleaner", {})),
            storage=StorageConfig(**storage_data),
            scheduler=SchedulerConfig(**merged_config.get("scheduler", {})),
            log=LogConfig(**merged_config.get("log", {})),
            web=WebConfig(**web_data),
            security=SecurityConfig(**security_data),
        )


def load_config(
    config_path: str | Path | None = None,
    local_config_path: str | Path | None = None,
) -> AppConfig:
    return AppConfig.from_yaml(config_path, local_config_path)
