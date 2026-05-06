"""
CLI 命令行入口。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import typer
from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from smart_extractor import __version__
from smart_extractor.config import load_config
from smart_extractor.utils.encoding import configure_utf8_io
from smart_extractor.utils.logger import setup_logger
from smart_extractor.web.database_admin import (
    backup_task_store_database,
    migrate_task_store_database,
    restore_task_store_database,
)

APP_VERSION = __version__

try:
    configure_utf8_io()
except Exception as exc:
    logger.debug("Windows 终端编码切换失败: {}", exc)

app = typer.Typer(
    name="smart-extractor",
    help="基于 LLM 的网页自动字段抽取工具",
    add_completion=False,
    rich_markup_mode="rich",
)

console = Console()


def _show_banner() -> None:
    banner = """
[bold cyan]+==================================================+
|    Smart Data Extractor v{version:<22}  |
|    Schema And Auto Extraction                    |
+==================================================+[/bold cyan]
""".format(version=APP_VERSION)
    rprint(banner)


def _parse_selected_fields(raw_value: str) -> list[str]:
    return [item.strip() for item in str(raw_value or "").split(",") if item.strip()]


def _resolve_schema_mode(schema_name: str, selected_fields: list[str]) -> str:
    normalized = str(schema_name or "auto").strip().lower() or "auto"
    if normalized == "auto" and selected_fields:
        return "auto + 指定字段"
    return normalized


def _build_schema_table(schema_names: list[str]) -> Table:
    table = Table(title="可用 Schema", show_lines=True)
    table.add_column("名称", style="cyan", no_wrap=True)
    table.add_column("模式说明", style="white")
    for name in schema_names:
        mode = "AI 自动识别页面类型" if name == "auto" else "固定 Schema 抽取"
        table.add_row(name, mode)
    return table


def _build_task_store(config_file: Optional[str] = None):
    app_config = load_config(config_file)
    from smart_extractor.web.task_store import SQLiteTaskStore

    return app_config, SQLiteTaskStore(
        Path(app_config.storage.output_dir) / "web_tasks.db",
        database_url=(
            app_config.storage.task_store_database_url
            or app_config.storage.database_url
        ),
        default_tenant_id=app_config.security.default_tenant_id,
        sqlite_busy_timeout_ms=app_config.storage.sqlite_busy_timeout_ms,
        sqlite_enable_wal=app_config.storage.sqlite_enable_wal,
        sqlite_synchronous=app_config.storage.sqlite_synchronous,
    )


def _read_urls(url_file: str) -> list[str]:
    url_path = Path(url_file)
    if not url_path.exists():
        console.print(f"[bold red][FAIL] URL 文件不存在: {url_file}[/]")
        raise typer.Exit(code=1)
    urls = [
        line.strip()
        for line in url_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if not urls:
        console.print("[bold red][FAIL] URL 文件为空[/]")
        raise typer.Exit(code=1)
    return urls


@app.command()
def extract(
    url: str = typer.Argument(..., help="目标网页 URL"),
    schema_name: str = typer.Option(
        "auto", "--schema", "-s", help="Schema 名称，默认 auto"
    ),
    output_format: str = typer.Option("json", "--format", "-f", help="输出格式"),
    collection: str = typer.Option("default", "--collection", "-c", help="集合名"),
    selected_fields: str = typer.Option(
        "",
        "--fields",
        help="可选，逗号分隔的指定字段，例如 title,content,publish_date",
    ),
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    static_mode: bool = typer.Option(False, "--static", help="使用静态抓取"),
    selector: Optional[str] = typer.Option(None, "--selector", help="CSS 选择器"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="显示详细日志"),
):
    """提取单个网页，可用固定 Schema 或自动模式。"""
    _show_banner()

    app_config = load_config(config_file)
    if verbose:
        app_config.log.level = "DEBUG"
    setup_logger(app_config.log)

    from smart_extractor.pipeline import ExtractionPipeline

    fields = _parse_selected_fields(selected_fields)
    mode_label = _resolve_schema_mode(schema_name, fields)

    console.print(f"\n[bold green]目标 URL:[/] {url}")
    console.print(f"[bold green]模式:[/] {mode_label}")
    console.print(f"[bold green]格式:[/] {output_format}")
    console.print(f"[bold green]字段:[/] {', '.join(fields) if fields else '自动选择'}")
    console.print()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("正在初始化 Pipeline...", total=5)
        with ExtractionPipeline(
            config=app_config,
            use_dynamic_fetcher=not static_mode,
        ) as pipeline:
            progress.update(task, advance=1, description="正在抓取网页...")
            result = pipeline.run(
                url=url,
                schema_name=schema_name,
                storage_format=output_format,
                collection_name=collection,
                css_selector=selector,
                selected_fields=fields,
            )
            progress.update(task, advance=4, description="处理完成")

    console.print()
    if result.success:
        formatted_text = (
            getattr(result.data, "formatted_text", "") if result.data else ""
        )
        console.print(
            Panel(
                f"[bold green][PASS] 提取成功[/]\n\n"
                f"耗时: {result.elapsed_ms:.0f}ms\n"
                f"质量分: {result.validation.quality_score:.1%}\n"
                f"保存至: {result.storage_path or '-'}",
                title="执行结果",
                border_style="green",
            )
        )
        if formatted_text:
            console.print("\n[bold cyan]润色结果:[/]")
            console.print(formatted_text)
        elif result.data:
            console.print_json(result.data.model_dump_json(indent=2))
        if result.extractor_stats:
            console.print(
                f"\nLLM 调用: {int(result.extractor_stats.get('total_calls', 0) or 0)} 次 | "
                f"tokens: {int(result.extractor_stats.get('total_tokens', 0) or 0)} | "
                f"估算成本: ${float(result.extractor_stats.get('estimated_cost_usd', 0.0) or 0.0):.6f}"
            )
    else:
        console.print(
            Panel(
                f"[bold red][FAIL] 提取失败[/]\n\n"
                f"耗时: {result.elapsed_ms:.0f}ms\n"
                f"错误: {result.error}",
                title="执行结果",
                border_style="red",
            )
        )
        raise typer.Exit(code=1)


@app.command()
def batch(
    url_file: str = typer.Argument(..., help="URL 列表文件路径"),
    schema_name: str = typer.Option(
        "auto", "--schema", "-s", help="Schema 名称，默认 auto"
    ),
    output_format: str = typer.Option("json", "--format", "-f", help="输出格式"),
    collection: str = typer.Option("batch_result", "--collection", "-c", help="集合名"),
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    static_mode: bool = typer.Option(False, "--static", help="使用静态抓取"),
    workers: int = typer.Option(0, "--workers", "-w", help="并发数"),
):
    """批量提取网页，可用固定 Schema 或自动模式。"""
    _show_banner()

    urls = _read_urls(url_file)

    app_config = load_config(config_file)
    setup_logger(app_config.log)

    from smart_extractor.pipeline import ExtractionPipeline

    max_workers = workers if workers > 0 else app_config.scheduler.max_concurrency
    console.print(f"\n[bold green]Schema:[/] {schema_name}")
    console.print(f"[bold green]并发:[/] {max_workers}")
    console.print(f"[bold green]URL 数量:[/] {len(urls)}")
    console.print()
    with ExtractionPipeline(
        config=app_config,
        use_dynamic_fetcher=not static_mode,
    ) as pipeline:
        results = pipeline.run_batch(
            urls=urls,
            schema_name=schema_name,
            storage_format=output_format,
            collection_name=collection,
            max_workers=max_workers,
        )

    table = Table(title="批量提取结果", show_lines=True)
    table.add_column("序号", style="dim", width=4)
    table.add_column("URL", style="cyan", max_width=50)
    table.add_column("状态", width=8)
    table.add_column("质量", width=8)
    table.add_column("耗时", width=10)

    success = sum(1 for item in results if item.success)
    for index, item in enumerate(results, start=1):
        table.add_row(
            str(index),
            item.url[:50],
            "成功" if item.success else "失败",
            f"{item.validation.quality_score:.0%}" if item.validation else "-",
            f"{item.elapsed_ms:.0f}ms",
        )

    console.print(table)
    console.print(
        f"\n总计: {len(results)} | 成功: {success} | 失败: {len(results) - success}"
    )


@app.command()
def schemas(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """列出当前可用的内置和自定义 Schema。"""
    _show_banner()
    app_config = load_config(config_file)
    setup_logger(app_config.log)

    from smart_extractor.pipeline import ExtractionPipeline

    with ExtractionPipeline(config=app_config, use_dynamic_fetcher=False) as pipeline:
        registry = pipeline.get_schema_registry()
        schema_names = ["auto", *sorted(registry.list_schemas())]

    console.print(_build_schema_table(schema_names))
    console.print(
        "\n提示：`auto` 会让 AI 自动判断页面类型；指定 schema 时会按固定字段结构输出。"
    )


@app.command()
def config(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """查看当前配置。"""
    _show_banner()
    app_config = load_config(config_file)
    config_dict = app_config.model_dump()
    if config_dict.get("llm", {}).get("api_key"):
        key = config_dict["llm"]["api_key"]
        config_dict["llm"]["api_key"] = (
            key[:8] + "***" + key[-4:] if len(key) > 12 else "***"
        )
    console.print_json(
        json.dumps(config_dict, ensure_ascii=False, indent=2, default=str)
    )


@app.command("test-api")
def test_api(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """测试 LLM 接口是否可用。"""
    _show_banner()
    app_config = load_config(config_file)
    setup_logger(app_config.log)

    from smart_extractor.extractor.llm_extractor import LLMExtractor

    try:
        extractor = LLMExtractor(app_config.llm)
        result = extractor.extract_dynamic(
            text="标题：OpenAI 发布新模型。正文：这是一段用于接口联调的测试文本。",
            source_url="https://example.com/test",
        )
        console.print(
            Panel(
                f"[bold green][PASS] API 连接成功[/]\n\n"
                f"页面类型: {result.page_type}\n"
                f"字段: {', '.join(result.selected_fields)}",
                title="测试结果",
                border_style="green",
            )
        )
    except Exception as exc:
        console.print(
            Panel(
                f"[bold red][FAIL] API 连接失败[/]\n\n错误: {exc}",
                title="测试结果",
                border_style="red",
            )
        )
        raise typer.Exit(code=1)


@app.command()
def web(
    host: str = typer.Option("127.0.0.1", "--host", "-H", help="监听地址"),
    port: int = typer.Option(8000, "--port", "-p", help="监听端口"),
    reload: bool = typer.Option(False, "--reload", help="开发模式自动重载"),
):
    """启动 Web 仪表盘。"""
    _show_banner()

    try:
        import uvicorn
    except ImportError:
        console.print("[bold red][FAIL] 未安装 uvicorn，请运行: uv add uvicorn[/]")
        raise typer.Exit(code=1)

    console.print(f"\n[bold green]Web 仪表盘启动中...[/]")
    console.print(f"  地址: [bold cyan]http://{host}:{port}[/]")
    console.print(f"  重载: {'开启' if reload else '关闭'}")
    console.print("  结束服务: [bold yellow]Ctrl+Z[/]")

    uvicorn.run(
        "smart_extractor.web.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="warning",
    )


@app.command("web-worker")
def web_worker(
    poll_interval: float = typer.Option(
        0.0,
        "--poll-interval",
        help="轮询间隔秒数，默认读取配置",
    ),
):
    """启动 Web 队列 worker。"""
    _show_banner()
    app_config = load_config()
    setup_logger(app_config.log)

    from smart_extractor.web.routes import create_task_worker

    effective_poll_interval = (
        poll_interval
        if poll_interval > 0
        else app_config.web.worker_poll_interval_seconds
    )
    worker = create_task_worker()

    console.print("\n[bold green]Web 队列 worker 启动中...[/]")
    console.print(f"  worker_id: [bold cyan]{worker.worker_id}[/]")
    console.print(
        f"  轮询间隔: [bold cyan]{effective_poll_interval:.1f}s[/]"
    )
    console.print(
        f"  分发模式: [bold cyan]{app_config.web.task_dispatch_mode}[/]"
    )
    console.print("  结束服务: [bold yellow]Ctrl+C[/]")

    try:
        worker.run_forever(poll_interval_seconds=effective_poll_interval)
    except KeyboardInterrupt:
        console.print("\n[bold yellow]队列 worker 已停止[/]")


@app.command()
def runtime(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """查看任务、监控与成本概览。"""
    _show_banner()
    _, task_store = _build_task_store(config_file)
    stats = task_store.stats()
    insights = task_store.build_dashboard_insights()
    summary = insights.get("summary", {})

    table = Table(title="Runtime 概览", show_lines=True)
    table.add_column("指标", style="cyan")
    table.add_column("值", style="white")
    rows = [
        ("总任务数", stats.get("total", 0)),
        ("成功任务", stats.get("success", 0)),
        ("失败任务", stats.get("failed", 0)),
        ("活跃监控", summary.get("active_monitors", 0)),
        ("高优先级告警", summary.get("high_priority_alerts", 0)),
        ("LLM 调用次数", summary.get("llm_total_calls", 0)),
        ("Prompt Tokens", summary.get("llm_prompt_tokens", 0)),
        ("Completion Tokens", summary.get("llm_completion_tokens", 0)),
        (
            "估算成本(USD)",
            f"{float(summary.get('llm_estimated_cost_usd', 0.0) or 0.0):.6f}",
        ),
        (
            "站点记忆估算节省(USD)",
            f"{float(summary.get('site_memory_estimated_saved_cost_usd', 0.0) or 0.0):.6f}",
        ),
    ]
    for key, value in rows:
        table.add_row(str(key), str(value))
    console.print(table)


@app.command()
def monitors(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    limit: int = typer.Option(20, "--limit", help="返回数量"),
):
    """列出已保存监控与通知策略。"""
    _show_banner()
    app_config, task_store = _build_task_store(config_file)
    from smart_extractor.extractor.learned_profile_store import LearnedProfileStore
    from smart_extractor.web.management_helpers import serialize_monitor

    learned_profile_store = LearnedProfileStore(
        Path(app_config.storage.output_dir) / "learned_profiles.json"
    )
    items = [
        serialize_monitor(item, learned_profile_store)
        for item in task_store.list_monitors(limit=limit)
    ]
    table = Table(title="监控列表", show_lines=True)
    table.add_column("名称", style="cyan")
    table.add_column("域名")
    table.add_column("状态")
    table.add_column("通知策略")
    for item in items:
        policy = item.get("notification_policy_summary", {})
        summary = []
        if policy.get("digest_only"):
            summary.append("仅Digest")
        if policy.get("quiet_hours_enabled"):
            summary.append(
                f"静默{int(policy.get('quiet_hours_start', 22)):02d}-{int(policy.get('quiet_hours_end', 8)):02d}"
            )
        if int(policy.get("notification_cooldown_minutes", 0) or 0) > 0:
            summary.append(f"冷却{int(policy['notification_cooldown_minutes'])}m")
        if int(policy.get("min_change_count", 0) or 0) > 0:
            summary.append(f"变化>={int(policy['min_change_count'])}")
        table.add_row(
            str(item.get("name") or "-"),
            urlparse(str(item.get("url") or "")).netloc or "-",
            str(item.get("schedule_status_label") or "-"),
            " / ".join(summary) if summary else "默认",
        )
    console.print(table)


@app.command()
def templates(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    limit: int = typer.Option(20, "--limit", help="返回数量"),
):
    """列出已保存模板。"""
    _show_banner()
    _, task_store = _build_task_store(config_file)
    table = Table(title="模板列表", show_lines=True)
    table.add_column("名称", style="cyan")
    table.add_column("页面类型")
    table.add_column("字段数")
    table.add_column("最近使用")
    for item in task_store.list_templates(limit=limit):
        table.add_row(
            item.name or "-",
            item.page_type or "unknown",
            str(len(item.selected_fields or [])),
            item.last_used_at or "-",
        )
    console.print(table)


@app.command("template-from-task")
def template_from_task(
    task_id: str = typer.Argument(..., help="成功任务 ID"),
    name: str = typer.Option(..., "--name", help="模板名称"),
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """从成功任务直接沉淀模板。"""
    _show_banner()
    _, task_store = _build_task_store(config_file)
    detail = task_store.get_task_detail_payload(task_id)
    if not detail:
        console.print(f"[bold red][FAIL] 任务不存在: {task_id}[/]")
        raise typer.Exit(code=1)
    if str(detail.get("status") or "").strip().lower() != "success":
        console.print("[bold red][FAIL] 仅成功任务可生成模板[/]")
        raise typer.Exit(code=1)
    data = detail.get("data") if isinstance(detail.get("data"), dict) else {}
    from smart_extractor.web.task_insights import normalize_task_data

    selected_fields = list(data.get("selected_fields") or normalize_task_data(data).keys())
    field_labels = data.get("field_labels") if isinstance(data.get("field_labels"), dict) else {}
    template = task_store.create_or_update_template(
        name=name.strip(),
        url=str(detail.get("url") or "").strip(),
        page_type=str(data.get("page_type") or "unknown"),
        schema_name=str(detail.get("schema_name") or "auto"),
        storage_format=str(detail.get("storage_format") or "json"),
        use_static=False,
        selected_fields=list(selected_fields),
        field_labels=dict(field_labels),
        profile={"source_task_id": task_id, "business_goal": "从成功任务沉淀模板"},
    )
    console.print(f"[bold green][PASS] 模板已保存: {template.template_id}[/]")


@app.command("smoke-sites")
def smoke_sites(
    url_file: str = typer.Argument(..., help="URL 列表文件路径"),
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    static_mode: bool = typer.Option(True, "--static/--dynamic", help="抓取模式"),
):
    """执行真实站点烟雾验证，检查抓取与正文清洗是否可用。"""
    _show_banner()
    urls = _read_urls(url_file)
    app_config = load_config(config_file)
    setup_logger(app_config.log)
    from smart_extractor.cleaner.html_cleaner import HTMLCleaner
    from smart_extractor.fetcher.playwright import PlaywrightFetcher
    from smart_extractor.fetcher.static import StaticFetcher

    fetcher = (
        StaticFetcher(app_config.fetcher)
        if static_mode
        else PlaywrightFetcher(app_config.fetcher)
    )
    cleaner = HTMLCleaner(app_config.cleaner)
    table = Table(title="站点烟雾验证", show_lines=True)
    table.add_column("URL", style="cyan", max_width=48)
    table.add_column("状态")
    table.add_column("正文长度")
    table.add_column("备注", max_width=40)
    success = 0
    try:
        for url in urls:
            result = fetcher.fetch(url)
            if not result.is_success:
                table.add_row(
                    url[:48],
                    "失败",
                    "-",
                    str(result.error or result.status_code),
                )
                continue
            cleaned_text = cleaner.clean(result.html)
            note = "命中壳页" if result.is_shell_page else "可继续做结构化抽取"
            if cleaned_text.strip() and not result.is_shell_page:
                success += 1
            table.add_row(
                url[:48],
                "成功" if cleaned_text.strip() and not result.is_shell_page else "风险",
                str(len(cleaned_text)),
                note,
            )
    finally:
        fetcher.close()
    console.print(table)
    console.print(f"\n通过 {success}/{len(urls)}")


@app.command("db-migrate")
def db_migrate(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """初始化或迁移任务治理数据库结构。"""
    _show_banner()
    app_config = load_config(config_file)
    setup_logger(app_config.log)
    result = migrate_task_store_database(app_config)
    console.print(
        Panel(
            f"[bold green][PASS] 数据库迁移完成[/]\n\n"
            f"数据库: {result['database_url']}\n"
            f"方言: {result['dialect']}\n"
            f"表数量: {len(result['tables'])}",
            title="数据库运维",
            border_style="green",
        )
    )


@app.command("db-backup")
def db_backup(
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
    output_dir: str = typer.Option("", "--output-dir", help="备份输出目录"),
):
    """导出任务治理数据库逻辑备份。"""
    _show_banner()
    app_config = load_config(config_file)
    setup_logger(app_config.log)
    backup_path = backup_task_store_database(
        app_config,
        backup_path=output_dir,
    )
    console.print(
        Panel(
            f"[bold green][PASS] 数据库备份完成[/]\n\n"
            f"备份文件: {backup_path}",
            title="数据库运维",
            border_style="green",
        )
    )


@app.command("db-restore")
def db_restore(
    backup_file: str = typer.Argument(..., help="备份文件路径"),
    config_file: Optional[str] = typer.Option(None, "--config", help="配置文件路径"),
):
    """从逻辑备份恢复任务治理数据库。"""
    _show_banner()
    app_config = load_config(config_file)
    setup_logger(app_config.log)
    result = restore_task_store_database(
        app_config,
        backup_file=backup_file,
    )
    restored_total = sum(int(value or 0) for value in result["restored_rows"].values())
    console.print(
        Panel(
            f"[bold green][PASS] 数据库恢复完成[/]\n\n"
            f"备份文件: {result['backup_file']}\n"
            f"数据库: {result['database_url']}\n"
            f"恢复记录数: {restored_total}",
            title="数据库运维",
            border_style="green",
        )
    )


if __name__ == "__main__":
    app()
