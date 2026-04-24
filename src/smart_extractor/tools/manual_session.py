"""
手动登录态注入工具。

使用与抓取器一致的持久化浏览器 Profile 启动有头浏览器，
方便人工完成登录、验证码或安全验证，并将会话状态保存回项目目录。
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from playwright.sync_api import sync_playwright

from smart_extractor.config import load_config
from smart_extractor.fetcher.playwright import _ANTI_DETECT_SCRIPT, _DEFAULT_HEADERS
from smart_extractor.utils.anti_detect import get_random_user_agent
from smart_extractor.utils.encoding import configure_utf8_io


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smart-extractor-session",
        description="启动可复用的 Playwright 持久化浏览器会话，供人工登录或完成验证。",
    )
    parser.add_argument(
        "--url",
        default="https://www.zhipin.com/",
        help="启动后自动打开的页面 URL，默认是 BOSS 直聘首页。",
    )
    parser.add_argument(
        "--profile-dir",
        default="",
        help="持久化浏览器 Profile 目录，默认读取配置文件中的 fetcher.persistent_context_dir。",
    )
    parser.add_argument(
        "--state-path",
        default="",
        help="导出的 storage_state 文件路径，默认读取配置文件中的 fetcher.storage_state_path。",
    )
    parser.add_argument(
        "--fresh-profile",
        action="store_true",
        help="启动前清空旧的浏览器 Profile 目录。",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="以无头模式启动。手动登录通常不需要这个选项。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="页面打开超时时间，单位毫秒。默认使用项目配置。",
    )
    return parser


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    config = load_config()
    fetcher_config = config.fetcher
    output_root = Path(config.storage.output_dir)

    profile_dir = Path(
        args.profile_dir
        or fetcher_config.persistent_context_dir
        or (output_root / "playwright" / "profile")
    )
    state_path = Path(
        args.state_path
        or fetcher_config.storage_state_path
        or (output_root / "playwright" / "state.json")
    )
    return profile_dir, state_path


def main(argv: list[str] | None = None) -> int:
    configure_utf8_io()
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config()
    fetcher_config = config.fetcher
    profile_dir, state_path = _resolve_paths(args)

    if args.fresh_profile and profile_dir.exists():
        shutil.rmtree(profile_dir)

    profile_dir.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    user_agent = fetcher_config.user_agent or get_random_user_agent()
    timeout = args.timeout or fetcher_config.timeout

    print(f"[session] profile 目录: {profile_dir}")
    print(f"[session] state 文件: {state_path}")
    print(f"[session] 打开页面: {args.url}")
    print("[session] 浏览器启动后，请在页面中手动完成登录、验证码或安全验证。")
    print("[session] 完成后回到终端按回车，脚本会保存会话状态并退出。")

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=bool(args.headless),
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-default-browser-check",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
            viewport={
                "width": fetcher_config.viewport_width,
                "height": fetcher_config.viewport_height,
            },
            user_agent=user_agent,
            locale=fetcher_config.locale,
            timezone_id=fetcher_config.timezone_id,
            extra_http_headers=dict(_DEFAULT_HEADERS),
            ignore_https_errors=not fetcher_config.verify_ssl,
        )
        context.add_init_script(_ANTI_DETECT_SCRIPT)

        page = context.pages[0] if context.pages else context.new_page()
        page.goto(args.url, timeout=timeout, wait_until="domcontentloaded")
        page.bring_to_front()

        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout, 5000))
        except Exception:
            pass

        input("\n[session] 完成操作后按回车保存并退出...")
        context.storage_state(path=str(state_path))

    print(f"[session] 会话已保存到: {state_path}")
    print(f"[session] 持久化 Profile 已保留在: {profile_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
