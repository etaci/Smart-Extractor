from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.playwright import PlaywrightFetcher


class DummyContext:
    def __init__(self):
        self.init_scripts = []
        self.storage_state_calls = []
        self.closed = False
        self.browser = object()

    def add_init_script(self, script):
        self.init_scripts.append(script)

    def storage_state(self, path):
        self.storage_state_calls.append(path)

    def close(self):
        self.closed = True


class DummyBrowser:
    def __init__(self, context):
        self.context = context
        self.calls = []

    def new_context(self, **kwargs):
        self.calls.append(kwargs)
        return self.context


class DummyLocator:
    def __init__(self, texts):
        self.texts = list(texts)
        self.index = 0

    def inner_text(self, timeout):
        if self.index >= len(self.texts):
            return self.texts[-1]
        value = self.texts[self.index]
        self.index += 1
        return value


class DummyPage:
    def __init__(self, texts):
        self.locator_instance = DummyLocator(texts)
        self.wait_calls = []
        self.reload_calls = []
        self.mouse = self
        self.evaluate_calls = 0

    def locator(self, selector):
        assert selector == "body"
        return self.locator_instance

    def wait_for_timeout(self, timeout):
        self.wait_calls.append(timeout)

    def move(self, x, y):
        return (x, y)

    def evaluate(self, script):
        self.evaluate_calls += 1

    def reload(self, timeout, wait_until):
        self.reload_calls.append((timeout, wait_until))

    def wait_for_load_state(self, state, timeout):
        return (state, timeout)


class DummyChromium:
    def __init__(self, context):
        self.context = context
        self.calls = []

    def launch_persistent_context(self, user_data_dir, **kwargs):
        self.calls.append((user_data_dir, kwargs))
        return self.context


class DummyPlaywright:
    def __init__(self, chromium):
        self.chromium = chromium


def test_playwright_fetcher_builds_context_options_with_profile(tmp_path):
    storage_state_path = tmp_path / "state.json"
    storage_state_path.write_text("{}", encoding="utf-8")

    fetcher = PlaywrightFetcher(
        FetcherConfig(
            viewport_width=1440,
            viewport_height=900,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            verify_ssl=False,
            user_agent="test-agent",
            storage_state_path=str(storage_state_path),
        )
    )

    options = fetcher._build_context_options("test-agent")

    assert options["viewport"] == {"width": 1440, "height": 900}
    assert options["locale"] == "zh-CN"
    assert options["timezone_id"] == "Asia/Shanghai"
    assert options["ignore_https_errors"] is True
    assert options["storage_state"] == str(storage_state_path)
    assert options["extra_http_headers"]["Accept-Language"].startswith("zh-CN")


def test_playwright_fetcher_creates_context_and_injects_anti_detect(tmp_path):
    storage_state_path = tmp_path / "state.json"
    storage_state_path.write_text("{}", encoding="utf-8")
    context = DummyContext()
    browser = DummyBrowser(context)

    fetcher = PlaywrightFetcher(
        FetcherConfig(
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            user_agent="fixed-agent",
            storage_state_path=str(storage_state_path),
        )
    )
    fetcher._browser = browser
    fetcher._initialized = True

    actual_context = fetcher._get_context()

    assert actual_context is context
    assert len(browser.calls) == 1
    assert browser.calls[0]["user_agent"] == "fixed-agent"
    assert browser.calls[0]["storage_state"] == str(storage_state_path)
    assert context.init_scripts
    assert "webdriver" in context.init_scripts[0]


def test_playwright_fetcher_uses_persistent_profile_when_configured(tmp_path):
    context = DummyContext()
    chromium = DummyChromium(context)
    storage_state_path = tmp_path / "state.json"
    storage_state_path.write_text("{}", encoding="utf-8")
    fetcher = PlaywrightFetcher(
        FetcherConfig(
            user_agent="fixed-agent",
            persistent_context_dir=str(tmp_path / "profile"),
            storage_state_path=str(storage_state_path),
        )
    )
    fetcher._playwright = DummyPlaywright(chromium)

    browser = fetcher._ensure_browser()

    assert browser is context.browser
    assert fetcher._context is context
    assert fetcher._uses_persistent_context is True
    assert len(chromium.calls) == 1
    assert chromium.calls[0][0] == str(tmp_path / "profile")
    assert chromium.calls[0][1]["user_agent"] == "fixed-agent"
    assert "storage_state" not in chromium.calls[0][1]


def test_playwright_fetcher_persists_storage_state_on_close(tmp_path):
    storage_state_path = tmp_path / "persisted" / "state.json"
    context = DummyContext()

    fetcher = PlaywrightFetcher(FetcherConfig(storage_state_path=str(storage_state_path)))
    fetcher._context = context

    fetcher.close()

    assert context.storage_state_calls == [str(storage_state_path)]
    assert context.closed is True
    assert fetcher._context is None


def test_playwright_fetcher_waits_until_meaningful_content():
    fetcher = PlaywrightFetcher(FetcherConfig(wait_after_load=2000))
    page = DummyPage(
        [
            "加载中，请稍候",
            "加载中，请稍候",
            "这是一段已经完成渲染的正文内容。" * 6,
        ]
    )

    fetcher._wait_for_meaningful_content(page)

    assert page.wait_calls == [500, 500]


def test_playwright_fetcher_detects_shell_page():
    fetcher = PlaywrightFetcher(FetcherConfig())
    page = DummyPage(["加载中，请稍候"])

    assert fetcher._looks_like_shell_page(page) is True


def test_playwright_fetcher_retries_when_shell_page_detected():
    fetcher = PlaywrightFetcher(FetcherConfig(timeout=5000, wait_after_load=1000))
    fetcher._wait_for_meaningful_content = lambda page: None
    page = DummyPage(
        [
            "加载中，请稍候",
            "这是一段已经完成渲染的正文内容。" * 6,
        ]
    )

    fetcher._stabilize_page(page, "https://example.com")

    assert page.reload_calls == [(5000, "domcontentloaded")]
    assert page.evaluate_calls == 1


def test_playwright_fetcher_marks_shell_page_in_fetch_result():
    class DummyResponse:
        status = 200
        headers = {"content-type": "text/html"}

    class FetchPage(DummyPage):
        def __init__(self):
            super().__init__(["加载中，请稍候"])

        def goto(self, url, timeout, wait_until):
            return DummyResponse()

        def wait_for_selector(self, selector, timeout):
            return (selector, timeout)

        def content(self):
            return "<html><body>加载中，请稍候</body></html>"

        def close(self):
            return None

    fetcher = PlaywrightFetcher(FetcherConfig(timeout=5000, wait_after_load=0))
    fetcher._create_page = lambda: FetchPage()
    fetcher._stabilize_page = lambda page, url: None

    result = fetcher.fetch("https://example.com")

    assert result.is_success is True
    assert result.is_shell_page is True
