import asyncio

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.playwright_async import AsyncPlaywrightFetcher


class DummyAsyncLocator:
    def __init__(self, texts):
        self.texts = list(texts)
        self.index = 0

    async def inner_text(self, timeout):
        if self.index >= len(self.texts):
            return self.texts[-1]
        value = self.texts[self.index]
        self.index += 1
        return value


class DummyAsyncMouse:
    async def move(self, x, y):
        return (x, y)


class DummyAsyncPage:
    def __init__(self, texts, title=""):
        self.locator_instance = DummyAsyncLocator(texts)
        self.mouse = DummyAsyncMouse()
        self.title_text = title
        self.wait_calls = []
        self.reload_calls = []
        self.evaluate_calls = 0

    def locator(self, selector):
        assert selector == "body"
        return self.locator_instance

    async def title(self):
        return self.title_text

    async def wait_for_timeout(self, timeout):
        self.wait_calls.append(timeout)

    async def evaluate(self, script):
        self.evaluate_calls += 1

    async def reload(self, timeout, wait_until):
        self.reload_calls.append((timeout, wait_until))

    async def wait_for_load_state(self, state, timeout):
        return (state, timeout)


def test_async_playwright_fetcher_retries_when_shell_page_detected():
    fetcher = AsyncPlaywrightFetcher(FetcherConfig(timeout=5000, wait_after_load=1000))
    page = DummyAsyncPage(
        [
            "加载中，请稍候",
            "这是一段已经完成渲染的正文内容。" * 6,
            "这是一段已经完成渲染的正文内容。" * 6,
        ]
    )

    async def _noop(_page):
        return None

    fetcher._wait_for_meaningful_content = _noop  # type: ignore[method-assign]
    asyncio.run(fetcher._stabilize_page(page, "https://example.com"))

    assert page.reload_calls == [(5000, "domcontentloaded")]
    assert page.evaluate_calls == 1
