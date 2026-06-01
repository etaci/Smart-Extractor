import httpx

from smart_extractor.fetcher.url_preflight import (
    _extract_canonical_url,
    preflight_url,
)


def test_preflight_marks_404_as_unreachable(monkeypatch):
    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def head(self, url, headers):
            return httpx.Response(
                404,
                request=httpx.Request("HEAD", url),
                headers={"content-type": "text/html"},
            )

    monkeypatch.setattr("smart_extractor.fetcher.url_preflight.httpx.Client", FakeClient)

    result = preflight_url("https://example.com/missing")

    assert result.reachable is False
    assert result.reason == "http_404"
    assert result.status_code == 404


def test_preflight_falls_back_to_probe_get_and_extracts_canonical(monkeypatch):
    class FakeStream:
        def __init__(self, response):
            self.response = response

        def __enter__(self):
            return self.response

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def head(self, url, headers):
            return httpx.Response(405, request=httpx.Request("HEAD", url))

        def stream(self, method, url, headers):
            html = '<html><head><link rel="canonical" href="/canonical"></head></html>'
            return FakeStream(
                httpx.Response(
                    200,
                    request=httpx.Request(method, url),
                    content=html.encode("utf-8"),
                )
            )

    monkeypatch.setattr("smart_extractor.fetcher.url_preflight.httpx.Client", FakeClient)

    result = preflight_url("https://example.com/old/canonical")

    assert result.reachable is True
    assert result.canonical_url == "https://example.com/canonical"
    assert result.target_url == "https://example.com/canonical"


def test_preflight_repairs_404_from_sitemap(monkeypatch):
    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def head(self, url, headers):
            if "/shop/" in str(url) and str(url).endswith("/missing-product"):
                return httpx.Response(
                    200,
                    request=httpx.Request("HEAD", url),
                    content=b"<html></html>",
                )
            return httpx.Response(404, request=httpx.Request("HEAD", url))

        def get(self, url, headers):
            assert str(url) == "https://example.com/sitemap.xml"
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                content=(
                    "<urlset><url><loc>https://example.com/shop/missing-product"
                    "</loc></url></urlset>"
                ).encode("utf-8"),
            )

    monkeypatch.setattr("smart_extractor.fetcher.url_preflight.httpx.Client", FakeClient)

    result = preflight_url("https://example.com/old/missing-product")

    assert result.reachable is True
    assert result.reason == "sitemap_fallback"
    assert result.final_url == "https://example.com/shop/missing-product"
    assert result.repair_reason == "sitemap_fallback"


def test_preflight_rejects_unsafe_canonical_to_home(monkeypatch):
    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def head(self, url, headers):
            return httpx.Response(
                200,
                request=httpx.Request("HEAD", url),
                content=b'<html><head><link rel="canonical" href="/"></head></html>',
            )

    monkeypatch.setattr("smart_extractor.fetcher.url_preflight.httpx.Client", FakeClient)

    result = preflight_url("https://example.com/products/widget")

    assert result.reachable is True
    assert result.target_url == "https://example.com/products/widget"
    assert result.canonical_url == ""
    assert result.headers["x-smart-canonical-rejected"] == "https://example.com/"


def test_extract_canonical_url_resolves_relative_href():
    html = '<html><head><link rel="canonical" href="../product"></head></html>'

    canonical = _extract_canonical_url(html, base_url="https://example.com/shop/item")

    assert canonical == "https://example.com/product"
