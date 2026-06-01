from smart_extractor.cleaner.structured_hints import build_structured_hints
from smart_extractor.config import AppConfig, FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.pipeline import ExtractionPipeline


def test_structured_hints_extracts_product_json_ld():
    html = """
    <html><head>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "Smart Extractor Pro",
        "brand": {"@type": "Brand", "name": "Acme"},
        "description": "A precise extraction tool.",
        "offers": {
          "@type": "Offer",
          "price": "29.99",
          "priceCurrency": "USD",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head><body>Skip to main content $9 footnote</body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=["name", "price", "availability", "brand", "summary"],
    )

    assert "name: Smart Extractor Pro" in hints
    assert "price: USD 29.99" in hints
    assert "availability: InStock" in hints
    assert "brand: Acme" in hints
    assert "summary: A precise extraction tool." in hints


def test_structured_hints_prefers_structured_sources_and_keeps_candidates():
    html = """
    <html><head>
      <meta property="product:price:amount" content="19.99">
      <meta property="product:price:currency" content="USD">
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "Candidate Widget",
        "offers": {
          "@type": "Offer",
          "price": "29.99",
          "priceCurrency": "USD",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head><body></body></html>
    """

    hints = build_structured_hints(html, selected_fields=["name", "price", "availability"])

    assert "price: USD 29.99" in hints
    assert "price_candidates:" in hints
    assert "USD 29.99" in hints
    assert "USD 19.99" in hints


def test_structured_hints_extracts_next_hydration_payload():
    html = """
    <html><head>
      <script id="__NEXT_DATA__" type="application/json">
      {
        "props": {
          "pageProps": {
            "product": {
              "name": "Hydrated Widget",
              "currentPrice": "49.00",
              "currency": "USD",
              "availability": "InStock"
            }
          }
        }
      }
      </script>
    </head><body><div id="__next"></div></body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=["name", "price", "availability"],
    )

    assert "name: Hydrated Widget" in hints
    assert "price: USD 49.00" in hints
    assert "availability: InStock" in hints


def test_structured_hints_extracts_shopify_product_variants():
    html = """
    <html><head>
      <script>
      window.ShopifyAnalytics = window.ShopifyAnalytics || {};
      window.ShopifyAnalytics.meta = {
        "product": {
          "id": 1,
          "title": "Shopify Widget",
          "vendor": "Acme",
          "variants": [{"price": "1299", "available": true}]
        }
      };
      </script>
    </head><body></body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=["name", "price", "availability"],
    )

    assert "name: Shopify Widget" in hints
    assert "price: 1299" in hints
    assert "availability: True" in hints


def test_structured_hints_extracts_common_ats_job_payloads():
    html = """
    <html><head>
      <script type="application/json">
      {
        "jobPosting": {
          "title": "Staff Backend Engineer",
          "company": {"name": "Acme AI"},
          "categories": {"location": "Remote"},
          "commitment": "Full-time",
          "jobReqId": "REQ-42",
          "description": "Build extraction systems"
        }
      }
      </script>
    </head><body></body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=["title", "company", "location", "employment_type", "job_id", "requirements"],
    )

    assert "title: Staff Backend Engineer" in hints
    assert "company: Acme AI" in hints
    assert "location: Remote" in hints
    assert "employment_type: Full-time" in hints
    assert "job_id: REQ-42" in hints
    assert "requirements: Build extraction systems" in hints


def test_structured_hints_extracts_workday_lever_style_job_payloads():
    html = """
    <html><head>
      <script type="application/json">
      {
        "posting": {
          "jobPostingTitle": "Principal Data Engineer",
          "locationsText": "Berlin, Germany",
          "jobRequisitionId": "WD-9001",
          "externalPath": "/jobs/principal-data-engineer",
          "description": "Own reliable data products"
        },
        "lever": {
          "text": "Platform Engineer",
          "categories": {
            "location": "Remote - US",
            "commitment": "Full-time"
          },
          "id": "LEV-123"
        }
      }
      </script>
    </head><body></body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=["title", "location", "job_id", "employment_type", "requirements"],
    )

    assert "title: Principal Data Engineer" in hints
    assert "location: Berlin, Germany" in hints
    assert "job_id: WD-9001" in hints
    assert "employment_type: Full-time" in hints
    assert "requirements: Own reliable data products" in hints


def test_structured_hints_extracts_job_article_policy_and_captured_json():
    html = """
    <html><head>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": "Senior Backend Engineer",
        "hiringOrganization": {"name": "Acme"},
        "jobLocation": {"name": "Remote"},
        "baseSalary": {"value": {"minValue": 100000, "maxValue": 150000, "unitText": "YEAR"}, "currency": "USD"}
      }
      </script>
      <script type="application/json" data-smart-captured-response="1">
      {
        "headline": "Policy Update",
        "datePublished": "2026-05-20",
        "publisher": {"name": "Digital Office"},
        "identifier": "ABC-2026-7",
        "articleBody": "Policy body text"
      }
      </script>
    </head><body></body></html>
    """

    hints = build_structured_hints(
        html,
        selected_fields=[
            "title",
            "company",
            "location",
            "salary_range",
            "publish_date",
            "agency",
            "policy_number",
            "content",
        ],
    )

    assert "title: Senior Backend Engineer" in hints
    assert "company: Acme" in hints
    assert "location: Remote" in hints
    assert "salary_range: USD 100000-150000 YEAR" in hints
    assert "publish_date: 2026-05-20" in hints
    assert "agency: Digital Office" in hints
    assert "policy_number: ABC-2026-7" in hints
    assert "content: Policy body text" in hints


def test_pipeline_prepends_structured_hints_before_noisy_text(test_config):
    html = """
    <html><head>
      <meta property="og:title" content="Canonical Product">
      <meta property="product:price:amount" content="19.99">
      <meta property="product:price:currency" content="USD">
    </head><body><nav>Skip to main content</nav><main>Buy now</main></body></html>
    """
    pipeline = ExtractionPipeline(
        config=AppConfig(fetcher=FetcherConfig(static_fallback_to_dynamic=False)),
        fetcher=_SingleResultFetcher(html),
    )

    result = pipeline.run(
        "https://example.com/product",
        schema_name="auto",
        selected_fields=["title", "price"],
        skip_storage=True,
    )

    assert "Structured extraction hints:" in result.cleaned_text
    assert "title: Canonical Product" in result.cleaned_text
    assert "price: USD 19.99" in result.cleaned_text
    pipeline.close()


class _SingleResultFetcher:
    def __init__(self, html: str):
        self.html = html

    def fetch(self, url: str) -> FetchResult:
        return FetchResult(url=url, html=self.html, status_code=200)

    def close(self) -> None:
        return None
