from pathlib import Path

import yaml

from smart_extractor.config import AppConfig
from smart_extractor.fetcher.static import StaticFetcher


def test_static_fetcher_enables_ssl_verification_by_default(monkeypatch):
    captured = {}

    class DummyClient:
        pass

    def fake_client(**kwargs):
        captured.update(kwargs)
        return DummyClient()

    monkeypatch.setattr("smart_extractor.fetcher.static.httpx.Client", fake_client)

    fetcher = StaticFetcher()
    client = fetcher._ensure_client()

    assert isinstance(client, DummyClient)
    assert captured["verify"] is True


def test_static_fetcher_allows_explicit_ssl_disable(monkeypatch):
    captured = {}

    class DummyClient:
        pass

    def fake_client(**kwargs):
        captured.update(kwargs)
        return DummyClient()

    monkeypatch.setattr("smart_extractor.fetcher.static.httpx.Client", fake_client)

    fetcher = StaticFetcher(config=AppConfig().fetcher.model_copy(update={"verify_ssl": False}))
    fetcher._ensure_client()

    assert captured["verify"] is False


def test_fetcher_verify_ssl_can_be_overridden_by_env(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_data = {
        "fetcher": {
            "verify_ssl": True,
        }
    }
    with open(config_path, "w", encoding="utf-8") as config_file:
        yaml.safe_dump(config_data, config_file, allow_unicode=True, sort_keys=False)

    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_VERIFY_SSL", "false")

    config = AppConfig.from_yaml(Path(config_path))

    assert config.fetcher.verify_ssl is False


def test_fetcher_browser_profile_can_be_overridden_by_env(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_data = {
        "fetcher": {
            "locale": "en-US",
            "timezone_id": "UTC",
            "storage_state_path": "state.json",
            "persistent_context_dir": "profile",
        }
    }
    with open(config_path, "w", encoding="utf-8") as config_file:
        yaml.safe_dump(config_data, config_file, allow_unicode=True, sort_keys=False)

    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_LOCALE", "zh-CN")
    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_TIMEZONE_ID", "Asia/Shanghai")
    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_STORAGE_STATE_PATH", str(tmp_path / "browser-state.json"))
    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_PERSISTENT_CONTEXT_DIR", str(tmp_path / "browser-profile"))

    config = AppConfig.from_yaml(Path(config_path))

    assert config.fetcher.locale == "zh-CN"
    assert config.fetcher.timezone_id == "Asia/Shanghai"
    assert config.fetcher.storage_state_path == str(tmp_path / "browser-state.json")
    assert config.fetcher.persistent_context_dir == str(tmp_path / "browser-profile")
