from pathlib import Path

import yaml

from smart_extractor.config import (
    load_config,
    load_raw_yaml_config,
    resolve_local_config_path,
    update_llm_basic_config,
)


def test_load_config_merges_default_local_and_env(monkeypatch, tmp_path):
    default_path = tmp_path / "config" / "default.yaml"
    local_path = resolve_local_config_path(default_path)
    default_path.parent.mkdir(parents=True, exist_ok=True)

    default_payload = {
        "llm": {
            "api_key": "",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "temperature": 0.0,
        },
        "web": {
            "api_token": "",
            "rate_limit_per_minute": 60,
            "task_dispatch_mode": "inline",
        },
    }
    local_payload = {
        "llm": {
            "base_url": "https://example.local/v1",
            "model": "gpt-local",
            "temperature": 0.6,
        }
    }
    default_path.write_text(
        yaml.safe_dump(default_payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    local_path.write_text(
        yaml.safe_dump(local_payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    monkeypatch.setenv("SMART_EXTRACTOR_API_KEY", "env-key")
    monkeypatch.setenv("SMART_EXTRACTOR_WEB_TASK_DISPATCH_MODE", "queue")
    monkeypatch.setenv("SMART_EXTRACTOR_WEB_START_BUILTIN_WORKER", "true")

    config = load_config(default_path)

    assert config.llm.api_key == "env-key"
    assert config.llm.base_url == "https://example.local/v1"
    assert config.llm.model == "gpt-local"
    assert config.llm.temperature == 0.6
    assert config.web.rate_limit_per_minute == 60
    assert config.web.task_dispatch_mode == "queue"
    assert config.web.start_builtin_worker is True


def test_update_llm_basic_config_writes_local_yaml(tmp_path):
    default_path = tmp_path / "config" / "default.yaml"
    default_path.parent.mkdir(parents=True, exist_ok=True)
    default_path.write_text("llm:\n  api_key: \"\"\n", encoding="utf-8")

    written_path = update_llm_basic_config(
        api_key="dashboard-key",
        base_url="https://example.local/v1",
        model="gpt-dashboard",
        temperature=0.7,
        config_path=resolve_local_config_path(default_path),
    )

    payload = yaml.safe_load(Path(written_path).read_text(encoding="utf-8"))
    assert written_path.name == "local.yaml"
    assert payload["llm"]["api_key"] == "dashboard-key"
    assert payload["llm"]["base_url"] == "https://example.local/v1"
    assert payload["llm"]["model"] == "gpt-dashboard"
    assert payload["llm"]["temperature"] == 0.7
    assert load_raw_yaml_config(default_path).get("llm", {}).get("api_key", "") == ""
