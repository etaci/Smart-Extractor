from pathlib import Path

from smart_extractor.tools.manual_session import build_parser, _resolve_paths


def test_manual_session_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])

    assert args.url == "https://www.zhipin.com/"
    assert args.fresh_profile is False
    assert args.headless is False


def test_manual_session_resolve_paths_from_cli(monkeypatch, tmp_path):
    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_PERSISTENT_CONTEXT_DIR", str(tmp_path / "profile-from-env"))
    monkeypatch.setenv("SMART_EXTRACTOR_FETCHER_STORAGE_STATE_PATH", str(tmp_path / "state-from-env.json"))

    parser = build_parser()
    args = parser.parse_args(
        [
            "--profile-dir",
            str(tmp_path / "profile-from-cli"),
            "--state-path",
            str(tmp_path / "state-from-cli.json"),
        ]
    )

    profile_dir, state_path = _resolve_paths(args)

    assert profile_dir == Path(tmp_path / "profile-from-cli")
    assert state_path == Path(tmp_path / "state-from-cli.json")
