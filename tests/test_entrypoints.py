from smart_extractor import __version__
from smart_extractor.main import main
from smart_extractor.web import management_route_groups, task_store_profiles


def test_main_delegates_to_cli(monkeypatch):
    called = {"value": False}

    def fake_app():
        called["value"] = True

    monkeypatch.setattr("smart_extractor.main.app", fake_app)

    main()

    assert called["value"] is True


def test_package_version_exposed():
    assert __version__ == "1.0.0"


def test_management_route_groups_exports():
    assert "register_config_routes" in management_route_groups.__all__
    assert callable(management_route_groups.register_monitor_routes)


def test_task_store_profiles_compat_exports():
    assert "upsert_monitor" in task_store_profiles.__all__
    assert callable(task_store_profiles.fetch_templates)
