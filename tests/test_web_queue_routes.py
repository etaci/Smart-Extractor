from tests.web_route_testkit import _build_test_client


def test_api_extract_queue_mode_enqueues_then_worker_completes(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(
        monkeypatch,
        tmp_path,
        dispatch_mode="queue",
    )

    captured = {}

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        captured["task_id"] = task_id
        captured["schema_name"] = schema_name
        captured["use_static"] = use_static
        captured["selected_fields"] = selected_fields or []
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=31.0,
            quality_score=0.93,
            data={"queued": True},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/extract",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/queue-article",
            "schema_name": "news",
            "storage_format": "json",
            "use_static": True,
            "selected_fields": ["title"],
        },
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    queued_detail = client.get(
        f"/api/task/{task_id}",
        headers={"X-API-Token": "test-token"},
    ).json()
    assert queued_detail["status"] == "queued"
    assert queued_detail["progress"]["stage"] == "任务已进入队列，等待 worker 处理"

    worker = routes_module.create_task_worker(worker_id="test-queue-worker")
    assert worker.run_once() is True

    completed_detail = client.get(
        f"/api/task/{task_id}",
        headers={"X-API-Token": "test-token"},
    ).json()
    assert completed_detail["status"] == "success"
    assert completed_detail["data"]["queued"] is True
    assert captured == {
        "task_id": task_id,
        "schema_name": "news",
        "use_static": True,
        "selected_fields": ["title"],
    }


def test_api_batch_queue_mode_updates_parent_after_worker_runs(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(
        monkeypatch,
        tmp_path,
        dispatch_mode="queue",
    )

    completed_task_ids = []

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        completed_task_ids.append(task_id)
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=18.0,
            quality_score=0.87,
            data={"task_id": task_id},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/batch",
        headers={"X-API-Token": "test-token"},
        json={
            "urls": ["https://example.com/q1", "https://example.com/q2"],
            "storage_format": "json",
        },
    )
    assert response.status_code == 200
    payload = response.json()

    queued_tasks = client.get(
        f"/api/dashboard?batch_group_id={payload['batch_group_id']}",
        headers={"X-API-Token": "test-token"},
    ).json()["tasks"]
    assert queued_tasks[0]["status"] == "queued"
    assert queued_tasks[0]["progress_stage"] == "批量任务已入队，等待 worker 处理（共 2 个 URL）"

    worker = routes_module.create_task_worker(worker_id="test-batch-worker")
    assert worker.run_once() is True
    assert worker.run_once() is True
    assert worker.run_once() is False

    refreshed_tasks = client.get(
        f"/api/dashboard?batch_group_id={payload['batch_group_id']}",
        headers={"X-API-Token": "test-token"},
    ).json()["tasks"]
    assert refreshed_tasks[0]["status"] == "success"
    assert refreshed_tasks[0]["completed_items"] == 2
    assert len(completed_task_ids) == 2


def test_api_extract_queue_mode_marks_task_failed_after_worker_failure(
    monkeypatch, tmp_path
):
    client, routes_module = _build_test_client(
        monkeypatch,
        tmp_path,
        dispatch_mode="queue",
    )

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        routes_module._task_store.mark_failed(
            task_id=task_id,
            elapsed_ms=9.0,
            error="mock queue failure",
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/extract",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/queue-fail",
            "schema_name": "news",
            "storage_format": "json",
        },
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    worker = routes_module.create_task_worker(worker_id="test-failure-worker")
    assert worker.run_once() is True

    failed_detail = client.get(
        f"/api/task/{task_id}",
        headers={"X-API-Token": "test-token"},
    ).json()
    assert failed_detail["status"] == "failed"
    assert failed_detail["error"] == "mock queue failure"
