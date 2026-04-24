from smart_extractor.web.notifier import evaluate_notification_policy


def test_evaluate_notification_policy_blocks_digest_only_changed_alert():
    should_send, reason = evaluate_notification_policy(
        {
            "profile": {
                "notify_on": ["changed", "error"],
                "webhook_url": "https://example.com/webhook",
                "digest_only": True,
            },
            "last_changed_fields": [{"field": "title"}],
            "selected_fields": ["title", "summary"],
        },
        "changed",
    )

    assert should_send is False
    assert "Digest" in reason


def test_evaluate_notification_policy_blocks_small_change_count():
    should_send, reason = evaluate_notification_policy(
        {
            "profile": {
                "notify_on": ["changed"],
                "webhook_url": "https://example.com/webhook",
                "min_change_count": 2,
            },
            "last_changed_fields": [{"field": "title"}],
            "selected_fields": ["title", "summary", "price"],
        },
        "changed",
    )

    assert should_send is False
    assert "未达到阈值" in reason


def test_evaluate_notification_policy_allows_error_bypass():
    should_send, reason = evaluate_notification_policy(
        {
            "profile": {
                "notify_on": ["changed"],
                "webhook_url": "https://example.com/webhook",
                "digest_only": True,
                "always_notify_error": True,
            }
        },
        "error",
    )

    assert should_send is True
    assert reason == ""
