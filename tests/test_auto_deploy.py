from claw.auto_deploy import (
    _is_claw_template_reset_reply_success,
    _is_retryable_create_429,
)


def test_create_429_pool_full_message_is_retryable():
    assert _is_retryable_create_429({
        "code": 429,
        "msg": "Mimo Claw使用中机器已达上限",
    })


def test_create_429_too_many_requests_message_is_retryable():
    assert _is_retryable_create_429({
        "code": 429,
        "msg": "Mimo Claw当前创建请求较多，请稍后重试",
    })


def test_create_non_429_error_is_not_retryable():
    assert not _is_retryable_create_429({
        "code": 500,
        "msg": "internal error",
    })


def test_create_malformed_response_is_not_retryable():
    assert not _is_retryable_create_429("HTTP_429")


def test_template_reset_reply_success_matches_expected_claw_ack():
    assert _is_claw_template_reset_reply_success(
        "好，写入模板并重启。已恢复为模板，重启中。"
        "搞定。AGENTS.md 和 SOUL.md 已恢复为官方模板，重启信号已发送。"
    )


def test_template_reset_reply_requires_restart_signal():
    assert not _is_claw_template_reset_reply_success(
        "AGENTS.md 和 SOUL.md 已恢复为官方模板。"
    )


def test_template_reset_reply_requires_both_files():
    assert not _is_claw_template_reset_reply_success(
        "AGENTS.md 已恢复为官方模板，重启信号已发送。"
    )
