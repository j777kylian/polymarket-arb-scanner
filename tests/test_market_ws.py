from polymarket_scanner.api.market_ws import (
    connection_subscribe_messages,
    diff_tokens,
    initial_market_message,
    parse_ws_messages,
    subscribe_message,
    subscribe_operation_message,
    unsubscribe_message,
    unsubscribe_operation_message,
)


def test_parse_book_and_ignore_pong() -> None:
    assert parse_ws_messages("PONG") == []
    events = parse_ws_messages(
        '{"event_type":"book","asset_id":"1","bids":[],"asks":[]}'
    )
    assert len(events) == 1
    assert events[0]["event_type"] == "book"
    many = parse_ws_messages(
        '[{"event_type":"price_change"},{"event_type":"book"}]'
    )
    assert len(many) == 2


def test_diff_tokens_added_removed() -> None:
    added, removed = diff_tokens({"a", "b"}, {"b", "c"})
    assert added == {"c"}
    assert removed == {"a"}


def test_official_initial_market_payload() -> None:
    msg = initial_market_message(["t1", "t2"])
    assert msg == {
        "assets_ids": ["t1", "t2"],
        "type": "market",
        "initial_dump": True,
        "custom_feature_enabled": True,
    }


def test_official_dynamic_subscribe_payload() -> None:
    msg = subscribe_operation_message(["t3"])
    assert msg == {
        "assets_ids": ["t3"],
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }
    assert "type" not in msg
    assert msg.get("type") != "market"


def test_official_dynamic_unsubscribe_payload() -> None:
    msg = unsubscribe_operation_message(["t1"])
    assert msg == {"assets_ids": ["t1"], "operation": "unsubscribe"}
    assert "type" not in msg
    unsub = unsubscribe_message(["t1"])
    assert unsub["operation"] == "unsubscribe"
    assert unsub.get("type") != "unsubscribe"


def test_connection_prefers_single_market_frame() -> None:
    msgs = connection_subscribe_messages(["a", "b", "c"], chunk_size=0)
    assert len(msgs) == 1
    assert msgs[0]["type"] == "market"
    assert msgs[0]["initial_dump"] is True
    assert msgs[0]["custom_feature_enabled"] is True
    assert msgs[0]["assets_ids"] == ["a", "b", "c"]


def test_chunked_connect_only_first_frame_is_type_market() -> None:
    ids = [f"t{i}" for i in range(5)]
    msgs = connection_subscribe_messages(ids, chunk_size=2)
    assert len(msgs) == 3
    assert msgs[0]["type"] == "market"
    assert msgs[0]["initial_dump"] is True
    for extra in msgs[1:]:
        assert extra["operation"] == "subscribe"
        assert extra.get("type") != "market"
        assert extra["custom_feature_enabled"] is True


def test_subscribe_message_compat_dynamic_is_operation() -> None:
    sub = subscribe_message(["t1", "t2"], initial_dump=True)
    assert sub["type"] == "market"
    dyn = subscribe_message(["t3"], initial_dump=False)
    assert dyn["operation"] == "subscribe"
    assert "type" not in dyn
