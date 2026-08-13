from polymarket_scanner.api.market_ws import (
    diff_tokens,
    parse_ws_messages,
    subscribe_message,
    unsubscribe_message,
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


def test_subscribe_unsubscribe_payloads() -> None:
    sub = subscribe_message(["t1", "t2"], initial_dump=True)
    assert sub["type"] == "market"
    assert sub["initial_dump"] is True
    assert sub["assets_ids"] == ["t1", "t2"]
    unsub = unsubscribe_message(["t1"])
    assert unsub["type"] == "unsubscribe"
    assert unsub["assets_ids"] == ["t1"]
