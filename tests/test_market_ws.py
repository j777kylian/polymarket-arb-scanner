from polymarket_scanner.api.market_ws import parse_ws_messages


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
