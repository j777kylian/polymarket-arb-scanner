from datetime import datetime, timezone

from polymarket_scanner.database import init_db
from polymarket_scanner.models import ArbDirection, OpportunitySignal
from polymarket_scanner.scanners.opportunity_tracker import close_episodes, sync_episodes
from polymarket_scanner.scheduler import get_dashboard_stats


def test_close_episodes_reason(tmp_path, monkeypatch) -> None:
    db = tmp_path / "e.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    now = datetime.now(timezone.utc)
    sig = OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=1,
        yes_vwap=0.4,
        no_vwap=0.5,
        gross_profit=1,
        fee_total=0,
        net_profit=1,
        net_profit_per_share=0.1,
        net_profit_rate=0.1,
        levels_used_yes=1,
        levels_used_no=1,
    )
    sync_episodes([sig], scanned_market_ids={"m1"}, now=now)
    n = close_episodes(market_ids={"m1"}, reason="market_resolved")
    assert n == 1
    stats = get_dashboard_stats()
    assert stats["active_opportunities"] == 0
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
