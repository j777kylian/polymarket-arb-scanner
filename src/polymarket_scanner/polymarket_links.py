"""Helpers for linking to Polymarket website pages."""

from __future__ import annotations


def polymarket_event_url(event_slug: str | None) -> str | None:
    if not event_slug:
        return None
    return f"https://polymarket.com/event/{event_slug}"


def polymarket_market_url(slug: str | None, event_slug: str | None = None) -> str | None:
    """Prefer event URL (website nav uses events); fall back to market slug."""
    if event_slug:
        return polymarket_event_url(event_slug)
    if slug:
        return f"https://polymarket.com/event/{slug}"
    return None
