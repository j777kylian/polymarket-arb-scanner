"""Safety constants — v1 is permanently read-only."""

from __future__ import annotations

# Hard-coded trading gate. Do not enable in v1.
TRADING_ENABLED: bool = False


def assert_trading_disabled() -> None:
    """Raise if trading is incorrectly enabled."""
    if TRADING_ENABLED:
        raise NotImplementedError(
            "REAL TRADING IS DISABLED in this read-only research/simulation tool. "
            "Setting TRADING_ENABLED=True is not supported and will not place orders."
        )


def guard_write_endpoint(method: str, path: str) -> None:
    """Block any attempt to hit trading write endpoints."""
    method_u = method.upper()
    path_l = path.lower()
    if method_u in {"POST", "PUT", "PATCH", "DELETE"} and (
        "/order" in path_l or "/orders" in path_l
    ):
        raise NotImplementedError(
            f"Blocked trading write attempt: {method_u} {path}. "
            "This scanner is read-only."
        )
