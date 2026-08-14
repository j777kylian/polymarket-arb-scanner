# Changelog

## Unreleased

Changes on `hermes/reliable-simulation` since `main` (`a555df2`):

- Added an optional positive `--duration-seconds` limit for Live Research daemons, with normal shutdown, lock release, WebSocket/paper-task cleanup, latency flushing, and stop-time reporting.
- Added live-run accounting for ready market pairs/books, signals, and API errors.
- Preserved signed WebSocket latency samples and surfaced host clock skew as an explicit insufficient-latency warning in dashboard metrics.
- Documented bounded paper simulation usage and acknowledged intentional script import ordering for Ruff.
- Added offline tests covering bounded daemon behavior, duration CLI validation, cleanup, live-run metrics, signed latency, and clock-skew detection.
