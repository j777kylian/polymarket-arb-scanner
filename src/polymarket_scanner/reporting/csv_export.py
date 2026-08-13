"""CSV export helpers."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable


def export_rows_to_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path
