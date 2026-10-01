"""Alert rules as pure functions.

The scoring logic in ``observability/__init__.py`` decides green/red for the
scorecard. These decide whether that red is worth waking someone for — and,
just as importantly, that the alerting is itself exercised by a test rather than
being an untested script, which is the habit that makes a monitor useless.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum


class Severity(str, Enum):
    PAGE = "page"
    WARN = "warn"
    INFO = "info"


@dataclass
class Alert:
    rule: str
    severity: Severity
    message: str

    def to_json(self) -> str:
        return json.dumps({**asdict(self), "severity": self.severity.value})


def freshness_alert(freshness_seconds: float | None, threshold: int) -> Alert | None:
    """Data went stale.

    A ``page``: no events landing means ingestion is down, which is the failure
    that costs the most and is easiest to mistake for "quiet day".
    """
    if freshness_seconds is None:
        return Alert(
            "freshness",
            Severity.PAGE,
            "no events present in the table at all",
        )
    if freshness_seconds > threshold:
        return Alert(
            "freshness",
            Severity.PAGE,
            f"newest event is {freshness_seconds:.0f}s old, over the {threshold}s threshold",
        )
    return None


def null_rate_alert(column: str, rate: float, threshold: float) -> Alert | None:
    """A column went mostly empty.

    ``warn`` rather than ``page``: a null column usually means an upstream schema
    change, which is worth knowing before someone reads the number.
    """
    if rate > threshold:
        return Alert(
            f"null_rate:{column}",
            Severity.WARN,
            f"{rate:.1%} of rows have null {column}, over the {threshold:.0%} threshold",
        )
    return None


def duplicate_alert(duplicate_ids: int, threshold: int) -> Alert | None:
    """Duplicate business keys appeared.

    ``page``: this means the upsert key is not doing its job, so the table has
    been double-counting revenue for an unknown amount of time.
    """
    if duplicate_ids > threshold:
        return Alert(
            "duplicates",
            Severity.PAGE,
            f"{duplicate_ids} duplicate event_id rows; the upsert key is not collapsing them",
        )
    return None


def schema_drift_alert(table: str, expected: set[str], actual: set[str]) -> Alert | None:
    """The table's columns no longer match what the job declared.

    ``page``, not ``warn``: a job that writes a shape nobody expects is either
    silently failing on some records or writing defaults that nobody chose.
    """
    missing = expected - actual
    unexpected = actual - expected

    if missing:
        return Alert(
            "schema_drift",
            Severity.PAGE,
            f"{table} is missing expected column(s): {', '.join(sorted(missing))}",
        )
    if unexpected:
        return Alert(
            "schema_drift",
            Severity.WARN,
            f"{table} has unexpected column(s): {', '.join(sorted(unexpected))}",
        )
    return None


__all__ = [
    "Alert",
    "Severity",
    "duplicate_alert",
    "freshness_alert",
    "null_rate_alert",
    "schema_drift_alert",
]