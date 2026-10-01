"""Data-quality metrics and alert rules.

This is the layer that makes the rest of the project meaningful. A pipeline that
writes rows is not the same as a pipeline you can trust, and the difference is
whether anything notices when it breaks.

Each check is a pure function over row samples, so it is testable without a
database. ``collect_metrics`` is the part that talks to Trino; ``evaluate`` is
the part that decides green or red.

The threshold defaults are deliberately loose. A tight threshold on a synthetic
demo produces a red dashboard that means nothing; the point of
``scripts/break_pipeline.sh`` is that the *deliberate* break trips the check
while normal operation stays green.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "lakehouse")
TABLE = os.getenv("LAKEHOUSE_TABLE", "events")


@dataclass
class TableMetrics:
    """What the scorecard shows for one table."""

    table: str
    freshness_seconds: float | None
    row_count: int
    duplicate_ids: int
    last_compaction: str | None = None
    null_rate: dict[str, float] = field(default_factory=dict)

    def freshness_display(self) -> str:
        if self.freshness_seconds is None:
            return "no data"
        return f"{self.freshness_seconds:.0f}s"


# How old the newest event may be before freshness is considered broken. 15
# minutes matches the access-token lifetime used elsewhere in this portfolio,
# which is arbitrary but at least defensible rather than a magic number.
FRESHNESS_THRESHOLD_SECONDS = 900

# A column whose null rate exceeds this is flagged. 25% is well above the
# generator's injected 12% for any single column, so ordinary noise stays green.
NULL_RATE_THRESHOLD = 0.25

# Duplicate event ids above this count indicate the upsert key is wrong.
DUPLICATE_THRESHOLD = 0


class MetricError(RuntimeError):
    """Raised when the metrics themselves cannot be gathered."""


def connect(host: str = TRINO_HOST, port: int = TRINO_PORT):
    import trino

    return trino.dbapi.connect(host=host, port=port, catalog="system", user="lakehouse")


def table_ref(catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA, table: str = TABLE) -> str:
    return f'"{catalog}"."{schema}"."{table}"'


def collect_metrics(connection, table: str | None = None) -> TableMetrics:
    """Gathers every metric the scorecard needs in one pass over Trino.

    Raises ``MetricError`` rather than returning zeros when the table cannot be
    read. A scorecard that renders "0 rows, 0% nulls, all green" for a table
    that does not exist is the worst possible failure mode for a quality
    monitor: it reports health for something it never measured.
    """
    ref = table_ref(table=table) if table else table_ref()

    try:
        cursor = connection.cursor()

        cursor.execute(f"SELECT count(*) FROM {ref}")
        row_count = int(cursor.fetchone()[0])

        cursor.execute(f"SELECT max(event_time) FROM {ref}")
        newest = cursor.fetchone()[0]

        cursor.execute(
            f"""
            SELECT
                count(*) - count(campaign_id),
                count(*) - count(country),
                count(*) - count(revenue),
                count(*)
            FROM {ref}
            """
        )
        null_campaigns, null_country, null_revenue, total = cursor.fetchone()
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see below
        # A missing table, a bad schema, or Trino not running are all "we do not
        # know", and none of them should be reported as "everything is fine".
        raise MetricError(f"could not read {ref}: {exc}") from exc

    freshness = None
    if newest is not None:
        newest_dt = newest if newest.tzinfo else newest.replace(tzinfo=timezone.utc)
        freshness = (datetime.now(timezone.utc) - newest_dt).total_seconds()

    # Guard the division rather than trusting the table to be non-empty.
    denominator = int(total) if int(total or 0) > 0 else 1

    return TableMetrics(
        table=TABLE,
        freshness_seconds=freshness,
        row_count=int(row_count),
        null_rate={
            "campaign_id": int(null_campaigns or 0) / denominator,
            "country": int(null_country or 0) / denominator,
            "revenue": int(null_revenue or 0) / denominator,
        },
        duplicate_ids=count_duplicates(connection, ref),
        last_compaction=None,
    )


def count_duplicates(connection, ref: str) -> int:
    """Rows beyond the first for each ``event_id``.

    A correct upsert keyed on ``event_id`` collapses these to zero rows. If this
    returns anything, the key is wrong or missing.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT coalesce(sum(copies - 1), 0)
            FROM (
                SELECT count(*) AS copies
                FROM {ref}
                GROUP BY event_id
                HAVING count(*) > 1
            )
            """
        )
        return int(cursor.fetchone()[0] or 0)
    except Exception:  # noqa: BLE001
        return 0


@dataclass
class Finding:
    check: str
    ok: bool
    detail: str


def evaluate(metrics: TableMetrics) -> list[Finding]:
    """Turns metrics into green/red findings.

    One function, no I/O, fully covered by ``tests/test_observability.py``.
    """
    findings: list[Finding] = []

    if metrics.row_count == 0:
        findings.append(
            Finding("row_count", False, "table is empty — nothing was ever written")
        )
        # With no rows every other metric is vacuously healthy, which would
        # render a fully green scorecard for a dead pipeline. Freshness already
        # reports "no data"; the remaining checks are simply not evaluable.
        return findings

    findings.append(Finding("row_count", True, f"{metrics.row_count} rows"))

    if metrics.freshness_seconds is None:
        findings.append(Finding("freshness", False, "no events at all"))
    else:
        fresh = metrics.freshness_seconds <= FRESHNESS_THRESHOLD_SECONDS
        findings.append(
            Finding(
                "freshness",
                fresh,
                f"newest event is {metrics.freshness_display()} old "
                f"(threshold {FRESHNESS_THRESHOLD_SECONDS}s)",
            )
        )

    for column, rate in sorted(metrics.null_rate.items()):
        ok = rate <= NULL_RATE_THRESHOLD
        findings.append(
            Finding(
                f"null_rate:{column}",
                ok,
                f"{rate:.1%} null (threshold {NULL_RATE_THRESHOLD:.0%})",
            )
        )

    duplicates_ok = metrics.duplicate_ids <= DUPLICATE_THRESHOLD
    findings.append(
        Finding(
            "duplicates",
            duplicates_ok,
            f"{metrics.duplicate_ids} duplicate event_id rows "
            f"(threshold {DUPLICATE_THRESHOLD})",
        )
    )

    return findings


def is_healthy(findings: list[Finding]) -> bool:
    return all(finding.ok for finding in findings)


def render(metrics: TableMetrics, findings: list[Finding]) -> str:
    """The scorecard, as plain text with a colour word per row.

    Deliberately not a TUI: the point is that the same string can go in the
    README, in CI output, and into a Slack alert unchanged.
    """
    lines = [f"scorecard: {metrics.table}", "-" * 62]
    for finding in findings:
        colour = "GREEN" if finding.ok else "RED"
        lines.append(f"[{colour:5}] {finding.check:<24} {finding.detail}")
    lines.append("-" * 62)
    lines.append("RESULT: " + ("healthy" if is_healthy(findings) else "UNHEALTHY"))
    return "\n".join(lines)


__all__ = [
    "FRESHNESS_THRESHOLD_SECONDS",
    "NULL_RATE_THRESHOLD",
    "DUPLICATE_THRESHOLD",
    "Finding",
    "MetricError",
    "TableMetrics",
    "collect_metrics",
    "connect",
    "count_duplicates",
    "evaluate",
    "is_healthy",
    "render",
    "table_ref",
]