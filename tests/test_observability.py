"""These tests need no Docker, no Trino, no Kafka.

That is the point. ``evaluate`` and the alert rules are pure functions over
plain dataclasses, so the thing that decides whether the project is healthy can
be tested on a laptop in a second — and, more importantly, the alerting cannot
be "it would definitely fire in production".
"""

from __future__ import annotations

from observability import (
    DUPLICATE_THRESHOLD,
    FRESHNESS_THRESHOLD_SECONDS,
    NULL_RATE_THRESHOLD,
    TableMetrics,
    evaluate,
    is_healthy,
    render,
)
from observability.alerts import (
    Severity,
    duplicate_alert,
    freshness_alert,
    null_rate_alert,
    schema_drift_alert,
)


def healthy_metrics(**overrides) -> TableMetrics:
    base = {
        "table": "events",
        "freshness_seconds": 30.0,
        "row_count": 1000,
        "null_rate": {"campaign_id": 0.10, "country": 0.10, "revenue": 0.10},
        "duplicate_ids": 0,
        "last_compaction": None,
    }
    base.update(overrides)
    return TableMetrics(**base)


class TestEvaluate:
    def test_a_healthy_table_produces_no_red_findings(self):
        findings = evaluate(healthy_metrics())

        assert is_healthy(findings)
        assert all(f.ok for f in findings)

    def test_an_empty_table_is_unhealthy(self):
        # The regression this guards: row_count 0 made every other check
        # vacuously pass, so a dead pipeline rendered fully green.
        findings = evaluate(healthy_metrics(row_count=0))

        assert not is_healthy(findings)
        assert [f.check for f in findings] == ["row_count"]

    def test_stale_data_trips_freshness(self):
        findings = evaluate(
            healthy_metrics(freshness_seconds=FRESHNESS_THRESHOLD_SECONDS + 1)
        )

        freshness = next(f for f in findings if f.check == "freshness")
        assert not freshness.ok

    def test_freshness_exactly_at_threshold_stays_green(self):
        findings = evaluate(healthy_metrics(freshness_seconds=FRESHNESS_THRESHOLD_SECONDS))

        freshness = next(f for f in findings if f.check == "freshness")
        assert freshness.ok

    def test_a_column_that_is_all_null_trips_its_null_rate(self):
        findings = evaluate(
            healthy_metrics(null_rate={"campaign_id": 1.0, "country": 0.1, "revenue": 0.1})
        )

        assert not is_healthy(findings)
        assert not next(f for f in findings if f.check == "null_rate:campaign_id").ok

    def test_duplicate_ids_trip_the_duplicate_check(self):
        findings = evaluate(healthy_metrics(duplicate_ids=DUPLICATE_THRESHOLD + 1))

        assert not is_healthy(findings)
        assert not next(f for f in findings if f.check == "duplicates").ok

    def test_render_marks_each_row_green_or_red(self):
        text = render(healthy_metrics(), evaluate(healthy_metrics()))

        assert "GREEN" in text
        assert "RESULT: healthy" in text

    def test_render_says_unhealthy_when_a_check_fails(self):
        metrics = healthy_metrics(row_count=0)
        text = render(metrics, evaluate(metrics))

        assert "RED" in text
        assert "RESULT: UNHEALTHY" in text


class TestFreshnessAlert:
    def test_pages_when_data_is_stale(self):
        alert = freshness_alert(3600, FRESHNESS_THRESHOLD_SECONDS)

        assert alert is not None
        assert alert.severity is Severity.PAGE

    def test_pages_when_there_is_no_data_at_all(self):
        alert = freshness_alert(None, FRESHNESS_THRESHOLD_SECONDS)

        assert alert is not None
        assert "no events" in alert.message

    def test_stays_silent_when_fresh(self):
        assert freshness_alert(10, FRESHNESS_THRESHOLD_SECONDS) is None


class TestNullRateAlert:
    def test_warns_over_threshold(self):
        alert = null_rate_alert("country", 0.9, NULL_RATE_THRESHOLD)

        assert alert is not None
        assert alert.severity is Severity.WARN

    def test_stays_silent_under_threshold(self):
        assert null_rate_alert("country", 0.1, NULL_RATE_THRESHOLD) is None


class TestDuplicateAlert:
    def test_pages_on_any_duplicate(self):
        alert = duplicate_alert(1, DUPLICATE_THRESHOLD)

        assert alert is not None
        assert alert.severity is Severity.PAGE

    def test_stays_silent_at_zero(self):
        assert duplicate_alert(0, DUPLICATE_THRESHOLD) is None


class TestSchemaDriftAlert:
    EXPECTED = {"event_id", "user_id", "event_time", "revenue"}

    def test_pages_on_a_missing_column(self):
        alert = schema_drift_alert("events", self.EXPECTED, self.EXPECTED - {"revenue"})

        assert alert is not None
        assert alert.severity is Severity.PAGE
        assert "revenue" in alert.message

    def test_warns_on_an_unexpected_column(self):
        alert = schema_drift_alert("events", self.EXPECTED, self.EXPECTED | {"surprise"})

        assert alert is not None
        assert alert.severity is Severity.WARN

    def test_stays_silent_when_the_schema_matches(self):
        assert schema_drift_alert("events", self.EXPECTED, set(self.EXPECTED)) is None

    def test_alert_serialises_with_its_severity(self):
        alert = duplicate_alert(3, DUPLICATE_THRESHOLD)
        assert alert is not None

        assert '"severity": "page"' in alert.to_json()

class TestGeneratorTimestampFormat:
    """The generator's timestamp format is load-bearing.

    Flink's CAST(string AS TIMESTAMP_LTZ) fails on Python's default
    isoformat() because it emits 6 fractional digits. That failure surfaces as
    a DateTimeParseException deep in the Kafka source, which reads like a
    connector problem rather than a formatting one.
    """

    def test_event_time_is_millisecond_precision_with_a_z_suffix(self):
        import re
        from generator import _iso
        from datetime import datetime, timezone

        rendered = _iso(datetime(2026, 10, 1, 1, 7, 54, 148790, tzinfo=timezone.utc))

        assert rendered == "2026-10-01T01:07:54.148Z"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", rendered)

    def test_a_generated_event_carries_the_parsable_timestamp(self):
        import re
        import random
        from datetime import datetime, timezone
        from generator import make_event

        event = make_event("v1", datetime.now(timezone.utc), random.Random(0))

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", event.event_time
        )
