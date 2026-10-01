"""Run the scorecard against a live Trino and exit non-zero when red.

Exit code is the interface: this is meant to sit in CI or a cron, so a red
scorecard fails a job rather than printing something a human might not read.
"""

from __future__ import annotations

import argparse
import sys

from . import MetricError, collect_metrics, connect, evaluate, is_healthy, render


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=None, help="Trino host (default $TRINO_HOST)")
    parser.add_argument("--port", type=int, default=None, help="Trino port (default $TRINO_PORT)")
    args = parser.parse_args(argv)

    import os

    kwargs = {}
    if args.host:
        kwargs["host"] = args.host
    if args.port:
        kwargs["port"] = args.port

    try:
        connection = connect(**kwargs)
        metrics = collect_metrics(connection)
    except MetricError as exc:
        # "I could not measure" is not "healthy". Exit 2 to distinguish an
        # unreachable Trino from a reachable Trino holding bad data.
        print(f"UNHEALTHY — {exc}", file=sys.stderr)
        return 2

    findings = evaluate(metrics)
    print(render(metrics, findings))

    return 0 if is_healthy(findings) else 1


if __name__ == "__main__":
    raise SystemExit(main())