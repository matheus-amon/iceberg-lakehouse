#!/usr/bin/env python
"""Create the Iceberg JDBC catalog schema Trino needs, before Trino starts.

## Why this has to exist

Trino's JDBC Iceberg catalog opens a connection and queries. It never calls
`JdbcCatalog.initialize()`, and that method is what creates `iceberg_tables`.
So out of the box the catalog loads ("Added catalog iceberg") and then every
query fails:

    Error listing schemas for catalog iceberg:
    Failed to execute query: SELECT DISTINCT table_namespace FROM iceberg_tables
    WHERE catalog_name = ?

The DDL below is transcribed from iceberg-core 1.7.1 — `JdbcUtil.V0_CREATE_CATALOG_SQL`
and `JdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE_SQL` — rather than invented, so
it stays exactly the shape the library's own queries expect.

`init-catalog-tables = false` in trino/etc/catalog/iceberg.properties stops
Iceberg from trying to create them itself when it does get initialised, which
would otherwise fail on the SQLite file's locking behaviour.

## Ownership

The volume mounts root-owned, so this has to run as root to create the file —
but the file must end up owned by the `trino` user (uid 1000 in the Trino image)
or Trino cannot write to its own catalog and every CREATE SCHEMA fails with
SQLITE_READONLY. Hence the chown at the end, driven by CATALOG_OWNER.

## Usage

    docker compose run --rm catalog-init

Idempotent: it uses CREATE TABLE IF NOT EXISTS and can be re-run freely.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

# Transcribed from iceberg-core 1.7.1, JdbcUtil:
#   V0_CREATE_CATALOG_SQL
#   CREATE_NAMESPACE_PROPERTIES_TABLE_SQL
# SQLite takes IF NOT EXISTS where the library's bare CREATE TABLE would fail
# on a second run; the column list is otherwise identical.
CREATE_CATALOG = """
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
)
"""

CREATE_NAMESPACE_PROPERTIES = """
CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255),
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
)
"""

# `iceberg_type` is added by V1_UPDATE_CATALOG_SQL when schema-version=V1. The
# V1 path is for views; we stay on V0, so the column is absent by design.
EXPECTED_COLUMNS = {
    "iceberg_tables": {"catalog_name", "table_namespace", "table_name", "metadata_location"},
    "iceberg_namespace_properties": {"catalog_name", "namespace", "property_key", "property_value"},
}


def main() -> int:
    db_path = Path(os.getenv("CATALOG_DB_PATH", "/var/lib/trino/iceberg-catalog.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    exit_code = create_schema(db_path)
    if exit_code != 0:
        return exit_code

    chown_to_trino(db_path)
    return 0


def create_schema(db_path: Path) -> int:
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(CREATE_CATALOG)
        connection.execute(CREATE_NAMESPACE_PROPERTIES)
        connection.commit()

        # Verify rather than assume. The failure this script exists to prevent
        # is a catalog that loads and then cannot be queried, which looks
        # healthy right up until the first SELECT.
        for table, expected in EXPECTED_COLUMNS.items():
            actual = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
            missing = expected - actual
            if missing:
                print(f"FAIL: {table} is missing columns {sorted(missing)}", file=sys.stderr)
                return 1

        print(f"catalog schema ready in {db_path}")
        for table in EXPECTED_COLUMNS:
            print(f"  {table}: OK")
        return 0
    finally:
        connection.close()


def chown_to_trino(db_path: Path) -> None:
    """Hand the file to the Trino user.

    Without this the catalog is Trino's to read and nobody's to write, and the
    failure surfaces as SQLITE_READONLY on the first CREATE SCHEMA rather than
    anything that points at file ownership.

    The *directory* is chowned, not just the file. SQLite needs to create a
    rollback journal next to the database on every write, so a writable file in
    an unwritable directory still fails — and the error names the journal, not
    the ownership, which sends you looking in the wrong place.
    """
    owner = os.getenv("CATALOG_OWNER")
    if not owner:
        return

    result = subprocess.run(
        ["chown", "-R", owner, str(db_path.parent)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(
            f"WARN: could not chown {db_path.parent} to {owner}: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return

    print(f"  ownership: {owner}")


if __name__ == "__main__":
    raise SystemExit(main())