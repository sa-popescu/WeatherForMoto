#!/usr/bin/env python3
"""Migrate existing local SQLite data into Turso.

Usage:
  python migrate_to_turso.py --source ../backend/app.db

The backend is configured to use Turso only, so this script is needed to
move legacy SQLite data into the Turso database before switching the app.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Any

TURSO_URL_ENV = "TURSO_DATABASE_URL"
TURSO_TOKEN_ENV = "TURSO_AUTH_TOKEN"
TABLES_TO_COPY = [
    "users",
    "sessions",
    "alert_prefs",
    "push_subscriptions",
    "alert_events",
    "saved_routes",
    "ride_logs",
    "hazard_reports",
    "auth_codes",
    "password_reset_tokens",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate local SQLite DB into a Turso database.")
    parser.add_argument(
        "--source",
        default=os.path.join(os.path.dirname(__file__), "app.db"),
        help="Path to local SQLite source database.",
    )
    parser.add_argument(
        "--turso-url",
        default=os.getenv(TURSO_URL_ENV, ""),
        help=f"Turso database URL (or set ${TURSO_URL_ENV}).",
    )
    parser.add_argument(
        "--turso-token",
        default=os.getenv(TURSO_TOKEN_ENV, ""),
        help=f"Turso auth token (or set ${TURSO_TOKEN_ENV}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Continue even if the target Turso database already contains rows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without writing to Turso.",
    )
    return parser.parse_args()


def get_row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    try:
        return dict(row)
    except Exception:
        try:
            return {k: row[k] for k in range(len(row))}
        except Exception:
            raise RuntimeError("Unable to convert row to dict")


def connect_local(source_path: str) -> sqlite3.Connection:
    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"SQLite source database not found: {source_path}")
    conn = sqlite3.connect(source_path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_turso(turso_url: str, turso_token: str) -> Any:
    if not turso_url or not turso_token:
        raise RuntimeError(
            f"Missing Turso configuration. Set {TURSO_URL_ENV} and {TURSO_TOKEN_ENV}."
        )
    import libsql_experimental as libsql  # type: ignore

    return libsql.connect(turso_url, auth_token=turso_token)


def ensure_turso_schema() -> None:
    from auth_alerts import init_db

    init_db()


def count_rows(conn: Any, table: str) -> int:
    cur = conn.execute(f"SELECT COUNT(*) AS c FROM {table}")
    row = cur.fetchone()
    if row is None:
        return 0
    return int(row[0] if isinstance(row, (tuple, list)) else row["c"])


def copy_table(source_conn: sqlite3.Connection, dest_conn: Any, table: str, dry_run: bool) -> int:
    source_cur = source_conn.execute(f"SELECT * FROM {table}")
    rows = source_cur.fetchall()
    if not rows:
        return 0

    columns = rows[0].keys()
    cols_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f"INSERT OR REPLACE INTO {table} ({cols_sql}) VALUES ({placeholders})"

    values = [tuple(get_row_dict(row)[col] for col in columns) for row in rows]
    if not dry_run:
        dest_conn.executemany(insert_sql, values)
        dest_conn.commit()
    return len(values)


def main() -> int:
    args = parse_args()

    print("[migrate_to_turso] source:", args.source)
    print("[migrate_to_turso] turso url:", "set" if args.turso_url else "missing")
    print("[migrate_to_turso] force:", args.force)
    print("[migrate_to_turso] dry-run:", args.dry_run)

    if not args.turso_url or not args.turso_token:
        print(
            f"ERROR: Both --turso-url and --turso-token are required, or set ${TURSO_URL_ENV} and ${TURSO_TOKEN_ENV}.",
            file=sys.stderr,
        )
        return 1

    local_conn = connect_local(args.source)
    try:
        turso_conn = connect_turso(args.turso_url, args.turso_token)
    except Exception as exc:
        print(f"ERROR: Could not connect to Turso: {exc}", file=sys.stderr)
        return 1

    if not args.dry_run:
        print("[migrate_to_turso] ensuring Turso schema...")
        ensure_turso_schema()
        turso_conn = connect_turso(args.turso_url, args.turso_token)

    dest_has_data = False
    for table in TABLES_TO_COPY:
        try:
            count = count_rows(turso_conn, table)
        except Exception:
            count = 0
        if count > 0:
            dest_has_data = True
            print(f"[migrate_to_turso] target table {table} contains {count} rows")

    if dest_has_data and not args.force:
        print(
            "ERROR: Target Turso database already contains data. Use --force to proceed anyway.",
            file=sys.stderr,
        )
        return 1

    total_copied = 0
    for table in TABLES_TO_COPY:
        try:
            copied = copy_table(local_conn, turso_conn, table, args.dry_run)
            print(f"[migrate_to_turso] copied {copied} rows into {table}")
            total_copied += copied
        except sqlite3.OperationalError as exc:
            print(f"[migrate_to_turso] skipped table {table}: {exc}")
        except Exception as exc:
            print(f"ERROR: Failed copying table {table}: {exc}", file=sys.stderr)
            return 1

    print(f"[migrate_to_turso] migration complete: {total_copied} rows copied")
    if args.dry_run:
        print("[migrate_to_turso] dry run only; no changes were written to Turso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
