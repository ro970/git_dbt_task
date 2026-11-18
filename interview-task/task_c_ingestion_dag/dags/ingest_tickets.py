"""Incrementally ingest ticket snapshots from the local mock API."""
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

API_BASE_URL = "http://localhost:8123"
PAGE_SIZE = 10
WATERMARK_LOOKBACK = timedelta(minutes=5)
DB_PATH = Path(__file__).parent.parent / "tickets.db"
SNAPSHOT_COLUMNS = (
    "id",
    "subject",
    "status",
    "priority",
    "created_at",
    "updated_at",
)


def ensure_tickets_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER NOT NULL,
            subject TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _extracted_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            _extracted_at TEXT NOT NULL
        )
        """
    )
    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(tickets)")
    }
    if "_extracted_at" not in columns:
        connection.execute("ALTER TABLE tickets ADD COLUMN _extracted_at TEXT")
        connection.execute(
            "UPDATE tickets SET _extracted_at = ? WHERE _extracted_at IS NULL",
            (datetime.now(timezone.utc).isoformat(),),
        )
    connection.execute(
        """
        INSERT OR IGNORE INTO ingestion_state (id, _extracted_at)
        SELECT 1, MAX(_extracted_at)
          FROM tickets
        HAVING MAX(_extracted_at) IS NOT NULL
        """
    )


def parse_utc_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def format_api_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dag(
    dag_id="ingest_tickets",
    schedule=None,  # trigger manually while you're building/testing
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["interview-task"],
)
def ingest_tickets():

    @task
    def get_watermark() -> str | None:
        """
        Clamp the source watermark to when we last read the API, then look
        back slightly to avoid missing updates at the API's second precision.
        """
        with closing(sqlite3.connect(DB_PATH)) as connection, connection:
            ensure_tickets_table(connection)
            result = connection.execute(
                """
                SELECT MAX(tickets.updated_at), MAX(ingestion_state._extracted_at)
                  FROM tickets
                  LEFT JOIN ingestion_state
                    ON ingestion_state.id = 1
                """
            ).fetchone()
        if result[0] is None or result[1] is None:
            return None

        source_watermark = parse_utc_timestamp(result[0])
        last_extracted_at = parse_utc_timestamp(result[1])
        watermark = min(source_watermark, last_extracted_at)
        return format_api_timestamp(watermark - WATERMARK_LOOKBACK)

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract(watermark: str | None) -> dict[str, Any]:
        """
        Read every page before passing records to the downstream load task.
        A failed request raises the task, so no partial page set can be loaded.
        """
        extracted_at = datetime.now(timezone.utc).isoformat()
        records_by_snapshot = {}
        cursor = 0

        while True:
            params = {"page_size": PAGE_SIZE, "cursor": cursor}
            if watermark is not None:
                params["updated_since"] = watermark

            url = f"{API_BASE_URL}/tickets?{urlencode(params)}"
            with urlopen(url, timeout=30) as response:
                page = json.load(response)

            for record in page["results"]:
                snapshot_key = tuple(record[column] for column in SNAPSHOT_COLUMNS)
                records_by_snapshot[snapshot_key] = record

            next_cursor = page["next_cursor"]
            if next_cursor is None:
                return {
                    "records": list(records_by_snapshot.values()),
                    "extracted_at": extracted_at,
                }
            if not isinstance(next_cursor, int) or next_cursor <= cursor:
                raise ValueError(f"Invalid pagination cursor: {next_cursor!r}")
            cursor = next_cursor

    @task
    def load(extraction: dict[str, Any]) -> int:
        """
        Append new source snapshots while ignoring exact snapshots already
        loaded during the watermark lookback.
        """
        records = extraction["records"]
        extracted_at = extraction["extracted_at"]
        with closing(sqlite3.connect(DB_PATH)) as connection, connection:
            ensure_tickets_table(connection)
            existing_snapshots = {
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT id, subject, status, priority, created_at, updated_at
                    FROM tickets
                    """
                )
            }
            rows_to_insert = []
            for record in records:
                snapshot = tuple(record[column] for column in SNAPSHOT_COLUMNS)
                if snapshot in existing_snapshots:
                    continue
                existing_snapshots.add(snapshot)
                rows_to_insert.append(
                    (
                        *snapshot,
                        datetime.now(timezone.utc).isoformat(),
                        extracted_at,
                    )
                )

            connection.executemany(
                """
                INSERT INTO tickets (
                    id, subject, status, priority, created_at, updated_at,
                    _ingested_at, _extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )
            connection.execute(
                """
                INSERT INTO ingestion_state (id, _extracted_at)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE
                    SET _extracted_at = excluded._extracted_at
                """,
                (extracted_at,),
            )
        return len(rows_to_insert)

    watermark = get_watermark()
    extraction = extract(watermark)
    load(extraction)


ingest_tickets()
