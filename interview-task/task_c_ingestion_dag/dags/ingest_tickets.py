"""Incrementally ingest ticket snapshots from the local mock API."""
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

API_BASE_URL = "http://localhost:8123"
PAGE_SIZE = 10
DB_PATH = Path(__file__).parent.parent / "tickets.db"


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
            _ingested_at TEXT NOT NULL
        )
        """
    )


@dag(
    dag_id="ingest_tickets",
    schedule=None,  # trigger manually while you're building/testing
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["interview-task"],
)
def ingest_tickets():

    @task
    def get_watermark() -> str | None:
        """
        Return the ISO8601 timestamp of the most recent record we've already
        ingested, or None if this is the first run (full load).
        """
        with closing(sqlite3.connect(DB_PATH)) as connection, connection:
            ensure_tickets_table(connection)
            result = connection.execute(
                "SELECT MAX(updated_at) FROM tickets"
            ).fetchone()
        return result[0]

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract(watermark: str | None) -> list[dict]:
        """
        Read every page before passing records to the downstream load task.
        A failed request raises the task, so no partial page set can be loaded.
        """
        records = []
        cursor = 0

        while True:
            params = {"page_size": PAGE_SIZE, "cursor": cursor}
            if watermark is not None:
                params["updated_since"] = watermark

            url = f"{API_BASE_URL}/tickets?{urlencode(params)}"
            with urlopen(url, timeout=30) as response:
                page = json.load(response)

            records.extend(page["results"])
            next_cursor = page["next_cursor"]
            if next_cursor is None:
                return records
            if not isinstance(next_cursor, int) or next_cursor <= cursor:
                raise ValueError(f"Invalid pagination cursor: {next_cursor!r}")
            cursor = next_cursor

    @task
    def load(records: list[dict]) -> int:
        """
        Append each source snapshot; the raw table intentionally preserves
        earlier versions of a ticket.
        """
        with closing(sqlite3.connect(DB_PATH)) as connection, connection:
            ensure_tickets_table(connection)
            connection.executemany(
                """
                INSERT INTO tickets (
                    id, subject, status, priority, created_at, updated_at,
                    _ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        record["id"],
                        record["subject"],
                        record["status"],
                        record["priority"],
                        record["created_at"],
                        record["updated_at"],
                        datetime.now(timezone.utc).isoformat(),
                    )
                    for record in records
                ],
            )
        return len(records)

    watermark = get_watermark()
    records = extract(watermark)
    load(records)


ingest_tickets()
