"""
A tiny, dependency-free mock REST API standing in for a real third-party
source system. Stdlib only (http.server) so the candidate doesn't need to
install anything beyond what they'd already need to run Airflow.

Simulates the two things real source APIs make you deal with:
  - cursor-based pagination (a page of results + a `next_cursor`)
  - incremental filtering via an `updated_since` query param, and a handful
    of records that get "updated" (bumped updated_at, changed fields) each
    time the server restarts, so a second incremental run has real new/
    changed data to pick up.

Run with:  python mock_api/server.py
Then browse http://localhost:8123/tickets?page_size=2 in a browser or curl
it to see the shape of the data.
"""
import json
import random
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = 8123

_LOCK = threading.Lock()
_NOW = datetime.now(timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_tickets():
    statuses = ["open", "pending", "resolved", "closed"]
    tickets = []
    for i in range(1, 48):
        created = _NOW - timedelta(days=random.randint(1, 90))
        updated = created + timedelta(hours=random.randint(0, 48))
        tickets.append(
            {
                "id": i,
                "subject": f"Sample ticket #{i}",
                "status": random.choice(statuses),
                "priority": random.choice(["low", "normal", "high"]),
                "created_at": _iso(created),
                "updated_at": _iso(updated),
            }
        )
    # Force a handful of records to look freshly updated "just now" so an
    # incremental run against a prior watermark has something new to fetch.
    for i in (3, 17, 44):
        tickets[i - 1]["status"] = "resolved"
        tickets[i - 1]["updated_at"] = _iso(_NOW - timedelta(minutes=random.randint(1, 5)))
    return sorted(tickets, key=lambda t: t["updated_at"])


TICKETS = _seed_tickets()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep console quiet

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._send_json({"status": "ok"})
            return

        if parsed.path != "/tickets":
            self._send_json({"error": "not found"}, status=404)
            return

        with _LOCK:
            page_size = int(qs.get("page_size", ["10"])[0])
            cursor = int(qs.get("cursor", ["0"])[0])
            updated_since = qs.get("updated_since", [None])[0]

            data = TICKETS
            if updated_since:
                data = [t for t in data if t["updated_at"] > updated_since]

            page = data[cursor : cursor + page_size]
            next_cursor = cursor + page_size if cursor + page_size < len(data) else None

            self._send_json(
                {
                    "results": page,
                    "next_cursor": next_cursor,
                    "total_matching": len(data),
                }
            )

    def do_POST(self):
        """
        Test-only helper endpoint: POST /_test/touch/<id> marks a ticket as
        freshly updated (bumps updated_at to now, flips its status), so you
        can simulate "new upstream activity happened between DAG runs"
        without waiting or restarting the server. Not something a real
        source API would expose — just a convenience for exercising your
        incremental logic on demand.
        """
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")

        if len(parts) == 3 and parts[0] == "_test" and parts[1] == "touch":
            ticket_id = int(parts[2])
            with _LOCK:
                for t in TICKETS:
                    if t["id"] == ticket_id:
                        t["updated_at"] = _iso(datetime.now(timezone.utc))
                        t["status"] = "reopened"
                        self._send_json({"touched": ticket_id, "updated_at": t["updated_at"]})
                        return
                self._send_json({"error": f"no ticket with id {ticket_id}"}, status=404)
                return

        self._send_json({"error": "not found"}, status=404)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", PORT), Handler)
    print(f"Mock ticketing API running on http://localhost:{PORT}  (Ctrl+C to stop)")
    print(f"Try: http://localhost:{PORT}/tickets?page_size=5")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
