# Task C: Write an ingestion DAG

## The scenario

We need to pull data from a third-party "support ticketing" API into our
warehouse on a schedule. This task uses a small mock version of that API
(stdlib Python, no external dependencies) standing in for a real one — the
shape of the problem (pagination, incremental loads, insert-only writes) is
exactly what a real source integration looks like.

## Setup

You'll need `apache-airflow` installed (any 2.x or 3.x is fine — adjust
import paths as needed for whichever you have; note if you had to change
anything for your version). No other services, credentials, or network
access required — everything runs on localhost with SQLite.

**Terminal 1** — start the mock API and leave it running:
```bash
python mock_api/server.py
```
Confirm it's up: open `http://localhost:8123/tickets?page_size=5` in a
browser, or `python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8123/health').read())"`.

**Terminal 2** — point Airflow at this folder's `dags/` directory and start
it however you'd normally run Airflow locally (`airflow standalone`,
`astro dev start`, or just importing/running the DAG file directly — your
choice, tell us which you used).

## The API

`GET http://localhost:8123/tickets`

Query params:
- `page_size` (default 10) — max records per page
- `cursor` (default 0) — pagination offset; response includes the
  `next_cursor` to pass on the next request, or `null` when there's no more
  data
- `updated_since` (optional) — ISO8601 UTC string (`2026-07-20T00:00:00Z`);
  only returns records with `updated_at` strictly after this value

Response shape:
```json
{
  "results": [
    {"id": 1, "subject": "...", "status": "open", "priority": "normal",
     "created_at": "2026-07-01T12:00:00Z", "updated_at": "2026-07-02T08:00:00Z"}
  ],
  "next_cursor": 10,
  "total_matching": 47
}
```

There is no authentication on this mock (a real one would have API keys/
OAuth — assume that's handled and not the focus of this exercise).

There's also a test-only helper, `POST /_test/touch/<id>`, which marks a
given ticket as freshly updated (bumps its `updated_at` to now, flips its
status). Use it between DAG runs to simulate "new activity happened
upstream" so you can actually verify your incremental logic picks up
changes, rather than just re-running against static data:
```bash
python -c "import urllib.request as u; print(u.urlopen(u.Request('http://localhost:8123/_test/touch/1', method='POST')).read())"
```

## Your task

Open `dags/ingest_tickets.py` — it has the DAG skeleton, constants, and a
`TODO` at each point you need to fill in. Build a DAG that:

1. **Paginates** through `/tickets` until `next_cursor` is `null`, collecting
   all results.
2. **Loads incrementally**: on first run, pull everything. On subsequent
   runs, only pull records updated since the last successful run (i.e.
   maintain some kind of watermark/bookmark) — don't just re-pull all 47
   records every time.
3. **Writes insert-only** into the local SQLite file `tickets.db` (a table
   `tickets`) — this matches how our real ingestion DAGs land data: every
   row you write gets a fresh `_ingested_at` timestamp, and rows are always
   appended, never updated or deleted in place. If the same ticket `id`
   comes back in a later run with a changed status, that's a *new* row, not
   an overwrite of the old one — the table ends up holding every snapshot
   we've ever ingested of every ticket. (Collapsing that down to "one row
   per ticket, latest wins" is a downstream/transform-layer job, not
   something this DAG does — think of this table as our raw layer.)
4. Uses the **TaskFlow API** (`@dag`/`@task` decorators), not classic
   operators.
5. Handles the obvious failure mode: what happens if the API call fails
   partway through pagination? You don't need a bulletproof retry system,
   but say what you'd do and why (a code comment or a sentence in your
   writeup is fine).

You do not need to build a custom Airflow Hook/Connection for this — a plain
`requests`/`urllib` call to `http://localhost:8123` is completely fine, and
is intentionally simpler than production so you can focus on the DAG logic.

## What we're looking for

- Correct pagination loop (terminates correctly, doesn't drop or duplicate
  records across pages).
- A sane approach to "what's new since last time" — where does the
  watermark live, and why there (Airflow Variable? XCom from the previous
  run? querying `MAX(updated_at)` from the destination table itself)?
- A genuinely insert-only write — every call to your load task appends new
  rows and never mutates or removes an existing row, even for a ticket `id`
  it's seen before. Prove this to yourself: run the DAG twice in a row with
  no new upstream activity and row count shouldn't grow (nothing new to
  fetch); then use `/_test/touch/<id>` and run again — row count should grow
  by exactly the number of touched tickets, and you should be able to find
  more than one row for that ticket `id` in the table afterwards.
- Readable TaskFlow-style DAG structure — sensible task boundaries, no
  giant single function doing everything.
- You don't need to write unit tests for this one, but you're welcome to.

We're not grading on production-hardened error handling, retry/backoff
policies, or perfect config-driven generality — just on whether the core
ingestion logic (pagination + incremental + insert-only load) is correct and
the DAG is structured in a way a teammate could read and extend.
