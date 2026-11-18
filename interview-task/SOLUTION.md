# Data Engineering Exercise — Solution

This document summarizes the implementation, the issues found during
validation, and the commands and checks used for all three tasks.

## Task A — dbt latest-campaign model

The unit test in
[`task_a_dbt_bug/models/intermediate/_intermediate__models.yml`](task_a_dbt_bug/models/intermediate/_intermediate__models.yml)
is enabled. The original model partitioned `ROW_NUMBER()` by both
`customer_id` and `campaign_name`, so ranking restarted for every campaign,
and it did not filter to `rown = 1`. As a result, it could return multiple
touches per customer instead of one latest touch. The fix is to partition
only by `customer_id` and keep only `rown = 1`.

The unit-test input and expected fixtures are CSV files, so both fixture
configurations specify `format: csv`. Without that format, dbt treated the
fixture rows as dictionaries and failed to parse the CSV input.

The model
[`task_a_dbt_bug/models/intermediate/int_customer_latest_campaign.sql`](task_a_dbt_bug/models/intermediate/int_customer_latest_campaign.sql)
uses `ROW_NUMBER()` partitioned by `customer_id`, orders each customer's
campaign dates descending, and keeps row 1. This produces the most recent
campaign for each customer. The fixture-based unit test checks that the newest
date wins. Even without the unit test, the existing `unique` data test on
`customer_id` would fail when multiple touches for one customer reached the
model, so `dbt build` in CI would block the change before production.

### Validation and issue encountered

With dbt 1.12.7, the initial `dbt build --select test_type:unit` selected no
nodes. Running the unit test before its staging relation existed also failed
because `stg_campaign_attendance` had not been built. I seeded the inputs and
built the models before rerunning the test; the focused unit test then passed.

Run the complete project build from `task_a_dbt_bug`:

```shell
dbt build --profiles-dir .
```

Final result:

```text
Completed successfully
PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=8
```

The eight successful resources were two seeds, two staging views, one
intermediate table, one unit test, and two data tests.

## Task B — SQL style refactor

[`task_b_style_refactor/models/int_customer_order_summary.sql`](task_b_style_refactor/models/int_customer_order_summary.sql)
was reformatted to follow the house guide:

- SQL keywords, leading commas, clause rivers, and padded aliases follow the
  guide's conventions.
- Short table aliases were replaced with `orders`, `customers`,
  `order_line_items`, and `products`.
- CTEs now describe their contents: `customer_orders`,
  `order_line_details`, `order_totals`, `latest_order_per_customer`,
  `customer_lifetime_totals`, and `customer_order_summary`.
- The final statement selects from `customer_order_summary` and applies
  `ORDER BY customer_id` in the outer query, where ordering is guaranteed.
- The query's calculations, output columns, and grain were preserved.
- The guide's original trailing space was restored; its rules and wording
  were not changed.

During validation, DuckDB rejected `GROUP BY ALL` combined with `QUALIFY` in
the same query block. I separated order aggregation into `order_totals` and
latest-order ranking into `latest_order_per_customer`. The refactored query
then executed successfully. I ran both the original and refactored SQL
against the same DuckDB fixture; their output rows matched.

### Logic risks noted, not changed

This task requested a style-only refactor, so these potential data-quality
issues are reported rather than silently changed:

- Duplicate `product_id` keys in `stg_products` or duplicate `customer_id`
  keys in `stg_customers` can multiply joined rows, double-counting
  `order_value` and `lifetime_spend`.
- `order_status NOT IN (...)` excludes rows whose `order_status` is `NULL`.
- If a customer has multiple orders with the same `order_date`, the
  `ROW_NUMBER()` ordering has no tie-breaker, so the selected latest order
  is not deterministic.

## Task C — Incremental ticket ingestion

[`task_c_ingestion_dag/dags/ingest_tickets.py`](task_c_ingestion_dag/dags/ingest_tickets.py)
uses three TaskFlow tasks: `get_watermark`, `extract`, and `load`.

- `extract` follows `next_cursor` until it is `null`, collects all pages
  before handing data to `load`, and de-duplicates identical snapshots
  across pages.
- Each extracted snapshot records `_extracted_at`. The watermark is the
  earlier of the maximum source `updated_at` and the last successful
  extraction time. A five-minute lookback accounts for the API's
  second-level, strict-`>` filter.
- Exact snapshots already in `tickets` are skipped. Changed snapshots are
  appended with fresh `_ingested_at` values; existing raw rows are not
  overwritten or deleted.
- A small `ingestion_state` table advances the extraction timestamp only
  after a successful load, including successful runs with no new rows. This
  avoids advancing the bookmark past future-dated source records while still
  allowing the bookmark to progress.
- `max_active_runs=1` prevents overlapping DAG runs. If an API request fails
  during pagination, extraction fails and `load` does not run; Airflow retries
  the extraction task.
- Existing SQLite databases are migrated to add `_extracted_at`.

### How it was run and verified

The mock API was started with:

```shell
python mock_api/server.py
```

With the Airflow DAG folder set to `task_c_ingestion_dag/dags`, the DAG was
run using Airflow 2.11.2:

```shell
airflow dags test ingest_tickets 2026-11-21
```

The actual Airflow TaskFlow run completed successfully: extraction returned
47 tickets and the load task inserted 47 rows.

Additional isolated checks used a temporary SQLite database:

| Check | Result |
|---|---:|
| Initial full load | 47 rows inserted |
| Repeat with no upstream changes | 0 rows inserted |
| Touch ticket 5 and run again | 1 new snapshot inserted |
| Set a stored source timestamp in 2099, then touch ticket 6 | Ticket 6 was still extracted and inserted |
| Duplicate record on two pages | One snapshot returned |
| Simulated API failure on the second page | Extraction failed; no partial load occurred |
| Existing database without `_extracted_at` | Migration and future-timestamp clamp passed |

Airflow emitted a Windows platform-support warning during the local run, but
the DAG imported, registered, and executed successfully. Airflow recommends
WSL2 or Linux containers for Windows development environments.
