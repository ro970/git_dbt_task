# Task A: Find and fix the bug

## Setup

This is a minimal dbt project that runs against DuckDB (no server/credentials
needed). Please install and run it for real — we want to see your actual
workflow (running the test, reading the failure, iterating), not just a
written diff:

```bash
pip install dbt-core dbt-duckdb
cd task_a_dbt_bug
dbt seed
dbt build
```

## The scenario

We track marketing campaigns that customers "attend" (e.g. a webinar, an
email click, a demo). A customer can attend the same campaign more than once,
and can attend multiple campaigns.

`models/intermediate/int_customer_latest_campaign.sql` is supposed to
produce **one row per customer**: their most recent campaign touch. This
feeds a mart used for "who did we last talk to, and about what" reporting.

There is a unit test already written for this model, with fixtures already
prepared, in `tests/fixtures/`. The test is currently **commented out** in
`models/intermediate/_intermediate__models.yml`.

## Your task

1. Uncomment the unit test block in `_intermediate__models.yml`.
2. Run it: `dbt build --select test_type:unit`.
3. It will fail. Work out why.
4. Fix the model so the test passes — show us the green run.
5. Briefly say (a couple of sentences is fine) how you'd have caught this
   before it reached production, if the unit test hadn't already existed.

There is exactly one intentional bug. Everything else in the model works as
intended.
