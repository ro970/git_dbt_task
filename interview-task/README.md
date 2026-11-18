# Data Engineer Technical Exercise

Welcome! This exercise is split into three independent tasks. All use a
small, fictional retail/support dataset (customers, orders, tickets) —
nothing here reflects any real company or dataset.

Please write and actually run the code for each task rather than just
reasoning about it on paper — we'll review your solution and process
together afterwards (what you tried, what broke, how you debugged it), so
we want to see the real thing, not just a diff.

You do not need a real data warehouse or any cloud services to complete any
of these, everything runs locally:
- Task A needs `dbt-core` + `dbt-duckdb` installed locally (both free/open
  source, no server).
- Task B needs nothing but a text editor.
- Task C needs `apache-airflow` installed locally (2.x or 3.x, either is
  fine) — no other services, credentials, or network access. Everything
  runs on localhost.

- **Task A — `task_a_dbt_bug/`**: a dbt model with a subtle logic bug and a
  disabled unit test that should have caught it. Find it, fix it, get the
  test green.
- **Task B — `task_b_style_refactor/`**: a working but "messy" SQL model plus
  a short house style guide. Refactor the model to match the style guide,
  exactly as you'd do joining an existing team with established conventions.
- **Task C — `task_c_ingestion_dag/`**: a mock third-party API and a DAG
  skeleton. Write an Airflow ingestion pipeline that paginates, loads
  incrementally, and upserts idempotently.

Time budget: we suggest ~30-45 minutes per task if done live, or please
don't spend more than ~2-2.5 hours total if done as a take-home. We care
more about your reasoning and how you communicate it than about a "perfect"
diff.
