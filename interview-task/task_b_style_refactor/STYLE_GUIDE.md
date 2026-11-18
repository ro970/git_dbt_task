# SQL style guide (dbt models)

These rules are pervasive across our dbt models. Apply them even though
they're a bit unusual relative to generic SQL style guides you may have seen
elsewhere — consistency across the codebase matters more than any individual
rule's merit.

## Keyword casing

SQL keywords and functions are UPPERCASE (`SELECT`, `FROM`, `WHERE`, `JOIN`,
`LEFT JOIN`, `ON`, `AND`, `OR`, `CASE WHEN THEN ELSE END`, `GROUP BY`,
`ORDER BY`, `QUALIFY`, `UNION ALL`, `PARTITION BY`, `DISTINCT`, `IN`,
`COALESCE`, `ROUND`, `CAST`, `ROW_NUMBER`, etc). Table/column/alias names are
lowercase snake_case.

## River alignment

Right-align clause keywords in a `SELECT` block so they share a column —
`FROM`, `WHERE`, `JOIN`, and multi-word `LEFT JOIN` all end flush against the
same vertical line, with the clause body starting right after:

```sql
   SELECT orders.order_id                                     AS order_id
         ,orders.customer_id                                  AS customer_id
     FROM {{ ref('stg_orders') }} orders
     JOIN {{ ref('stg_customers') }} customers
       ON orders.customer_id = customers.customer_id
LEFT JOIN order_line_totals
       ON orders.order_id = order_line_totals.order_id
    WHERE 1 = 1
      AND orders.order_status NOT IN ('cancelled'
                                      ,'test'
                                      )
```

The rule is a **shared right-hand river, not a shared left indent**: every
clause keyword on the "keyword column" — `SELECT`/`FROM`/`WHERE`/`JOIN`/
`LEFT JOIN`/`ON`/`AND`/`GROUP BY` when it starts a line — is right-padded with
leading spaces so its *last* character lands in the same column, regardless
of the keyword's own length. `ON`/`AND` continuing a `JOIN` line up their
last character with `JOIN`'s last character; `ON`/`AND` continuing a
`LEFT JOIN` line up with `LEFT JOIN`'s last character instead (one column to
the left of the plain `JOIN` case, since `LEFT JOIN` is longer). Do **not**
align `ON` under where the joined table/`ref()` expression starts — align it
under the join keyword's own end. Stack multiple `AND` conditions the same
way, one river per block.

## Leading commas

Every column list, `GROUP BY`/`ORDER BY` list, and `IN (...)` list uses one
leading comma per line, the comma aligned under the first character of the
line above it:

```sql
SELECT customers.customer_id
      ,customers.customer_name
      ,customers.signup_date
```

## `GROUP BY ALL` vs explicit list

For aggregation queries, prefer `GROUP BY ALL` (no explicit column list) —
it's simpler to keep correct as the select list changes. Only write an
explicit leading-comma `GROUP BY` list if the grouping columns are non-obvious
from the `SELECT` list.

## Padded `AS`

Within a single `SELECT` list, pad every `AS <alias>` with spaces so all the
`AS` keywords in that block line up in one column, measured from the first
character after that block's own leading comma/`SELECT` keyword (i.e.
relative to that block's shared left indent, not column 1 of the file). The
column is set per-block by whichever expression in that block is longest —
recompute it for each `SELECT`/CTE, don't reuse one fixed width for the whole
file:

```sql
    ,order_totals AS (SELECT customer_id
                            ,SUM(line_total)              AS lifetime_spend
                            ,MAX(order_date)              AS most_recent_order_date
                       FROM ...
```

This still applies even when the aligned expression is a long `CASE`/window
function — the `AS` column just moves further right for that block.

## CTEs

Start with `WITH <name> AS (SELECT ...)`; chain further CTEs with a leading
comma indented under `WITH`; give each CTE a descriptive business-meaning
snake_case name (`customer_orders`, `latest_order_per_customer`,
`order_line_totals`) — there's no rigid `source`/`staged`/`final` triad
requirement, name a CTE for what it represents. The final statement is
commonly a plain `SELECT * FROM <last_cte>`.

```sql
WITH customer_orders AS (SELECT customer_id
                                ,order_id
                            FROM {{ ref('stg_orders') }}
                         )
    ,order_totals AS (SELECT customer_id
                            ,SUM(line_total) AS lifetime_spend
                        FROM ...
                     )
SELECT *
  FROM order_totals
```

## CASE expressions

`CASE`/`WHEN`/`THEN`/`ELSE`/`END` each on their own line, all aligned
directly under `CASE`; a multi-condition `WHEN ... AND ...` indents the `AND`
under the condition it continues:

```sql
,CASE WHEN order_status = 'refunded'
       AND refund_amount IS NOT NULL
      THEN refund_amount
      WHEN order_status = 'refunded'
      THEN line_total
      ELSE 0
 END                                     AS refunded_amount
```

Short CASE expressions nested inside an aggregate (e.g. pivot-style
`MAX(CASE WHEN ... THEN ... END)` columns) may stay on one line if that
matches the surrounding block's own style — check the file you're editing.

## Comments

No file header/footer banners. A sparing single-line `--` comment is fine to
explain non-obvious business logic — don't over-comment.

## `ref()`/`source()`

Any reference to another model is `{{ ref('model_name') }}`, single-quoted,
aliased immediately after: `FROM {{ ref('stg_orders') }} orders`. Any
reference to a raw/unmodeled source table is `{{ source('source_name',
'table_name') }}`.
