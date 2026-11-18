# Task B: Refactor to house style

## The scenario

You've just joined a data team that has an established
SQL formatting convention for their dbt models, documented in
`STYLE_GUIDE.md`. A model was written quickly by someone who didn't follow
it. Your job: bring it into line with the guide, without changing its
behaviour.

`models/int_customer_order_summary.sql` computes, per customer, their most
recent order and a lifetime spend total, joining across orders, order line
items, and a product catalogue. It works, but it doesn't follow the team's
conventions.

## Your task

1. Read `STYLE_GUIDE.md` in full.
2. Refactor `models/int_customer_order_summary.sql` to match it: keyword
   casing, the "river" alignment of `SELECT`/`FROM`/`JOIN`/`WHERE`, leading
   commas, padded `AS` aliases, CTE naming, etc.
3. Do **not** change what the query computes or its output columns/grain —
   this is a pure style/readability pass, not a logic rewrite. (If you spot
   an actual bug while you're in there, note it separately rather than
   silently fixing it — flag it and explain what you found.)
4. We'll talk through your diff together and ask you to justify a couple of
   the formatting choices, so you understand *why* the guide asks for them,
   not just that you pattern-matched it.

There's no dbt project scaffolding needed for this one — it's a pure
read-SQL-write-SQL exercise. A text editor is all you need.
