This folder intentionally contains only `int_customer_order_summary.sql`.

The model references four upstream models via `ref()` that don't exist in
this folder (`stg_orders`, `stg_customers`, `stg_order_line_items`,
`stg_products`). That's fine — this task is a pure text/SQL formatting
exercise, not something you need to run. The referenced models are a
customer table, an orders table, an order line items table (one row per
product per order), and a product catalogue table, each with the columns
used in the query. If it'd help to see them, ask — but you shouldn't need to
in order to complete the refactor.
