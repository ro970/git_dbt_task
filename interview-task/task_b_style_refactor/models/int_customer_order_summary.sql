WITH customer_orders AS (SELECT orders.customer_id
                              ,orders.order_id
                              ,orders.order_date
                              ,orders.order_status
                              ,customers.customer_name
                              ,customers.region
                           FROM {{ ref('stg_orders') }} orders
                      LEFT JOIN {{ ref('stg_customers') }} customers
                             ON orders.customer_id = customers.customer_id
                          WHERE 1 = 1
                            AND orders.order_status NOT IN ('cancelled'
                                                           ,'test'
                                                           )
                       )
    ,order_line_details AS (SELECT order_line_items.order_id
                                  ,order_line_items.product_id
                                  ,products.product_name
                                  ,products.category
                                  ,order_line_items.quantity
                                  ,order_line_items.unit_price
                                  ,order_line_items.quantity * order_line_items.unit_price AS line_total
                              FROM {{ ref('stg_order_line_items') }} order_line_items
                         LEFT JOIN {{ ref('stg_products') }} products
                                ON order_line_items.product_id = products.product_id
                            )
    ,order_totals AS (SELECT customer_orders.customer_id
                            ,customer_orders.order_id
                            ,customer_orders.order_date
                            ,SUM(order_line_details.line_total) AS order_value
                        FROM customer_orders
                   LEFT JOIN order_line_details
                          ON customer_orders.order_id = order_line_details.order_id
                    GROUP BY ALL
                   )
    ,latest_order_per_customer AS (SELECT order_totals.customer_id
                                         ,order_totals.order_id
                                         ,order_totals.order_date
                                         ,order_totals.order_value
                                     FROM order_totals
                                  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_totals.customer_id
                                                                 ORDER BY order_totals.order_date DESC) = 1
                                   )
    ,customer_lifetime_totals AS (SELECT customer_orders.customer_id
                                        ,SUM(order_line_details.line_total)                AS lifetime_spend
                                        ,COUNT(DISTINCT customer_orders.order_id)          AS total_orders
                                    FROM customer_orders
                               LEFT JOIN order_line_details
                                      ON customer_orders.order_id = order_line_details.order_id
                                GROUP BY ALL
                              )
    ,customer_order_summary AS (SELECT latest_order_per_customer.customer_id
                                      ,latest_order_per_customer.order_id                                            AS most_recent_order_id
                                      ,latest_order_per_customer.order_date                                          AS most_recent_order_date
                                      ,latest_order_per_customer.order_value                                         AS most_recent_order_value
                                      ,customer_lifetime_totals.lifetime_spend
                                      ,customer_lifetime_totals.total_orders
                                      ,MAX(CASE WHEN customer_orders.region = 'EMEA' THEN 1 ELSE 0 END)              AS is_emea_customer
                                  FROM latest_order_per_customer
                             LEFT JOIN customer_lifetime_totals
                                    ON latest_order_per_customer.customer_id = customer_lifetime_totals.customer_id
                             LEFT JOIN customer_orders
                                    ON latest_order_per_customer.customer_id = customer_orders.customer_id
                              GROUP BY ALL
                              ORDER BY latest_order_per_customer.customer_id
                              )
SELECT *
  FROM customer_order_summary
