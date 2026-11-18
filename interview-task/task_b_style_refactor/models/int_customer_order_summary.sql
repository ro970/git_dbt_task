WITH customer_orders AS (SELECT o.customer_id
                              ,o.order_id
                              ,o.order_date
                              ,o.order_status
                              ,c.customer_name
                              ,c.region
                          FROM {{ ref('stg_orders') }} o
                     LEFT JOIN {{ ref('stg_customers') }} c
                            ON o.customer_id = c.customer_id
                         WHERE 1 = 1
                           AND o.order_status NOT IN ('cancelled'
                                                     ,'test'
                                                     )
                       )
    ,line_items AS (SELECT li.order_id
                          ,li.product_id
                          ,p.product_name
                          ,p.category
                          ,li.quantity
                          ,li.unit_price
                          ,li.quantity * li.unit_price AS line_total
                      FROM {{ ref('stg_order_line_items') }} li
                 LEFT JOIN {{ ref('stg_products') }} p
                        ON li.product_id = p.product_id
                   )
    ,order_totals AS (SELECT customer_orders.customer_id
                            ,customer_orders.order_id
                            ,customer_orders.order_date
                            ,SUM(line_items.line_total) AS order_value
                        FROM customer_orders
                   LEFT JOIN line_items
                          ON customer_orders.order_id = line_items.order_id
                     GROUP BY customer_orders.customer_id
                             ,customer_orders.order_id
                             ,customer_orders.order_date
                     QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_orders.customer_id
                                                    ORDER BY customer_orders.order_date DESC) = 1
                     )
    ,lifetime AS (SELECT customer_orders.customer_id
                        ,SUM(line_items.line_total)               AS lifetime_spend
                        ,COUNT(DISTINCT customer_orders.order_id) AS total_orders
                    FROM customer_orders
               LEFT JOIN line_items
                      ON customer_orders.order_id = line_items.order_id
               GROUP BY ALL
                 )
SELECT order_totals.customer_id
      ,order_totals.order_id                                            AS most_recent_order_id
      ,order_totals.order_date                                          AS most_recent_order_date
      ,order_totals.order_value                                         AS most_recent_order_value
      ,lifetime.lifetime_spend
      ,lifetime.total_orders
      ,MAX(CASE WHEN customer_orders.region = 'EMEA' THEN 1 ELSE 0 END) AS is_emea_customer
  FROM order_totals
LEFT JOIN lifetime
       ON order_totals.customer_id = lifetime.customer_id
LEFT JOIN customer_orders
       ON order_totals.customer_id = customer_orders.customer_id
 GROUP BY ALL
 ORDER BY order_totals.customer_id
