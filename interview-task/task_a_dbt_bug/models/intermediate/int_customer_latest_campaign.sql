WITH attendance AS (

    SELECT customer_id
          ,campaign_name
          ,campaign_date
          ,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY campaign_date DESC) AS rown
      FROM {{ ref('stg_campaign_attendance') }}

)

SELECT customer_id
      ,campaign_name           AS latest_campaign_name
      ,campaign_date           AS latest_campaign_date
  FROM attendance
 WHERE rown = 1
