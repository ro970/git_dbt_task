SELECT attendance_id
      ,customer_id
      ,campaign_name
      ,CAST(campaign_date AS DATE)   AS campaign_date
  FROM {{ ref('raw_campaign_attendance') }}
