-- Query 42 auction houses clicks data 2025
WITH clicks_with_new_id AS (
  SELECT
    programid,
    CASE
      WHEN programid = 'arosfrimarken'                        THEN '3915'
      WHEN programid = 'bruketauktion'                        THEN '3952'
      WHEN programid = '605'                                  THEN '3768'
      WHEN programid = 'skeleton' 
        AND STARTS_WITH(url, 'https://www.conap.se')          THEN '3902'
      WHEN programid = 'skeleton' 
        AND STARTS_WITH(url, 'https://www.dyrgripen.se')      THEN '53'
      ELSE programid
    END AS new_programid,
    timestamp
  FROM `barnebys-analytics.tracking.click`
  WHERE timestamp >= '2025-01-01'
    AND timestamp < '2026-01-01'
)

SELECT
  new_programid AS programid,
  FORMAT_TIMESTAMP('%Y-%m', timestamp) AS month,
  COUNT(*) AS clicks
FROM clicks_with_new_id
WHERE new_programid IN (
  '3915', '3952', '3862', '3900', '3902', '3898', '44',
  '53', '230', '3922', '90', '3768', '3031', '3601', '3756', '2949', 
  '3849', '107', '2659', '3925', '72', '3687', '125', '3865', '3950', '3723', 
  '3923', '3904', '283', '3764', '3722', '3869', '3663', '3916'
)
GROUP BY month, new_programid
ORDER BY month, new_programid