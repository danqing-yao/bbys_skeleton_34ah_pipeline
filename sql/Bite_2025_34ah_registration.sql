-- Query 42 auction houses registration data 2025
WITH registrations_with_new_id AS (
  SELECT
    programId,
    CASE
      WHEN programId = 'arosfrimarken'                        THEN '3915'
      WHEN programId = 'bruketauktion'                        THEN '3952'
      WHEN programId = '605'                                  THEN '3768'
      WHEN programId = 'skeleton'
        AND STARTS_WITH(url, 'https://www.conap.se')          THEN '3902'
      WHEN programId = 'skeleton'
        AND STARTS_WITH(url, 'https://www.dyrgripen.se')      THEN '53'
      ELSE programId
    END AS new_programId,
    category,
    action,
    source,
    sessionId,
    timestamp
  FROM `barnebys-analytics.tracking.events`
  WHERE
    timestamp >= DATETIME("2025-01-01")
    AND timestamp < DATETIME("2026-01-01")
    AND category = 'registration'
    AND action = 'completed'
)

SELECT
  new_programId AS programId,
  category,
  action,
  source,
  sessionId,
  timestamp
FROM registrations_with_new_id
WHERE new_programId IN (
  '3915', '3952', '3862', '3900', '3902', '3898', '44',
  '53', '230', '3922', '90', '3768', '3031', '3601', '3756', '2949', 
  '3849', '107', '2659', '3925', '72', '3687', '125', '3865', '3950', '3723', 
  '3923', '3904', '283', '3764', '3722', '3869', '3663', '3916'
)
ORDER BY timestamp