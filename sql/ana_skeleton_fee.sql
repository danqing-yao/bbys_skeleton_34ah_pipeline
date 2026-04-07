CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.ana_skeleton_fee`
AS

WITH

-- Step 1: Aggregate transaction data by auction_house_id + month
monthly_winning AS (
  SELECT
    auction_house_id,
    aucton_house_name,
    month,
    SUM(CASE WHEN winning_sign = 'win' THEN hammeredprice ELSE 0 END)     AS month_hammeredprice,
    SUM(CASE WHEN winning_sign = 'win' THEN hammeredprice_eur ELSE 0 END)  AS month_hammeredprice_eur
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
  WHERE month IS NOT NULL
  GROUP BY auction_house_id, aucton_house_name, month
),

-- EUR exchange rates
eur_rates AS (
  SELECT currency, euro_rate_2025_avg
  FROM `barnebys-skeleton.42ah.raw_currency_eur`
),

-- Step 2: Convert min_fee and fixed_fee to EUR
pricing_with_eur AS (
  SELECT
    p.*,
    ROUND(p.min_fee   * r.euro_rate_2025_avg, 2) AS min_fee_eur,
    ROUND(p.fixed_fee * r.euro_rate_2025_avg, 2) AS fixed_fee_eur
  FROM `barnebys-skeleton.42ah.raw_skeleton_pricing` p
  LEFT JOIN eur_rates r ON p.currency = r.currency
),

-- Step 3: Calculate skeleton_fee_eur using hammeredprice_eur
skeleton_fee_calc AS (
  SELECT
    m.auction_house_id,
    m.aucton_house_name,
    m.month,
    m.month_hammeredprice,
    m.month_hammeredprice_eur,
    p.type          AS pricing_type,
    p.rate,
    p.min_fee,
    p.min_fee_eur,
    p.fixed_fee,
    p.fixed_fee_eur,
    p.currency      AS fee_currency,
    CASE p.type
      WHEN 'Percent' THEN ROUND(m.month_hammeredprice_eur * p.rate, 2)
      WHEN 'Fixed'   THEN p.fixed_fee_eur
      WHEN 'Hybrid'  THEN ROUND(GREATEST(m.month_hammeredprice_eur * p.rate, p.min_fee_eur), 2)
    END AS skeleton_fee_eur
  FROM monthly_winning m
  LEFT JOIN pricing_with_eur p
    ON m.auction_house_id = p.auction_house_id
),

-- Step 4: Aggregate barnebys_increment_eur
barnebys_increment_monthly AS (
  SELECT
    auction_house_id,
    month,
    SUM(barnebys_increment_eur) AS barnebys_increment_eur
  FROM `barnebys-skeleton.42ah.ana_barnebys_increment`
  WHERE month IS NOT NULL
    AND barnebys_increment_eur IS NOT NULL
  GROUP BY auction_house_id, month
),

-- Step 5: Barnebys winning hammeredprice_eur
bbys_hammeredprice AS (
  SELECT
    auction_house_id,
    month,
    SUM(hammeredprice_eur) AS bbys_hammeredprice_eur
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
  WHERE winning_sign = 'win'
    AND user_source_byfirstbid = 'barnebys'
    AND month IS NOT NULL
  GROUP BY auction_house_id, month
),

-- Step 6: Clicks per auction house per month
clicks_monthly AS (
  SELECT
    CAST(programid AS INT64) AS auction_house_id,
    month,
    SUM(clicks) AS clicks
  FROM `barnebys-skeleton.42ah.raw_bite_clicks`
  GROUP BY programid, month
),

-- Step 7: Barnebys bidders per auction house per month
bidders_monthly AS (
  SELECT
    auction_house_id,
    month,
    COUNT(DISTINCT WebUserid) AS bidders
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids`
  WHERE user_source_byfirstbid = 'barnebys'
    AND month IS NOT NULL
  GROUP BY auction_house_id, month
),

-- Step 8: Barnebys registrations per auction house per month
registrations_monthly AS (
  SELECT
    CAST(programId AS INT64) AS auction_house_id,
    FORMAT_TIMESTAMP('%Y-%m', timestamp) AS month,
    COUNT(sessionId) AS registrations
  FROM `barnebys-skeleton.42ah.raw_bite_registrations`
  WHERE source = 'barnebys'
  GROUP BY programId, FORMAT_TIMESTAMP('%Y-%m', timestamp)
),

-- Step 9: Barnebys winners per auction house per month
winners_monthly AS (
  SELECT
    auction_house_id,
    month,
    COUNT(DISTINCT WebUserid) AS winners
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
  WHERE user_source_byfirstbid = 'barnebys'
    AND winning_sign = 'win'
    AND month IS NOT NULL
  GROUP BY auction_house_id, month
),

-- Step 10: Generate all months 2025-01 to 2025-12
all_months AS (
  SELECT month FROM UNNEST([
    '2025-01','2025-02','2025-03','2025-04','2025-05','2025-06',
    '2025-07','2025-08','2025-09','2025-10','2025-11','2025-12'
  ]) AS month
),

-- Fixed: all Fixed auction houses
fixed_ah AS (
  SELECT auction_house_id
  FROM `barnebys-skeleton.42ah.raw_skeleton_pricing`
  WHERE type = 'Fixed'
),

-- Hybrid: exclude 283, 3950, 72 (mid-year partnerships)
hybrid_ah AS (
  SELECT auction_house_id
  FROM `barnebys-skeleton.42ah.raw_skeleton_pricing`
  WHERE type = 'Hybrid'
    AND auction_house_id NOT IN (283, 3950, 72)
),

-- Generate required ah + month combinations
required_months AS (
  -- Fixed: full 12 months
  SELECT f.auction_house_id, m.month
  FROM fixed_ah f
  CROSS JOIN all_months m

  UNION ALL

  -- Hybrid (except 2659): full 12 months
  SELECT h.auction_house_id, m.month
  FROM hybrid_ah h
  CROSS JOIN all_months m
  WHERE h.auction_house_id != 2659

  UNION ALL

  -- 2659: only March, May, July
  SELECT 2659 AS auction_house_id, month
  FROM UNNEST(['2025-03', '2025-05', '2025-07']) AS month
),

-- Find missing months
missing_months AS (
  SELECT r.auction_house_id, r.month
  FROM required_months r
  LEFT JOIN skeleton_fee_calc s
    ON r.auction_house_id = s.auction_house_id
    AND r.month = s.month
  WHERE s.auction_house_id IS NULL
),

-- Generate filled rows for missing months
filled_rows AS (
  SELECT
    m.auction_house_id,
    ah.aucton_house_name,
    m.month,
    0                     AS month_hammeredprice,
    0                     AS month_hammeredprice_eur,
    p.type                AS pricing_type,
    p.rate,
    p.min_fee,
    p.min_fee_eur,
    p.fixed_fee,
    p.fixed_fee_eur,
    p.currency            AS fee_currency,
    CASE p.type
      WHEN 'Fixed'  THEN p.fixed_fee_eur
      WHEN 'Hybrid' THEN p.min_fee_eur
    END                   AS skeleton_fee_eur,
    0                     AS barnebys_increment_eur,
    0                     AS bbys_hammeredprice_eur,
    0                     AS clicks,
    0                     AS registrations,
    0                     AS bidders,
    0                     AS winners
  FROM missing_months m
  LEFT JOIN pricing_with_eur p
    ON m.auction_house_id = p.auction_house_id
  LEFT JOIN (
    SELECT DISTINCT auction_house_id, aucton_house_name
    FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
  ) ah
    ON m.auction_house_id = ah.auction_house_id
),

-- Final output: original data + filled rows
final_data AS (
  SELECT
    s.auction_house_id,
    s.aucton_house_name,
    s.month,
    s.month_hammeredprice,
    s.month_hammeredprice_eur,
    s.pricing_type,
    s.rate,
    s.min_fee,
    s.min_fee_eur,
    s.fixed_fee,
    s.fixed_fee_eur,
    s.fee_currency,
    s.skeleton_fee_eur,
    COALESCE(bi.barnebys_increment_eur, 0)  AS barnebys_increment_eur,
    COALESCE(bh.bbys_hammeredprice_eur, 0)  AS bbys_hammeredprice_eur,
    COALESCE(cl.clicks, 0)                  AS clicks,
    COALESCE(re.registrations, 0)           AS registrations,
    COALESCE(bd.bidders, 0)                 AS bidders,
    COALESCE(wi.winners, 0)                 AS winners
  FROM skeleton_fee_calc s
  LEFT JOIN barnebys_increment_monthly bi ON s.auction_house_id = bi.auction_house_id AND s.month = bi.month
  LEFT JOIN bbys_hammeredprice bh         ON s.auction_house_id = bh.auction_house_id AND s.month = bh.month
  LEFT JOIN clicks_monthly cl             ON s.auction_house_id = cl.auction_house_id AND s.month = cl.month
  LEFT JOIN registrations_monthly re      ON s.auction_house_id = re.auction_house_id AND s.month = re.month
  LEFT JOIN bidders_monthly bd            ON s.auction_house_id = bd.auction_house_id AND s.month = bd.month
  LEFT JOIN winners_monthly wi            ON s.auction_house_id = wi.auction_house_id AND s.month = wi.month

  UNION ALL

  SELECT * FROM filled_rows
)

SELECT
  *,
  CASE
    WHEN SUM(CASE WHEN month NOT LIKE '%-06' AND month NOT LIKE '%-07'
                  THEN barnebys_increment_eur ELSE 0 END)
         OVER (PARTITION BY auction_house_id) >= 100000 THEN 'High'
    WHEN SUM(CASE WHEN month NOT LIKE '%-06' AND month NOT LIKE '%-07'
                  THEN barnebys_increment_eur ELSE 0 END)
         OVER (PARTITION BY auction_house_id) < 10000  THEN 'Low'
    ELSE 'Mid'
  END AS bbys_tier
FROM final_data
ORDER BY auction_house_id, month