-- =====================================================
-- ana_lot_price_tiers
-- Purpose: Assign price tier to each lot based on max_bid_price
-- Based on: proc_skeleton_auctions_with_enteredbids
-- =====================================================

CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.ana_lot_price_tiers`
OPTIONS(
  description = "Lot-level price tiers based on max bid price distribution with percentile ranges"
)
AS

WITH

-- Step 1: Calculate max_bid_price for each lot
lot_max_prices AS (
  SELECT
    CAST(id AS STRING) AS inventoryId,
    auction_house_id,
    aucton_house_name,
    COALESCE(
      MAX(hammeredprice_eur),
      MAX(value)
    ) AS max_bid_price
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids`
  GROUP BY id, auction_house_id, aucton_house_name
),

-- Step 2: Calculate percentiles across all lots
price_percentiles AS (
  SELECT
    APPROX_QUANTILES(max_bid_price, 100)[OFFSET(50)] AS p50_threshold,
    APPROX_QUANTILES(max_bid_price, 100)[OFFSET(80)] AS p80_threshold,
    MIN(max_bid_price) AS overall_min,
    MAX(max_bid_price) AS overall_max
  FROM lot_max_prices
),

-- Step 3: Assign tier to each lot
lots_with_tiers AS (
  SELECT
    l.inventoryId,
    l.auction_house_id,
    l.aucton_house_name,
    l.max_bid_price,
    p.p50_threshold,
    p.p80_threshold,
    p.overall_min,
    p.overall_max,
    CASE
      WHEN l.max_bid_price < p.p50_threshold THEN 'Low'
      WHEN l.max_bid_price < p.p80_threshold THEN 'Mid'
      ELSE 'High'
    END AS price_tier,
    CASE
      WHEN l.max_bid_price < p.p50_threshold THEN 1
      WHEN l.max_bid_price < p.p80_threshold THEN 2
      ELSE 3
    END AS tier_order
  FROM lot_max_prices l
  CROSS JOIN price_percentiles p
)

SELECT
  inventoryId,
  auction_house_id,
  aucton_house_name,
  max_bid_price,
  price_tier,
  tier_order,
  CASE
    WHEN price_tier = 'Low' THEN
      CONCAT(
        CAST(CAST(FLOOR(overall_min) AS INT64) AS STRING),
        ' - ',
        CAST(CAST(FLOOR(p50_threshold) AS INT64) AS STRING)
      )
    WHEN price_tier = 'Mid' THEN
      CONCAT(
        CAST(CAST(FLOOR(p50_threshold) AS INT64) AS STRING),
        ' - ',
        CAST(CAST(FLOOR(p80_threshold) AS INT64) AS STRING)
      )
    WHEN price_tier = 'High' THEN
      CONCAT(
        CAST(CAST(FLOOR(p80_threshold) AS INT64) AS STRING),
        ' - ',
        CAST(CAST(CEIL(overall_max) AS INT64) AS STRING)
      )
  END AS tier_range,
  CASE
    WHEN price_tier = 'Low'  THEN 'Bottom 50%'
    WHEN price_tier = 'Mid'  THEN '50% – 80%'
    WHEN price_tier = 'High' THEN 'Top 20%'
  END AS percentile_range,
  p50_threshold,
  p80_threshold
FROM lots_with_tiers
ORDER BY inventoryId;