-- =====================================================
-- proc_skeleton_auctions_with_winning_sign
-- =====================================================
-- Step 1: Filter records with hammeredprice
-- Step 2: Mark winning_sign with 3 rules
--         - Rule 1: enteredbidId = bidid → win
--         - Rule 2: No bidid match → value >= hammeredprice → win
--         - Rule 3: No win at all → synthetic win
-- Step 3: Keep only smallest win per id + WebUserid
-- Step 4: Clean invalid records
-- Step 5: Replace win value with hammeredprice
-- Step 6: Add synthetic wins for id with no win
-- =====================================================

CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
CLUSTER BY id, auction_house_id
OPTIONS(
  description = "Skeleton auctions with winning_sign, synthetic wins added for unmatched inventories"
)
AS

WITH

-- Step 1: Filter hammeredprice IS NOT NULL
bids_with_hammer AS (
  SELECT *
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids`
  WHERE hammeredprice IS NOT NULL
),

-- Step 2a: Check if each id has enteredbidId = bidid match
inventory_has_winning_bidid AS (
  SELECT
    id,
    auction_house_id,
    MAX(CASE
      WHEN enteredbidId = CAST(bidid AS STRING) THEN 1
      ELSE 0
    END) AS has_bidid_match
  FROM bids_with_hammer
  GROUP BY id, auction_house_id
),

-- EUR exchange rates
eur_rates AS (
  SELECT currency, euro_rate_2025_avg
  FROM `barnebys-skeleton.42ah.raw_currency_eur`
),

-- Step 2b: Mark winning_sign
bids_with_winning_sign AS (
  SELECT
    b.*,
    i.has_bidid_match,
    CASE
      -- Rule 1: Has bidid match
      WHEN i.has_bidid_match = 1 THEN
        CASE
          WHEN b.enteredbidId = CAST(b.bidid AS STRING) THEN 'win'
          ELSE 'not_win'
        END
      -- Rule 2: No bidid match, use value vs hammeredprice
      ELSE
        CASE
          WHEN b.value >= b.hammeredprice THEN 'win'
          ELSE 'not_win'
        END
    END AS winning_sign
  FROM bids_with_hammer b
  LEFT JOIN inventory_has_winning_bidid i 
    ON b.id = i.id
    AND b.auction_house_id = i.auction_house_id
),

-- Step 3: Keep only smallest win per id + WebUserid
bids_with_rank AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id, auction_house_id, WebUserid, winning_sign
      ORDER BY value ASC
    ) AS win_rank
  FROM bids_with_winning_sign
),

bids_filtered AS (
  SELECT * EXCEPT(win_rank, has_bidid_match)
  FROM bids_with_rank
  WHERE
    winning_sign = 'not_win'
    OR (winning_sign = 'win' AND win_rank = 1)
),

-- Step 3b: 每个 lot 只保留最早出价的 winner
bids_dedup_winner AS (
  SELECT * EXCEPT(winner_rank)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, auction_house_id, winning_sign
        ORDER BY timestamp ASC, WebUserid ASC  -- 最早出价，timestamp 相同时取较小的 WebUserid
      ) AS winner_rank
    FROM bids_filtered
    WHERE winning_sign = 'win'
  )
  WHERE winner_rank = 1

  UNION ALL

  SELECT * FROM bids_filtered WHERE winning_sign = 'not_win'
),

-- Step 4: Clean invalid records
-- Remove: WebUserid IS NULL + not_win + value > hammeredprice
bids_cleaned AS (
  SELECT *
  FROM bids_dedup_winner
  WHERE NOT (
    WebUserid IS NULL
    AND winning_sign = 'not_win'
    AND value > hammeredprice
  )
),

-- Step 5: Replace win value with hammeredprice
bids_after_value_replacement AS (
  SELECT
    * EXCEPT(value),
    CASE
      WHEN winning_sign = 'win' THEN hammeredprice
      ELSE value
    END AS value
  FROM bids_cleaned
),

-- Step 6a: Identify id with no win at all
inventories_needing_synthetic_win AS (
  SELECT id, auction_house_id
  FROM bids_after_value_replacement
  GROUP BY id, auction_house_id
  HAVING
    SUM(CASE WHEN winning_sign = 'win' THEN 1 ELSE 0 END) = 0
    AND MAX(value) < MAX(hammeredprice)
),


-- Step 6b: Create synthetic win records
synthetic_wins AS (
  SELECT
    b.auctionid,
    b.startdate,
    b.enddate,
    b.month,
    b.id,
    b.skeletoncategory,
    b.bidid,
    b.winnerid,
    b.hammeredprice,
    b.initialbidprice,
    b.buyerspremium,
    b.currentcommission,
    b.auction_house_id,
    b.aucton_house_name,
    b.is_skeleton_client,
    CAST(NULL AS STRING)       AS url,
    b.category_name,
    b.currency,
    CAST(NULL AS STRING)       AS source,
    CAST(NULL AS INT64)        AS sessionId,
    CAST(NULL AS TIMESTAMP)    AS timestamp,
    b.bidid                    AS EnteredBidId,
    CAST(b.winnerid AS STRING) AS WebUserid,
    CAST(NULL AS STRING)       AS bidtype,
    FALSE                      AS has_enteredbid_match,
    'synthetic'                AS match_type,
    CAST(NULL AS STRING)       AS user_source_byfirstbid,
    'win'                      AS winning_sign,
    b.hammeredprice            AS value
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (PARTITION BY b.id, b.auction_house_id ORDER BY b.value DESC) AS rn
    FROM bids_after_value_replacement b
    INNER JOIN inventories_needing_synthetic_win i 
      ON b.id = i.id
      AND b.auction_house_id = i.auction_house_id
  ) b
  WHERE rn = 1
),

-- Step 6c: Combine
-- Get user_source_byfirstbid for synthetic wins from existing bids
synthetic_source AS (
  SELECT
    id,
    auction_house_id,
    MAX(user_source_byfirstbid) AS user_source_byfirstbid
  FROM bids_after_value_replacement
  WHERE user_source_byfirstbid IS NOT NULL
  GROUP BY id, auction_house_id
),

bids_final AS (
  SELECT
    auctionid, startdate, enddate, month, id, skeletoncategory,
    bidid, winnerid, hammeredprice, initialbidprice,
    buyerspremium, currentcommission, auction_house_id, is_skeleton_client,
    url, category_name, value, currency, source, sessionId, timestamp,
    EnteredBidId, WebUserid, bidtype, has_enteredbid_match,
    match_type, user_source_byfirstbid, winning_sign, aucton_house_name
  FROM bids_after_value_replacement

  UNION ALL

  SELECT
    auctionid, startdate, enddate, month, b.id, skeletoncategory,
    bidid, winnerid, hammeredprice, initialbidprice,
    buyerspremium, currentcommission, b.auction_house_id, is_skeleton_client,
    url, category_name,
    hammeredprice AS value,
    currency, source, sessionId, timestamp,
    CAST(bidid AS STRING) AS EnteredBidId,
    CAST(winnerid AS STRING) AS WebUserid,
    CAST(NULL AS STRING) AS bidtype,
    FALSE AS has_enteredbid_match,
    'synthetic' AS match_type,
    ss.user_source_byfirstbid, 
    'win' AS winning_sign,
    aucton_house_name
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (PARTITION BY b.id, b.auction_house_id ORDER BY b.value DESC) AS rn
    FROM bids_after_value_replacement b
    INNER JOIN inventories_needing_synthetic_win i 
      ON b.id = i.id
      AND b.auction_house_id = i.auction_house_id
  ) b
  LEFT JOIN synthetic_source ss
    ON b.id = ss.id
    AND b.auction_house_id = ss.auction_house_id
  WHERE rn = 1
)

-- Final output
SELECT
  auctionid,
  startdate,
  enddate,
  month,
  id,
  skeletoncategory,
  bidid,
  winnerid,
  hammeredprice,
  initialbidprice,
  ROUND(buyerspremium, 2)        AS buyerspremium,
  ROUND(currentcommission, 2)    AS currentcommission,
  CASE WHEN winning_sign = 'win' 
    THEN buyerspremium + currentcommission 
    ELSE NULL 
  END AS total_commission,
  CASE WHEN winning_sign = 'win' 
    THEN ROUND(hammeredprice * 0.015, 2) 
    ELSE NULL 
  END AS ah_commission,
  ROUND(hammeredprice * r.euro_rate_2025_avg, 4) AS hammeredprice_eur,
  ROUND(initialbidprice * r.euro_rate_2025_avg, 4) AS initialbidprice_eur,
  CASE WHEN winning_sign = 'win'
    THEN ROUND((buyerspremium + currentcommission) * r.euro_rate_2025_avg, 4)
    ELSE NULL
  END AS total_commission_eur,
  auction_house_id,
  aucton_house_name,
  is_skeleton_client,
  url,
  category_name,
  value,
  bids_final.currency,
  source,
  user_source_byfirstbid,
  sessionId,
  timestamp,
  enteredbidId,
  WebUserid,
  bidtype,
  has_enteredbid_match,
  match_type,
  winning_sign
FROM bids_final
LEFT JOIN eur_rates r ON bids_final.currency = r.currency;