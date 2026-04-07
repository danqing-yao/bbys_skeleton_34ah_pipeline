-- =====================================================
-- raw_bite_bids_clean with deduplication
-- =====================================================
-- Deduplication logic:
--   For same inventoryId + value, keep only the first bid (earliest timestamp)
-- =====================================================

CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.raw_bite_bids_clean`
CLUSTER BY inventoryId, auction_house_id
OPTIONS(
  description = "Cleaned Bite bids enriched with lot category and auction house info - deduplicated by inventoryId + value, keeping earliest timestamp"
)
AS

WITH bids_enriched AS (
  SELECT 
    b.programId,
    ah.aucton_house_name,
    ah.is_skeleton_client,
    b.inventoryId,
    b.url,
    l.category_name,
    b.value,
    b.currency,
    b.source,
    b.sessionId,
    b.timestamp
  FROM `barnebys-skeleton.42ah.raw_bite_bids` b
  LEFT JOIN `barnebys-skeleton.42ah.raw_bbys_lots` l
    ON CAST(b.inventoryId AS STRING) = l.inventoryId
    AND CAST(b.programId AS STRING) = CAST(l.auction_house_id AS STRING) 

  LEFT JOIN `barnebys-skeleton.42ah.raw_auction_house` ah
    ON CAST(b.programId AS STRING) = CAST(ah.aucton_house_id AS STRING)
),

bids_ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY programid, inventoryId, value
      ORDER BY timestamp ASC
    ) AS rn
  FROM bids_enriched
)

SELECT
  programId,
  programId as auction_house_id,
  aucton_house_name,
  is_skeleton_client,
  inventoryId,
  url,
  category_name,
  value,
  currency,
  source,
  sessionId,
  timestamp
FROM bids_ranked
WHERE rn = 1;
