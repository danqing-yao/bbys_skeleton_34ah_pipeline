-- Change the data type
/*
CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.raw_skeleton_auctions`
AS
SELECT
  * EXCEPT(hammeredprice, initialbidprice, buyerspremium, currentcommission),
  SAFE_CAST(hammeredprice AS INT64)        AS hammeredprice,
  CAST(SAFE_CAST(initialbidprice AS NUMERIC) AS INT64) AS initialbidprice,
  SAFE_CAST(buyerspremium AS FLOAT64)      AS buyerspremium,
  SAFE_CAST(currentcommission AS FLOAT64)  AS currentcommission
FROM `barnebys-skeleton.42ah.raw_skeleton_auctions`;
*/


-- ============================================================
-- Skeleton Auctions + Bite Bids (proc_skeleton_auctions_with_bids)
-- ------------------------------------------------------------
-- Purpose : Join Skeleton auction/inventory records with Bite bids data to enable source attribution analysis. One row per bid per inventory item.
-- Source  : raw_skeleton_auctions (a) + raw_bite_bids_clean (b)
-- Join    : a.Id = b.inventoryId (LEFT JOIN, bids expand rows)
-- Filter  : Remove records where both Bite and hammeredprice are absent (no bid activity, no sale recorded).
--           - Has Bite match (b.sessionId IS NOT NULL) → keep
--           - No Bite but has hammeredprice → keep (source = NULL)
--           - No Bite and no hammeredprice → drop
-- Cluster : Id, auction_house_id
-- ============================================================

/*
CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.proc_skeleton_auctions_with_bite_bids`
CLUSTER BY Id, auction_house_id
OPTIONS(
  description = "Skeleton auctions joined with Bite bids data, one row per bid. Unmatched Bite records retained only if hammeredprice is not null."
)
AS
SELECT
  a.*,
  b.auction_house_id,
  b.aucton_house_name,
  b.is_skeleton_client,
  b.url,
  b.category_name,
  b.value,
  b.currency,
  b.source,
  b.sessionId,
  b.timestamp
FROM `barnebys-skeleton.42ah.raw_skeleton_auctions` a
LEFT JOIN `barnebys-skeleton.42ah.raw_bite_bids_clean` b
  ON a.Id = b.inventoryId
  AND a.ah_id = cast(b.auction_house_id AS STRING)

WHERE
  b.sessionId IS NOT NULL
  OR a.hammeredprice IS NOT NULL
*/

CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.proc_skeleton_auctions_with_bite_bids`
CLUSTER BY Id, auction_house_id
OPTIONS(
  description = "Skeleton auctions joined with Bite bids data, one row per bid. Unmatched Bite records retained only if hammeredprice is not null."
)
AS

WITH ah_ref AS (
  SELECT DISTINCT
    CAST(aucton_house_id AS STRING) AS ah_id,
    CAST(aucton_house_id AS INT64)  AS auction_house_id,
    aucton_house_name
  FROM `barnebys-skeleton.42ah.raw_auction_house`
)

SELECT
  a.*,
  COALESCE(b.auction_house_id, ah.auction_house_id)   AS auction_house_id,
  COALESCE(b.aucton_house_name, ah.aucton_house_name) AS aucton_house_name,
  b.is_skeleton_client,
  b.url,
  b.category_name,
  b.value,
  b.currency,
  b.source,
  b.sessionId,
  b.timestamp
FROM `barnebys-skeleton.42ah.raw_skeleton_auctions` a
LEFT JOIN `barnebys-skeleton.42ah.raw_bite_bids_clean` b
  ON a.Id = b.inventoryId
  AND a.ah_id = CAST(b.auction_house_id AS STRING)
LEFT JOIN ah_ref ah
  ON a.ah_id = ah.ah_id
WHERE
  b.sessionId IS NOT NULL
  OR a.hammeredprice IS NOT NULL
