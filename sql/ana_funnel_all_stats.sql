CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.ana_funnel_all_stats`
AS

WITH

clicks_per_ah AS (
  SELECT
    CAST(programid AS STRING) AS auction_house_id,
    SUM(clicks) AS clicks
  FROM `barnebys-skeleton.42ah.raw_bite_clicks`
  WHERE month BETWEEN '2025-01' AND '2025-12'
    AND month NOT IN ('2025-06', '2025-07')
  GROUP BY programid
),

total_items_per_ah AS (
  SELECT
    CAST(auction_house_id AS STRING) AS auction_house_id,
    COUNT(DISTINCT id) AS total_items_with_bids
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids`
  WHERE month BETWEEN '2025-01' AND '2025-12'
    AND month NOT IN ('2025-06', '2025-07')
  GROUP BY auction_house_id
),

registered_per_ah AS (
  SELECT
    CAST(programId AS STRING) AS auction_house_id,
    COALESCE(source, 'uncertain') AS source,
    COUNT(sessionId) AS registered
  FROM `barnebys-skeleton.42ah.raw_bite_registrations`
  WHERE FORMAT_TIMESTAMP('%Y-%m', timestamp) BETWEEN '2025-01' AND '2025-12'
    AND FORMAT_TIMESTAMP('%Y-%m', timestamp) NOT IN ('2025-06', '2025-07')
  GROUP BY programId, COALESCE(source, 'uncertain')
),

stats_by_source_temp_per_ah AS (
  SELECT
    CAST(auction_house_id AS STRING) AS auction_house_id,
    COALESCE(user_source_byfirstbid, 'uncertain') AS user_source_byfirstbid,
    COUNT(DISTINCT WebUserid) AS bidders,
    COUNT(*) AS bid,
    COUNT(DISTINCT CASE
      WHEN WebUserid IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning` w
          WHERE w.WebUserid = t.WebUserid
            AND CAST(w.auction_house_id AS STRING) = CAST(t.auction_house_id AS STRING)
            AND w.winning_sign = 'win'
            AND w.month BETWEEN '2025-01' AND '2025-12'
            AND w.month NOT IN ('2025-06', '2025-07')
        )
      THEN WebUserid
    END) AS unique_underbidders,
    COUNT(*) / NULLIF(COUNT(DISTINCT WebUserid), 0) AS avg_bids_per_user
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids` t
  WHERE month BETWEEN '2025-01' AND '2025-12'
    AND month NOT IN ('2025-06', '2025-07')
  GROUP BY auction_house_id, COALESCE(user_source_byfirstbid, 'uncertain')
),

stats_by_source_winning_per_ah AS (
  SELECT
    CAST(auction_house_id AS STRING) AS auction_house_id,
    COALESCE(user_source_byfirstbid, 'uncertain') AS user_source_byfirstbid,
    COUNT(DISTINCT CASE WHEN winning_sign = 'win' THEN WebUserid END) AS winners,
    COUNT(DISTINCT CASE WHEN winning_sign = 'win' THEN id END) AS total_winning_lots,
    SUM(CASE WHEN winning_sign = 'win' THEN hammeredprice ELSE 0 END) AS total_winning_value,
    SUM(CASE WHEN winning_sign = 'win' THEN hammeredprice_eur ELSE 0 END) AS total_winning_value_eur,
    SUM(CASE WHEN winning_sign = 'win' THEN buyerspremium ELSE 0 END) AS buyerspremium,
    SUM(CASE WHEN winning_sign = 'win' THEN currentcommission ELSE 0 END) AS currentcommission,
    SUM(CASE WHEN winning_sign = 'win' THEN buyerspremium + currentcommission ELSE 0 END) AS total_commission,
    SUM(CASE WHEN winning_sign = 'win' THEN total_commission_eur ELSE 0 END) AS total_commission_eur
  FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_winning`
  WHERE month BETWEEN '2025-01' AND '2025-12'
    AND month NOT IN ('2025-06', '2025-07')
  GROUP BY auction_house_id, COALESCE(user_source_byfirstbid, 'uncertain')
),

users_multiple_lots_per_ah AS (
  SELECT
    auction_house_id,
    user_source_byfirstbid,
    COUNT(DISTINCT WebUserid) AS users_bid_more_than_one_lot
  FROM (
    SELECT
      CAST(auction_house_id AS STRING) AS auction_house_id,
      WebUserid,
      COALESCE(user_source_byfirstbid, 'uncertain') AS user_source_byfirstbid,
      COUNT(DISTINCT id) AS lot_count
    FROM `barnebys-skeleton.42ah.proc_skeleton_auctions_with_enteredbids`
    WHERE WebUserid IS NOT NULL
      AND month BETWEEN '2025-01' AND '2025-12'
      AND month NOT IN ('2025-06', '2025-07')
    GROUP BY auction_house_id, WebUserid, COALESCE(user_source_byfirstbid, 'uncertain')
    HAVING COUNT(DISTINCT id) > 1
  )
  GROUP BY auction_house_id, user_source_byfirstbid
),

barnebys_increment_per_ah AS (
  SELECT
    CAST(auction_house_id AS STRING) AS auction_house_id,
    COALESCE(user_source_byfirstbid, 'uncertain') AS user_source_byfirstbid,
    SUM(barnebys_increment) AS barnebys_increment,
    SUM(barnebys_increment_eur) AS barnebys_increment_eur
  FROM `barnebys-skeleton.42ah.ana_barnebys_increment`
  WHERE month BETWEEN '2025-01' AND '2025-12'
    AND month NOT IN ('2025-06', '2025-07')
    AND barnebys_increment IS NOT NULL
  GROUP BY auction_house_id, COALESCE(user_source_byfirstbid, 'uncertain')
),

stats_per_ah AS (
  SELECT
    COALESCE(st.auction_house_id, sw.auction_house_id, ru.auction_house_id) AS auction_house_id,
    COALESCE(st.user_source_byfirstbid, sw.user_source_byfirstbid, ru.source) AS user_source,
    COALESCE(cl.clicks, 0) AS clicks,
    COALESCE(ti.total_items_with_bids, 0) AS total_items_with_bids,
    COALESCE(ru.registered, 0) AS registered,
    COALESCE(st.bidders, 0) AS bidders,
    COALESCE(st.bid, 0) AS bid,
    COALESCE(st.unique_underbidders, 0) AS unique_underbidders,
    ROUND(COALESCE(st.avg_bids_per_user, 0), 2) AS avg_bids_per_user,
    COALESCE(um.users_bid_more_than_one_lot, 0) AS users_bid_more_than_one_lot,
    COALESCE(sw.winners, 0) AS winners,
    COALESCE(sw.total_winning_lots, 0) AS total_winning_lots,
    COALESCE(sw.total_winning_value, 0) AS total_winning_value,
    COALESCE(sw.total_winning_value_eur, 0) AS total_winning_value_eur,
    ROUND(COALESCE(sw.total_winning_lots, 0) / NULLIF(COALESCE(sw.winners, 0), 0), 2) AS avg_winning_lots_per_user,
    COALESCE(sw.buyerspremium, 0) AS buyerspremium,
    COALESCE(sw.currentcommission, 0) AS currentcommission,
    COALESCE(sw.total_commission, 0) AS total_commission,
    COALESCE(sw.total_commission_eur, 0) AS total_commission_eur,
    COALESCE(bi.barnebys_increment, 0) AS barnebys_increment,
    COALESCE(bi.barnebys_increment_eur, 0) AS barnebys_increment_eur
  FROM stats_by_source_temp_per_ah st
  FULL OUTER JOIN stats_by_source_winning_per_ah sw
    ON st.auction_house_id = sw.auction_house_id
    AND st.user_source_byfirstbid = sw.user_source_byfirstbid
  FULL OUTER JOIN registered_per_ah ru
    ON st.auction_house_id = ru.auction_house_id
    AND st.user_source_byfirstbid = ru.source
  LEFT JOIN clicks_per_ah cl ON st.auction_house_id = cl.auction_house_id
  LEFT JOIN total_items_per_ah ti ON st.auction_house_id = ti.auction_house_id
  LEFT JOIN users_multiple_lots_per_ah um
    ON st.auction_house_id = um.auction_house_id
    AND st.user_source_byfirstbid = um.user_source_byfirstbid
  LEFT JOIN barnebys_increment_per_ah bi
    ON st.auction_house_id = bi.auction_house_id
    AND st.user_source_byfirstbid = bi.user_source_byfirstbid
),

stats_per_ah_with_total AS (
  SELECT * FROM stats_per_ah
  UNION ALL
  SELECT
    auction_house_id,
    'Total' AS user_source,
    MAX(clicks) AS clicks,
    MAX(total_items_with_bids) AS total_items_with_bids,
    SUM(registered) AS registered,
    SUM(bidders) AS bidders,
    SUM(bid) AS bid,
    SUM(unique_underbidders) AS unique_underbidders,
    ROUND(SUM(bid) / NULLIF(SUM(bidders), 0), 2) AS avg_bids_per_user,
    SUM(users_bid_more_than_one_lot) AS users_bid_more_than_one_lot,
    SUM(winners) AS winners,
    SUM(total_winning_lots) AS total_winning_lots,
    SUM(total_winning_value) AS total_winning_value,
    SUM(total_winning_value_eur) AS total_winning_value_eur,
    ROUND(SUM(total_winning_lots) / NULLIF(SUM(winners), 0), 2) AS avg_winning_lots_per_user,
    SUM(buyerspremium) AS buyerspremium,
    SUM(currentcommission) AS currentcommission,
    SUM(total_commission) AS total_commission,
    SUM(total_commission_eur) AS total_commission_eur,
    SUM(barnebys_increment) AS barnebys_increment,
    SUM(barnebys_increment_eur) AS barnebys_increment_eur
  FROM stats_per_ah
  GROUP BY auction_house_id
),

stats_overall AS (
  SELECT
    'Total' AS auction_house_id,
    user_source,
    SUM(clicks) AS clicks,
    SUM(total_items_with_bids) AS total_items_with_bids,
    SUM(registered) AS registered,
    SUM(bidders) AS bidders,
    SUM(bid) AS bid,
    SUM(unique_underbidders) AS unique_underbidders,
    ROUND(SUM(bid) / NULLIF(SUM(bidders), 0), 2) AS avg_bids_per_user,
    SUM(users_bid_more_than_one_lot) AS users_bid_more_than_one_lot,
    SUM(winners) AS winners,
    SUM(total_winning_lots) AS total_winning_lots,
    SUM(total_winning_value) AS total_winning_value,
    SUM(total_winning_value_eur) AS total_winning_value_eur,
    ROUND(SUM(total_winning_lots) / NULLIF(SUM(winners), 0), 2) AS avg_winning_lots_per_user,
    SUM(buyerspremium) AS buyerspremium,
    SUM(currentcommission) AS currentcommission,
    SUM(total_commission) AS total_commission,
    SUM(total_commission_eur) AS total_commission_eur,
    SUM(barnebys_increment) AS barnebys_increment,
    SUM(barnebys_increment_eur) AS barnebys_increment_eur
  FROM stats_per_ah
  GROUP BY user_source

  UNION ALL

  SELECT
    'Total' AS auction_house_id,
    'Total' AS user_source,
    SUM(clicks) AS clicks,
    SUM(total_items_with_bids) AS total_items_with_bids,
    SUM(registered) AS registered,
    SUM(bidders) AS bidders,
    SUM(bid) AS bid,
    SUM(unique_underbidders) AS unique_underbidders,
    ROUND(SUM(bid) / NULLIF(SUM(bidders), 0), 2) AS avg_bids_per_user,
    SUM(users_bid_more_than_one_lot) AS users_bid_more_than_one_lot,
    SUM(winners) AS winners,
    SUM(total_winning_lots) AS total_winning_lots,
    SUM(total_winning_value) AS total_winning_value,
    SUM(total_winning_value_eur) AS total_winning_value_eur,
    ROUND(SUM(total_winning_lots) / NULLIF(SUM(winners), 0), 2) AS avg_winning_lots_per_user,
    SUM(buyerspremium) AS buyerspremium,
    SUM(currentcommission) AS currentcommission,
    SUM(total_commission) AS total_commission,
    SUM(total_commission_eur) AS total_commission_eur,
    SUM(barnebys_increment) AS barnebys_increment,
    SUM(barnebys_increment_eur) AS barnebys_increment_eur
  FROM stats_per_ah
)

SELECT * FROM stats_per_ah_with_total
UNION ALL
SELECT * FROM stats_overall