CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.ana_funnel_excluded67_unpivoted`
AS

SELECT auction_house_id, user_source, 'clicks' AS event_type,
  CASE WHEN user_source IN ('barnebys', 'Total') THEN clicks ELSE NULL END AS value
FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_items_with_bids'       AS event_type, total_items_with_bids       AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'registered'                  AS event_type, registered                  AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'bidders'                     AS event_type, bidders                     AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'bid'                         AS event_type, bid                         AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'unique_underbidders'         AS event_type, unique_underbidders         AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'avg_bids_per_user'           AS event_type, avg_bids_per_user           AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'users_bid_more_than_one_lot' AS event_type, users_bid_more_than_one_lot AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'winners'                     AS event_type, winners                     AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_winning_lots'          AS event_type, total_winning_lots          AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_winning_value'         AS event_type, total_winning_value         AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'avg_winning_lots_per_user'   AS event_type, avg_winning_lots_per_user   AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'buyerspremium'               AS event_type, buyerspremium               AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'currentcommission'           AS event_type, currentcommission           AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_commission'            AS event_type, total_commission            AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'barnebys_increment'          AS event_type, barnebys_increment          AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_winning_value_eur'     AS event_type, total_winning_value_eur     AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'total_commission_eur'        AS event_type, total_commission_eur        AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`
UNION ALL
SELECT auction_house_id, user_source, 'barnebys_increment_eur'      AS event_type, barnebys_increment_eur      AS value FROM `barnebys-skeleton.42ah.ana_funnel_all_stats`