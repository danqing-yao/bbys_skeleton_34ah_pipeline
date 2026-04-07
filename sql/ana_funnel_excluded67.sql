CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.ana_funnel_excluded67`
AS

WITH
ah_ref AS (
  SELECT DISTINCT
    CAST(aucton_house_id AS STRING) AS auction_house_id,
    aucton_house_name
  FROM `barnebys-skeleton.42ah.raw_auction_house`
  WHERE aucton_house_id IN (3915, 3952, 3862, 3900, 3902, 3898, 44,
           53, 230, 3922, 90, 3768, 3031, 3601, 3756, 2949, 
           3849, 107, 2659, 3925, 72, 3687, 125, 3865, 3950, 3723, 
           3923, 3904, 283, 3764, 3722, 3869, 3663, 3916)
),

totals_per_ah AS (
  SELECT
    auction_house_id,
    MAX(CASE WHEN event_type = 'clicks' AND user_source = 'barnebys' THEN value END) AS total_clicks,
    MAX(CASE WHEN event_type = 'registered'        AND user_source = 'Total' THEN value END) AS total_registered,
    MAX(CASE WHEN event_type = 'bidders'           AND user_source = 'Total' THEN value END) AS total_bidders,
    MAX(CASE WHEN event_type = 'bid'               AND user_source = 'Total' THEN value END) AS total_bid,
    MAX(CASE WHEN event_type = 'winners'           AND user_source = 'Total' THEN value END) AS total_winners,
    MAX(CASE WHEN event_type = 'total_winning_lots' AND user_source = 'Total' THEN value END) AS total_winning_lots,
    MAX(CASE WHEN event_type = 'total_winning_value' AND user_source = 'Total' THEN value END) AS total_winning_value
  FROM `barnebys-skeleton.42ah.ana_funnel_excluded67_unpivoted`
  GROUP BY auction_house_id
),

tv AS (
  SELECT auction_house_id, event_type, value AS total_value
  FROM `barnebys-skeleton.42ah.ana_funnel_excluded67_unpivoted`
  WHERE user_source = 'barnebys' AND event_type = 'clicks'
  UNION ALL
  SELECT auction_house_id, event_type, value AS total_value
  FROM `barnebys-skeleton.42ah.ana_funnel_excluded67_unpivoted`
  WHERE user_source = 'Total' AND event_type != 'clicks'
)

SELECT
  u.auction_house_id,
  COALESCE(ah.aucton_house_name, 'Total') AS aucton_house_name,
  u.event_type,
  u.user_source,
  CASE u.event_type
    WHEN 'clicks'                      THEN 1
    WHEN 'registered'                  THEN 2
    WHEN 'bidders'                     THEN 3
    WHEN 'winners'                     THEN 4
    WHEN 'bid'                         THEN 5
    WHEN 'total_winning_lots'          THEN 6
    WHEN 'total_winning_value'         THEN 7
    WHEN 'avg_bids_per_user'           THEN 8
    WHEN 'avg_winning_lots_per_user'   THEN 9
    WHEN 'total_items_with_bids'       THEN 10
    WHEN 'unique_underbidders'         THEN 11
    WHEN 'users_bid_more_than_one_lot' THEN 12
    WHEN 'buyerspremium'               THEN 13
    WHEN 'currentcommission'           THEN 14
    WHEN 'total_commission'            THEN 15
    WHEN 'barnebys_increment'          THEN 16
    WHEN 'total_winning_value_eur'     THEN 17
    WHEN 'total_commission_eur'        THEN 18
    WHEN 'barnebys_increment_eur'      THEN 19
    ELSE 99
  END AS event_order,
  u.value,
  tv.total_value,
  CASE
    WHEN u.user_source = 'barnebys' AND u.event_type = 'registered'
      THEN ROUND(u.value / NULLIF(t.total_registered, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'bidders'
      THEN ROUND(u.value / NULLIF(t.total_bidders, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'bid'
      THEN ROUND(u.value / NULLIF(t.total_bid, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'winners'
      THEN ROUND(u.value / NULLIF(t.total_winners, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'total_winning_lots'
      THEN ROUND(u.value / NULLIF(t.total_winning_lots, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'total_winning_value'
      THEN ROUND(u.value / NULLIF(t.total_winning_value, 0), 4)
    ELSE NULL
  END AS percentage_of_total,
  CASE
    WHEN u.user_source = 'barnebys' AND u.event_type = 'registered'
      THEN ROUND(u.value / NULLIF(t.total_clicks, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'bidders'
      THEN ROUND(u.value / NULLIF(t.total_clicks, 0), 4)
    WHEN u.user_source = 'barnebys' AND u.event_type = 'winners'
      THEN ROUND(u.value / NULLIF(t.total_clicks, 0), 4)
    ELSE NULL
  END AS percentage_of_clicks
FROM `barnebys-skeleton.42ah.ana_funnel_excluded67_unpivoted` u
LEFT JOIN ah_ref ah ON u.auction_house_id = ah.auction_house_id
LEFT JOIN totals_per_ah t ON u.auction_house_id = t.auction_house_id
LEFT JOIN tv ON u.auction_house_id = tv.auction_house_id
  AND u.event_type = tv.event_type
ORDER BY
  u.auction_house_id,
  event_order,
  CASE
    WHEN user_source = 'barnebys'  THEN 1
    WHEN user_source = 'other'     THEN 2
    WHEN user_source = 'uncertain' THEN 3
    WHEN user_source = 'Total'     THEN 4
  END
;