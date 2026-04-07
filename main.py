# main.py

from config import CONFIG, SKELETON_HOUSES, SKELETON_CONN_BASE, BBYS_AWS_CONN, BBYS_AZURE_CONN, AH_IDS
from extract import extract_skeleton, extract_bbys_aws, extract_bbys_azure
from load import load_to_bq
from google.cloud import bigquery
import subprocess
import time

SQL_AUCTIONS_TEMPLATE = """
SELECT
    '{ah_id}'   AS ah_id,
    a.auctionid,
    a.startdate,
    a.enddate,
    FORMAT(a.enddate, 'yyyy-MM') AS month,
    i.id,
    uc.name     AS skeletoncategory,
    i.enddate   AS inventory_enddate,
    iw.amount   AS hammeredprice,
    COALESCE(bal.initialbidprice, iw.amount) AS initialbidprice,
    iw.bidid,
    wt.userid   AS winnerid,
    iw.amount * iw.buyerspremium / 100 AS buyerspremium,
    iw.currentcommission
FROM Auction a
JOIN Inventory i ON i.auctionsessionid = a.auctionid
LEFT JOIN unifiedcategory AS uc ON i.categoryid = uc.categoryid
LEFT JOIN inventorywon AS iw ON iw.inventoryid = i.id
LEFT JOIN winnertracking AS wt ON iw.winnertrackingid = wt.id
LEFT JOIN (
    SELECT inventoryid, MIN(currenthighbid) AS initialbidprice
    FROM BidAuditLog
    WHERE currenthighbid IS NOT NULL
    GROUP BY inventoryid
) bal ON iw.inventoryid = bal.inventoryid
WHERE a.enddate >= '{skeleton_start}'
  AND a.enddate <  '{skeleton_end}'
ORDER BY startdate
"""

SQL_ENTEREDBIDS_TEMPLATE = """
WITH auction_inventory AS (
    SELECT
        a.auctionid,
        a.enddate,
        i.id AS inventoryid
    FROM Auction a
    JOIN Inventory i ON i.auctionsessionid = a.auctionid
    WHERE a.enddate >= '{skeleton_start}'
      AND a.enddate <  '{skeleton_end}'
)
SELECT
    '{ah_id}'  AS ah_id,
    ai.*,
    e.EnteredBidId,
    e.WebUserid,
    e.amount   AS bid_amount,
    e.bidtime,
    e.bidtype
FROM auction_inventory ai
LEFT JOIN EnteredBid e ON e.inventoryid = ai.inventoryid
WHERE e.bidtime IS NOT NULL
ORDER BY ai.inventoryid, e.bidtime
"""

SQL_BBYS_AWS_TEMPLATE = """
SELECT
    l.lot_id,
    l.title,
    SUBSTRING_INDEX(l.url, '/', -1) AS inventoryId,
    l.url,
    l.auction_house_id              AS ah_id,
    l.category_id,
    c.name                          AS category_name,
    l.created,
    l.updated
FROM lots_archived_31052025 l
LEFT JOIN categories c ON l.category_id = c.category_id
WHERE l.auction_house_id IN ({ah_ids})
  AND l.created >= '{bbys_aws_start}'
  AND l.created <  '{bbys_aws_end}'
"""

SQL_BBYS_AZURE_TEMPLATE = """
SELECT
    l.lot_id,
    l.title,
    SUBSTRING_INDEX(l.url, '/', -1) AS inventoryId,
    l.url,
    l.auction_house_id              AS ah_id,
    l.category_id,
    c.name                          AS category_name,
    l.created,
    l.updated
FROM lots l
LEFT JOIN categories c ON l.category_id = c.category_id
WHERE l.auction_house_id IN ({ah_ids})
  AND l.created >= '{bbys_azure_start}'
  AND l.created <  '{bbys_azure_end}'
"""


def run_bq_sql(sql_file, project):
    client = bigquery.Client(project=project)
    with open(sql_file, 'r') as f:
        sql = f.read()
    print(f"🔄 Running {sql_file}...")
    try:
        job = client.query(sql)
        job.result()
        print(f"✅ {sql_file} done\n")
    except Exception as e:
        print(f"❌ {sql_file} failed: {e}\n")
        raise


if __name__ == "__main__":
    print("🚀 Starting 42ah pipeline...\n")

    # -----------------------------------------------
    # 1. Skeleton: loop through all houses
    # -----------------------------------------------
    for i, ah in enumerate(SKELETON_HOUSES):
        conn_config = {**SKELETON_CONN_BASE, "database": ah["database"]}
        mode = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"

        print(f"🏛️  Processing {ah['database']} (ah_id: {ah['ah_id']})...")

        for attempt in range(3):
            try:
                sql = SQL_AUCTIONS_TEMPLATE.format(
                    ah_id=ah["ah_id"],
                    skeleton_start=CONFIG["skeleton_start"],
                    skeleton_end=CONFIG["skeleton_end"]
                )
                df = extract_skeleton(sql, conn_config)
                load_to_bq(
                    df,
                    f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.raw_skeleton_auctions",
                    CONFIG['bq_project'],
                    mode=mode
                )

                sql = SQL_ENTEREDBIDS_TEMPLATE.format(
                    ah_id=ah["ah_id"],
                    skeleton_start=CONFIG["skeleton_start"],
                    skeleton_end=CONFIG["skeleton_end"]
                )
                df = extract_skeleton(sql, conn_config)
                load_to_bq(
                    df,
                    f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.raw_skeleton_enteredbids",
                    CONFIG['bq_project'],
                    mode=mode
                )
                break

            except Exception as e:
                if '28000' in str(e) and attempt < 2:
                    print("⚠️  Token expired, refreshing az login...")
                    subprocess.run(["az", "login"], check=True)
                    time.sleep(10)
                else:
                    raise


    # -----------------------------------------------
    # 2. Barnebys AWS: all 5 ah in one query
    # -----------------------------------------------
    ah_ids_str = "'" + "','".join(AH_IDS) + "'"

    sql = SQL_BBYS_AWS_TEMPLATE.format(
        ah_ids=ah_ids_str,
        bbys_aws_start=CONFIG["bbys_aws_start"],
        bbys_aws_end=CONFIG["bbys_aws_end"]
    )
    df = extract_bbys_aws(sql, BBYS_AWS_CONN)
    load_to_bq(
        df,
        f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.raw_bbys_aws_lots",
        CONFIG['bq_project']
    )

    # -----------------------------------------------
    # 3. Barnebys Azure: all 5 ah in one query
    # -----------------------------------------------
    sql = SQL_BBYS_AZURE_TEMPLATE.format(
        ah_ids=ah_ids_str,
        bbys_azure_start=CONFIG["bbys_azure_start"],
        bbys_azure_end=CONFIG["bbys_azure_end"]
    )
    df = extract_bbys_azure(sql, BBYS_AZURE_CONN)
    load_to_bq(
        df,
        f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.raw_bbys_azure_lots",
        CONFIG['bq_project']
    )

    print("🎉 Raw tables loaded! Starting BigQuery SQL processing...\n")

    # -----------------------------------------------
    # 4. Run BigQuery SQL layers
    # -----------------------------------------------
    sql_files = [
        "sql/update_raw_bite_bids.sql",
        "sql/raw_bbys_lots.sql",
        "sql/raw_bite_bids_clean.sql",
        "sql/proc_skeleton_auctions_with_bite_bids.sql",
        "sql/proc_skeleton_auctions_with_enteredbids.sql",
        "sql/proc_skeleton_auctions_with_winning.sql",
        "sql/ana_lot_price_tiers.sql",
        "sql/ana_bids_with_price_tier.sql",
        "sql/ana_winning_bids_with_price_tier.sql",
        "sql/ana_barnebys_increment.sql",
        "sql/ana_funnel_all_stats.sql",
        "sql/ana_funnel_excluded67_unpivoted.sql",
        "sql/ana_funnel_excluded67.sql",
        "sql/ana_skeleton_fee.sql",
    ]

    for sql_file in sql_files:
        run_bq_sql(sql_file, CONFIG['bq_project'])

    print("🎉 All done! Pipeline complete.")