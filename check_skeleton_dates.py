from config import CONFIG, SKELETON_HOUSES, SKELETON_CONN_BASE
from extract import extract_skeleton
import pandas as pd

SQL_CHECK_DATES = """
SELECT 
    '{ah_id}'                               AS ah_id,
    '{database}'                            AS database_name,
    MIN(FORMAT(enddate, 'yyyy-MM'))         AS min_month,
    MAX(FORMAT(enddate, 'yyyy-MM'))         AS max_month,
    SUM(CASE WHEN FORMAT(enddate, 'yyyy-MM') LIKE '2024%' THEN 1 ELSE 0 END) AS cnt_2024,
    SUM(CASE WHEN FORMAT(enddate, 'yyyy-MM') LIKE '2026%' THEN 1 ELSE 0 END) AS cnt_2026
FROM Auction
WHERE enddate IS NOT NULL
"""

results = []

for ah in SKELETON_HOUSES:
    conn_config = {**SKELETON_CONN_BASE, "database": ah["database"]}
    print(f"Checking {ah['database']} (ah_id: {ah['ah_id']})...")
    
    try:
        sql = SQL_CHECK_DATES.format(
            ah_id=ah["ah_id"],
            database=ah["database"]
        )
        df = extract_skeleton(sql, conn_config)
        results.append(df)
    except Exception as e:
        print(f"❌ {ah['database']} failed: {e}")
        results.append(pd.DataFrame([{
            'ah_id': ah['ah_id'],
            'database_name': ah['database'],
            'min_month': None,
            'max_month': None,
            'cnt_2024': None,
            'cnt_2026': None
        }]))

final = pd.concat(results, ignore_index=True)
print("\n=== Results ===")
print(final.to_string(index=False))

# 保存到 csv
final.to_csv('skeleton_date_check.csv', index=False)
print("\n✅ Saved to skeleton_date_check.csv")