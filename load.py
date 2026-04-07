# load.py

from google.cloud import bigquery

def load_to_bq(df, table_id, project, mode="WRITE_TRUNCATE"):
    print(f"📤 Loading to {table_id} (mode: {mode})...")
    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        write_disposition=mode
    )
    client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    ).result()
    print(f"✅ {len(df)} rows loaded to {table_id}\n")