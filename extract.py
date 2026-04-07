# extract.py

import pyodbc
import pymysql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from azure.identity import AzureCliCredential
import struct
import time


# -----------------------------------------------
# Skeleton (SQL Server) - Azure AD token auth
# -----------------------------------------------
def extract_skeleton(query, conn_config):
    print("🔗 Connecting to Skeleton (SQL Server)...")
    
    for token_attempt in range(3):
        try:
            credential = AzureCliCredential()
            token = credential.get_token("https://database.windows.net/.default")
            break
        except Exception:
            if token_attempt < 2:
                time.sleep(5)
            else:
                raise

    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn_config['server']};"
        f"DATABASE={conn_config['database']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )

    with pyodbc.connect(conn_str, attrs_before={1256: token_struct}) as conn:
        df = pd.read_sql(query, conn)
    
    print(f"✅ Skeleton: {len(df)} rows extracted")
    return df


# -----------------------------------------------
# Barnebys AWS (MySQL via SSH Tunnel)
# -----------------------------------------------
def extract_bbys_aws(query, conn_config):
    print("🔗 Connecting to Barnebys AWS (MySQL via SSH)...")

    with SSHTunnelForwarder(
        (conn_config['ssh_host'], conn_config['ssh_port']),
        ssh_username=conn_config['ssh_username'],
        ssh_pkey=conn_config['ssh_key_file'],
        remote_bind_address=(conn_config['mysql_host'], conn_config['mysql_port'])
    ) as tunnel:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=conn_config['mysql_user'],
            password=conn_config['mysql_password'],
            database=conn_config['mysql_db']
        )
        df = pd.read_sql(query, conn)
        conn.close()

    print(f"✅ Barnebys AWS: {len(df)} rows extracted")
    return df


# -----------------------------------------------
# Barnebys Azure (MySQL - direct connection)
# -----------------------------------------------
def extract_bbys_azure(query, conn_config):
    print("🔗 Connecting to Barnebys Azure (MySQL)...")

    conn = pymysql.connect(
        host=conn_config['host'],
        database=conn_config['database'],
        user=conn_config['username'],
        password=conn_config['password'],
        ssl={'ssl': True}
    )
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"✅ Barnebys Azure: {len(df)} rows extracted")
    return df