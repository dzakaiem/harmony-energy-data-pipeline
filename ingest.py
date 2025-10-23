import sqlite3, requests, pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta, UTC
import logging, time
from typing import List, Dict, Any  # for input type hints

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

base = "https://api.neso.energy/api/3/action"
RID  = "f93d1835-75bc-43e5-84ad-12472b180a98"  # Resource ID- Historic GB Generation Mix 

cols = [
    "DATETIME","GAS","COAL","NUCLEAR","WIND","WIND_EMB","HYDRO",
    "IMPORTS","BIOMASS","OTHER","SOLAR","STORAGE", 
    "GENERATION","CARBON_INTENSITY"
]

dp_path = "data/generation.db"
table  = "mix"

def build_time_window():
    #take first few years to backfill, then only 3 days form then on
    now = datetime.now(UTC)
    start_iso = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ") #3 days ago in utc
    #strftime turns datetime into astring in format you choose 
    end_iso   = now.strftime("%Y-%m-%dT%H:%M:%SZ") #now in utc
    return start_iso, end_iso 

def build_sql_query(cols: List[str], RID: str, start_iso: str, end_iso: str):
    # build SQL (quote identifiers), call API
    cols_sql = ", ".join([f'"{c}"' for c in cols]) # one long string - "DATETIME", "GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "HYDRO", "IMPORTS", "BIOMASS", "OTHER", "SOLAR", "STORAGE", "GENERATION", "CARBON_INTENSITY"

    sql = ( #sql query to send to API
        f'SELECT {cols_sql} ' 
        f'FROM "{RID}" '
        f'WHERE "DATETIME" >= \'{start_iso}\' AND "DATETIME" < \'{end_iso}\' '
        f'ORDER BY "DATETIME"'
    )
    return sql

def fetch_records_from_api(base: str, sql: str):
    #fetch records using SQL query
    resp = requests.get(f"{base}/datastore_search_sql", params={"sql": sql}, timeout=60)
    if resp.status_code != 200:
        raise SystemExit(f"HTTP {resp.status_code}") #program immediately and prints your message. (It exits like sys.exit(...) with that text.)
    records = resp.json()["result"]["records"]
    if not records:
        print("No rows returned")
    logging.info("API ok (200). Records fetched: %d", len(records))
    return records #lsit of dits - records = [
#   {"DATETIME": "2025-10-20T20:30:00Z", "GAS": "11691.0", "COAL": "4312.0"},
#   {"DATETIME": "2025-10-20T21:00:00Z", "GAS": "12050.0", "COAL": "4100.0"},
# ]

def to_dataframe_clean(records: List[Dict[str, Any]], cols: List[str]):
    #change datatypes, drop NaN's for date col, drop duplicate date records, sort by date
    df = pd.DataFrame.from_records(records)[cols] #creates df from records (the reponse)
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce") 
    other_cols = [c for c in cols if c != "DATETIME"] 
    for c in other_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce") 
    df = df.dropna(subset=["DATETIME"]) 
    df = df.drop_duplicates(subset=["DATETIME"]) 
    df = df.sort_values("DATETIME") 
    logging.info("After clean: %d rows", len(df))
    return df

def ensure_table_exists(cur: sqlite3.Cursor, table: str): #curser for db and table of db 
    #create table if not exists in db
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            DATETIME TEXT PRIMARY KEY,
            GAS REAL, COAL REAL, NUCLEAR REAL, WIND REAL, WIND_EMB REAL, HYDRO REAL,
            IMPORTS REAL, BIOMASS REAL, OTHER REAL, SOLAR REAL, STORAGE REAL,
            GENERATION REAL, CARBON_INTENSITY REAL
        )
    """)

def build_upsert_sql_query(cols: List[str], table: str): #cols: list of col names table:name of table in db
    col_list = ",".join(cols)                        # i.e.DATETIME,GAS,...
    placeholders = ",".join(["?"] * len(cols))       # i.e.?,?,?...

    update_parts = []
    for col in cols:
        if col != "DATETIME":
            update_parts.append(f"{col}=excluded.{col}") 
    updates = ", ".join(update_parts) # join into one string: "GAS=excluded.GAS, WIND=excluded.WIND, ..."

    upsert_sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) " 
        f"ON CONFLICT(DATETIME) DO UPDATE SET {updates}"
    )
    return upsert_sql #retunr upsert query 

def build_rows(df: pd.DataFrame, cols: List[str]): 
    #takes df and builds a nested list to represent df. e.g. [["2025-10-21T10:00:00Z", 12050.0, 4100.0, 8200.0],["2025-10-21T10:30:00Z", 11800.0, 4200.0, 7900.0]
    rows = []
    for _, r in df.iterrows(): # r is a series object - a row of values 
        vals = [] 
        for col in cols:
            if col == "DATETIME":
                vals.append(r["DATETIME"].strftime("%Y-%m-%dT%H:%M:%SZ"))
            else:
                vals.append(r[col])
        rows.append(vals) # is the new data fecthed 
 
    return rows

def main():
    t0 = time.time()
    logging.info("Ingest started")
    start_iso, end_iso = build_time_window()
    logging.info("Window %s → %s", start_iso, end_iso)
    sql = build_sql_query(cols, RID, start_iso, end_iso)
    records = fetch_records_from_api(base, sql)
    df = to_dataframe_clean(records, cols)

    con = sqlite3.connect(dp_path)
    cur = con.cursor()
    ensure_table_exists(cur, table)

    upsert_query = build_upsert_sql_query(cols, table)
    rows = build_rows(df, cols)

    # run
    cur.executemany(upsert_query, rows) #used t iterate over df and apply upserts
    con.commit()
    con.close()

    logging.info("Upserted %d rows into %s → table '%s' (%.2fs)", len(rows), dp_path, table, time.time() - t0)
    print(f"Upserted {len(rows)} rows into {dp_path} → table '{table}'")

if __name__ == "__main__":
    main()
