# ingest.py
# What this does (in one breath):
# Builds a SQL query, calls the NESO API, gets JSON, makes a DataFrame, drops bad rows, 
# UPSERTS into data/generation.db keyed by DATETIME.

import sqlite3, requests, pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta, UTC
import logging, time

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
    # 1) time window (last 14 days) as ISO Z strings
    now = datetime.now(UTC)
    start_iso = (now - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ") #14 days ago in utc
    #strftime turns datetime into astring in format you choose 
    # so z is just. thing to say - 'hey this is a ute format' but its still a string obejct
    end_iso   = now.strftime("%Y-%m-%dT%H:%M:%SZ") #now in utc
    return start_iso, end_iso #UTC strings

def build_sql_query(cols, RID, start_iso, end_iso):
    # 2) build SQL (quote identifiers), call API
    cols_sql = ", ".join([f'"{c}"' for c in cols]) # one long string - "DATETIME", "GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "HYDRO", "IMPORTS", "BIOMASS", "OTHER", "SOLAR", "STORAGE", "GENERATION", "CARBON_INTENSITY"
    # ^so double quotes needed per column name or sql defualts to lower case string and then won't recognise column name since column name sis case. sensitive

    sql = ( #sql query to send to API
        f'SELECT {cols_sql} ' #need double quotes sleecting since uppercase column names in dataset
        f'FROM "{RID}" '
        f'WHERE "DATETIME" >= \'{start_iso}\' AND "DATETIME" < \'{end_iso}\' '
        f'ORDER BY "DATETIME"'
    )
    return sql #the sql query for API

def fetch_records_from_api(base, sql):
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


#change datatypes, drop NaN's, drop duplicate date records, sort by date
def to_dataframe_clean(records, cols):
    # 3) to DataFrame + light clean
    df = pd.DataFrame.from_records(records)[cols] #creates df from records (the reponse) - thene sleects only [COLS] - the columns we want
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce") #covert datetime col into real one in utc, if value, cant be mad einto UTC  , make it nAT (empty placeholder) 
    other_cols = [c for c in cols if c != "DATETIME"] #other columns ecept from date
    for c in other_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce") #pd.to_numeric() is smart: it looks at the data and picks the right numeric dtype automatically- like float
    df = df.dropna(subset=["DATETIME"]) #drop rows were datetime missing values
    df = df.drop_duplicates(subset=["DATETIME"]) #remove duplicates with me datetime
    df = df.sort_values("DATETIME") #reorder rows - oldest to newest 
    logging.info("After clean: %d rows", len(df))
    return df

#create table if not exists in db
def ensure_table(cur, table): #curser for db and table of db 
    # 4) ensure table, then UPSERT by DATETIME (simplest version)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            DATETIME TEXT PRIMARY KEY,
            GAS REAL, COAL REAL, NUCLEAR REAL, WIND REAL, WIND_EMB REAL, HYDRO REAL,
            IMPORTS REAL, BIOMASS REAL, OTHER REAL, SOLAR REAL, STORAGE REAL,
            GENERATION REAL, CARBON_INTENSITY REAL
        )
    """)

def build_upsert_sql(cols, table): #cols: list of col names table:name of table in db
    # build pieces
    col_list = ",".join(cols)                        # DATETIME,GAS,...
    placeholders = ",".join(["?"] * len(cols))       # ?,?,?...

    update_parts = []
    for col in cols:
        if col != "DATETIME":
            update_parts.append(f"{col}=excluded.{col}") # e.g. "GAS=excluded.GAS"
    updates = ", ".join(update_parts) # join into one string: "GAS=excluded.GAS, WIND=excluded.WIND, ..."

    #IMPORTANT NOTE upset_sql is an sql query to apply an upset. rows is the newly grabbed rows to apply the upset with
    upsert_sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) " #insetr into ur table wiht ur usual specififed columns...
        f"ON CONFLICT(DATETIME) DO UPDATE SET {updates}" #if there is already a row wiht same datetime, update it to value within updates
    )
    return upsert_sql #retunr upsert query 

def build_rows(df, cols): #pd df, cols is a lsit of strings 
    # build rows in COLS order
    rows = []
    for _, r in df.iterrows(): # r is a series object - a row of values 
        vals = [] 
        for col in cols:
            if col == "DATETIME":
                vals.append(r["DATETIME"].strftime("%Y-%m-%dT%H:%M:%SZ"))
            else:
                vals.append(r[col])
        rows.append(vals) # is the new data fecthed 
        #so rows would be a nested lsit of rows -look something like 
        # rows == [
    #   ["2025-10-20T20:30:00Z", 11691.0, 4312.0, 32218.0],
    #   ["2025-10-20T21:00:00Z", 12050.0, 4100.0, 32500.0]
    # ]
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
    ensure_table(cur, table)

    upsert_sql = build_upsert_sql(cols, table)
    rows = build_rows(df, cols)

    # run
    cur.executemany(upsert_sql, rows)
    con.commit()
    con.close()

    logging.info("Upserted %d rows into %s → table '%s' (%.2fs)", len(rows), dp_path, table, time.time() - t0)
    print(f"Upserted {len(rows)} rows into {dp_path} → table '{table}'")

if __name__ == "__main__":
    main()
