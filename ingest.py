# ingest.py
# What this does (in one breath):
# Builds a SQL query, calls the NESO API, gets JSON, makes a DataFrame, drops bad rows, 
# UPSERTS into data/generation.db keyed by DATETIME.
import os, sqlite3, json
from urllib.parse import quote
import requests, pandas as pd
from datetime import datetime, timedelta, UTC

# NESO CKAN API bits
BASE = "https://api.neso.energy/api/3/action"
RID  = "f93d1835-75bc-43e5-84ad-12472b180a98"  # Historic GB Generation Mix

# Columns we’ll store (keep it focused)
COLS = [
    "DATETIME","GAS","COAL","NUCLEAR","WIND","WIND_EMB","HYDRO",
    "IMPORTS","BIOMASS","OTHER","SOLAR","STORAGE",
    "GENERATION","CARBON_INTENSITY"
]

DB_PATH = "data/generation.db"
TABLE   = "mix"

def iso_z(dtobj: datetime) -> str:
    return dtobj.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")

def default_window():
    now = datetime.now(UTC)
    start = now - timedelta(days=14)             # last 14 days (change if you want)
    return iso_z(start), iso_z(now)

def build_sql(start_iso: str, end_iso: str) -> str:
    col_list = ", ".join([f'"{c}"' for c in COLS])
    return (
        f'SELECT {col_list} '
        f'FROM "{RID}" '
        f'WHERE "DATETIME" >= \'{start_iso}\' AND "DATETIME" < \'{end_iso}\' '
        f'ORDER BY "DATETIME"'
    )

def fetch_records(sql: str) -> list[dict]:
    url = f"{BASE}/datastore_search_sql?sql=" + quote(sql, safe="")
    r = requests.get(url, timeout=60)
    if r.status_code >= 400:
        raise SystemExit(f"NESO HTTP {r.status_code}\n{r.text[:1200]}")
    payload = r.json()
    if not payload.get("success"):
        raise SystemExit(json.dumps(payload, indent=2)[:1200])
    return payload["result"]["records"]

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # datetime + numerics
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce")
    df = df.dropna(subset=["DATETIME"])
    for c in [c for c in df.columns if c != "DATETIME"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # simple validation: drop negatives
    numeric = [c for c in df.columns if c != "DATETIME"]
    df = df.loc[~(df[numeric] < 0).any(axis=1)]
    # dedupe + sort
    df = df.drop_duplicates(subset=["DATETIME"]).sort_values("DATETIME")
    return df

def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # store DATETIME as TEXT (ISO Z) for simplicity; make it UNIQUE to enable upserts
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            DATETIME TEXT PRIMARY KEY,
            GAS REAL, COAL REAL, NUCLEAR REAL, WIND REAL, WIND_EMB REAL, HYDRO REAL,
            IMPORTS REAL, BIOMASS REAL, OTHER REAL, SOLAR REAL, STORAGE REAL,
            GENERATION REAL, CARBON_INTENSITY REAL
        )
    """)
    con.commit()
    con.close()

def upsert(df: pd.DataFrame):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # INSERT … ON CONFLICT(DATETIME) DO UPDATE
    placeholders = ",".join(["?"] * len(COLS))
    updates = ",".join([f"{c}=excluded.{c}" for c in COLS if c != "DATETIME"])
    sql = f"""
        INSERT INTO {TABLE} ({",".join(COLS)})
        VALUES ({placeholders})
        ON CONFLICT(DATETIME) DO UPDATE SET {updates}
    """
    rows = []
    for _, r in df.iterrows():
        # store DATETIME as ISO Z text
        values = [r["DATETIME"].strftime("%Y-%m-%dT%H:%M:%SZ")] + [r[c] for c in COLS if c != "DATETIME"]
        rows.append(values)
    cur.executemany(sql, rows)
    con.commit()
    con.close()
    return len(rows)

if __name__ == "__main__":
    start_iso, end_iso = default_window()
    ensure_db()
    sql = build_sql(start_iso, end_iso)
    recs = fetch_records(sql)
    if not recs:
        print("No rows returned.")
        raise SystemExit(0)
    df = pd.DataFrame(recs)[COLS]
    df = clean(df)
    n = upsert(df)
    print(f"Upserted {n} rows into {DB_PATH} → table '{TABLE}'")
