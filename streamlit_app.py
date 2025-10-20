# streamlit_app.py
# ChatGPT said:
# Gotcha — here’s the script in plain steps:
# Start the Streamlit app
# Sets the page title/layout and shows the header “GB Generation Mix — NESO”.
# Pick a date range
# Two date pickers appear: “Start (UTC date)” (defaults to 7 days ago) and “End (UTC date)” (defaults to today).
# These are converted to ISO timestamps covering midnight→23:59:59 UTC.
# Load data from SQLite
# Connects to data/generation.db, table mix.
# Runs SELECT * ... WHERE DATETIME between your start/end ordered by time.
# Converts the DATETIME column to timezone-aware timestamps.
# Handle “no data”
# If nothing comes back, it warns you to run python ingest.py and stops.
# Show quick metrics
# Displays the number of rows returned.
# Calculates and shows the average carbon intensity (gCO₂/kWh) over the selected period.
# Prepare fuel series
# Defines a list of fuel types (GAS, WIND, NUCLEAR, etc.).
# Keeps only the ones that actually exist as columns in your data.
# Plot generation over time
# Line chart of generation (MW) for the available fuel columns vs. DATETIME.
# Plot carbon intensity over time
# Line chart of CARBON_INTENSITY vs. DATETIME.
# That’s it — a simple date-filtered dashboard backed by a SQLite table.
import streamlit as st, pandas as pd, sqlite3
from datetime import date, timedelta

st.set_page_config(page_title="GB Generation Mix", layout="wide")
st.title("GB Generation Mix — NESO")

DB_PATH = "data/generation.db"
TABLE   = "mix"

def load_data(start_iso: str | None, end_iso: str | None) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    where, params = [], {}
    if start_iso: where.append("DATETIME >= :s"); params["s"] = start_iso
    if end_iso:   where.append("DATETIME < :e");  params["e"] = end_iso
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM {TABLE} {where_sql} ORDER BY DATETIME"
    df = pd.read_sql_query(sql, con, params=params)
    con.close()
    if not df.empty:
        df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True)
    return df

# controls
c1, c2 = st.columns(2)
d_end   = c2.date_input("End (UTC date)", value=date.today())
d_start = c1.date_input("Start (UTC date)", value=date.today() - timedelta(days=7))
start_iso = f"{d_start}T00:00:00Z"
end_iso   = f"{d_end}T23:59:59Z"

df = load_data(start_iso, end_iso)

if df.empty:
    st.warning("No data yet. Run `python ingest.py` first.")
    st.stop()

st.metric("Rows", len(df))
st.metric("Avg carbon intensity (gCO₂/kWh)", f"{df['CARBON_INTENSITY'].mean():.1f}")

fuels = ["GAS","WIND","NUCLEAR","SOLAR","HYDRO","COAL","BIOMASS","IMPORTS","OTHER","STORAGE"]
fuels = [c for c in fuels if c in df.columns]
st.subheader("Generation (MW) by fuel")
st.line_chart(df.set_index("DATETIME")[fuels])

st.subheader("Carbon intensity (gCO₂/kWh)")
st.line_chart(df.set_index("DATETIME")["CARBON_INTENSITY"])
