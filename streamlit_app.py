# streamlit_app.py
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
# streamlit_app.py — ultra-simple with one function

import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta
import logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

db_path = "data/generation.db"
table   = "mix"

def main():
    st.set_page_config(page_title="GB Generation Mix", layout="wide")
    st.title("GB Generation Mix — NESO")

    # 1) Pick dates (default: last 7 days)
    c1, c2 = st.columns(2)
    d_start = c1.date_input("Start (UTC day)", value=date.today() - timedelta(days=7)) #Calling c1.date_input(...) places a date picker inside the left column.
# Calling c2.date_input(...) places another date picker inside the right column.
    d_end   = c2.date_input("End (UTC day)",   value=date.today())
    start_iso = f"{d_start}T00:00:00Z" #turns start date intoa. timestamp 
    end_iso   = f"{d_end}T23:59:59Z"

    logging.info("App query window %s → %s", start_iso, end_iso)

#takes the input dates and uses sql query to make a df
    # 2) Load from SQLite between start/end
    t0 = time.time()
    con = sqlite3.connect(db_path)
    sql = f"""
      SELECT * FROM {table}
      WHERE DATETIME >= '{start_iso}' AND DATETIME <= '{end_iso}'
      ORDER BY DATETIME
    """
    df = pd.read_sql_query(sql, con)
    con.close()
    logging.info("SQL returned %d rows in %.2fs", len(df), time.time() - t0)

    # 3) Handle empty + fix datetime
    if df.empty:
        st.warning("No data yet. Run `python ingest.py` first.")
        st.stop()
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce") #makes utc column values and fills misinng with NaT (missing placeholder)
    df = df.dropna(subset=["DATETIME"]) #drops rows with missing datetime column vlaue 

    # 4) Quick metrics
    st.metric("Rows", len(df))
    if "CARBON_INTENSITY" in df.columns and not df["CARBON_INTENSITY"].empty: #if column there an not empty...
        st.metric("Avg carbon intensity (gCO₂/kWh)", f"{df['CARBON_INTENSITY'].mean():.1f}") #get the mean and do to idp

    # 5) Charts
    fuels = ["GAS","WIND","NUCLEAR","SOLAR","HYDRO","COAL","BIOMASS","IMPORTS","OTHER","STORAGE"]#fuels we want to plot 
    fuels = [c for c in fuels if c in df.columns] #Keep only the fuels that actually exist as columns in your DataFrame.

    st.subheader("Generation (MW) by fuel")
    if fuels:
        st.line_chart(df.set_index("DATETIME")[fuels]) #use dateetime as x axis anf fuels on y

    st.subheader("Average carbon intensity (gCO₂/kWh)") # seond chart being made
    if "CARBON_INTENSITY" in df.columns:
        st.line_chart(df.set_index("DATETIME")["CARBON_INTENSITY"]) #of carbon intensity over time 

if __name__ == "__main__":
    main()
