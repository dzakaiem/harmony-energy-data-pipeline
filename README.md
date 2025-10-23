# harmony-energy-data-pipeline

Python-based data pipeline + Streamlit app for exploring Great Britain’s electricity generation mix using NESO’s “Historic GB Generation Mix” dataset.
What it does
Ingest: queries the NESO API for a time window, lightly cleans the data, and upserts into a local SQLite DB (data/generation.db) keyed by DATETIME.
Serve: Streamlit UI to pick a date range and visualize:
Generation (MW) by fuel over time
Average carbon intensity (gCO₂/kWh) over time
Schedule: GitHub Actions runs ingestion daily and commits the updated DB.