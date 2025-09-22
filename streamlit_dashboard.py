#!/usr/bin/env python3
# Streamlit dashboard with a simple single-password gate (compatible with all Streamlit versions)

import os
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# --- Page setup (must be first Streamlit call) ---
st.set_page_config(page_title="SAM.gov - Africa Opportunities", layout="wide")

# --- DB path: prefer repo copy (for cloud), else local home copy ---
REPO_DB = Path(__file__).parent / "data" / "opportunities.db"
HOME_DB = Path.home() / "sam_africa_data" / "opportunities.db"
DB_PATH = REPO_DB if REPO_DB.exists() else HOME_DB

# --- Rerun helper (works on new and old Streamlit) ---
def _rerun():
    try:
        st.rerun()
    except AttributeError:
        # for older versions
        st.experimental_rerun()

# --- ONE-PASSWORD GATE ---
def password_gate():
    """
    Require a single shared password before showing the app.
    Reads the secret from (in order): st.secrets["app_password"] or env APP_PASSWORD.
    Stores auth in session_state so users don't have to re-enter on every interaction.
    """
    secret_pw = st.secrets.get("app_password") if hasattr(st, "secrets") else None
    if not secret_pw:
        secret_pw = os.environ.get("APP_PASSWORD")

    if not secret_pw:
        st.warning("No app_password secret configured. Skipping password gate (dev mode).")
        return  # show the app

    if st.session_state.get("authed") is True:
        if st.sidebar.button("Lock"):
            st.session_state.authed = False
            _rerun()
        return  # already authenticated

    st.title("🔒 Private Dashboard")
    st.markdown("Enter the access code to continue.")
    with st.form("login_form", clear_on_submit=False):
        code = st.text_input("Access code", type="password")
        submit = st.form_submit_button("Enter")

    if submit:
        if code == secret_pw:
            st.session_state.authed = True
            _rerun()
        else:
            st.error("Incorrect code.")
            st.stop()
    else:
        st.stop()

@st.cache_data(ttl=60)
def load_data():
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM opportunities ORDER BY PostedDate DESC", conn)
    finally:
        conn.close()
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    if "PostedDate" in df.columns:
        df["PostedDate_parsed"] = pd.to_datetime(df["PostedDate"], errors="coerce", infer_datetime_format=True)
    else:
        df["PostedDate_parsed"] = pd.NaT
    return df

def main():
    password_gate()  # require the shared password

    st.title("SAM.gov — Contract Opportunities (African countries)")
    df = load_data()

    if df.empty:
        st.info(
            "No data found yet. Run the loader:\n\n"
            "`python3 download_and_update.py`\n\n"
            "or wait for the nightly refresh."
        )
        return

    st.sidebar.header("Filters")

    pop_vals = sorted([v for v in df.get("PopCountry", pd.Series([], dtype=str)).dropna().unique().tolist() if str(v).strip()])
    cc_vals  = sorted([v for v in df.get("CountryCode", pd.Series([], dtype=str)).dropna().unique().tolist() if str(v).strip()])

    selected_pops = st.sidebar.multiselect("Filter PopCountry (contains)", pop_vals)
    selected_cc   = st.sidebar.multiselect("Filter CountryCode (exact)", cc_vals)

    from_date = st.sidebar.date_input("From date (optional)", value=None)
    to_date   = st.sidebar.date_input("To date (optional)",   value=None)

    q = st.sidebar.text_input("Full-text search (Title, Description, Awardee)")

    filtered = df.copy()

    if selected_pops:
        lowered = [sp.lower() for sp in selected_pops]
        filtered = filtered[filtered["PopCountry"].astype(str).str.lower().apply(lambda x: any(sp in x for sp in lowered))]

    if selected_cc:
        filtered = filtered[filtered["CountryCode"].astype(str).isin(selected_cc)]

    if q:
        ql = q.lower()
        filtered = filtered[
            filtered.apply(
                lambda r: (ql in str(r.get("Title","")).lower())
                          or (ql in str(r.get("Description","")).lower())
                          or (ql in str(r.get("Awardee","")).lower()),
                axis=1
            )
        ]

    if from_date:
        filtered = filtered[filtered["PostedDate_parsed"].notna()]
        filtered = filtered[filtered["PostedDate_parsed"] >= pd.to_datetime(from_date)]
    if to_date:
        filtered = filtered[filtered["PostedDate_parsed"].notna()]
        filtered = filtered[filtered["PostedDate_parsed"] <= pd.to_datetime(to_date)]

    st.write(f"Results: {len(filtered)} rows")
    show_df = filtered.drop(columns=["PostedDate_parsed"], errors="ignore")
    st.dataframe(show_df, use_container_width=True)

    csv_bytes = show_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download results as CSV", data=csv_bytes, file_name="sam_africa_results.csv", mime="text/csv")

if __name__ == "__main__":
    main()
