#!/usr/bin/env python3
# Fixed Streamlit dashboard - simplified with working links

import os
import json
import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# --- ensure libs ---
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
except ModuleNotFoundError:
    import sys, subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-aggrid>=0.3.5"])
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

try:
    from streamlit_js_eval import streamlit_js_eval
except ModuleNotFoundError:
    import sys, subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-js-eval>=0.1.7"])
    from streamlit_js_eval import streamlit_js_eval

# ---------- Page setup ----------
st.set_page_config(page_title="SAM.gov - Africa Opportunities", layout="wide")

# ---------- DB path ----------
REPO_DB = Path(__file__).parent / "data" / "opportunities.db"
HOME_DB = Path.home() / "sam_africa_data" / "opportunities.db"
DB_PATH = REPO_DB if REPO_DB.exists() else HOME_DB

STATE_DIR = (REPO_DB.parent if REPO_DB.exists() else HOME_DB.parent)
STATE_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH = STATE_DIR / ".last_ids.json"

# ---------- GitHub Action trigger ----------
GH_OWNER   = st.secrets.get("github_owner", "JWKSOA")
GH_REPO    = st.secrets.get("github_repo", "SAM.gov-Africa-Dashboard")
GH_WF_FILE = st.secrets.get("github_workflow_filename", "update-sam-db.yml")
GH_TOKEN   = st.secrets.get("github_token", "")

# ---------- African Countries Mapping ----------
AFRICAN_COUNTRIES = {
    "ALGERIA": "DZA", "ANGOLA": "AGO", "BENIN": "BEN", "BOTSWANA": "BWA",
    "BURKINA FASO": "BFA", "BURUNDI": "BDI", "CABO VERDE": "CPV", "CAMEROON": "CMR",
    "CENTRAL AFRICAN REPUBLIC": "CAF", "CHAD": "TCD", "COMOROS": "COM",
    "CONGO": "COG", "DEMOCRATIC REPUBLIC OF THE CONGO": "COD", "DJIBOUTI": "DJI",
    "EGYPT": "EGY", "EQUATORIAL GUINEA": "GNQ", "ERITREA": "ERI", "ESWATINI": "SWZ",
    "ETHIOPIA": "ETH", "GABON": "GAB", "GAMBIA": "GMB", "GHANA": "GHA",
    "GUINEA": "GIN", "GUINEA-BISSAU": "GNB", "IVORY COAST": "CIV", "KENYA": "KEN",
    "LESOTHO": "LSO", "LIBERIA": "LBR", "LIBYA": "LBY", "MADAGASCAR": "MDG",
    "MALAWI": "MWI", "MALI": "MLI", "MAURITANIA": "MRT", "MAURITIUS": "MUS",
    "MOROCCO": "MAR", "MOZAMBIQUE": "MOZ", "NAMIBIA": "NAM", "NIGER": "NER",
    "NIGERIA": "NGA", "RWANDA": "RWA", "SAO TOME AND PRINCIPE": "STP",
    "SENEGAL": "SEN", "SEYCHELLES": "SYC", "SIERRA LEONE": "SLE", "SOMALIA": "SOM",
    "SOUTH AFRICA": "ZAF", "SOUTH SUDAN": "SSD", "SUDAN": "SDN", "TANZANIA": "TZA",
    "TOGO": "TGO", "TUNISIA": "TUN", "UGANDA": "UGA", "ZAMBIA": "ZMB", "ZIMBABWE": "ZWE"
}

# ---------- Helpers ----------
def extract_iso3_from_display(value: str) -> str | None:
    """Extract ISO3 code from 'COUNTRY NAME (ISO3)' format"""
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    match = re.search(r'\(([A-Z]{3})\)$', s)
    if match:
        return match.group(1)
    if len(s) == 3 and s.isalpha():
        return s.upper()
    return None

def trigger_github_workflow(ref: str = "main") -> bool:
    if not GH_TOKEN:
        st.error("Missing github_token in Streamlit secrets.")
        return False
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/actions/workflows/{GH_WF_FILE}/dispatches"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"ref": ref}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code in (201, 204):
            st.success("Refresh requested. Check GitHub Actions tab.")
            return True
        else:
            st.error(f"GitHub API error {r.status_code}")
            return False
    except Exception as e:
        st.error(f"Failed: {e}")
        return False

# ---------- Load from SQLite ----------
@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query('SELECT * FROM opportunities ORDER BY "PostedDate" DESC', conn)
    finally:
        conn.close()

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # Parse dates
    if "PostedDate" in df.columns:
        ts = pd.to_datetime(df["PostedDate"], errors="coerce", utc=True)
        df["PostedDate_parsed"] = ts
        df["PostedDate_norm"] = ts.dt.tz_convert(None).dt.normalize()
    else:
        df["PostedDate_parsed"] = pd.NaT
        df["PostedDate_norm"] = pd.NaT

    # Clean NaN values
    non_time = [c for c in df.columns if not c.startswith("PostedDate_")]
    df[non_time] = df[non_time].replace({np.nan: ""})

    # Extract ISO3 for mapping
    df["PopCountry_iso3"] = df.get("PopCountry", pd.Series([""]*len(df))).apply(extract_iso3_from_display)

    return df

# ---------- Snapshot ----------
def _load_previous_ids() -> set[str]:
    try:
        if SNAPSHOT_PATH.exists():
            data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return set(map(str, data))
    except Exception:
        pass
    return set()

def _save_current_ids(ids: set[str]):
    try:
        SNAPSHOT_PATH.write_text(json.dumps(sorted(list(ids))), encoding="utf-8")
    except Exception:
        pass

def _current_ids(df: pd.DataFrame) -> set[str]:
    if "NoticeID" in df.columns:
        return set(df["NoticeID"].astype(str).tolist())
    return set()

# ---------- Visuals ----------
def render_visuals(df_page: pd.DataFrame, tab_key: str):
    if df_page.empty:
        st.info("No data in this date range.")
        return

    st.subheader("Contract Distribution by Country")
    
    m = df_page.dropna(subset=["PopCountry_iso3"]).groupby("PopCountry_iso3").size().reset_index(name="opps")
    if not m.empty:
        fig = px.choropleth(m, locations="PopCountry_iso3", locationmode="ISO-3",
                            color="opps", color_continuous_scale="Plasma",
                            title="African Contract Opportunities")
        fig.update_geos(scope="africa", showcountries=True)
        st.plotly_chart(fig, use_container_width=True)

    # Filterable table
    st.markdown("### Agency × Country Filter")
    
    if {"Department/Ind.Agency","PopCountry"}.issubset(df_page.columns):
        col1, col2 = st.columns(2)
        
        unique_countries = sorted([c for c in df_page["PopCountry"].unique() if c])
        unique_agencies = sorted([a for a in df_page["Department/Ind.Agency"].unique() if a])
        
        with col1:
            selected_country = st.selectbox(
                "Select Country",
                ["All Countries"] + unique_countries,
                key=f"country_{tab_key}"
            )
        
        with col2:
            selected_agency = st.selectbox(
                "Select Agency", 
                ["All Agencies"] + unique_agencies,
                key=f"agency_{tab_key}"
            )
        
        filtered = df_page.copy()
        if selected_country != "All Countries":
            filtered = filtered[filtered["PopCountry"] == selected_country]
        if selected_agency != "All Agencies":
            filtered = filtered[filtered["Department/Ind.Agency"] == selected_agency]
        
        if not filtered.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Contracts", len(filtered))
            with col2:
                st.metric("Countries", filtered["PopCountry"].nunique())
            with col3:
                st.metric("Agencies", filtered["Department/Ind.Agency"].nunique())

# ---------- Grid ----------
def render_grid(df_page: pd.DataFrame, tab_key: str):
    if df_page.empty:
        st.info("No data available")
        return
        
    work = df_page.copy()
    
    display_cols = ["PostedDate","Title","Department/Ind.Agency","PopCountry","CountryCode","Link"]
    for c in display_cols:
        if c not in work.columns:
            work[c] = ""
    
    gb = GridOptionsBuilder.from_dataframe(work[display_cols])
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    
    grid = AgGrid(
        work[display_cols],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        height=420,
        fit_columns_on_grid_load=True,
        enable_enterprise_modules=False,
    )

    # Handle selection
    selected = grid.get("selected_rows", pd.DataFrame())
    
    if isinstance(selected, pd.DataFrame) and not selected.empty:
        row_data = selected.iloc[0].to_dict()
    elif isinstance(selected, list) and len(selected) > 0:
        row_data = selected[0]
    else:
        row_data = None
    
    if row_data:
        st.divider()
        st.subheader("Contract Details")
        
        # Direct link
        link = str(row_data.get("Link", "")).strip()
        if link and link != "nan":
            st.markdown(f"🔗 **[Open on SAM.gov]({link})**")
        
        # Description
        with st.expander("Description", expanded=True):
            desc = str(row_data.get("Description", "")).strip()
            st.write(desc if desc and desc != "nan" else "(No description)")
        
        # Details in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Basic Info**")
            for f in ["NoticeID","Title","PostedDate","Type","Department/Ind.Agency"]:
                if f in row_data:
                    v = row_data[f]
                    if v and str(v) != "nan":
                        st.write(f"**{f}:** {v}")
        
        with col2:
            st.markdown("**Location & Contact**")
            for f in ["PopCountry","CountryCode","PrimaryContactEmail","PrimaryContactPhone"]:
                if f in row_data:
                    v = row_data[f]
                    if v and str(v) != "nan":
                        st.write(f"**{f}:** {v}")

# ---------- Main ----------
def main():
    st.title("🌍 SAM.gov — African Contract Opportunities")

    with st.expander("📊 Data Controls"):
        if st.button("🔄 Fetch Latest Data"):
            trigger_github_workflow()

    df = load_data()

    if df.empty:
        st.error("No data loaded. Click 'Fetch Latest Data' and wait for GitHub Action.")
        return

    # Metrics
    current_ids = _current_ids(df)
    prev_ids = _load_previous_ids()
    new_count = len(current_ids - prev_ids) if prev_ids else 0
    _save_current_ids(current_ids)

    c1, c2, c3 = st.columns(3)
    with c1:
        if df["PostedDate_parsed"].notna().any():
            st.metric("Last Updated", str(df["PostedDate_parsed"].max().date()))
    with c2:
        st.metric("New Opportunities", new_count)
    with c3:
        st.metric("Total Contracts", len(df))

    st.divider()

    # Tabs
    today = pd.Timestamp.today().normalize()
    five_years_ago = today - pd.DateOffset(years=5)

    tabs = st.tabs(["Last 7 Days","Last 30 Days","Last 365 Days","Last 5 Years","Archive (5+ Years)"])
    
    windows = [
        (today - pd.Timedelta(days=7), today + pd.Timedelta(days=1)),
        (today - pd.Timedelta(days=30), today + pd.Timedelta(days=1)),
        (today - pd.Timedelta(days=365), today + pd.Timedelta(days=1)),
        (five_years_ago, today + pd.Timedelta(days=1)),
        (None, five_years_ago),
    ]

    ts = df["PostedDate_norm"]
    for tab, (start, end), slug in zip(tabs, windows, ["7d","30d","365d","5y","arch"]):
        with tab:
            if start is None:
                df_page = df[ts.notna() & (ts < end)]
            else:
                df_page = df[ts.notna() & (ts >= start) & (ts < end)]
            
            if not df_page.empty:
                render_visuals(df_page.copy(), tab_key=slug)
                st.divider()
            
            render_grid(df_page.copy(), tab_key=slug)

if __name__ == "__main__":
    main()