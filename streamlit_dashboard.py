#!/usr/bin/env python3
# Fixed Streamlit dashboard with proper links, country display, and filterable table

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

try:
    import pycountry
except ModuleNotFoundError:
    import sys, subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pycountry>=22.3.5"])
    import pycountry

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
MONEY_RX = re.compile(r"\$|,|\s")
SUFFIX_RX = re.compile(r"(?i)\s*([KMB])\b")

def _parse_money_to_float(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    m = SUFFIX_RX.search(s)
    mult = 1.0
    if m:
        suf = m.group(1).upper()
        mult = 1e3 if suf == "K" else 1e6 if suf == "M" else 1e9 if suf == "B" else 1.0
        s = SUFFIX_RX.sub("", s)
    s = MONEY_RX.sub("", s)
    try:
        return float(s) * mult
    except Exception:
        return None

def _format_currency(x: float) -> str:
    return f"${x:,.2f}"

def _cleanup_nan(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({np.nan: ""})

def extract_iso3_from_display(value: str) -> str | None:
    """Extract ISO3 code from 'COUNTRY NAME (ISO3)' format"""
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    # Check if it's in our display format
    match = re.search(r'\(([A-Z]{3})\)$', s)
    if match:
        return match.group(1)
    # Check if it's just an ISO3 code
    if len(s) == 3 and s.isalpha():
        return s.upper()
    return None

LIKELY_ID_COLS = [
    "NoticeID","NoticeId","Notice ID","NoticeNumber","Notice Number",
    "SolicitationNumber","SAMNoticeID","ID","uid","sam_id","Link","URL","NoticeLink"
]

def _find_id_column(df: pd.DataFrame) -> str | None:
    for c in LIKELY_ID_COLS:
        if c in df.columns:
            return c
    return None

def ensure_proper_link(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure Link column contains proper SAM.gov URLs"""
    if "Link" in df.columns:
        def clean_link(link):
            if pd.isna(link) or not link:
                return ""
            link = str(link).strip()
            if not link or link == "nan":
                return ""
            # If it's already a full URL, keep it
            if link.startswith("http://") or link.startswith("https://"):
                return link
            # If it's a path, prepend sam.gov
            if link.startswith("/opp/"):
                return f"https://sam.gov{link}"
            elif link.startswith("opp/"):
                return f"https://sam.gov/{link}"
            else:
                # Assume it's a notice ID
                return f"https://sam.gov/opp/{link}/view"
        
        df["Link"] = df["Link"].apply(clean_link)
    
    return df

# ---------- GitHub Action trigger ----------
def trigger_github_workflow(ref: str = "main") -> bool:
    if not GH_TOKEN:
        st.error("Missing github_token in Streamlit secrets. Cannot trigger refresh.")
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
            st.success("Refresh requested. The GitHub Action will update the DB and push to the repo.")
            st.caption("Give it a few minutes, then click 'Rerun' in the app.")
            return True
        else:
            st.error(f"GitHub API error {r.status_code}: {r.text}")
            return False
    except Exception as e:
        st.error(f"Failed to call GitHub API: {e}")
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

    # Ensure proper links
    df = ensure_proper_link(df)

    # PostedDate parsing
    if "PostedDate" in df.columns:
        ts = pd.to_datetime(df["PostedDate"], errors="coerce", utc=True)
        df["PostedDate_parsed"] = ts
        df["PostedDate_norm"]   = ts.dt.tz_convert(None).dt.normalize()
    else:
        df["PostedDate_parsed"] = pd.NaT
        df["PostedDate_norm"]   = pd.NaT

    # Display-only enrichments
    if "PrimaryContactFullName" in df.columns:
        df = df.drop(columns=["PrimaryContactFullName"])
    for col in ("SecondaryContactEmail","SecondaryContactPhone"):
        if col not in df.columns:
            df[col] = ""

    # Keep Award$ column as requested
    if "Award$" in df.columns:
        df["Award$+"] = df["Award$"].astype(str)
        parsed = df["Award$"].apply(_parse_money_to_float)
        df["_AwardAmountNumeric"] = parsed
        mask = parsed.notna()
        df.loc[mask,  "Award$"] = parsed[mask].apply(_format_currency)
        df.loc[~mask, "Award$"] = "See Award$+"

    # NaN -> blank for non-time columns
    non_time = [c for c in df.columns if not c.startswith("PostedDate_")]
    df[non_time] = df[non_time].replace({np.nan: ""})

    # Extract ISO3 codes for mapping
    df["PopCountry_iso3"] = df.get("PopCountry", pd.Series([""]*len(df))).apply(extract_iso3_from_display)
    df["CountryCode_iso3"] = df.get("CountryCode", pd.Series([""]*len(df))).apply(extract_iso3_from_display)

    return df

# ---------- Snapshot for "New Opportunities Added" ----------
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
    id_col = _find_id_column(df)
    if id_col is None:
        t = df.get("Title", pd.Series([""]*len(df))).astype(str)
        p = df.get("PostedDate", pd.Series([""]*len(df))).astype(str)
        return set((t + "|" + p).tolist())
    return set(df[id_col].astype(str).tolist())

# ---------- Visuals with Filterable Table ----------
def render_visuals(df_page: pd.DataFrame, tab_key: str):
    if df_page.empty:
        st.info("No data in this date range yet.")
        return

    # Choropleth for PopCountry
    st.subheader("Contract Distribution by Country")
    
    m = df_page.dropna(subset=["PopCountry_iso3"]).groupby("PopCountry_iso3").size().reset_index(name="opps")
    if not m.empty:
        fig = px.choropleth(m, locations="PopCountry_iso3", locationmode="ISO-3",
                            color="opps", color_continuous_scale="Plasma",
                            title="African Contract Opportunities by Country")
        fig.update_geos(scope="africa", showcountries=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No mappable PopCountry values.")

    # FILTERABLE Agency × Country table
    st.markdown("### Agency × Country Distribution (Filterable)")
    
    if {"Department/Ind.Agency","PopCountry"}.issubset(df_page.columns):
        col1, col2 = st.columns(2)
        
        # Get unique values for filters
        unique_countries = sorted([c for c in df_page["PopCountry"].unique() if c])
        unique_agencies = sorted([a for a in df_page["Department/Ind.Agency"].unique() if a])
        
        with col1:
            selected_country = st.selectbox(
                "Select Country",
                ["All Countries"] + unique_countries,
                key=f"country_filter_{tab_key}"
            )
        
        with col2:
            selected_agency = st.selectbox(
                "Select Agency",
                ["All Agencies"] + unique_agencies,
                key=f"agency_filter_{tab_key}"
            )
        
        # Apply filters
        filtered_df = df_page.copy()
        if selected_country != "All Countries":
            filtered_df = filtered_df[filtered_df["PopCountry"] == selected_country]
        if selected_agency != "All Agencies":
            filtered_df = filtered_df[filtered_df["Department/Ind.Agency"] == selected_agency]
        
        # Show results
        if not filtered_df.empty:
            # Create summary statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Contracts", len(filtered_df))
            with col2:
                st.metric("Countries", filtered_df["PopCountry"].nunique())
            with col3:
                st.metric("Agencies", filtered_df["Department/Ind.Agency"].nunique())
            
            # Show detailed breakdown
            if selected_country == "All Countries" and selected_agency == "All Agencies":
                # Show pivot table
                cnt = (filtered_df.assign(count=1)
                      .pivot_table(index="PopCountry", columns="Department/Ind.Agency",
                                 values="count", aggfunc="sum", fill_value=0))
                st.dataframe(cnt.sort_index(), use_container_width=True, height=400)
            else:
                # Show filtered results as a list
                display_cols = ["PostedDate", "Title", "PopCountry", "Department/Ind.Agency", "Type"]
                st.dataframe(filtered_df[display_cols].sort_values("PostedDate", ascending=False), 
                           use_container_width=True, height=400)
        else:
            st.info("No contracts match the selected filters.")
    else:
        st.info("Need columns: Department/Ind.Agency and PopCountry.")

# ---------- Grid with proper Link column ----------
COMPACT_COL_ORDER = ["PostedDate","Title","Department/Ind.Agency","PopCountry","CountryCode","Link"]

def render_grid(df_page: pd.DataFrame, tab_key: str):
    work = df_page.copy()
    
    # Ensure all display columns exist
    for c in COMPACT_COL_ORDER:
        if c not in work.columns:
            work[c] = ""
    
    # Simplify Link column for display
    if "Link" in work.columns:
        work["Link_Display"] = work["Link"].apply(lambda x: "🔗 View" if x else "")
    else:
        work["Link_Display"] = ""
    
    # Update display columns to use Link_Display instead of Link
    display_cols = ["PostedDate","Title","Department/Ind.Agency","PopCountry","CountryCode","Link_Display"]
    
    gb = GridOptionsBuilder.from_dataframe(work[display_cols])
    gb.configure_default_column(filter=True, sortable=True, resizable=True, floatingFilter=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    
    if "_AwardAmountNumeric" in work.columns:
        gb.configure_column("_AwardAmountNumeric", hide=True)

    grid = AgGrid(
        work[display_cols],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.FILTERING_CHANGED | GridUpdateMode.SORTING_CHANGED,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        height=420,
        fit_columns_on_grid_load=True,
        enable_enterprise_modules=False,
        allow_unsafe_jscode=False,
    )

    # Handle selection
    selected_rows = grid.get("selected_rows", None)
    
    has_selection = False
    selected_data = None
    
    if selected_rows is not None:
        if isinstance(selected_rows, pd.DataFrame):
            if not selected_rows.empty:
                has_selection = True
                # Get the original row from df_page using index
                idx = selected_rows.index[0]
                if idx < len(df_page):
                    selected_data = df_page.iloc[idx].to_dict()
        elif isinstance(selected_rows, list) and len(selected_rows) > 0:
            has_selection = True
            selected_data = selected_rows[0]
        elif isinstance(selected_rows, dict):
            has_selection = True
            selected_data = selected_rows
    
    if has_selection and selected_data:
        st.divider()
        st.subheader("Contract Details")

        # Get the actual SAM link from the original data
        sam_link = str(selected_data.get("Link", "")).strip()
        if not sam_link or sam_link == "nan":
            sam_link = ""
        
        # Clean HTML tags if present
        if "<a href=" in sam_link:
            match = re.search(r'href="([^"]+)"', sam_link)
            if match:
                sam_link = match.group(1)

        # Action buttons
        c1, c2, _ = st.columns([1.5, 1.5, 5])
        with c1:
            if sam_link:
                st.markdown(f"[🔗 Open in SAM.gov]({sam_link})")
            else:
                st.info("No SAM Link")
        with c2:
            if st.button(f"📋 Copy Link ({tab_key})"):
                if sam_link:
                    streamlit_js_eval(jsCode=f"navigator.clipboard.writeText('{sam_link}')")
                    st.success("✅ Copied!")
                else:
                    st.warning("No link available")

        # Description
        with st.expander("📋 Full Description", expanded=True):
            desc_val = ""
            for cand in ("Description", "FullDescription", "Summary", "DescriptionText"):
                if cand in selected_data:
                    desc_val = str(selected_data.get(cand, ""))
                    if desc_val and desc_val != "nan":
                        break
            st.write(desc_val or "(No description available)")

        # All fields
        st.markdown("### 📊 All Contract Information")
        
        # Organize fields
        basic_fields = ["NoticeID", "Title", "PostedDate", "Type", "Department/Ind.Agency", 
                       "Sub-Tier", "Office"]
        location_fields = ["PopCountry", "CountryCode"]
        contact_fields = ["PrimaryContactTitle", "PrimaryContactEmail", "PrimaryContactPhone",
                         "SecondaryContactEmail", "SecondaryContactPhone"]
        award_fields = ["AwardNumber", "AwardDate", "Award$", "Award$+", "Awardee"]
        other_fields = ["OrganizationType"]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Basic Information")
            for field in basic_fields:
                if field in selected_data:
                    val = selected_data[field]
                    if val and str(val) != "nan" and str(val) != "":
                        st.write(f"**{field}:** {val}")
            
            st.markdown("#### Location")
            for field in location_fields:
                if field in selected_data:
                    val = selected_data[field]
                    if val and str(val) != "nan" and str(val) != "":
                        st.write(f"**{field}:** {val}")
            
            st.markdown("#### Award Information")
            for field in award_fields:
                if field in selected_data:
                    val = selected_data[field]
                    if val and str(val) != "nan" and str(val) != "":
                        st.write(f"**{field}:** {val}")
        
        with col2:
            st.markdown("#### Contact Information")
            for field in contact_fields:
                if field in selected_data:
                    val = selected_data[field]
                    if val and str(val) != "nan" and str(val) != "":
                        st.write(f"**{field}:** {val}")
            
            st.markdown("#### Other Information")
            for field in other_fields:
                if field in selected_data:
                    val = selected_data[field]
                    if val and str(val) != "nan" and str(val) != "":
                        st.write(f"**{field}:** {val}")
            
            # SAM.gov Link
            if sam_link:
                st.markdown(f"**Direct Link:** [Open on SAM.gov]({sam_link})")

# ---------- Main ----------
def main():
    st.title("🌍 SAM.gov — African Contract Opportunities")

    # Data controls
    with st.expander("📊 Data Controls", expanded=False):
        st.caption("Trigger a GitHub Action to fetch the latest SAM.gov data")
        if st.button("🔄 Fetch Latest Data (GitHub Action)"):
            trigger_github_workflow(ref="main")

    df = load_data()

    # Last updated metric
    if df.get("PostedDate_parsed") is not None and df["PostedDate_parsed"].notna().any():
        last_dt = df["PostedDate_parsed"].dropna().max()
        st.caption(f"Last updated (max PostedDate in DB): {last_dt}")
    else:
        st.caption("Last updated: unknown")

    if df.empty:
        st.error("No data loaded. Ensure your GitHub Action has run and pushed data/opportunities.db to the repo.")
        return

    # Metrics
    current_ids = _current_ids(df)
    prev_ids    = _load_previous_ids()
    new_count   = len(current_ids - prev_ids) if prev_ids else len(current_ids)
    _save_current_ids(current_ids)

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        if df["PostedDate_parsed"].notna().any():
            st.metric("Last Updated", str(df["PostedDate_parsed"].dropna().max().date()))
        else:
            st.metric("Last Updated", "(unknown)")
    with c2:
        st.metric("New Opportunities", new_count)
    with c3:
        st.metric("Total Contracts", len(df))

    st.divider()

    # Tabs
    today_norm     = pd.Timestamp.today().normalize()
    five_years_ago = today_norm - pd.DateOffset(years=5)

    tab_names = ["Last 7 Days","Last 30 Days","Last 365 Days","Last 5 Years","Archive (5+ Years)"]
    tab_slugs = ["7d","30d","365d","5y","arch"]
    tabs = st.tabs(tab_names)

    windows = [
        (today_norm - pd.Timedelta(days=7),   today_norm + pd.Timedelta(days=1)),
        (today_norm - pd.Timedelta(days=30),  today_norm + pd.Timedelta(days=1)),
        (today_norm - pd.Timedelta(days=365), today_norm + pd.Timedelta(days=1)),
        (five_years_ago,                       today_norm + pd.Timedelta(days=1)),
        (None,                                  five_years_ago),
    ]

    ts = df["PostedDate_norm"]
    for tab, slug, (start, end) in zip(tabs, tab_slugs, windows):
        with tab:
            if start is None:
                # Archive tab - show ALL historical data
                df_page = df[ts.notna() & (ts < end)].copy()
                if df_page.empty:
                    st.warning(
                        "📚 **No historical contracts found yet.**\n\n"
                        "To load ALL historical data (back to 2002):\n"
                        "1. Run: `python bootstrap_historical.py`\n"
                        "2. This will fetch all available archives\n"
                        "3. Refresh this dashboard to see the data"
                    )
                else:
                    st.info(f"📚 Showing {len(df_page)} historical contracts (5+ years old)")
            else:
                df_page = df[ts.notna() & (ts >= start) & (ts < end)].copy()

            if not df_page.empty:
                render_visuals(df_page, tab_key=slug)
                st.divider()
            
            render_grid(df_page, tab_key=slug)

    st.caption("💡 Tip: Click on any row to see full contract details. Links in the grid are clickable!")

if __name__ == "__main__":
    main()