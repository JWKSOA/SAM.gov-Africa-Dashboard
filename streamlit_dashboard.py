#!/usr/bin/env python3
# DB-driven Streamlit dashboard using your existing SQLite pipeline (no CSV assumption).
# Implements:
#  1) Auto-run bootstrap_historical.py once at startup (no button).
#  2) Auto-run download_and_update.py every 24 hours without user action.
#  3) Fix/avoid SettingWithCopyWarning; work on .copy() and suppress pandas warning AG Grid triggers.
#  4) Remove “stale” gating; always load data after auto-runs.
#  5) Keep all UX features you asked for: Award$+, SecondaryContact*, NaN→blank,
#     Excel-style filters (AG Grid), row details drawer, Copy SAM Link,
#     Tabs (7/30/365/5y/Archive>5y) using normalized Timestamps, ISO-3 maps,
#     “New Opportunities Added” metric, and your password gate.

import os
import sys
import json
import re
import sqlite3
import subprocess
import warnings
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st

# --- Safe auto-install guards (local convenience only) ---
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-aggrid>=0.3.5"])
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

try:
    from streamlit_js_eval import streamlit_js_eval
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-js-eval>=0.1.7"])
    from streamlit_js_eval import streamlit_js_eval

try:
    import pycountry
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pycountry>=22.3.5"])
    import pycountry

# Silence pandas SettingWithCopy warnings (AG Grid causes these internally)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# ---------- Page setup ----------
st.set_page_config(page_title="SAM.gov - Africa Opportunities", layout="wide")

# ---------- DB path: prefer repo copy (for cloud), else local home copy ----------
REPO_DB = Path(__file__).parent / "data" / "opportunities.db"
HOME_DB  = Path.home() / "sam_africa_data" / "opportunities.db"
DB_PATH  = REPO_DB if REPO_DB.exists() else HOME_DB

# Scripts we call to populate/refresh data (your originals)
DOWNLOAD_SCRIPT  = Path(__file__).parent / "download_and_update.py"
BOOTSTRAP_SCRIPT = Path(__file__).parent / "bootstrap_historical.py"

# Bookkeeping files to control auto-runs (kept alongside DB)
STATE_DIR = (REPO_DB.parent if REPO_DB.exists() else HOME_DB.parent)
STATE_DIR.mkdir(parents=True, exist_ok=True)
BOOTSTRAP_FLAG  = STATE_DIR / ".historical_bootstrapped"       # presence means we've run bootstrap successfully
LAST_REFRESH_TS = STATE_DIR / ".last_download_refresh_utc.txt" # ISO timestamp of last download_and_update run
SNAPSHOT_PATH   = STATE_DIR / ".last_ids.json"                 # for “New Opportunities Added”

# ---------- Shared helpers ----------
def _rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()

def _run_script(path: Path, label: str) -> bool:
    """Run a Python script synchronously and show status. Returns True on success."""
    if not path.exists():
        st.warning(f"{label}: script not found at {path.name}.")
        return False
    try:
        res = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, check=True)
        # Show last lines (helpful while not flooding the UI)
        tail = (res.stdout or "").strip()
        if tail:
            st.caption(f"{label} log tail:\n{tail[-800:]}")
        return True
    except subprocess.CalledProcessError as e:
        st.error(f"{label} failed.\n{e.stderr or e.stdout or e}")
        return False

# ---------- ONE-PASSWORD GATE (unchanged) ----------
def password_gate():
    secret_pw = st.secrets.get("app_password") if hasattr(st, "secrets") else None
    if not secret_pw:
        secret_pw = os.environ.get("APP_PASSWORD")

    if not secret_pw:
        st.warning("No app_password configured. Skipping password gate (dev mode).")
        return

    if st.session_state.get("authed") is True:
        if st.sidebar.button("Lock"):
            st.session_state.authed = False
            _rerun()
        return
        return

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

# ---------- Money parsing/formatting ----------
MONEY_RX  = re.compile(r"\$|,|\s")
SUFFIX_RX = re.compile(r"(?i)\s*([KMB])\b")  # K, M, B

def _parse_money_to_float(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    m = SUFFIX_RX.search(s); mult = 1.0
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

# ---------- Country code normalization (ISO-3 for maps) ----------
CC_OVERRIDES = {
    "Congo, Dem. Rep.": "COD",
    "Congo, Rep.": "COG",
    "Ivory Coast": "CIV",
    "Eswatini": "SWZ",
    "Cabo Verde": "CPV",
    "The Gambia": "GMB",
    "São Tomé and Príncipe": "STP",
    "Sao Tome and Principe": "STP",
    "Tanzania": "TZA",
}
def to_iso3(x: str) -> str | None:
    if not x or not str(x).strip():
        return None
    s = str(x).strip()
    if len(s) == 3 and s.isalpha():
        return s.upper()
    if len(s) == 2 and s.isalpha():
        c = pycountry.countries.get(alpha_2=s.upper())
        return getattr(c, "alpha_3", None)
    if s in CC_OVERRIDES:
        return CC_OVERRIDES[s]
    try:
        c = pycountry.countries.lookup(s)
        return c.alpha_3
    except Exception:
        return None

# ---------- ID helpers ----------
LIKELY_ID_COLS = [
    "NoticeID","NoticeId","Notice ID","NoticeNumber","Notice Number",
    "SolicitationNumber","SAMNoticeID","ID","uid","sam_id","Link","URL","NoticeLink"
]
def _find_id_column(df: pd.DataFrame) -> str | None:
    for c in LIKELY_ID_COLS:
        if c in df.columns:
            return c
    return None

def add_copy_link_column(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    link_col = None
    for cand in ("SAM Link","SAM_Link","Link","URL","NoticeLink"):
        if cand in df.columns:
            link_col = cand; break
    if link_col is None:
        id_col = _find_id_column(df)
        if id_col is not None:
            def mk(row):
                _id = str(row.get(id_col, "")).strip()
                return f"https://sam.gov/opp/{_id}/view" if _id else ""
            df["SAM Link"] = df.apply(mk, axis=1)
            link_col = "SAM Link"
        else:
            df["SAM Link"] = ""; link_col = "SAM Link"
    df["Copy SAM Link"] = "Copy"
    return df, link_col

# ---------- Data load from your DB (unchanged pipeline; enriched for UI) ----------
@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM opportunities ORDER BY PostedDate DESC", conn)
    finally:
        conn.close()

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # Parse PostedDate to tz-aware UTC, then create normalized naive Timestamps for filtering
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

    # SAM link & ISO codes for maps
    df, _ = add_copy_link_column(df)
    df["CountryCode_iso3"] = df.get("CountryCode", pd.Series([""]*len(df))).apply(to_iso3)
    df["PopCountry_iso3"]  = df.get("PopCountry",  pd.Series([""]*len(df))).apply(to_iso3)

    return df

# ---------- New items snapshot ----------
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

# ---------- Visuals ----------
def render_visuals(df_page: pd.DataFrame):
    if df_page.empty:
        st.info("No data in this date range yet."); return

    left, right = st.columns(2)
    with left:
        m = df_page.dropna(subset=["PopCountry_iso3"]).groupby("PopCountry_iso3").size().reset_index(name="opps")
        if not m.empty:
            fig = px.choropleth(m, locations="PopCountry_iso3", locationmode="ISO-3",
                                color="opps", color_continuous_scale="Plasma",
                                title="By PopCountry")
            fig.update_geos(scope="africa", showcountries=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No mappable PopCountry values.")

    with right:
        m2 = df_page.dropna(subset=["CountryCode_iso3"]).groupby("CountryCode_iso3").size().reset_index(name="opps")
        if not m2.empty:
            fig2 = px.choropleth(m2, locations="CountryCode_iso3", locationmode="ISO-3",
                                 color="opps", color_continuous_scale="Viridis",
                                 title="By CountryCode")
            fig2.update_geos(scope="africa", showcountries=True)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("No mappable CountryCode values.")

    left2, right2 = st.columns(2)
    with left2:
        col = "Department/Ind.Agency"
        if col in df_page.columns:
            counts = df_page[col].replace({"": "(blank)"}).value_counts().reset_index()
            counts.columns = [col, "count"]
            top_n = 12
            if len(counts) > top_n:
                top = counts.iloc[:top_n]
                other = counts.iloc[top_n:]["count"].sum()
                counts = pd.concat([top, pd.DataFrame({col:["Other"], "count":[other]})], ignore_index=True)
            fig3 = px.pie(counts, values="count", names=col, title="% Breakdown of Top US Agencies")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.warning("Column 'Department/Ind.Agency' not found.")

    with right2:
        if {"Department/Ind.Agency","PopCountry"}.issubset(df_page.columns):
            pivot = (df_page.assign(count=1)
                            .pivot_table(index="PopCountry", columns="Department/Ind.Agency",
                                         values="count", aggfunc="sum", fill_value=0))
            if not pivot.empty:
                pct = pivot.div(pivot.sum(axis=1).replace(0, np.nan), axis=0) * 100
                pct = pct.fillna(0).sort_index()
                fig4 = px.imshow(pct, aspect="auto",
                                 labels=dict(x="Agency", y="Country", color="% of Opps"),
                                 title="Agency → Country % Breakdown (per country)")
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.warning("Not enough data for country/agency matrix.")
        else:
            st.warning("Need both 'Department/Ind.Agency' and 'PopCountry' columns.")

# ---------- Grid (compact + details drawer + Excel filters) ----------
COMPACT_COL_ORDER = ["PostedDate","Title","Department/Ind.Agency","PopCountry","CountryCode","Copy SAM Link"]

def render_grid(df_page: pd.DataFrame):
    work = df_page.copy()  # avoid SettingWithCopyWarning
    for c in COMPACT_COL_ORDER:
        if c not in work.columns:
            work[c] = ""

    display_cols = COMPACT_COL_ORDER[:]
    gb = GridOptionsBuilder.from_dataframe(work[display_cols])
    gb.configure_default_column(filter=True, sortable=True, resizable=True, floatingFilter=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    if "_AwardAmountNumeric" in work.columns:
        gb.configure_column("_AwardAmountNumeric", hide=True)

    grid = AgGrid(
        work[display_cols],
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.FILTERING_CHANGED | GridUpdateMode.SORTING_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
        height=420,
        fit_columns_on_grid_load=True,
        enable_enterprise_modules=False,
        allow_unsafe_jscode=False,
    )

    raw_sel = grid.get("selected_rows", [])
    if isinstance(raw_sel, pd.DataFrame):
        rows = raw_sel.to_dict(orient="records")
    elif isinstance(raw_sel, dict):
        rows = [raw_sel]
    elif isinstance(raw_sel, list):
        rows = raw_sel
    else:
        rows = []

    if len(rows) > 0:
        row = rows[0]
        st.divider()
        st.subheader("Row details")

        sam_link = ""
        for c in ("SAM Link","SAM_Link","Link","URL","NoticeLink"):
            sam_link = str(row.get(c, "")).strip()
            if sam_link:
                break

        c1, c2, _ = st.columns([1,1,6])
        with c1:
            st.link_button("Open SAM Link", sam_link or "#", disabled=not bool(sam_link))
        with c2:
            if st.button("Copy SAM Link"):
                if sam_link:
                    streamlit_js_eval(jsCode=f"navigator.clipboard.writeText('{sam_link}')")
                    st.success("Copied link to clipboard.")
                else:
                    st.warning("No link available in this row.")

        desc_val = None
        for cand in ("Description","FullDescription","Summary","DescriptionText","opportunity_description"):
            if cand in work.columns:
                desc_val = row.get(cand, "")
                if desc_val: break
        with st.expander("Full Description", expanded=True):
            st.write(desc_val or "(No description text in this row)")

        st.markdown("**All fields**")
        other = {}
        for c in work.columns:
            if c in ("PostedDate_parsed","PostedDate_norm","CountryCode_iso3","PopCountry_iso3","_AwardAmountNumeric"):
                continue
            other[c] = row.get(c, "")
        keys = list(other.keys()); mid = int(np.ceil(len(keys)/2)) if keys else 0
        colL, colR = st.columns(2)
        with colL:
            for k in keys[:mid]:
                st.markdown(f"**{k}:**  {other[k] if other[k] != '' else '(blank)'}")
        with colR:
            for k in keys[mid:]:
                st.markdown(f"**{k}:**  {other[k] if other[k] != '' else '(blank)'}")

# ---------- Auto-run data loaders ----------
def ensure_historical_loaded():
    """Run bootstrap_historical.py exactly once (creates >5y archive)."""
    if BOOTSTRAP_FLAG.exists():
        return
    st.info("Loading historical data (one-time)…")
    ok = _run_script(BOOTSTRAP_SCRIPT, "Historical bootstrap")
    if ok:
        BOOTSTRAP_FLAG.write_text("done", encoding="utf-8")
        st.cache_data.clear()

def ensure_daily_update():
    """Run download_and_update.py if last run is >= 24 hours ago."""
    try:
        last = pd.Timestamp(LAST_REFRESH_TS.read_text(encoding="utf-8")) if LAST_REFRESH_TS.exists() else None
    except Exception:
        last = None
    now = pd.Timestamp.utcnow()
    must_run = (last is None) or ((now - last) >= pd.Timedelta(hours=24))
    if must_run:
        st.info("Refreshing daily data…")
        ok = _run_script(DOWNLOAD_SCRIPT, "Daily refresh")
        if ok:
            LAST_REFRESH_TS.write_text(now.isoformat(), encoding="utf-8")
            st.cache_data.clear()

# ---------- Main UI ----------
def main():
    password_gate()
    password_gate()

    st.title("SAM.gov — Contract Opportunities (African countries)")

    # 1) Historical data (one-time, at startup)
    ensure_historical_loaded()

    # 2) Daily refresh (every 24h, no user action)
    ensure_daily_update()

    # Load data from DB (after auto-runs)
    df = load_data()

    # Last updated (from DB content)
    if df.get("PostedDate_parsed") is not None and df["PostedDate_parsed"].notna().any():
        last_dt = df["PostedDate_parsed"].dropna().max()
        st.caption(f"Last updated (max PostedDate in DB): {last_dt}")
    else:
        last_dt = None
        st.caption("Last updated: unknown")

    if df.empty:
        st.error("No data loaded from the database yet. Please check your loader scripts.")
        return

    # New Opportunities Added
    current_ids = _current_ids(df)
    prev_ids    = _load_previous_ids()
    new_count   = len(current_ids - prev_ids) if prev_ids else len(current_ids)
    _save_current_ids(current_ids)

    colA, colB = st.columns([1,1])
    with colA:
        if df["PostedDate_parsed"].notna().any():
            st.metric("Last Updated", str(df["PostedDate_parsed"].dropna().max().date()))
        else:
            st.metric("Last Updated", "(unknown)")
    with colB:
        st.metric("New Contract Opportunities Added", new_count)

    st.divider()

    # Date-range tabs using normalized Timestamps
    today_norm    = pd.Timestamp.today().normalize()
    five_years_ago = today_norm - pd.DateOffset(years=5)

    tabs = st.tabs(["Last 7 Days","Last 30 Days","Last 365 Days","Last 5 Years","Archive (5+ Years)"])
    # Half-open windows [start, end)
    windows = [
        (today_norm - pd.Timedelta(days=7),   today_norm + pd.Timedelta(days=1)),
        (today_norm - pd.Timedelta(days=30),  today_norm + pd.Timedelta(days=1)),
        (today_norm - pd.Timedelta(days=365), today_norm + pd.Timedelta(days=1)),
        (five_years_ago,                      today_norm + pd.Timedelta(days=1)),
        (None,                                five_years_ago),  # Archive: < five_years_ago
    ]

    ts = df["PostedDate_norm"]
    for tab, (start, end) in zip(tabs, windows):
        with tab:
            if start is None:
                df_page = df[ts.notna() & (ts < end)].copy()
            else:
                df_page = df[ts.notna() & (ts >= start) & (ts < end)].copy()
            render_visuals(df_page)
            st.divider()
            render_grid(df_page)

    st.caption("Tip: Use the column filter icons in the table header to filter by Contains / Equals / Does not contain, like Excel/Sheets.")

if __name__ == "__main__":
    main()
