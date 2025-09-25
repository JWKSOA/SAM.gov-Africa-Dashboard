#!/usr/bin/env python3
# Streamlit dashboard with a single shared password gate + "Last refreshed" indicator

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

# ---------- Page setup (must be first Streamlit call) ----------
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
        st.warning("No app_password secret configured. Skipping password gate (dev mode).")
        return  # allow through for local development

    if st.session_state.get("authed") is True:
        if st.sidebar.button("Lock"):
            st.session_state.authed = False
            _rerun()
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

    # Parse PostedDate (safe)
    if "PostedDate" in df.columns:
        df["PostedDate_parsed"] = pd.to_datetime(df["PostedDate"], errors="coerce", infer_datetime_format=True)
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

    st.title("SAM.gov — Contract Opportunities (African countries)")

    # 1) Historical data (one-time, at startup)
    ensure_historical_loaded()

    # 2) Daily refresh (every 24h, no user action)
    ensure_daily_update()

    # Load data from DB (after auto-runs)
    df = load_data()

    # ---- Last refreshed indicator (NEW) ----
    if not df.empty and "PostedDate" in df.columns:
        last_dt = pd.to_datetime(df["PostedDate"], errors="coerce").max()
        # Display a friendly timestamp; falls back if NaT
        if pd.notna(last_dt):
            st.caption(f"Last updated (max PostedDate in DB): {last_dt}")
        else:
            st.caption("Last updated: (could not parse PostedDate)")
    else:
        st.caption("Last updated: (no data yet)")

    if df.empty:
        st.error("No data loaded from the database yet. Please check your loader scripts.")
        return

    # Sidebar filters
    st.sidebar.header("Filters")

    pop_vals = sorted([v for v in df.get("PopCountry", pd.Series([], dtype=str)).dropna().unique().tolist() if str(v).strip()])
    cc_vals  = sorted([v for v in df.get("CountryCode", pd.Series([], dtype=str)).dropna().unique().tolist() if str(v).strip()])

    selected_pops = st.sidebar.multiselect("Filter PopCountry (contains)", pop_vals)
    selected_cc   = st.sidebar.multiselect("Filter CountryCode (exact)", cc_vals)

    from_date = st.sidebar.date_input("From date (optional)", value=None)
    to_date   = st.sidebar.date_input("To date (optional)",   value=None)

    q = st.sidebar.text_input("Full-text search (Title, Description, Awardee)")

    # Apply filters
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

    # Export
    csv_bytes = show_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download results as CSV", data=csv_bytes, file_name="sam_africa_results.csv", mime="text/csv")

if __name__ == "__main__":
    main()
