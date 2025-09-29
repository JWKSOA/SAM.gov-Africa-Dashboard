#!/usr/bin/env python3
# Streamlit Cloud–safe dashboard that reads data/opportunities.db committed by your GitHub Action.
# Changes here:
#  - Synthesize and SHOW a "SAM Link" (clickable) when missing; Copy button works.
#  - REMOVE CountryCode visualizations (keep only PopCountry visuals).
#  - Agency×Country table: keep only PopCountry; removed CountryCode version.
#  - Keep Africa filter logic unchanged (still uses both PopCountry & CountryCode).

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

st.set_page_config(page_title="SAM.gov - Africa Opportunities", layout="wide")

REPO_DB = Path(__file__).parent / "data" / "opportunities.db"
HOME_DB = Path.home() / "sam_africa_data" / "opportunities.db"
DB_PATH = REPO_DB if REPO_DB.exists() else HOME_DB

STATE_DIR = (REPO_DB.parent if REPO_DB.exists() else HOME_DB.parent)
STATE_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH = STATE_DIR / ".last_ids.json"

COMPACT_COL_ORDER = ["PostedDate","Title","Department/Ind.Agency","PopCountry","CountryCode","SAM Link"]

def _find_id_column(df: pd.DataFrame) -> str | None:
    for c in ["NoticeID","NoticeId","Notice ID","NoticeNumber","Notice Number","SolicitationNumber","SAMNoticeID","ID","uid","sam_id","Link","URL","NoticeLink"]:
        if c in df.columns:
            return c
    return None

def add_sam_link_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure there's a visible 'SAM Link' column. If missing, synthesize from NoticeID."""
    if "SAM Link" in df.columns and df["SAM Link"].astype(str).str.strip().any():
        return df
    link_col = None
    for c in ("SAM Link","SAM_Link","Link","URL","NoticeLink"):
        if c in df.columns and df[c].astype(str).str.strip().any():
            link_col = c; break
    if link_col is not None and link_col != "SAM Link":
        df["SAM Link"] = df[link_col].astype(str)
        return df
    # synthesize from NoticeID
    id_col = _find_id_column(df)
    if id_col is not None:
        def mk(row):
            nid = str(row.get(id_col,"")).strip()
            return f"https://sam.gov/opp/{nid}/view" if nid else ""
        df["SAM Link"] = df.apply(mk, axis=1)
    else:
        df["SAM Link"] = ""
    return df

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

    # Dates
    if "PostedDate" in df.columns:
        ts = pd.to_datetime(df["PostedDate"], errors="coerce", utc=True)
        df["PostedDate_parsed"] = ts
        df["PostedDate_norm"]   = ts.dt.tz_convert(None).dt.normalize()
    else:
        df["PostedDate_parsed"] = pd.NaT
        df["PostedDate_norm"]   = pd.NaT

    # Cleanups
    if "PrimaryContactFullName" in df.columns:
        df = df.drop(columns=["PrimaryContactFullName"])
    for col in ("SecondaryContactEmail","SecondaryContactPhone"):
        if col not in df.columns:
            df[col] = ""

    # Award$+
    if "Award$" in df.columns:
        def _parse_money_to_float(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return None
            s = str(val).strip()
            if not s: return None
            m = re.search(r"(?i)\s*([KMB])\b", s); mult = 1.0
            if m:
                suf = m.group(1).upper()
                mult = 1e3 if suf=="K" else 1e6 if suf=="M" else 1e9 if suf=="B" else 1.0
                s = re.sub(r"(?i)\s*([KMB])\b","",s)
            s = re.sub(r"[$,\s]","",s)
            try: return float(s)*mult
            except: return None
        def _fmt(x: float) -> str: return f"${x:,.2f}"
        df["Award$+"] = df["Award$"].astype(str)
        parsed = df["Award$"].apply(_parse_money_to_float)
        df["_AwardAmountNumeric"] = parsed
        mask = parsed.notna()
        df.loc[mask, "Award$"]  = parsed[mask].apply(_fmt)
        df.loc[~mask, "Award$"] = "See Award$+"

    # Links
    df = add_sam_link_column(df)

    # NaN→blank (non-time)
    non_time = [c for c in df.columns if not c.startswith("PostedDate_")]
    df[non_time] = df[non_time].replace({np.nan: ""})

    return df

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

def render_visuals_popcountry(df_page: pd.DataFrame):
    if df_page.empty:
        st.info("No data in this date range yet."); return

    # Heatmap by PopCountry ONLY (CountryCode visuals removed)
    left, right = st.columns(2)
    with left:
        if "PopCountry" in df_page.columns:
            counts = df_page.groupby("PopCountry").size().reset_index(name="opps")
            if not counts.empty:
                fig = px.choropleth(counts, locations="PopCountry", locationmode="country names",
                                    color="opps", color_continuous_scale="Plasma",
                                    title="By PopCountry")
                fig.update_geos(scope="africa", showcountries=True)
                st.plotly_chart(fig, use_container_width=True)
    with right:
        # Agency × PopCountry counts table
        if {"Department/Ind.Agency","PopCountry"}.issubset(df_page.columns):
            cnt = (df_page.assign(count=1)
                          .pivot_table(index="PopCountry", columns="Department/Ind.Agency",
                                       values="count", aggfunc="sum", fill_value=0))
            st.dataframe(cnt.sort_index(), use_container_width=True)

def render_grid(df_page: pd.DataFrame, tab_slug: str):
    work = df_page.copy()
    for c in COMPACT_COL_ORDER:
        if c not in work.columns:
            work[c] = ""
    display_cols = [c for c in COMPACT_COL_ORDER if c in work.columns]

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
        st.subheader(f"Row details — {tab_slug}")

        sam_link = str(row.get("SAM Link","")).strip()
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

        with st.expander("Full Description", expanded=True):
            desc_val = None
            for cand in ("Description","FullDescription","Summary","DescriptionText","opportunity_description"):
                if cand in work.columns:
                    desc_val = row.get(cand, "")
                    if desc_val: break
            st.write(desc_val or "(No description text in this row)")

        st.markdown("**All fields**")
        other = {}
        for c in work.columns:
            if c in ("PostedDate_parsed","PostedDate_norm","_AwardAmountNumeric"):
                continue
            other[c] = row.get(c, "")
        keys = list(other.keys())
        mid = int(np.ceil(len(keys)/2)) if keys else 0
        colL, colR = st.columns(2)
        with colL:
            for k in keys[:mid]:
                st.markdown(f"**{k}:**  {other[k] if other[k] != '' else '(blank)'}")
        with colR:
            for k in keys[mid:]:
                st.markdown(f"**{k}:**  {other[k] if other[k] != '' else '(blank)'}")

def main():
    st.title("SAM.gov — Contract Opportunities (African countries)")

    # Optional: GitHub Action trigger (Fetch latest) stays as-is if you added it before

    df = load_data()
    if df.get("PostedDate_parsed") is not None and df["PostedDate_parsed"].notna().any():
        last_dt = df["PostedDate_parsed"].dropna().max()
        st.caption(f"Last updated (max PostedDate in DB): {last_dt}")
    else:
        st.caption("Last updated: unknown")

    if df.empty:
        st.error("No data loaded. Ensure your nightly GitHub Action wrote data/opportunities.db to the repo.")
        return

    current_ids = _current_ids(df)
    prev_ids    = _load_previous_ids()
    new_count   = len(current_ids - prev_ids) if prev_ids else len(current_ids)
    _save_current_ids(current_ids)

    c1, c2 = st.columns([1,1])
    with c1:
        if df["PostedDate_parsed"].notna().any():
            st.metric("Last Updated", str(df["PostedDate_parsed"].dropna().max().date()))
        else:
            st.metric("Last Updated", "(unknown)")
    with c2:
        st.metric("New Contract Opportunities Added", new_count)

    st.divider()

    today_norm     = pd.Timestamp.today().normalize()
    five_years_ago = today_norm - pd.DateOffset(years=5)

    tab_names = ["Last 7 Days","Last 30 Days","Last 365 Days","Last 5 Years","Archive (5+ Years)"]
    slugs     = ["7d","30d","365d","5y","arch"]
    tabs = st.tabs(tab_names)

    windows = [
        (today_norm - pd.Timedelta(days=7),   today_norm + pd.Timedelta(days=1)),  # [start, end)
        (today_norm - pd.Timedelta(days=30),  today_norm + pd.Timedelta(days=1)),
        (today_norm - pd.Timedelta(days=365), today_norm + pd.Timedelta(days=1)),
        (five_years_ago,                       today_norm + pd.Timedelta(days=1)),
        (None,                                  five_years_ago),                   # archive: < five_years_ago
    ]

    ts = df["PostedDate_norm"]
    for tab, slug, (start, end) in zip(tabs, slugs, windows):
        with tab:
            if start is None:
                df_page = df[ts.notna() & (ts < end)].copy()
                if df_page.empty:
                    earliest = df["PostedDate_parsed"].dropna().min()
                    st.warning(
                        "No rows in Archive (5+ Years). "
                        f"Earliest PostedDate in DB: **{earliest}**"
                    )
            else:
                df_page = df[ts.notna() & (ts >= start) & (ts < end)].copy()

            render_visuals_popcountry(df_page)
            st.divider()
            render_grid(df_page, tab_slug=slug)

    st.caption("Tip: use the header filter icons to search each column (Contains / Equals / etc.).")

if __name__ == "__main__":
    main()
