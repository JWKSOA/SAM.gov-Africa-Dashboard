#!/usr/bin/env python3
"""
download_and_update.py

Daily job to:
- Download ContractOpportunitiesFullCSV.csv from SAM endpoints (no page scrape).
- Stream ingest in chunks (tolerant encodings).
- Filter rows for African countries (PopCountry or CountryCode).
- Insert only NEW NoticeIDs into SQLite.
- In CI (GitHub Actions), DO NOT persist raw CSVs to the repo (avoid >100MB pushes).
- Locally, optionally keep a dated CSV in ~/sam_africa_data (never the repo).

This script honors SAM_DATA_DIR for DB location (set to $GITHUB_WORKSPACE/data in CI).
"""

import os
import sys
import sqlite3
import datetime
import tempfile
import shutil
import re
from pathlib import Path
from time import sleep

import requests
import pandas as pd

from africa_countries import AFRICA_NAMES, AFRICA_ISO3

# ---------- Config ----------
PRIMARY_CSV_URL = (
    "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
    "Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public"
)
FALLBACK_CSV_URL = (
    "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv"
)

CSV_FILENAME_BASE = "ContractOpportunitiesFullCSV.csv"

SAM_DATA_DIR = os.environ.get("SAM_DATA_DIR")
LOCAL_DATA_DIR = Path(SAM_DATA_DIR).expanduser().resolve() if SAM_DATA_DIR else (Path.home() / "sam_africa_data")
DB_PATH = LOCAL_DATA_DIR / "opportunities.db"

CHUNK_SIZE = 50_000  # rows per chunk when streaming the CSV
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Detect CI
CI_MODE = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

# ---------- Small utils ----------
def norm(s: str) -> str:
    """Normalize a column name: lowercase and strip non-alphanumerics."""
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# Common historical variants that can serve as a unique ID column
ID_CANDIDATES_NORM = {
    "noticeid","noticeidnumber","noticeidno","documentid","solicitationnumber",
    "solicitationid","opportunityid","referencenumber","referenceid","refid","solnumber",
}

# ---------- Net helpers ----------
def robust_get(url, *, stream=False, timeout=300, max_retries=3, backoff=3):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, stream=stream, timeout=timeout)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} server error")
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                sleep(backoff * attempt)
            else:
                raise last_err

def resolve_csv_url():
    env_url = os.environ.get("SAM_CSV_URL")
    if env_url:
        return env_url
    try:
        _ = robust_get(PRIMARY_CSV_URL, stream=True, timeout=60, max_retries=2)
        return PRIMARY_CSV_URL
    except Exception:
        return FALLBACK_CSV_URL

def download_csv(url, dest_path: Path):
    print(f"Downloading CSV from: {url}")
    r = robust_get(url, stream=True, timeout=300, max_retries=3)
    with open(dest_path, "wb") as f:
        shutil.copyfileobj(r.raw, f)
    print(f"Saved CSV to temp: {dest_path}")

# ---------- DB ----------
def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        # performance pragmas
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        # table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            "NoticeID" TEXT UNIQUE,
            "Title" TEXT,
            "Department/Ind.Agency" TEXT,
            "Sub-Tier" TEXT,
            "Office" TEXT,
            "PostedDate" TEXT,
            "Type" TEXT,
            "PopCountry" TEXT,
            "AwardNumber" TEXT,
            "AwardDate" TEXT,
            "Award$" TEXT,
            "Awardee" TEXT,
            "PrimaryContactTitle" TEXT,
            "PrimaryContactFullName" TEXT,
            "PrimaryContactEmail" TEXT,
            "PrimaryContactPhone" TEXT,
            "OrganizationType" TEXT,
            "CountryCode" TEXT,
            "Link" TEXT,
            "Description" TEXT
        )
        """)
        conn.commit()
    finally:
        conn.close()

def insert_new_rows_chunk(cur, df_chunk) -> int:
    keep_cols = [
        "Title","Department/Ind.Agency","Sub-Tier","Office","PostedDate","Type","PopCountry",
        "AwardNumber","AwardDate","Award$","Awardee","PrimaryContactTitle","PrimaryContactFullName",
        "PrimaryContactEmail","PrimaryContactPhone","OrganizationType","CountryCode","Link","Description",
    ]
    # Ensure kept columns exist
    for c in keep_cols:
        if c not in df_chunk.columns:
            df_chunk[c] = None
    if "NoticeID" not in df_chunk.columns:
        df_chunk["NoticeID"] = None

    df_chunk["NoticeID"] = df_chunk["NoticeID"].astype(str)

    cols_for_insert = ['"NoticeID"'] + [f'"{c}"' for c in keep_cols]
    placeholders = ", ".join(["?"] * len(cols_for_insert))
    columns_sql = ", ".join(cols_for_insert)
    sql = f"INSERT INTO opportunities ({columns_sql}) VALUES ({placeholders})"

    inserted = 0
    for _, row in df_chunk.iterrows():
        nid = (row.get("NoticeID") or "").strip()
        if not nid or nid.lower() == "nan":
            continue
        vals = [nid] + [str(row.get(c) or "") for c in keep_cols]
        try:
            cur.execute(sql, vals)
            inserted += 1
        except sqlite3.IntegrityError:
            continue
    return inserted

# ---------- Africa filter ----------
def filter_african_rows(df):
    for c in ("PopCountry", "CountryCode"):
        if c not in df.columns:
            df[c] = ""
    def row_matches(row):
        pop = str(row.get("PopCountry", "") or "")
        cc = str(row.get("CountryCode", "") or "")
        if cc.upper() in AFRICA_ISO3:
            return True
        tokens = re.split(r'[,;/\|]', pop)
        for t in tokens:
            t_strip = t.strip()
            if not t_strip:
                continue
            if t_strip.lower() in AFRICA_NAMES:
                return True
            if any(code in t_strip.upper() for code in AFRICA_ISO3):
                return True
            for name in AFRICA_NAMES:
                if name in t_strip.lower():
                    return True
        return False
    return df[df.apply(row_matches, axis=1)].copy()

# ---------- NoticeID detection / synthesis ----------
def ensure_notice_id_column(df):
    if "NoticeID" in df.columns:
        return df
    normalized_map = {norm(c): c for c in df.columns}
    for key, actual in normalized_map.items():
        if key in ID_CANDIDATES_NORM or "noticeid" in key or "documentid" in key or "opportunityid" in key:
            df["NoticeID"] = df[actual].astype(str)
            return df
    # Fallback: synthesize from stable fields
    import hashlib
    def make_hash(row):
        parts = [
            str(row.get("Title") or ""),
            str(row.get("PostedDate") or ""),
            str(row.get("Type") or ""),
            str(row.get("Link") or ""),
            str(row.get("AwardNumber") or ""),
            str(row.get("CountryCode") or ""),
            str(row.get("PopCountry") or ""),
        ]
        s = "|".join(parts).strip() or str(row.to_dict())
        return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:24]
    df["NoticeID"] = df.apply(make_hash, axis=1)
    return df

# ---------- CSV streaming (chunked, tolerant) ----------
def iter_csv_chunks(path: Path):
    enc_trials = [
        ("utf-8", "replace"),
        ("utf-8-sig", "strict"),
        ("cp1252", "replace"),
        ("latin-1", "replace"),
    ]
    last_err = None
    for enc, enc_err in enc_trials:
        try:
            print(f"Reading CSV in chunks with encoding={enc}, errors={enc_err} ...")
            iterator = pd.read_csv(
                path,
                dtype=str,
                encoding=enc,
                encoding_errors=enc_err,  # pandas 2.x
                engine="python",
                on_bad_lines="skip",
                chunksize=CHUNK_SIZE,
            )
            for chunk in iterator:
                chunk.columns = [c.strip() for c in chunk.columns]
                yield chunk
            return
        except Exception as e:
            last_err = e
            print(f"  Failed with encoding={enc}: {e}")
            continue
    raise last_err

def main():
    ensure_db()

    csv_url = resolve_csv_url()
    today = datetime.date.today()

    # Always work in temp; never write CSV into the repo in CI
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / CSV_FILENAME_BASE
        try:
            download_csv(csv_url, tmp)
        except Exception as e:
            print("Failed to download CSV:", e)
            sys.exit(1)

        total_matched = 0
        total_inserted = 0

        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            for chunk in iter_csv_chunks(tmp):
                matched = filter_african_rows(chunk)
                if matched.empty:
                    continue
                matched = ensure_notice_id_column(matched)
                inserted = insert_new_rows_chunk(cur, matched)
                total_matched += len(matched)
                total_inserted += inserted
            conn.commit()
            # Small DB tune-up each run
            cur.execute('PRAGMA optimize;')
            cur.execute('VACUUM;')
            conn.commit()
        finally:
            conn.close()

        print(f"Found {total_matched} rows that reference African countries.")
        print(f"Inserted {total_inserted} new rows into DB.")

        if CI_MODE:
            print("CI mode: NOT saving the raw CSV in the repo.")
        else:
            # Local convenience: keep a dated CSV in ~/sam_africa_data (not in repo)
            dest = LOCAL_DATA_DIR / f"ContractOpportunitiesFullCSV_{today.strftime('%m_%d_%Y')}.csv"
            shutil.copyfile(tmp, dest)
            print(f"Saved a copy locally at {dest} (NOT committed to git).")

    print("Done.")

if __name__ == "__main__":
    main()
