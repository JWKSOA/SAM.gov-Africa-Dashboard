#!/usr/bin/env python3
"""
bootstrap_historical.py

One-time (or occasional) backfill of ALL historical Contract Opportunities
(archived by fiscal year) + current full CSV. Filters to African countries
and inserts only NEW rows (robust NoticeID detection / synthesis).
"""

import os
import sys
import sqlite3
import datetime
import tempfile
import shutil
import re
import hashlib
from pathlib import Path
from time import sleep

import requests
import pandas as pd

from africa_countries import AFRICA_NAMES, AFRICA_ISO3

PRIMARY_CSV_URL = (
    "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
    "Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public"
)
FALLBACK_CSV_URL = (
    "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv"
)

ARCHIVE_BASES = [
    "https://s3.amazonaws.com/falextracts/Contract%20Opportunities/Archived%20Data",
    "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/Archived%20Data",
]
ARCHIVE_FILENAME_TEMPLATE = "FY{YEAR}_archived_opportunities.csv"

CSV_FILENAME_BASE = "ContractOpportunitiesFullCSV.csv"
SAM_DATA_DIR = os.environ.get("SAM_DATA_DIR")
LOCAL_DATA_DIR = Path(SAM_DATA_DIR).expanduser().resolve() if SAM_DATA_DIR else (Path.home() / "sam_africa_data")
DB_PATH = LOCAL_DATA_DIR / "opportunities.db"

KEEP_COLUMNS = [
    "Title","Department/Ind.Agency","Sub-Tier","Office","PostedDate","Type","PopCountry",
    "AwardNumber","AwardDate","Award$","Awardee","PrimaryContactTitle","PrimaryContactFullName",
    "PrimaryContactEmail","PrimaryContactPhone","OrganizationType","CountryCode","Link","Description",
]

CHUNK_SIZE = 50000
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

ID_CANDIDATES_NORM = {
    "noticeid","noticeidnumber","noticeidno","documentid","solicitationnumber",
    "solicitationid","opportunityid","referencenumber","referenceid","refid","solnumber",
}

def robust_get(url, *, stream=False, timeout=120, max_retries=3, backoff=3):
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

def download_to(path, url):
    r = robust_get(url, stream=True, timeout=600, max_retries=3)
    with open(path, "wb") as f:
        shutil.copyfileobj(r.raw, f)

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    keep_defs = ",\n        ".join([f"\"{c}\" TEXT" for c in KEEP_COLUMNS])
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        "NoticeID" TEXT UNIQUE,
        {keep_defs}
    )
    """)
    conn.commit()
    conn.close()

def ensure_notice_id_column(df):
    if "NoticeID" in df.columns:
        return df
    normalized_map = {norm(c): c for c in df.columns}
    for key, actual in normalized_map.items():
        if key in ID_CANDIDATES_NORM or "noticeid" in key or "documentid" in key or "opportunityid" in key:
            df["NoticeID"] = df[actual].astype(str)
            return df
    # synthesize surrogate
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

def insert_new_rows_chunk(cur, df_chunk) -> int:
    for c in KEEP_COLUMNS:
        if c not in df_chunk.columns:
            df_chunk[c] = None
    if "NoticeID" not in df_chunk.columns:
        df_chunk["NoticeID"] = None
    df_chunk["NoticeID"] = df_chunk["NoticeID"].astype(str)

    cols_for_insert = ['"NoticeID"'] + [f'"{c}"' for c in KEEP_COLUMNS]
    placeholders = ", ".join(["?"] * len(cols_for_insert))
    columns_sql = ", ".join(cols_for_insert)
    sql = f"INSERT INTO opportunities ({columns_sql}) VALUES ({placeholders})"

    inserted = 0
    for _, row in df_chunk.iterrows():
        nid = (row.get("NoticeID") or "").strip()
        if not nid or nid.lower() == "nan":
            continue
        vals = [nid] + [str(row.get(c) or "") for c in KEEP_COLUMNS]
        try:
            cur.execute(sql, vals)
            inserted += 1
        except sqlite3.IntegrityError:
            continue
    return inserted

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

def iter_csv_chunks(path):
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
                encoding_errors=enc_err,
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

def ingest_file_into_db(path, label):
    total_matched = 0
    total_inserted = 0
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        for chunk in iter_csv_chunks(path):
            matched = filter_african_rows(chunk)
            if matched.empty:
                continue
            matched = ensure_notice_id_column(matched)
            inserted = insert_new_rows_chunk(cur, matched)
            total_matched += len(matched)
            total_inserted += inserted
        conn.commit()
    finally:
        conn.close()
    print(f"[{label}] matched={total_matched}, inserted={total_inserted}")

def backfill_archives(start_year=1970, end_year=None):
    if end_year is None:
        today = datetime.date.today()
        fy = today.year if today.month < 10 else today.year + 1
        end_year = fy
    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        for year in range(start_year, end_year + 1):
            filename = ARCHIVE_FILENAME_TEMPLATE.format(YEAR=year)
            found = False
            for base in ARCHIVE_BASES:
                url = f"{base}/{filename}"
                try:
                    print(f"Checking FY{year} archive: {url}")
                    tmp = tmpdir / filename
                    download_to(tmp, url)
                    print(f"  Downloaded FY{year} archive.")
                    ingest_file_into_db(tmp, f"FY{year}")
                    found = True
                    break
                except Exception as e:
                    print(f"  Not available at this location ({e}); trying next...")
            if not found:
                print(f"FY{year} archive not found; skipping.")
    print("Archive backfill complete.")

def ingest_current_full():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / CSV_FILENAME_BASE
        url = os.environ.get("SAM_CSV_URL") or PRIMARY_CSV_URL
        try:
            print(f"Downloading current full CSV from: {url}")
            download_to(tmp, url)
        except Exception as e:
            print(f"Primary failed ({e}); trying fallback...")
            download_to(tmp, FALLBACK_CSV_URL)
        ingest_file_into_db(tmp, "CURRENT")

def main():
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ensure_db()
    backfill_archives(start_year=1970, end_year=None)
    ingest_current_full()
    print("Historical backfill done.")

if __name__ == "__main__":
    main()
