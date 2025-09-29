#!/usr/bin/env python3
"""
bootstrap_historical.py

One-time (or occasional) backfill of ALL historical Contract Opportunities
(archived by fiscal year) + current full CSV. Filters to African countries
and inserts only NEW rows (robust NoticeID detection / synthesis).

Enhancements:
- Fast HEAD probe to skip missing years quickly (no long GET timeouts).
- Streamed download with progress logs.
- Multiple filename patterns per year (older archives sometimes vary).
- Clear ingest logs: matched / inserted / duplicate-skipped per file.
- Optional bounded backfill via env: START_YEAR / END_YEAR.
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
# Try several historical name patterns per FY
ARCHIVE_NAME_PATTERNS = [
    "FY{YEAR}_archived_opportunities.csv",
    "Contract_Opportunities_FY{YEAR}.csv",
    "FY{YEAR}_opportunities.csv",
]

CSV_FILENAME_BASE = "ContractOpportunitiesFullCSV.csv"
SAM_DATA_DIR = os.environ.get("SAM_DATA_DIR")
LOCAL_DATA_DIR = Path(SAM_DATA_DIR).expanduser().resolve() if SAM_DATA_DIR else (Path.home() / "sam_africa_data")
DB_PATH = LOCAL_DATA_DIR / "opportunities.db"

KEEP_COLUMNS = [
    "Title","Department/Ind.Agency","Sub-Tier","Office","PostedDate","Type","PopCountry",
    "AwardNumber","AwardDate","Award$","Awardee","PrimaryContactTitle","PrimaryContactFullName",
    "PrimaryContactEmail","PrimaryContactPhone","OrganizationType","CountryCode","Link","Description",
]

CHUNK_SIZE = 50_000
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- Networking knobs ---
HEAD_TIMEOUT = 10
GET_TIMEOUT = 120
MAX_RETRIES = 2
BACKOFF_SECS = 3
PROGRESS_CHUNK = 5 * 1024 * 1024  # 5 MB

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

ID_CANDIDATES_NORM = {
    "noticeid","noticeidnumber","noticeidno","documentid","solicitationnumber",
    "solicitationid","opportunityid","referencenumber","referenceid","refid","solnumber",
}

def exists_fast(url: str) -> bool:
    """Quick HEAD probe: skip years instantly if not present."""
    try:
        r = requests.head(url, timeout=HEAD_TIMEOUT)
        # Treat 200/206/302/403 as "likely exists" (S3 sometimes 403 on HEAD)
        return r.status_code in (200, 206, 302, 403)
    except Exception:
        return False

def robust_get(url, *, stream=False, timeout=GET_TIMEOUT, max_retries=MAX_RETRIES, backoff=BACKOFF_SECS):
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
            print(f"    attempt {attempt}/{max_retries} failed: {e}", flush=True)
            if attempt < max_retries:
                sleep(backoff * attempt)
    raise last_err

def download_to(path: Path, url: str):
    r = robust_get(url, stream=True, timeout=GET_TIMEOUT, max_retries=MAX_RETRIES)
    with open(path, "wb") as f:
        downloaded = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded % PROGRESS_CHUNK < 1024 * 1024:
                print(f"      downloaded ~{downloaded // (1024*1024)} MB …", flush=True)

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        # Performance pragmas
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        keep_defs = ",\n        ".join([f"\"{c}\" TEXT" for c in KEEP_COLUMNS])
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            "NoticeID" TEXT UNIQUE,
            {keep_defs}
        )
        """)
        conn.commit()
    finally:
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

def insert_new_rows_chunk(cur, df_chunk) -> tuple[int, int]:
    """Insert a chunk row-by-row into 'opportunities'. Returns: (inserted_count, duplicate_skipped_count)"""
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
    dupes = 0
    for _, row in df_chunk.iterrows():
        nid = (row.get("NoticeID") or "").strip()
        if not nid or nid.lower() == "nan":
            continue
        vals = [nid] + [str(row.get(c) or "") for c in KEEP_COLUMNS]
        try:
            cur.execute(sql, vals)
            inserted += 1
        except sqlite3.IntegrityError:
            dupes += 1
            continue
    return inserted, dupes

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
            print(f"    reading CSV in chunks (encoding={enc}, errors={enc_err}) …", flush=True)
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
            print(f"      failed with encoding={enc}: {e}", flush=True)
            continue
    raise last_err

def ingest_file_into_db(path: Path, label: str):
    total_matched = 0
    total_inserted = 0
    total_dupes = 0
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        for i, chunk in enumerate(iter_csv_chunks(path), start=1):
            matched = filter_african_rows(chunk)
            if matched.empty:
                continue
            matched = ensure_notice_id_column(matched)
            inserted, dupes = insert_new_rows_chunk(cur, matched)
            total_matched += len(matched)
            total_inserted += inserted
            total_dupes += dupes
            if i % 2 == 0:
                conn.commit()
            print(f"      chunk {i}: matched={len(matched)} inserted={inserted} dupes={dupes}", flush=True)
        conn.commit()
        # keep DB lean
        cur.execute('PRAGMA optimize;')
        cur.execute('VACUUM;')
        conn.commit()
    finally:
        conn.close()
    print(f"[{label}] matched={total_matched}, inserted={total_inserted}, duplicate_skipped={total_dupes}", flush=True)

def backfill_archives(start_year=1970, end_year=None):
    if end_year is None:
        today = datetime.date.today()
        fy = today.year if today.month < 10 else today.year + 1
        end_year = fy

    print(f"Backfilling archives from FY{start_year} to FY{end_year} …", flush=True)

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        for year in range(start_year, end_year + 1):
            print(f"\nChecking FY{year} …", flush=True)
            found = False
            for base in ARCHIVE_BASES:
                for pat in ARCHIVE_NAME_PATTERNS:
                    filename = pat.format(YEAR=year)
                    url = f"{base}/{filename}"
                    print(f"  probing {url}", flush=True)
                    if not exists_fast(url):
                        print("    not present (HEAD)", flush=True)
                        continue
                    try:
                        tmp = tmpdir / filename
                        print("    exists — downloading …", flush=True)
                        download_to(tmp, url)
                        print("    downloaded — ingesting …", flush=True)
                        ingest_file_into_db(tmp, f"FY{year}")
                        found = True
                        break
                    except Exception as e:
                        print(f"    download/ingest failed: {e}", flush=True)
                if found:
                    break
            if not found:
                print(f"FY{year} archive not found at known locations; skipping.", flush=True)

    print("Archive backfill complete.", flush=True)

def robust_get_csv(url, *, timeout=300):
    last_err = None
    for attempt in range(1, 3):
        try:
            r = requests.get(url, stream=True, timeout=timeout)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} server error")
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            print(f"  (current) attempt {attempt}/2 failed: {e}", flush=True)
            if attempt < 2:
                sleep(2 * attempt)
    raise last_err

def ingest_current_full():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / CSV_FILENAME_BASE
        url = os.environ.get("SAM_CSV_URL") or PRIMARY_CSV_URL
        try:
            print(f"Downloading current full CSV from: {url}", flush=True)
            r = robust_get_csv(url, timeout=300)
        except Exception as e:
            print(f"Primary failed ({e}); trying fallback…", flush=True)
            r = robust_get_csv(FALLBACK_CSV_URL, timeout=300)
        with open(tmp, "wb") as f:
            shutil.copyfileobj(r.raw, f)
        ingest_file_into_db(tmp, "CURRENT")

def main():
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ensure_db()

    start_env = os.environ.get("START_YEAR")
    end_env = os.environ.get("END_YEAR")
    if start_env and end_env:
        try:
            s = int(start_env); e = int(end_env)
            backfill_archives(start_year=s, end_year=e)
        except Exception:
            print(f"Ignoring invalid START/END envs: {start_env}..{end_env}", flush=True)
            backfill_archives(start_year=1970, end_year=None)
    else:
        backfill_archives(start_year=1970, end_year=None)

    ingest_current_full()
    print("Historical backfill done.", flush=True)

if __name__ == "__main__":
    main()
