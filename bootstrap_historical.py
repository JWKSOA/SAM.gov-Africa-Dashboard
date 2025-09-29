#!/usr/bin/env python3
"""
bootstrap_historical.py - FIXED VERSION
Fetches ALL historical years, properly deduplicates, and captures all African contracts
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

# Complete list of 54 African countries with ISO3 codes
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

# Expanded mappings for all variations
AFRICA_MAPPINGS = {
    # Include all variations (lowercase for matching)
    "algeria": "DZA", "dza": "DZA", "algérie": "DZA",
    "angola": "AGO", "ago": "AGO",
    "benin": "BEN", "ben": "BEN", "bénin": "BEN",
    "botswana": "BWA", "bwa": "BWA",
    "burkina faso": "BFA", "bfa": "BFA", "burkina": "BFA",
    "burundi": "BDI", "bdi": "BDI",
    "cabo verde": "CPV", "cpv": "CPV", "cape verde": "CPV",
    "cameroon": "CMR", "cmr": "CMR", "cameroun": "CMR",
    "central african republic": "CAF", "caf": "CAF", "car": "CAF",
    "chad": "TCD", "tcd": "TCD", "tchad": "TCD",
    "comoros": "COM", "com": "COM", "comores": "COM",
    "congo": "COG", "cog": "COG", "congo-brazzaville": "COG", "congo brazzaville": "COG", "republic of congo": "COG",
    "democratic republic of the congo": "COD", "cod": "COD", "drc": "COD", "dr congo": "COD", "congo-kinshasa": "COD", "zaire": "COD",
    "djibouti": "DJI", "dji": "DJI",
    "egypt": "EGY", "egy": "EGY", "misr": "EGY",
    "equatorial guinea": "GNQ", "gnq": "GNQ",
    "eritrea": "ERI", "eri": "ERI",
    "eswatini": "SWZ", "swz": "SWZ", "swaziland": "SWZ",
    "ethiopia": "ETH", "eth": "ETH",
    "gabon": "GAB", "gab": "GAB",
    "gambia": "GMB", "gmb": "GMB", "the gambia": "GMB",
    "ghana": "GHA", "gha": "GHA",
    "guinea": "GIN", "gin": "GIN", "guinée": "GIN",
    "guinea-bissau": "GNB", "gnb": "GNB", "guinea bissau": "GNB",
    "ivory coast": "CIV", "civ": "CIV", "côte d'ivoire": "CIV", "cote d'ivoire": "CIV", "cote divoire": "CIV",
    "kenya": "KEN", "ken": "KEN",
    "lesotho": "LSO", "lso": "LSO",
    "liberia": "LBR", "lbr": "LBR",
    "libya": "LBY", "lby": "LBY",
    "madagascar": "MDG", "mdg": "MDG",
    "malawi": "MWI", "mwi": "MWI",
    "mali": "MLI", "mli": "MLI",
    "mauritania": "MRT", "mrt": "MRT", "mauritanie": "MRT",
    "mauritius": "MUS", "mus": "MUS", "maurice": "MUS",
    "morocco": "MAR", "mar": "MAR", "maroc": "MAR",
    "mozambique": "MOZ", "moz": "MOZ", "moçambique": "MOZ",
    "namibia": "NAM", "nam": "NAM",
    "niger": "NER", "ner": "NER",
    "nigeria": "NGA", "nga": "NGA",
    "rwanda": "RWA", "rwa": "RWA",
    "são tomé and príncipe": "STP", "stp": "STP", "sao tome and principe": "STP", "são tomé": "STP",
    "senegal": "SEN", "sen": "SEN", "sénégal": "SEN",
    "seychelles": "SYC", "syc": "SYC",
    "sierra leone": "SLE", "sle": "SLE",
    "somalia": "SOM", "som": "SOM",
    "south africa": "ZAF", "zaf": "ZAF", "rsa": "ZAF",
    "south sudan": "SSD", "ssd": "SSD",
    "sudan": "SDN", "sdn": "SDN",
    "tanzania": "TZA", "tza": "TZA", "tanganyika": "TZA",
    "togo": "TGO", "tgo": "TGO",
    "tunisia": "TUN", "tun": "TUN", "tunisie": "TUN",
    "uganda": "UGA", "uga": "UGA",
    "zambia": "ZMB", "zmb": "ZMB",
    "zimbabwe": "ZWE", "zwe": "ZWE"
}

AFRICA_ISO3 = set(AFRICAN_COUNTRIES.values())

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

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

ID_CANDIDATES_NORM = {
    "noticeid","noticeidnumber","noticeidno","documentid","solicitationnumber",
    "solicitationid","opportunityid","referencenumber","referenceid","refid","solnumber",
}

def format_country_display(iso_code: str) -> str:
    """Convert ISO3 to 'COUNTRY NAME (ISO3)' format"""
    for country, code in AFRICAN_COUNTRIES.items():
        if code == iso_code:
            return f"{country} ({code})"
    return iso_code

def standardize_country_code(value: str) -> str:
    """Convert any country name or code to 'COUNTRY NAME (ISO3)' format"""
    if not value:
        return value
    
    cleaned = str(value).strip().lower()
    
    # Check if already ISO code
    if cleaned.upper() in AFRICA_ISO3:
        return format_country_display(cleaned.upper())
    
    # Try to match against mappings
    if cleaned in AFRICA_MAPPINGS:
        iso_code = AFRICA_MAPPINGS[cleaned]
        return format_country_display(iso_code)
    
    # Check partial matches
    for mapping_key, iso_code in AFRICA_MAPPINGS.items():
        if mapping_key in cleaned:
            return format_country_display(iso_code)
    
    return value

def exists_fast(url: str) -> bool:
    try:
        r = requests.head(url, timeout=10)
        return r.status_code in (200, 206, 302, 403)
    except Exception:
        return False

def robust_get(url, *, stream=False, timeout=120, max_retries=2, backoff=3):
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
    r = robust_get(url, stream=True, timeout=120, max_retries=2)
    with open(path, "wb") as f:
        downloaded = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded % (5 * 1024 * 1024) < 1024 * 1024:
                print(f"      downloaded ~{downloaded // (1024*1024)} MB …", flush=True)

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        keep_defs = ",\n        ".join([f'"{c}" TEXT' for c in KEEP_COLUMNS])
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

def filter_african_rows(df):
    for c in ("PopCountry", "CountryCode"):
        if c not in df.columns:
            df[c] = ""
    
    def row_matches(row):
        pop = str(row.get("PopCountry", "") or "").strip()
        cc = str(row.get("CountryCode", "") or "").strip()
        
        # More comprehensive matching
        pop_lower = pop.lower()
        cc_lower = cc.lower()
        
        # Check ISO codes
        if cc.upper() in AFRICA_ISO3 or pop.upper() in AFRICA_ISO3:
            return True
        
        # Check against all mappings
        if pop_lower in AFRICA_MAPPINGS or cc_lower in AFRICA_MAPPINGS:
            return True
        
        # Check partial matches for any African country
        for country_variant in AFRICA_MAPPINGS.keys():
            if country_variant in pop_lower or country_variant in cc_lower:
                return True
        
        # Check if any ISO3 code appears in the text
        for iso in AFRICA_ISO3:
            if iso in pop.upper() or iso in cc.upper():
                return True
        
        return False
    
    return df[df.apply(row_matches, axis=1)].copy()

def deduplicate_by_notice_id(conn):
    """Remove duplicates keeping the most recent based on PostedDate"""
    cur = conn.cursor()
    
    # Find duplicates and keep newest
    cur.execute("""
        DELETE FROM opportunities 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM opportunities 
            GROUP BY "NoticeID"
            HAVING COUNT(*) = 1
            
            UNION
            
            SELECT id FROM (
                SELECT id, "NoticeID", "PostedDate",
                       ROW_NUMBER() OVER (PARTITION BY "NoticeID" ORDER BY "PostedDate" DESC, id DESC) as rn
                FROM opportunities
                WHERE "NoticeID" IN (
                    SELECT "NoticeID" 
                    FROM opportunities 
                    GROUP BY "NoticeID" 
                    HAVING COUNT(*) > 1
                )
            ) WHERE rn = 1
        )
    """)
    
    deleted = cur.rowcount
    conn.commit()
    print(f"Removed {deleted} duplicate entries", flush=True)
    return deleted

def insert_new_rows_chunk(cur, df_chunk) -> tuple[int, int]:
    # Standardize country codes to display format
    if "PopCountry" in df_chunk.columns:
        df_chunk["PopCountry"] = df_chunk["PopCountry"].apply(standardize_country_code)
    if "CountryCode" in df_chunk.columns:
        df_chunk["CountryCode"] = df_chunk["CountryCode"].apply(standardize_country_code)
    
    # Preserve original Link column as-is
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

def iter_csv_chunks(path: Path):
    enc_trials = [
        ("utf-8", "replace"),
        ("utf-8-sig", "strict"),
        ("cp1252", "replace"),
        ("latin-1", "replace"),
        ("iso-8859-1", "replace"),
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
            if i % 10 == 0:
                print(f"      chunk {i}: matched={len(matched)} inserted={inserted} dupes={dupes}", flush=True)
        conn.commit()
    finally:
        conn.close()
    print(f"[{label}] matched={total_matched}, inserted={total_inserted}, duplicate_skipped={total_dupes}", flush=True)

def backfill_archives(start_year=1998, end_year=None):
    if end_year is None:
        today = datetime.date.today()
        fy = today.year if today.month < 10 else today.year + 1
        end_year = fy

    print(f"Backfilling archives from FY{start_year} to FY{end_year} …", flush=True)
    
    # Check ALL years from 1998 to current (SAM.gov has data from ~2002)
    available_years = list(range(1998, end_year + 1))

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        for year in available_years:
            filename = ARCHIVE_FILENAME_TEMPLATE.format(YEAR=year)
            print(f"\nChecking FY{year} …", flush=True)
            found = False
            for base in ARCHIVE_BASES:
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
            if not found:
                print(f"FY{year} archive not found; skipping.", flush=True)

    # After all ingestion, deduplicate
    print("\nPerforming global deduplication…", flush=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        deduplicate_by_notice_id(conn)
        cur = conn.cursor()
        cur.execute('PRAGMA optimize;')
        cur.execute('VACUUM;')
        conn.commit()
    finally:
        conn.close()
    
    print("Archive backfill complete.", flush=True)

def ingest_current_full():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / "ContractOpportunitiesFullCSV.csv"
        url = os.environ.get("SAM_CSV_URL") or PRIMARY_CSV_URL
        try:
            print(f"Downloading current full CSV from: {url}", flush=True)
            r = robust_get(url, stream=True, timeout=300, max_retries=3)
        except Exception as e:
            print(f"Primary failed ({e}); trying fallback…", flush=True)
            r = robust_get(FALLBACK_CSV_URL, stream=True, timeout=300, max_retries=3)
        with open(tmp, "wb") as f:
            shutil.copyfileobj(r.raw, f)
        ingest_file_into_db(tmp, "CURRENT")

def main():
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ensure_db()

    # Backfill ALL historical years starting from 1998
    backfill_archives(start_year=1998, end_year=None)
    
    # Ingest current
    ingest_current_full()
    
    # Final deduplication
    print("\nFinal deduplication pass…", flush=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        deduplicate_by_notice_id(conn)
    finally:
        conn.close()
    
    print("Historical backfill done.", flush=True)

if __name__ == "__main__":
    main()