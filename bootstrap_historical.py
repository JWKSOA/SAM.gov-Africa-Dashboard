#!/usr/bin/env python3
"""
bootstrap_historical.py

FIXED VERSION: Fetches ALL historical years and standardizes country codes
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

# EXPANDED African countries mapping with all variations
AFRICA_MAPPINGS = {
    # Algeria
    "algeria": "DZA", "dza": "DZA", "algérie": "DZA", "people's democratic republic of algeria": "DZA",
    # Angola
    "angola": "AGO", "ago": "AGO", "republic of angola": "AGO",
    # Benin
    "benin": "BEN", "ben": "BEN", "bénin": "BEN", "republic of benin": "BEN",
    # Botswana
    "botswana": "BWA", "bwa": "BWA", "republic of botswana": "BWA",
    # Burkina Faso
    "burkina faso": "BFA", "bfa": "BFA", "burkina": "BFA",
    # Burundi
    "burundi": "BDI", "bdi": "BDI", "republic of burundi": "BDI",
    # Cabo Verde / Cape Verde
    "cabo verde": "CPV", "cpv": "CPV", "cape verde": "CPV", "republic of cabo verde": "CPV",
    # Cameroon
    "cameroon": "CMR", "cmr": "CMR", "cameroun": "CMR", "republic of cameroon": "CMR",
    # Central African Republic
    "central african republic": "CAF", "caf": "CAF", "car": "CAF", "centrafrique": "CAF",
    # Chad
    "chad": "TCD", "tcd": "TCD", "tchad": "TCD", "republic of chad": "TCD",
    # Comoros
    "comoros": "COM", "com": "COM", "comores": "COM", "union of the comoros": "COM",
    # Congo (Brazzaville)
    "congo": "COG", "cog": "COG", "congo-brazzaville": "COG", "republic of the congo": "COG", "congo brazzaville": "COG",
    # Congo (Kinshasa) / DRC
    "democratic republic of the congo": "COD", "cod": "COD", "drc": "COD", "congo-kinshasa": "COD", 
    "dr congo": "COD", "congo kinshasa": "COD", "zaire": "COD",
    # Djibouti
    "djibouti": "DJI", "dji": "DJI", "republic of djibouti": "DJI",
    # Egypt
    "egypt": "EGY", "egy": "EGY", "arab republic of egypt": "EGY", "misr": "EGY",
    # Equatorial Guinea
    "equatorial guinea": "GNQ", "gnq": "GNQ", "guinea ecuatorial": "GNQ",
    # Eritrea
    "eritrea": "ERI", "eri": "ERI", "state of eritrea": "ERI",
    # Eswatini / Swaziland
    "eswatini": "SWZ", "swz": "SWZ", "swaziland": "SWZ", "kingdom of eswatini": "SWZ",
    # Ethiopia
    "ethiopia": "ETH", "eth": "ETH", "federal democratic republic of ethiopia": "ETH",
    # Gabon
    "gabon": "GAB", "gab": "GAB", "gabonese republic": "GAB",
    # Gambia
    "gambia": "GMB", "gmb": "GMB", "the gambia": "GMB", "republic of the gambia": "GMB",
    # Ghana
    "ghana": "GHA", "gha": "GHA", "republic of ghana": "GHA",
    # Guinea
    "guinea": "GIN", "gin": "GIN", "guinée": "GIN", "republic of guinea": "GIN",
    # Guinea-Bissau
    "guinea-bissau": "GNB", "gnb": "GNB", "guinea bissau": "GNB", "guinée-bissau": "GNB",
    # Ivory Coast / Côte d'Ivoire
    "ivory coast": "CIV", "civ": "CIV", "côte d'ivoire": "CIV", "cote d'ivoire": "CIV", "cote divoire": "CIV",
    # Kenya
    "kenya": "KEN", "ken": "KEN", "republic of kenya": "KEN",
    # Lesotho
    "lesotho": "LSO", "lso": "LSO", "kingdom of lesotho": "LSO",
    # Liberia
    "liberia": "LBR", "lbr": "LBR", "republic of liberia": "LBR",
    # Libya
    "libya": "LBY", "lby": "LBY", "state of libya": "LBY",
    # Madagascar
    "madagascar": "MDG", "mdg": "MDG", "republic of madagascar": "MDG",
    # Malawi
    "malawi": "MWI", "mwi": "MWI", "republic of malawi": "MWI",
    # Mali
    "mali": "MLI", "mli": "MLI", "republic of mali": "MLI",
    # Mauritania
    "mauritania": "MRT", "mrt": "MRT", "mauritanie": "MRT", "islamic republic of mauritania": "MRT",
    # Mauritius
    "mauritius": "MUS", "mus": "MUS", "republic of mauritius": "MUS",
    # Morocco
    "morocco": "MAR", "mar": "MAR", "maroc": "MAR", "kingdom of morocco": "MAR",
    # Mozambique
    "mozambique": "MOZ", "moz": "MOZ", "moçambique": "MOZ", "republic of mozambique": "MOZ",
    # Namibia
    "namibia": "NAM", "nam": "NAM", "republic of namibia": "NAM",
    # Niger
    "niger": "NER", "ner": "NER", "republic of niger": "NER",
    # Nigeria
    "nigeria": "NGA", "nga": "NGA", "federal republic of nigeria": "NGA",
    # Rwanda
    "rwanda": "RWA", "rwa": "RWA", "republic of rwanda": "RWA",
    # São Tomé and Príncipe
    "são tomé and príncipe": "STP", "stp": "STP", "sao tome and principe": "STP", "são tomé": "STP",
    # Senegal
    "senegal": "SEN", "sen": "SEN", "sénégal": "SEN", "republic of senegal": "SEN",
    # Seychelles
    "seychelles": "SYC", "syc": "SYC", "republic of seychelles": "SYC",
    # Sierra Leone
    "sierra leone": "SLE", "sle": "SLE", "republic of sierra leone": "SLE",
    # Somalia
    "somalia": "SOM", "som": "SOM", "federal republic of somalia": "SOM",
    # South Africa
    "south africa": "ZAF", "zaf": "ZAF", "republic of south africa": "ZAF", "rsa": "ZAF",
    # South Sudan
    "south sudan": "SSD", "ssd": "SSD", "republic of south sudan": "SSD",
    # Sudan
    "sudan": "SDN", "sdn": "SDN", "republic of the sudan": "SDN",
    # Tanzania
    "tanzania": "TZA", "tza": "TZA", "united republic of tanzania": "TZA", "tanganyika": "TZA",
    # Togo
    "togo": "TGO", "tgo": "TGO", "togolese republic": "TGO",
    # Tunisia
    "tunisia": "TUN", "tun": "TUN", "tunisie": "TUN", "republic of tunisia": "TUN",
    # Uganda
    "uganda": "UGA", "uga": "UGA", "republic of uganda": "UGA",
    # Zambia
    "zambia": "ZMB", "zmb": "ZMB", "republic of zambia": "ZMB",
    # Zimbabwe
    "zimbabwe": "ZWE", "zwe": "ZWE", "republic of zimbabwe": "ZWE"
}

# Set of all valid ISO codes for quick validation
AFRICA_ISO3 = set(['DZA', 'AGO', 'BEN', 'BWA', 'BFA', 'BDI', 'CPV', 'CMR', 'CAF', 'TCD', 
                    'COM', 'COG', 'COD', 'DJI', 'EGY', 'GNQ', 'ERI', 'SWZ', 'ETH', 'GAB', 
                    'GMB', 'GHA', 'GIN', 'GNB', 'CIV', 'KEN', 'LSO', 'LBR', 'LBY', 'MDG', 
                    'MWI', 'MLI', 'MRT', 'MUS', 'MAR', 'MOZ', 'NAM', 'NER', 'NGA', 'RWA', 
                    'STP', 'SEN', 'SYC', 'SLE', 'SOM', 'ZAF', 'SSD', 'SDN', 'TZA', 'TGO', 
                    'TUN', 'UGA', 'ZMB', 'ZWE'])

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

CHUNK_SIZE = 50_000
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

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

def standardize_country_code(value: str) -> str:
    """Convert any country name or code to standard ISO 3-letter code."""
    if not value:
        return value
    
    # Clean the input
    cleaned = str(value).strip().lower()
    
    # Check if it's already a valid ISO code
    if cleaned.upper() in AFRICA_ISO3:
        return cleaned.upper()
    
    # Try to match against our mappings
    if cleaned in AFRICA_MAPPINGS:
        return AFRICA_MAPPINGS[cleaned]
    
    # Return original if no match (will be filtered out later)
    return value

def exists_fast(url: str) -> bool:
    """Quick HEAD probe: skip years instantly if not present."""
    try:
        r = requests.head(url, timeout=HEAD_TIMEOUT)
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

def fix_sam_gov_links(df):
    """Fix SAM.gov links to ensure they're properly formatted."""
    if "Link" in df.columns:
        def fix_link(link):
            if pd.isna(link) or not link:
                return ""
            link = str(link).strip()
            if link.startswith("http"):
                return link
            elif link.startswith("/opp/"):
                return f"https://sam.gov{link}"
            elif link.startswith("opp/"):
                return f"https://sam.gov/{link}"
            else:
                # Try to extract notice ID and construct URL
                parts = link.split("/")
                for part in parts:
                    if part and len(part) > 10:  # Likely a notice ID
                        return f"https://sam.gov/opp/{part}/view"
                return link
        df["Link"] = df["Link"].apply(fix_link)
    return df

def insert_new_rows_chunk(cur, df_chunk) -> tuple[int, int]:
    """Insert a chunk row-by-row into 'opportunities'. Returns: (inserted_count, duplicate_skipped_count)"""
    # Standardize country codes before insertion
    if "PopCountry" in df_chunk.columns:
        df_chunk["PopCountry"] = df_chunk["PopCountry"].apply(standardize_country_code)
    if "CountryCode" in df_chunk.columns:
        df_chunk["CountryCode"] = df_chunk["CountryCode"].apply(standardize_country_code)
    
    # Fix SAM.gov links
    df_chunk = fix_sam_gov_links(df_chunk)
    
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
        pop = str(row.get("PopCountry", "") or "").strip()
        cc = str(row.get("CountryCode", "") or "").strip()
        
        # Check if already ISO code
        if cc.upper() in AFRICA_ISO3:
            return True
        if pop.upper() in AFRICA_ISO3:
            return True
            
        # Check if it's a country name that maps to Africa
        pop_lower = pop.lower()
        cc_lower = cc.lower()
        
        if pop_lower in AFRICA_MAPPINGS:
            return True
        if cc_lower in AFRICA_MAPPINGS:
            return True
            
        # Check for partial matches
        for country_name in AFRICA_MAPPINGS.keys():
            if country_name in pop_lower or country_name in cc_lower:
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
    
    # ALL available years according to SAM.gov
    available_years = [1970, 1980] + list(range(1998, 2026)) + [2030]

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        for year in available_years:
            if year < start_year or year > end_year:
                continue
                
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