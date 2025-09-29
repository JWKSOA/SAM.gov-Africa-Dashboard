#!/usr/bin/env python3
"""
download_and_update.py - FIXED VERSION
Daily job with proper country formatting and link preservation
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

# Complete list of 54 African countries
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

# Mapping variations
AFRICA_MAPPINGS = {
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
    "congo": "COG", "cog": "COG", "congo-brazzaville": "COG", "republic of congo": "COG",
    "democratic republic of the congo": "COD", "cod": "COD", "drc": "COD", "dr congo": "COD", "congo-kinshasa": "COD",
    "djibouti": "DJI", "dji": "DJI",
    "egypt": "EGY", "egy": "EGY",
    "equatorial guinea": "GNQ", "gnq": "GNQ",
    "eritrea": "ERI", "eri": "ERI",
    "eswatini": "SWZ", "swz": "SWZ", "swaziland": "SWZ",
    "ethiopia": "ETH", "eth": "ETH",
    "gabon": "GAB", "gab": "GAB",
    "gambia": "GMB", "gmb": "GMB", "the gambia": "GMB",
    "ghana": "GHA", "gha": "GHA",
    "guinea": "GIN", "gin": "GIN", "guinée": "GIN",
    "guinea-bissau": "GNB", "gnb": "GNB", "guinea bissau": "GNB",
    "ivory coast": "CIV", "civ": "CIV", "côte d'ivoire": "CIV", "cote d'ivoire": "CIV",
    "kenya": "KEN", "ken": "KEN",
    "lesotho": "LSO", "lso": "LSO",
    "liberia": "LBR", "lbr": "LBR",
    "libya": "LBY", "lby": "LBY",
    "madagascar": "MDG", "mdg": "MDG",
    "malawi": "MWI", "mwi": "MWI",
    "mali": "MLI", "mli": "MLI",
    "mauritania": "MRT", "mrt": "MRT",
    "mauritius": "MUS", "mus": "MUS",
    "morocco": "MAR", "mar": "MAR", "maroc": "MAR",
    "mozambique": "MOZ", "moz": "MOZ",
    "namibia": "NAM", "nam": "NAM",
    "niger": "NER", "ner": "NER",
    "nigeria": "NGA", "nga": "NGA",
    "rwanda": "RWA", "rwa": "RWA",
    "são tomé and príncipe": "STP", "stp": "STP", "sao tome and principe": "STP",
    "senegal": "SEN", "sen": "SEN", "sénégal": "SEN",
    "seychelles": "SYC", "syc": "SYC",
    "sierra leone": "SLE", "sle": "SLE",
    "somalia": "SOM", "som": "SOM",
    "south africa": "ZAF", "zaf": "ZAF", "rsa": "ZAF",
    "south sudan": "SSD", "ssd": "SSD",
    "sudan": "SDN", "sdn": "SDN",
    "tanzania": "TZA", "tza": "TZA",
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

SAM_DATA_DIR = os.environ.get("SAM_DATA_DIR")
LOCAL_DATA_DIR = Path(SAM_DATA_DIR).expanduser().resolve() if SAM_DATA_DIR else (Path.home() / "sam_africa_data")
DB_PATH = LOCAL_DATA_DIR / "opportunities.db"

CHUNK_SIZE = 50_000
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
CI_MODE = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

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

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("PRAGMA temp_store=MEMORY;")
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
    
    # Standardize country codes to display format
    if "PopCountry" in df_chunk.columns:
        df_chunk["PopCountry"] = df_chunk["PopCountry"].apply(standardize_country_code)
    if "CountryCode" in df_chunk.columns:
        df_chunk["CountryCode"] = df_chunk["CountryCode"].apply(standardize_country_code)
    
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

def filter_african_rows(df):
    for c in ("PopCountry", "CountryCode"):
        if c not in df.columns:
            df[c] = ""
    
    def row_matches(row):
        pop = str(row.get("PopCountry", "") or "").strip()
        cc = str(row.get("CountryCode", "") or "").strip()
        
        pop_lower = pop.lower()
        cc_lower = cc.lower()
        
        # Check ISO codes
        if cc.upper() in AFRICA_ISO3 or pop.upper() in AFRICA_ISO3:
            return True
        
        # Check against all mappings
        if pop_lower in AFRICA_MAPPINGS or cc_lower in AFRICA_MAPPINGS:
            return True
        
        # Check partial matches
        for country_variant in AFRICA_MAPPINGS.keys():
            if country_variant in pop_lower or country_variant in cc_lower:
                return True
        
        # Check ISO codes in text
        for iso in AFRICA_ISO3:
            if iso in pop.upper() or iso in cc.upper():
                return True
        
        return False
    
    return df[df.apply(row_matches, axis=1)].copy()

def ensure_notice_id_column(df):
    if "NoticeID" in df.columns:
        return df
    normalized_map = {norm(c): c for c in df.columns}
    for key, actual in normalized_map.items():
        if key in ID_CANDIDATES_NORM or "noticeid" in key or "documentid" in key or "opportunityid" in key:
            df["NoticeID"] = df[actual].astype(str)
            return df
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

def main():
    ensure_db()

    csv_url = resolve_csv_url()
    today = datetime.date.today()

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / "ContractOpportunitiesFullCSV.csv"
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
            dest = LOCAL_DATA_DIR / f"ContractOpportunitiesFullCSV_{today.strftime('%m_%d_%Y')}.csv"
            shutil.copyfile(tmp, dest)
            print(f"Saved a copy locally at {dest} (NOT committed to git).")

    print("Done.")

if __name__ == "__main__":
    main()