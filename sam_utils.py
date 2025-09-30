#!/usr/bin/env python3
"""
sam_utils.py - Shared utilities for SAM.gov data processing
Eliminates code duplication and provides centralized functionality
"""

import os
import re
import sqlite3
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass, field
import json

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Centralized configuration for SAM.gov data processing"""
    
    # Paths
    db_path: Path = field(default_factory=lambda: Path.home() / "sam_africa_data" / "opportunities.db")
    data_dir: Path = field(default_factory=lambda: Path.home() / "sam_africa_data")
    cache_dir: Path = field(default_factory=lambda: Path.home() / "sam_africa_data" / ".cache")
    
    # Processing
    chunk_size: int = 50_000
    max_retries: int = 3
    timeout_seconds: int = 300
    
    # URLs
    primary_csv_url: str = (
        "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
        "Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public"
    )
    fallback_csv_url: str = (
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv"
    )
    archive_bases: List[str] = field(default_factory=lambda: [
        "https://s3.amazonaws.com/falextracts/Contract%20Opportunities/Archived%20Data",
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/Archived%20Data",
    ])
    
    # Database
    keep_columns: List[str] = field(default_factory=lambda: [
        "Title", "Department/Ind.Agency", "Sub-Tier", "Office", "PostedDate", 
        "Type", "PopCountry", "AwardNumber", "AwardDate", "Award$", "Awardee",
        "PrimaryContactTitle", "PrimaryContactFullName", "PrimaryContactEmail",
        "PrimaryContactPhone", "OrganizationType", "CountryCode", "Link", "Description"
    ])
    
    def __post_init__(self):
        """Create necessary directories"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

# ============================================================================
# AFRICAN COUNTRIES DATA
# ============================================================================

class CountryManager:
    """Centralized country data management"""
    
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
    
    # Comprehensive mappings including variations
    MAPPINGS = {
        # Algeria
        "algeria": "DZA", "dza": "DZA", "algérie": "DZA", "dzair": "DZA",
        # Angola
        "angola": "AGO", "ago": "AGO",
        # Benin
        "benin": "BEN", "ben": "BEN", "bénin": "BEN", "dahomey": "BEN",
        # Botswana
        "botswana": "BWA", "bwa": "BWA", "bechuanaland": "BWA",
        # Burkina Faso
        "burkina faso": "BFA", "bfa": "BFA", "burkina": "BFA", "upper volta": "BFA",
        # Burundi
        "burundi": "BDI", "bdi": "BDI",
        # Cabo Verde
        "cabo verde": "CPV", "cpv": "CPV", "cape verde": "CPV", "cap vert": "CPV",
        # Cameroon
        "cameroon": "CMR", "cmr": "CMR", "cameroun": "CMR", "kamerun": "CMR",
        # Central African Republic
        "central african republic": "CAF", "caf": "CAF", "car": "CAF", "centrafrique": "CAF",
        # Chad
        "chad": "TCD", "tcd": "TCD", "tchad": "TCD",
        # Comoros
        "comoros": "COM", "com": "COM", "comores": "COM", "juzur al-qamar": "COM",
        # Congo (Brazzaville)
        "congo": "COG", "cog": "COG", "congo-brazzaville": "COG", "republic of congo": "COG",
        "congo brazzaville": "COG", "republic of the congo": "COG",
        # Congo (Kinshasa)
        "democratic republic of the congo": "COD", "cod": "COD", "drc": "COD", 
        "dr congo": "COD", "congo-kinshasa": "COD", "zaire": "COD", "congo kinshasa": "COD",
        # Djibouti
        "djibouti": "DJI", "dji": "DJI", "jabuuti": "DJI", "gabuuti": "DJI",
        # Egypt
        "egypt": "EGY", "egy": "EGY", "misr": "EGY", "masr": "EGY",
        # Equatorial Guinea
        "equatorial guinea": "GNQ", "gnq": "GNQ", "guinea ecuatorial": "GNQ",
        # Eritrea
        "eritrea": "ERI", "eri": "ERI", "ertra": "ERI",
        # Eswatini
        "eswatini": "SWZ", "swz": "SWZ", "swaziland": "SWZ", "ngwane": "SWZ",
        # Ethiopia
        "ethiopia": "ETH", "eth": "ETH", "abyssinia": "ETH", "ityop'ia": "ETH",
        # Gabon
        "gabon": "GAB", "gab": "GAB",
        # Gambia
        "gambia": "GMB", "gmb": "GMB", "the gambia": "GMB",
        # Ghana
        "ghana": "GHA", "gha": "GHA", "gold coast": "GHA",
        # Guinea
        "guinea": "GIN", "gin": "GIN", "guinée": "GIN", "guinea conakry": "GIN",
        # Guinea-Bissau
        "guinea-bissau": "GNB", "gnb": "GNB", "guinea bissau": "GNB", "guiné-bissau": "GNB",
        # Ivory Coast
        "ivory coast": "CIV", "civ": "CIV", "côte d'ivoire": "CIV", "cote d'ivoire": "CIV",
        "cote divoire": "CIV", "costa do marfim": "CIV",
        # Kenya
        "kenya": "KEN", "ken": "KEN",
        # Lesotho
        "lesotho": "LSO", "lso": "LSO", "basutoland": "LSO",
        # Liberia
        "liberia": "LBR", "lbr": "LBR",
        # Libya
        "libya": "LBY", "lby": "LBY", "libyan arab jamahiriya": "LBY",
        # Madagascar
        "madagascar": "MDG", "mdg": "MDG", "malagasy": "MDG",
        # Malawi
        "malawi": "MWI", "mwi": "MWI", "nyasaland": "MWI",
        # Mali
        "mali": "MLI", "mli": "MLI", "french sudan": "MLI",
        # Mauritania
        "mauritania": "MRT", "mrt": "MRT", "mauritanie": "MRT", "muritaniya": "MRT",
        # Mauritius
        "mauritius": "MUS", "mus": "MUS", "maurice": "MUS", "moris": "MUS",
        # Morocco
        "morocco": "MAR", "mar": "MAR", "maroc": "MAR", "maghrib": "MAR",
        # Mozambique
        "mozambique": "MOZ", "moz": "MOZ", "moçambique": "MOZ", "mocambique": "MOZ",
        # Namibia
        "namibia": "NAM", "nam": "NAM", "south west africa": "NAM",
        # Niger
        "niger": "NER", "ner": "NER",
        # Nigeria
        "nigeria": "NGA", "nga": "NGA",
        # Rwanda
        "rwanda": "RWA", "rwa": "RWA", "ruanda": "RWA",
        # São Tomé and Príncipe
        "são tomé and príncipe": "STP", "stp": "STP", "sao tome and principe": "STP",
        "são tomé": "STP", "sao tome": "STP",
        # Senegal
        "senegal": "SEN", "sen": "SEN", "sénégal": "SEN", "senegaal": "SEN",
        # Seychelles
        "seychelles": "SYC", "syc": "SYC", "sesel": "SYC",
        # Sierra Leone
        "sierra leone": "SLE", "sle": "SLE",
        # Somalia
        "somalia": "SOM", "som": "SOM", "soomaaliya": "SOM",
        # South Africa
        "south africa": "ZAF", "zaf": "ZAF", "rsa": "ZAF", "suid-afrika": "ZAF",
        # South Sudan
        "south sudan": "SSD", "ssd": "SSD", "southern sudan": "SSD",
        # Sudan
        "sudan": "SDN", "sdn": "SDN",
        # Tanzania
        "tanzania": "TZA", "tza": "TZA", "tanganyika": "TZA", "zanzibar": "TZA",
        # Togo
        "togo": "TGO", "tgo": "TGO", "togoland": "TGO",
        # Tunisia
        "tunisia": "TUN", "tun": "TUN", "tunisie": "TUN", "tunis": "TUN",
        # Uganda
        "uganda": "UGA", "uga": "UGA",
        # Zambia
        "zambia": "ZMB", "zmb": "ZMB", "northern rhodesia": "ZMB",
        # Zimbabwe
        "zimbabwe": "ZWE", "zwe": "ZWE", "rhodesia": "ZWE", "southern rhodesia": "ZWE"
    }
    
    def __init__(self):
        self.iso3_codes = set(self.AFRICAN_COUNTRIES.values())
        
    def format_country_display(self, iso_code: str) -> str:
        """Convert ISO3 to 'COUNTRY NAME (ISO3)' format"""
        for country, code in self.AFRICAN_COUNTRIES.items():
            if code == iso_code:
                return f"{country} ({code})"
        return iso_code
    
    def standardize_country_code(self, value: str) -> str:
        """Convert any country name or code to 'COUNTRY NAME (ISO3)' format"""
        if not value:
            return value
        
        cleaned = str(value).strip().lower()
        
        # Check if already ISO code
        if cleaned.upper() in self.iso3_codes:
            return self.format_country_display(cleaned.upper())
        
        # Try to match against mappings
        if cleaned in self.MAPPINGS:
            iso_code = self.MAPPINGS[cleaned]
            return self.format_country_display(iso_code)
        
        # Check partial matches
        for mapping_key, iso_code in self.MAPPINGS.items():
            if mapping_key in cleaned or cleaned in mapping_key:
                return self.format_country_display(iso_code)
        
        return value
    
    def is_african_country(self, value: str) -> bool:
        """Check if value represents an African country"""
        if not value:
            return False
            
        value_lower = str(value).lower().strip()
        
        # Check ISO codes
        if value.upper() in self.iso3_codes:
            return True
            
        # Check against mappings
        if value_lower in self.MAPPINGS:
            return True
            
        # Check partial matches
        for mapping_key in self.MAPPINGS.keys():
            if mapping_key in value_lower or value_lower in mapping_key:
                return True
                
        # Check for ISO codes within the text (e.g., "KENYA (KEN)")
        for iso in self.iso3_codes:
            if iso in value.upper():
                return True
                
        return False

# ============================================================================
# DATABASE MANAGEMENT
# ============================================================================

class DatabaseManager:
    """Manages all database operations with proper error handling"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_path = config.db_path
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA mmap_size=30000000000")
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def initialize_database(self):
        """Create tables and indexes if they don't exist"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Create main table
            columns_def = ",\n    ".join([f'"{col}" TEXT' for col in self.config.keep_columns])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    "NoticeID" TEXT UNIQUE NOT NULL,
                    {columns_def},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            indexes = [
                ('idx_notice_id', 'NoticeID'),
                ('idx_posted_date', 'PostedDate'),
                ('idx_pop_country', 'PopCountry'),
                ('idx_country_code', 'CountryCode'),
                ('idx_department', '"Department/Ind.Agency"'),
                ('idx_created_at', 'created_at'),
            ]
            
            for idx_name, column in indexes:
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name} 
                    ON opportunities({column})
                """)
            
            # Create composite indexes for common queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_country_date 
                ON opportunities(PopCountry, PostedDate DESC)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dept_date 
                ON opportunities("Department/Ind.Agency", PostedDate DESC)
            """)
            
            logger.info("Database initialized successfully")
    
    def get_last_update_date(self) -> Optional[datetime]:
        """Get the most recent PostedDate from database"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            result = cur.execute("""
                SELECT MAX(PostedDate) 
                FROM opportunities 
                WHERE PostedDate IS NOT NULL
            """).fetchone()
            
            if result and result[0]:
                try:
                    return pd.to_datetime(result[0])
                except:
                    return None
            return None
    
    def insert_batch(self, df: pd.DataFrame, country_manager: CountryManager) -> Tuple[int, int]:
        """Insert batch of records with deduplication"""
        if df.empty:
            return 0, 0
            
        # Standardize country codes
        if "PopCountry" in df.columns:
            df["PopCountry"] = df["PopCountry"].apply(country_manager.standardize_country_code)
        if "CountryCode" in df.columns:
            df["CountryCode"] = df["CountryCode"].apply(country_manager.standardize_country_code)
        
        # Ensure all required columns exist
        for col in self.config.keep_columns:
            if col not in df.columns:
                df[col] = None
                
        inserted = 0
        duplicates = 0
        
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Prepare insert statement
            columns = ['NoticeID'] + self.config.keep_columns
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f'"{col}"' for col in columns])
            
            sql = f"""
                INSERT OR IGNORE INTO opportunities ({columns_str}, updated_at)
                VALUES ({placeholders}, CURRENT_TIMESTAMP)
            """
            
            # Insert records
            for _, row in df.iterrows():
                notice_id = str(row.get('NoticeID', '')).strip()
                if not notice_id or notice_id.lower() == 'nan':
                    continue
                    
                values = [notice_id] + [str(row.get(col, '') or '') for col in self.config.keep_columns]
                
                cur.execute(sql, values)
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    duplicates += 1
                    
        logger.info(f"Inserted {inserted} records, skipped {duplicates} duplicates")
        return inserted, duplicates
    
    def optimize_database(self):
        """Optimize database performance"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Update statistics
            cur.execute("ANALYZE")
            
            # Optimize query planner
            cur.execute("PRAGMA optimize")
            
            # Check if VACUUM needed (if DB > 100MB and > 20% fragmentation)
            cur.execute("PRAGMA page_count")
            page_count = cur.fetchone()[0]
            cur.execute("PRAGMA freelist_count")
            freelist_count = cur.fetchone()[0]
            
            fragmentation = (freelist_count / page_count) * 100 if page_count > 0 else 0
            db_size_mb = (page_count * 4096) / (1024 * 1024)  # Assuming 4KB pages
            
            if db_size_mb > 100 and fragmentation > 20:
                logger.info(f"Running VACUUM (size: {db_size_mb:.1f}MB, fragmentation: {fragmentation:.1f}%)")
                conn.execute("VACUUM")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            stats = {}
            
            # Total records
            cur.execute("SELECT COUNT(*) FROM opportunities")
            stats['total_records'] = cur.fetchone()[0]
            
            # Records by country
            cur.execute("""
                SELECT PopCountry, COUNT(*) 
                FROM opportunities 
                WHERE PopCountry IS NOT NULL 
                GROUP BY PopCountry 
                ORDER BY COUNT(*) DESC
            """)
            stats['by_country'] = dict(cur.fetchall())
            
            # Recent records (last 30 days)
            cur.execute("""
                SELECT COUNT(*) 
                FROM opportunities 
                WHERE date(PostedDate) >= date('now', '-30 days')
            """)
            stats['recent_records'] = cur.fetchone()[0]
            
            # Database size
            cur.execute("PRAGMA page_count")
            page_count = cur.fetchone()[0]
            stats['size_mb'] = (page_count * 4096) / (1024 * 1024)
            
            return stats

# ============================================================================
# DATA PROCESSING
# ============================================================================

class DataProcessor:
    """Handles all data processing operations"""
    
    def __init__(self, config: Config, country_manager: CountryManager):
        self.config = config
        self.country_manager = country_manager
        
    def normalize_string(self, s: str) -> str:
        """Normalize string for comparison"""
        return re.sub(r'[^a-z0-9]+', '', (s or '').lower())
    
    def ensure_notice_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure NoticeID column exists"""
        if 'NoticeID' in df.columns:
            return df
            
        # Try to find ID column
        id_candidates = {
            'noticeid', 'noticeidnumber', 'documentid', 'solicitationnumber',
            'solicitationid', 'opportunityid', 'referencenumber', 'refid'
        }
        
        normalized_cols = {self.normalize_string(col): col for col in df.columns}
        
        for candidate in id_candidates:
            if candidate in normalized_cols:
                df['NoticeID'] = df[normalized_cols[candidate]].astype(str)
                return df
        
        # Generate hash-based ID if no ID column found
        def generate_id(row):
            parts = [
                str(row.get('Title', '')),
                str(row.get('PostedDate', '')),
                str(row.get('Type', '')),
                str(row.get('Link', '')),
                str(row.get('Department/Ind.Agency', '')),
                str(row.get('PopCountry', '')),
            ]
            content = '|'.join(parts)
            return hashlib.sha256(content.encode('utf-8', errors='ignore')).hexdigest()[:24]
        
        df['NoticeID'] = df.apply(generate_id, axis=1)
        return df
    
    def filter_african_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter dataframe for African countries only"""
        if df.empty:
            return df
            
        # Ensure country columns exist
        for col in ['PopCountry', 'CountryCode']:
            if col not in df.columns:
                df[col] = ''
        
        # Filter rows
        mask = df.apply(self._is_african_row, axis=1)
        return df[mask].copy()
    
    def _is_african_row(self, row) -> bool:
        """Check if a row is related to an African country"""
        pop_country = str(row.get('PopCountry', '') or '')
        country_code = str(row.get('CountryCode', '') or '')
        
        return (self.country_manager.is_african_country(pop_country) or 
                self.country_manager.is_african_country(country_code))
    
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data"""
        # Clean column names
        chunk.columns = [col.strip() for col in chunk.columns]
        
        # Filter for African countries
        african_rows = self.filter_african_rows(chunk)
        
        if african_rows.empty:
            return african_rows
        
        # Ensure NoticeID exists
        african_rows = self.ensure_notice_id(african_rows)
        
        # Standardize country codes
        if 'PopCountry' in african_rows.columns:
            african_rows['PopCountry'] = african_rows['PopCountry'].apply(
                self.country_manager.standardize_country_code
            )
        if 'CountryCode' in african_rows.columns:
            african_rows['CountryCode'] = african_rows['CountryCode'].apply(
                self.country_manager.standardize_country_code
            )
        
        return african_rows

# ============================================================================
# HTTP UTILITIES
# ============================================================================

class HTTPClient:
    """Robust HTTP client with retry logic"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def download_file(self, url: str, dest_path: Path, stream: bool = True) -> bool:
        """Download file with progress reporting"""
        try:
            logger.info(f"Downloading from {url}")
            
            response = self.session.get(
                url, 
                stream=stream, 
                timeout=self.config.timeout_seconds
            )
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(dest_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0 and downloaded % (10 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Download progress: {progress:.1f}%")
            
            logger.info(f"Downloaded to {dest_path}")
            return True
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def check_url_exists(self, url: str) -> bool:
        """Check if URL exists using HEAD request"""
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code in (200, 206, 302, 403)
        except:
            return False

# ============================================================================
# CSV READER
# ============================================================================

class CSVReader:
    """Handles CSV reading with multiple encoding fallbacks"""
    
    ENCODINGS = [
        ('utf-8', 'replace'),
        ('utf-8-sig', 'strict'),
        ('cp1252', 'replace'),
        ('latin-1', 'replace'),
        ('iso-8859-1', 'replace')
    ]
    
    def __init__(self, config: Config):
        self.config = config
    
    def read_csv_chunks(self, filepath: Path):
        """Read CSV in chunks with encoding detection"""
        last_error = None
        
        for encoding, errors in self.ENCODINGS:
            try:
                logger.info(f"Reading CSV with encoding={encoding}")
                
                for chunk in pd.read_csv(
                    filepath,
                    dtype=str,
                    encoding=encoding,
                    encoding_errors=errors,
                    on_bad_lines='skip',
                    chunksize=self.config.chunk_size,
                    low_memory=False
                ):
                    yield chunk
                    
                return  # Success
                
            except Exception as e:
                last_error = e
                logger.warning(f"Failed with {encoding}: {e}")
                continue
        
        # All encodings failed
        raise last_error

# ============================================================================
# CACHE MANAGER
# ============================================================================

class CacheManager:
    """Simple file-based cache for processed data"""
    
    def __init__(self, config: Config):
        self.cache_dir = config.cache_dir
        self.cache_dir.mkdir(exist_ok=True)
    
    def get_cache_path(self, key: str) -> Path:
        """Get cache file path for a key"""
        safe_key = re.sub(r'[^a-zA-Z0-9_-]', '_', key)
        return self.cache_dir / f"{safe_key}.json"
    
    def get(self, key: str, max_age: timedelta = timedelta(hours=1)) -> Optional[Any]:
        """Get cached value if not expired"""
        cache_path = self.get_cache_path(key)
        
        if not cache_path.exists():
            return None
        
        # Check age
        age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
        if age > max_age:
            return None
        
        try:
            with open(cache_path, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def set(self, key: str, value: Any):
        """Set cached value"""
        cache_path = self.get_cache_path(key)
        
        try:
            with open(cache_path, 'w') as f:
                json.dump(value, f, default=str)
        except Exception as e:
            logger.warning(f"Cache write failed: {e}")
    
    def clear(self):
        """Clear all cache files"""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()

# ============================================================================
# MAIN FACADE
# ============================================================================

class SAMDataSystem:
    """Main facade for all SAM.gov data operations"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.country_manager = CountryManager()
        self.db_manager = DatabaseManager(self.config)
        self.data_processor = DataProcessor(self.config, self.country_manager)
        self.http_client = HTTPClient(self.config)
        self.csv_reader = CSVReader(self.config)
        self.cache_manager = CacheManager(self.config)
        
        # Initialize database
        self.db_manager.initialize_database()
    
    def get_current_csv_url(self) -> str:
        """Determine which CSV URL to use"""
        if self.http_client.check_url_exists(self.config.primary_csv_url):
            return self.config.primary_csv_url
        else:
            logger.warning("Primary URL not available, using fallback")
            return self.config.fallback_csv_url
    
    def get_archive_url(self, year: int) -> Optional[str]:
        """Get archive URL for a specific year"""
        filename = f"FY{year}_archived_opportunities.csv"
        
        for base in self.config.archive_bases:
            url = f"{base}/{filename}"
            if self.http_client.check_url_exists(url):
                return url
        
        return None

# Create singleton instance
_system_instance = None

def get_system() -> SAMDataSystem:
    """Get singleton instance of SAM data system"""
    global _system_instance
    if _system_instance is None:
        _system_instance = SAMDataSystem()
    return _system_instance