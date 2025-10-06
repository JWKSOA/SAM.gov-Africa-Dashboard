#!/usr/bin/env python3
"""
sam_utils.py - Complete SAM.gov data handler with ALL SQL syntax errors fixed
Handles all 54 African countries with proper ISO3 codes from AFRINIC region
Fixed: Proper quoting for all column names with special characters
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
import numpy as np
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
# CONFIGURATION WITH EXACT SAM.GOV COLUMN NAMES FROM DOCUMENTATION
# ============================================================================

@dataclass
class Config:
    """Configuration with exact SAM.gov column names from Contract Opportunities Data Extract Documentation"""
    
    # Database location
    db_path: Path = field(default_factory=lambda: Path("data") / "opportunities.db")
    data_dir: Path = field(default_factory=lambda: Path("data"))
    cache_dir: Path = field(default_factory=lambda: Path("data") / ".cache")
    
    # Processing
    chunk_size: int = 10000  # Process in manageable chunks
    max_retries: int = 3
    timeout_seconds: int = 300
    
    # SAM.gov URLs
    current_csv_url: str = (
        "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
        "Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public"
    )
    
    archive_base_url: str = (
        "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
        "Contract%20Opportunities/Archived%20Data/"
    )
    
    # Alternative S3 URLs (fallback)
    s3_current_url: str = (
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/datagov/"
        "ContractOpportunitiesFullCSV.csv"
    )
    
    s3_archive_base: str = (
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/Archived%20Data/"
    )
    
    # EXACT column names from SAM.gov documentation
    sam_columns: Dict[str, str] = field(default_factory=lambda: {
        "NoticeId": "The ID of the notice",
        "Title": "The title of the opportunity",
        "Sol#": "The number of the solicitation",
        "Department/Ind.Agency": "The department (L1)",
        "CGAC": "Common Governmentwide Accounting Classification",
        "Sub-Tier": "The sub-tier (L2)",
        "FPDS Code": "Federal Procurement Data System code",
        "Office": "The office (L3)",
        "AAC Code": "Activity Address Code",
        "PostedDate": "Date posted (YYYY-MM-DD) (HH-MM-SS)",
        "Type": "The opportunity's current type",
        "BaseType": "The opportunity's original type",
        "ArchiveType": "Archive type",
        "ArchiveDate": "Date archived",
        "SetASideCode": "Set aside code",
        "SetASide": "Description of the set aside",
        "ResponseDeadLine": "Deadline date to respond",
        "NaicsCode": "NAICS code",
        "ClassificationCode": "Classification code",
        "PopStreetAddress": "Place of performance street address",
        "PopCity": "Place of performance city",
        "PopState": "Place of performance state",
        "PopZip": "Place of performance zip",
        "PopCountry": "Place of performance country",
        "Active": "If Active = Yes, then opportunity is active",
        "AwardNumber": "The award number",
        "AwardDate": "Date the opportunity was awarded",
        "Award$": "Monetary amount of the award",
        "Awardee": "Name and location of the awardee",
        "PrimaryContactTitle": "Title of the primary contact",
        "PrimaryContactFullName": "Primary contact's full name",
        "PrimaryContactEmail": "Primary contact's email",
        "PrimaryContactPhone": "Primary contact's phone number",
        "PrimaryContactFax": "Primary contact's fax number",
        "SecondaryContactTitle": "Title of the secondary contact",
        "SecondaryContactFullName": "Secondary contact's full name",
        "SecondaryContactEmail": "Secondary contact's email",
        "SecondaryContactPhone": "Secondary contact's phone number",
        "SecondaryContactFax": "Secondary contact's fax number",
        "OrganizationType": "Type of organization",
        "State": "Office address state",
        "City": "Office address city",
        "ZipCode": "Office address zip code",
        "CountryCode": "Office address country code",
        "AdditionalInfoLink": "Any additional info link",
        "Link": "The direct UI link to the opportunity",
        "Description": "Description of the opportunity"
    })
    
    def __post_init__(self):
        """Create necessary directories"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

# ============================================================================
# AFRICAN COUNTRIES DATA - Using CountryManager as the main class name
# ============================================================================

class CountryManager:
    """Manages identification of all 54 African countries using AFRINIC region codes"""
    
    # All 54 African countries with their ISO3 codes from AFRINIC
    # Based on https://www.nro.net/list-of-country-codes-in-the-afrinic-region/
    AFRICAN_COUNTRIES = {
        # North Africa
        "ALGERIA": "DZA",
        "EGYPT": "EGY", 
        "LIBYA": "LBY",
        "MOROCCO": "MAR",
        "SUDAN": "SDN",
        "TUNISIA": "TUN",
        
        # West Africa
        "BENIN": "BEN",
        "BURKINA FASO": "BFA",
        "CAPE VERDE": "CPV",
        "CÔTE D'IVOIRE": "CIV",
        "GAMBIA": "GMB",
        "GHANA": "GHA",
        "GUINEA": "GIN",
        "GUINEA-BISSAU": "GNB",
        "LIBERIA": "LBR",
        "MALI": "MLI",
        "MAURITANIA": "MRT",
        "NIGER": "NER",
        "NIGERIA": "NGA",
        "SENEGAL": "SEN",
        "SIERRA LEONE": "SLE",
        "TOGO": "TGO",
        
        # Central Africa
        "ANGOLA": "AGO",
        "CAMEROON": "CMR",
        "CENTRAL AFRICAN REPUBLIC": "CAF",
        "CHAD": "TCD",
        "CONGO": "COG",  # Republic of Congo
        "DEMOCRATIC REPUBLIC OF THE CONGO": "COD",  # DRC
        "EQUATORIAL GUINEA": "GNQ",
        "GABON": "GAB",
        "SÃO TOMÉ AND PRÍNCIPE": "STP",
        
        # East Africa
        "BURUNDI": "BDI",
        "COMOROS": "COM",
        "DJIBOUTI": "DJI",
        "ERITREA": "ERI",
        "ETHIOPIA": "ETH",
        "KENYA": "KEN",
        "MADAGASCAR": "MDG",
        "MALAWI": "MWI",
        "MAURITIUS": "MUS",
        "MOZAMBIQUE": "MOZ",
        "RWANDA": "RWA",
        "SEYCHELLES": "SYC",
        "SOMALIA": "SOM",
        "SOUTH SUDAN": "SSD",
        "TANZANIA": "TZA",
        "UGANDA": "UGA",
        "ZAMBIA": "ZMB",
        "ZIMBABWE": "ZWE",
        
        # Southern Africa
        "BOTSWANA": "BWA",
        "ESWATINI": "SWZ",  # Formerly Swaziland
        "LESOTHO": "LSO",
        "NAMIBIA": "NAM",
        "SOUTH AFRICA": "ZAF"
    }
    
    # Alternative names and spellings (names only, not codes)
    ALTERNATIVE_NAMES = {
        "CABO VERDE": "CPV",
        "CAPE VERDE ISLANDS": "CPV",
        "IVORY COAST": "CIV",
        "COTE D'IVOIRE": "CIV",
        "COTE DIVOIRE": "CIV",
        "DRC": "COD",
        "DR CONGO": "COD",
        "DEMOCRATIC REP OF CONGO": "COD",
        "DEMOCRATIC REPUBLIC OF CONGO": "COD",
        "CONGO, DEMOCRATIC REPUBLIC": "COD",
        "CONGO-KINSHASA": "COD",
        "CONGO KINSHASA": "COD",
        "CONGO-BRAZZAVILLE": "COG",
        "CONGO BRAZZAVILLE": "COG",
        "REPUBLIC OF THE CONGO": "COG",
        "CONGO, REPUBLIC OF": "COG",
        "SAO TOME AND PRINCIPE": "STP",
        "SAO TOME & PRINCIPE": "STP",
        "SAO TOME": "STP",
        "SWAZILAND": "SWZ",
        "KINGDOM OF ESWATINI": "SWZ",
        "THE GAMBIA": "GMB",
        "GAMBIA, THE": "GMB",
        "GUINEE": "GIN",
        "GUINEA BISSAU": "GNB",
        "GUINEE-BISSAU": "GNB",
        "GUINEE BISSAU": "GNB",
        "TANZANIE": "TZA",
        "UNITED REPUBLIC OF TANZANIA": "TZA",
        "REPUBLIQUE CENTRAFRICAINE": "CAF",
        "CAR": "CAF",  # Central African Republic
        "CENTRAL AFRICAN REP": "CAF",
        "RSA": "ZAF",  # Republic of South Africa
        "REPUBLIC OF SOUTH AFRICA": "ZAF",
        "SOUTH SUDAN, REPUBLIC OF": "SSD",
        "REPUBLIC OF SOUTH SUDAN": "SSD",
    }
    
    def __init__(self):
        # Create set of all ISO3 codes for quick lookup
        self.iso3_codes = set(self.AFRICAN_COUNTRIES.values())
        
        # Create reverse mapping
        self.iso_to_country = {v: k for k, v in self.AFRICAN_COUNTRIES.items()}
        
        # Combined lookup dictionary
        self.all_lookups = {}
        
        # Add main countries
        for country, iso in self.AFRICAN_COUNTRIES.items():
            self.all_lookups[country.upper()] = iso
            # DON'T add ISO codes as keys here - we want to check them explicitly
            
        # Add alternatives
        for alt, iso in self.ALTERNATIVE_NAMES.items():
            if iso:  # Only add if it maps to an African country
                self.all_lookups[alt.upper()] = iso
    
    def is_african_country(self, value: str) -> bool:
        """
        Check if value represents an African country
        Handles country names, ISO codes, and various formats
        IMPORTANT: Returns False for non-African ISO codes like ITA, SAU, CAN
        """
        if not value or pd.isna(value) or value == '':
            return False
            
        # Clean the value
        value_clean = str(value).upper().strip()
        
        # Remove common non-country values
        if value_clean in ['NONE', 'NULL', 'N/A', 'UNKNOWN', '']:
            return False
        
        # Check if it's a raw ISO3 code (like "ITA", "SAU", "CAN")
        # Only accept if it's an AFRICAN ISO3 code
        if len(value_clean) == 3 and value_clean.isalpha():
            # This is likely an ISO3 code
            return value_clean in self.iso3_codes
            
        # Check if it contains an African ISO code in parentheses (e.g., "KENYA (KEN)")
        if '(' in value_clean and ')' in value_clean:
            iso_match = re.search(r'\(([A-Z]{3})\)', value_clean)
            if iso_match:
                iso_code = iso_match.group(1)
                return iso_code in self.iso3_codes
        
        # Direct lookup match for country names
        if value_clean in self.all_lookups:
            return True
            
        # Check for partial country name matches (but be careful with short codes)
        if len(value_clean) > 3:  # Don't do partial matches on short strings
            for country_name in self.AFRICAN_COUNTRIES.keys():
                if country_name in value_clean or value_clean in country_name:
                    return True
                    
            # Check alternative names
            for alt_name in self.ALTERNATIVE_NAMES.keys():
                if alt_name in value_clean or value_clean in alt_name:
                    return True
                
        return False
    
    def standardize_country(self, value: str) -> str:
        """
        Standardize country to 'COUNTRY NAME (ISO3)' format
        This ensures consistent storage and display
        """
        if not value or pd.isna(value):
            return value
            
        value_clean = str(value).upper().strip()
        
        # If already in correct format
        if '(' in value and ')' in value:
            # Extract ISO code to verify
            iso_match = re.search(r'\(([A-Z]{3})\)', value)
            if iso_match and iso_match.group(1) in self.iso3_codes:
                return value  # Already correct
                
        # Look up the ISO code
        iso_code = None
        
        # Direct ISO code (3-letter code)
        if len(value_clean) == 3 and value_clean.isalpha() and value_clean in self.iso3_codes:
            iso_code = value_clean
            
        # Lookup table for country names
        elif value_clean in self.all_lookups:
            iso_code = self.all_lookups[value_clean]
            
        # Search for ISO in string
        elif '(' in value_clean and ')' in value_clean:
            iso_match = re.search(r'\(([A-Z]{3})\)', value_clean)
            if iso_match and iso_match.group(1) in self.iso3_codes:
                iso_code = iso_match.group(1)
                    
        # If we found an ISO code, format properly
        if iso_code and iso_code in self.iso_to_country:
            country_name = self.iso_to_country[iso_code]
            return f"{country_name} ({iso_code})"
            
        # Return original if not African (this shouldn't happen after filtering)
        return value
    
    def get_all_search_terms(self) -> List[str]:
        """Get all possible search terms for African countries"""
        terms = []
        
        # Add all ISO codes
        terms.extend(self.iso3_codes)
        
        # Add all country names
        terms.extend(self.AFRICAN_COUNTRIES.keys())
        
        # Add alternative names
        terms.extend([k for k, v in self.ALTERNATIVE_NAMES.items() if v])
        
        return terms

# Create an alias for backward compatibility
AfricanCountryManager = CountryManager

# ============================================================================
# DATABASE MANAGEMENT WITH PROPER DEDUPLICATION AND FIXED SQL QUOTING
# ============================================================================

class DatabaseManager:
    """Database operations with proper SAM.gov schema and deduplication"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_path = config.db_path
        
    @contextmanager
    def get_connection(self):
        """Get database connection with optimizations"""
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=10000")
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
    
    def needs_quoting(self, column_name: str) -> bool:
        """
        Check if a column name needs quoting in SQL
        Returns True if the column contains any special characters
        """
        # Check for any special characters that require quoting
        special_chars = ['/', '#', '$', '-', ' ', '.', '(', ')', '[', ']', '*', '&', '%', '@', '!']
        return any(char in column_name for char in special_chars)
    
    def quote_column(self, column_name: str) -> str:
        """
        Properly quote a column name for SQL if needed
        """
        if self.needs_quoting(column_name):
            return f'"{column_name}"'
        return column_name
    
    def initialize_database(self):
        """Create database with exact SAM.gov schema"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Drop existing table to start fresh
            cur.execute("DROP TABLE IF EXISTS opportunities")
            
            # Create table with exact SAM.gov column names
            # Note: Using quotes for columns with special characters
            cur.execute("""
                CREATE TABLE opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    NoticeId TEXT UNIQUE NOT NULL,
                    Title TEXT,
                    "Sol#" TEXT,
                    "Department/Ind.Agency" TEXT,
                    CGAC TEXT,
                    "Sub-Tier" TEXT,
                    "FPDS Code" TEXT,
                    Office TEXT,
                    "AAC Code" TEXT,
                    PostedDate TEXT,
                    PostedDate_normalized DATE,
                    Type TEXT,
                    BaseType TEXT,
                    ArchiveType TEXT,
                    ArchiveDate TEXT,
                    SetASideCode TEXT,
                    SetASide TEXT,
                    ResponseDeadLine TEXT,
                    NaicsCode TEXT,
                    ClassificationCode TEXT,
                    PopStreetAddress TEXT,
                    PopCity TEXT,
                    PopState TEXT,
                    PopZip TEXT,
                    PopCountry TEXT,
                    Active TEXT,
                    AwardNumber TEXT,
                    AwardDate TEXT,
                    "Award$" TEXT,
                    Awardee TEXT,
                    PrimaryContactTitle TEXT,
                    PrimaryContactFullName TEXT,
                    PrimaryContactEmail TEXT,
                    PrimaryContactPhone TEXT,
                    PrimaryContactFax TEXT,
                    SecondaryContactTitle TEXT,
                    SecondaryContactFullName TEXT,
                    SecondaryContactEmail TEXT,
                    SecondaryContactPhone TEXT,
                    SecondaryContactFax TEXT,
                    OrganizationType TEXT,
                    State TEXT,
                    City TEXT,
                    ZipCode TEXT,
                    CountryCode TEXT,
                    AdditionalInfoLink TEXT,
                    Link TEXT,
                    Description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX idx_notice_id ON opportunities(NoticeId)",
                "CREATE INDEX idx_posted_date ON opportunities(PostedDate)",
                "CREATE INDEX idx_posted_norm ON opportunities(PostedDate_normalized)",
                "CREATE INDEX idx_pop_country ON opportunities(PopCountry)",
                "CREATE INDEX idx_active ON opportunities(Active)",
                "CREATE INDEX idx_type ON opportunities(Type)",
                'CREATE INDEX idx_dept ON opportunities("Department/Ind.Agency")',
                "CREATE INDEX idx_country_date ON opportunities(PopCountry, PostedDate_normalized DESC)"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
            
            conn.commit()
            logger.info("Database initialized with SAM.gov schema")
    
    def normalize_posted_date(self, date_str: str) -> Optional[str]:
        """
        Normalize PostedDate from SAM.gov format to YYYY-MM-DD
        SAM.gov format: YYYY-MM-DD HH-MM-SS
        """
        if not date_str or pd.isna(date_str) or date_str == '':
            return None
            
        date_str = str(date_str).strip()
        
        # Already normalized
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
            
        # SAM.gov format with time
        if ' ' in date_str:
            # Split date and time
            date_part = date_str.split(' ')[0]
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
                return date_part
                
        # Try pandas parsing as fallback
        try:
            parsed = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
            
        return None
    
    def insert_or_update_batch(self, df: pd.DataFrame, source: str = "unknown") -> Tuple[int, int, int]:
        """
        Insert or update batch with deduplication
        Returns: (inserted, updated, skipped)
        """
        if df.empty:
            return 0, 0, 0
            
        inserted = 0
        updated = 0
        skipped = 0
        
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            for _, row in df.iterrows():
                # Get NoticeId
                notice_id = str(row.get('NoticeId', '')).strip()
                if not notice_id or notice_id in ['nan', 'None', '']:
                    skipped += 1
                    continue
                
                # Check if exists
                cur.execute("SELECT PostedDate FROM opportunities WHERE NoticeId = ?", (notice_id,))
                existing = cur.fetchone()
                
                if existing:
                    # Compare dates to keep most recent
                    existing_date = existing[0]
                    new_date = row.get('PostedDate', '')
                    
                    # Normalize dates for comparison
                    existing_norm = self.normalize_posted_date(existing_date)
                    new_norm = self.normalize_posted_date(new_date)
                    
                    # Update if new is more recent
                    if new_norm and existing_norm and new_norm > existing_norm:
                        # Build UPDATE statement with properly quoted columns
                        update_cols = []
                        update_vals = []
                        
                        for col in self.config.sam_columns.keys():
                            if col in row.index and col != 'NoticeId':
                                # Use the quote_column method for all columns
                                quoted_col = self.quote_column(col)
                                update_cols.append(f'{quoted_col} = ?')
                                update_vals.append(row[col] if pd.notna(row[col]) else None)
                        
                        # Add normalized date
                        update_cols.append('PostedDate_normalized = ?')
                        update_vals.append(new_norm)
                        
                        # Add updated timestamp
                        update_cols.append('updated_at = CURRENT_TIMESTAMP')
                        
                        # Execute update
                        update_vals.append(notice_id)
                        sql = f"UPDATE opportunities SET {', '.join(update_cols)} WHERE NoticeId = ?"
                        
                        try:
                            cur.execute(sql, update_vals)
                            updated += 1
                        except Exception as e:
                            logger.error(f"Update error for {notice_id}: {e}")
                            logger.debug(f"Failed SQL: {sql}")
                            skipped += 1
                    else:
                        skipped += 1
                else:
                    # Insert new record with properly quoted columns
                    columns = ['NoticeId']
                    values = [notice_id]
                    
                    # Add PostedDate_normalized
                    posted_date = row.get('PostedDate', '')
                    normalized_date = self.normalize_posted_date(posted_date)
                    columns.append('PostedDate_normalized')
                    values.append(normalized_date)
                    
                    # Add all other columns with proper quoting
                    for col in self.config.sam_columns.keys():
                        if col != 'NoticeId' and col in row.index:
                            # Use the quote_column method for proper quoting
                            quoted_col = self.quote_column(col)
                            columns.append(quoted_col)
                            values.append(row[col] if pd.notna(row[col]) else None)
                    
                    # Build and execute INSERT
                    placeholders = ','.join(['?' for _ in values])
                    columns_str = ','.join(columns)
                    
                    sql = f"INSERT OR IGNORE INTO opportunities ({columns_str}) VALUES ({placeholders})"
                    
                    try:
                        cur.execute(sql, values)
                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as e:
                        logger.error(f"Insert error for {notice_id}: {e}")
                        logger.debug(f"Failed SQL: {sql}")
                        logger.debug(f"Column count: {len(columns)}, Value count: {len(values)}")
                        skipped += 1
            
            conn.commit()
            
        logger.info(f"Batch from {source}: {inserted} inserted, {updated} updated, {skipped} skipped")
        return inserted, updated, skipped
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        stats = {
            'total_records': 0,
            'active_records': 0,
            'recent_7_days': 0,
            'recent_30_days': 0,
            'recent_year': 0,
            'recent_5_years': 0,
            'by_country': {},
            'by_year': {},
            'size_mb': 0
        }
        
        if not self.db_path.exists():
            return stats
            
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # Total records
                cur.execute("SELECT COUNT(*) FROM opportunities")
                stats['total_records'] = cur.fetchone()[0]
                
                # Active records
                cur.execute("SELECT COUNT(*) FROM opportunities WHERE Active = 'Yes'")
                stats['active_records'] = cur.fetchone()[0]
                
                # Recent records
                today = datetime.now().date().isoformat()
                
                for days, key in [(7, 'recent_7_days'), (30, 'recent_30_days'), 
                                  (365, 'recent_year'), (1825, 'recent_5_years')]:
                    cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
                    cur.execute("""
                        SELECT COUNT(*) FROM opportunities 
                        WHERE PostedDate_normalized >= ? AND PostedDate_normalized <= ?
                    """, (cutoff, today))
                    stats[key] = cur.fetchone()[0]
                
                # By country (top 20)
                cur.execute("""
                    SELECT PopCountry, COUNT(*) 
                    FROM opportunities 
                    WHERE PopCountry IS NOT NULL 
                    GROUP BY PopCountry 
                    ORDER BY COUNT(*) DESC
                    LIMIT 20
                """)
                stats['by_country'] = dict(cur.fetchall())
                
                # By year
                cur.execute("""
                    SELECT substr(PostedDate_normalized, 1, 4) as year, COUNT(*) 
                    FROM opportunities 
                    WHERE PostedDate_normalized IS NOT NULL 
                    GROUP BY year 
                    ORDER BY year DESC
                """)
                stats['by_year'] = dict(cur.fetchall())
                
                # Database size
                stats['size_mb'] = self.db_path.stat().st_size / (1024 * 1024)
                
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            
        return stats

# ============================================================================
# DATA PROCESSING
# ============================================================================

class DataProcessor:
    """Process SAM.gov data with African country filtering"""
    
    def __init__(self, config: Config, country_manager: CountryManager):
        self.config = config
        self.country_manager = country_manager
        
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Process a chunk of SAM.gov data
        Filter for African countries only
        """
        if chunk.empty:
            return chunk
            
        # Ensure PopCountry column exists
        if 'PopCountry' not in chunk.columns:
            logger.warning("No PopCountry column found in chunk")
            return pd.DataFrame()
            
        # Filter for African countries
        african_mask = chunk['PopCountry'].apply(self.country_manager.is_african_country)
        african_data = chunk[african_mask].copy()
        
        if not african_data.empty:
            # Standardize country names
            african_data['PopCountry'] = african_data['PopCountry'].apply(
                self.country_manager.standardize_country
            )
            
            logger.info(f"Found {len(african_data)} African opportunities in chunk")
            
        return african_data

# ============================================================================
# HTTP CLIENT
# ============================================================================

class HTTPClient:
    """HTTP client for downloading SAM.gov files"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create session with retry logic"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        return session
    
    def download_file(self, url: str, dest_path: Path, show_progress: bool = True) -> bool:
        """Download file with progress indication"""
        try:
            logger.info(f"Downloading from {url}")
            
            response = self.session.get(url, stream=True, timeout=self.config.timeout_seconds)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if show_progress and total_size > 0:
                            if downloaded % (10 * 1024 * 1024) == 0:  # Every 10MB
                                progress = (downloaded / total_size) * 100
                                logger.info(f"Progress: {progress:.1f}%")
                            
            logger.info(f"Download complete: {dest_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            
            # Try fallback URL if available
            if "sam.gov" in url and "s3.amazonaws.com" not in url:
                # Convert to S3 URL
                s3_url = url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback URL: {s3_url}")
                return self.download_file(s3_url, dest_path, show_progress)
                
            return False

# ============================================================================
# CSV READER
# ============================================================================

class CSVReader:
    """Read SAM.gov CSV files with proper encoding handling"""
    
    def __init__(self, config: Config):
        self.config = config
        
    def read_csv_chunks(self, filepath: Path, chunksize: int = None):
        """Read CSV in chunks with encoding detection"""
        if chunksize is None:
            chunksize = self.config.chunk_size
            
        # Try different encodings
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                logger.info(f"Reading CSV with encoding: {encoding}")
                
                # Read with all columns as strings to avoid type issues
                for chunk in pd.read_csv(
                    filepath,
                    encoding=encoding,
                    dtype=str,
                    chunksize=chunksize,
                    on_bad_lines='skip',
                    low_memory=False
                ):
                    yield chunk
                    
                return  # Success
                
            except UnicodeDecodeError:
                logger.warning(f"Failed with encoding {encoding}, trying next...")
                continue
            except Exception as e:
                logger.error(f"Error reading CSV with {encoding}: {e}")
                continue
                
        # If all encodings fail
        raise ValueError(f"Could not read CSV file with any encoding: {filepath}")

# ============================================================================
# MAIN SYSTEM
# ============================================================================

class SAMDataSystem:
    """Main system coordinating all SAM.gov data operations"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.country_manager = CountryManager()  # Using CountryManager
        self.db_manager = DatabaseManager(self.config)
        self.data_processor = DataProcessor(self.config, self.country_manager)
        self.http_client = HTTPClient(self.config)
        self.csv_reader = CSVReader(self.config)
        
        # Initialize database if it doesn't exist
        if not self.config.db_path.exists():
            logger.info("Database doesn't exist, initializing...")
            self.db_manager.initialize_database()
        
    def get_archive_years(self) -> List[int]:
        """Get list of all archive years to process"""
        # Based on your requirements: FY1998 through FY2025 (ignoring FY2030)
        years = list(range(1998, 2026))  # 1998 to 2025
        # Note: FY2030 file exists but we're ignoring it per requirements
        
        return years
    
    def get_archive_url(self, year: int) -> str:
        """Get archive URL for a specific fiscal year"""
        filename = f"FY{year}_archived_opportunities.csv"
        return f"{self.config.archive_base_url}{filename}?privacy=Public"
    
    def get_current_url(self) -> str:
        """Get URL for current opportunities CSV"""
        return self.config.current_csv_url

# Singleton instance
_system = None

def get_system() -> SAMDataSystem:
    """Get or create system instance"""
    global _system
    if _system is None:
        _system = SAMDataSystem()
    return _system