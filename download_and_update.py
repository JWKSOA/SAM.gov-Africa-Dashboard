#!/usr/bin/env python3
"""
download_and_update.py - Fixed version for daily updates
Properly handles date normalization and column mapping
"""

import os
import sys
import logging
import tempfile
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import get_system, Config, logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DailyUpdater:
    """Handles daily incremental updates with proper date handling"""
    
    def __init__(self, lookback_days: int = 7):
        """Initialize updater"""
        self.system = get_system()
        self.lookback_days = lookback_days
        self.new_by_country = {}
        
    def ensure_database_ready(self):
        """Ensure database has all required columns and normalization"""
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Get current columns
            cur.execute("PRAGMA table_info(opportunities)")
            existing_columns = {row[1] for row in cur.fetchall()}
            
            # Ensure PostedDate_normalized exists
            if 'PostedDate_normalized' not in existing_columns:
                logger.info("Adding PostedDate_normalized column...")
                cur.execute('''
                    ALTER TABLE opportunities 
                    ADD COLUMN PostedDate_normalized DATE
                ''')
                conn.commit()
                
                # Normalize existing dates
                logger.info("Normalizing existing dates...")
                self.normalize_existing_dates(cur)
                conn.commit()
            
            # Ensure indexes exist
            cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
            existing_indexes = {row[0] for row in cur.fetchall()}
            
            if 'idx_posted_date_normalized' not in existing_indexes:
                logger.info("Creating date index...")
                cur.execute('''
                    CREATE INDEX idx_posted_date_normalized 
                    ON opportunities(PostedDate_normalized)
                ''')
                conn.commit()
    
    def normalize_existing_dates(self, cur):
        """Normalize any existing dates that aren't normalized"""
        # Handle MM/DD/YYYY format (most common)
        cur.execute('''
            UPDATE opportunities 
            SET PostedDate_normalized = 
                substr(PostedDate, 7, 4) || '-' || 
                substr(PostedDate, 1, 2) || '-' || 
                substr(PostedDate, 4, 2)
            WHERE PostedDate LIKE '__/__/____'
            AND PostedDate_normalized IS NULL
        ''')
        
        # Handle YYYY-MM-DD format
        cur.execute('''
            UPDATE opportunities 
            SET PostedDate_normalized = date(substr(PostedDate, 1, 10))
            WHERE (PostedDate LIKE '____-__-__' OR PostedDate LIKE '____-__-__T%')
            AND PostedDate_normalized IS NULL
        ''')
    
    def normalize_date_value(self, date_str: str) -> Optional[str]:
        """Normalize a single date value to YYYY-MM-DD format"""
        if not date_str or date_str == 'nan' or date_str == '':
            return None
        
        try:
            # Try pandas parsing first
            parsed = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
        
        # Manual parsing for common formats
        date_str = str(date_str).strip()
        
        # MM/DD/YYYY format
        if len(date_str) >= 10 and '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                try:
                    month = int(parts[0])
                    day = int(parts[1])
                    year = int(parts[2][:4])  # Handle years with time attached
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                except:
                    pass
        
        # YYYY-MM-DD format
        if len(date_str) >= 10 and '-' in date_str[:10]:
            return date_str[:10]
        
        return None
    
    def get_cutoff_date(self) -> Tuple[datetime, str]:
        """Determine cutoff date for processing"""
        cutoff = datetime.now() - timedelta(days=self.lookback_days)
        cutoff_str = cutoff.strftime('%Y-%m-%d')
        logger.info(f"Processing records posted after {cutoff_str}")
        return cutoff, cutoff_str
    
    def process_incremental(self) -> Tuple[int, int, int]:
        """Process only new/recent records"""
        url = self.system.get_current_csv_url()
        cutoff_date, cutoff_str = self.get_cutoff_date()
        
        total_inserted = 0
        total_duplicates = 0
        total_old_skipped = 0
        total_processed = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            logger.info("Downloading current opportunities CSV...")
            if not self.system.http_client.download_file(url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0, 0
            
            # Process in chunks
            logger.info("Processing CSV chunks...")
            
            try:
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Process chunk (filter for African countries)
                    processed = self.system.data_processor.process_chunk(chunk)
                    
                    if processed.empty:
                        continue
                    
                    # Normalize dates before checking or inserting
                    if 'PostedDate' in processed.columns:
                        processed['PostedDate_normalized'] = processed['PostedDate'].apply(self.normalize_date_value)
                    
                    total_processed += len(processed)
                    
                    # Process each record
                    with self.system.db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        
                        for _, row in processed.iterrows():
                            notice_id = str(row.get('NoticeID', '')).strip()
                            if not notice_id or notice_id.lower() == 'nan':
                                continue
                            
                            # Check if record is recent enough
                            posted_date_norm = row.get('PostedDate_normalized')
                            if posted_date_norm and posted_date_norm < cutoff_str:
                                total_old_skipped += 1
                                continue
                            
                            # Check if already exists
                            cur.execute("SELECT 1 FROM opportunities WHERE NoticeID = ?", (notice_id,))
                            if cur.fetchone():
                                total_duplicates += 1
                                continue
                            
                            # Prepare values for insertion
                            columns_to_insert = ['NoticeID', 'PostedDate', 'PostedDate_normalized']
                            values_to_insert = [notice_id, row.get('PostedDate'), posted_date_norm]
                            
                            # Add other columns
                            for col in self.system.config.keep_columns:
                                if col in row:
                                    columns_to_insert.append(f'"{col}"')
                                    values_to_insert.append(str(row.get(col, '') or ''))
                            
                            # Build and execute insert
                            columns_str = ','.join(columns_to_insert)
                            placeholders = ','.join(['?' for _ in columns_to_insert])
                            
                            sql = f"""
                                INSERT OR IGNORE INTO opportunities ({columns_str}, updated_at)
                                VALUES ({placeholders}, CURRENT_TIMESTAMP)
                            """
                            
                            cur.execute(sql, values_to_insert)
                            if cur.rowcount > 0:
                                total_inserted += 1
                                # Track by country
                                country = row.get('PopCountry', 'Unknown')
                                self.new_by_country[country] = self.new_by_country.get(country, 0) + 1
                            else:
                                total_duplicates += 1
                    
                    # Log progress every 5 chunks
                    if chunk_num % 5 == 0:
                        logger.info(
                            f"Progress: Chunk {chunk_num}, "
                            f"{total_processed} African records found, "
                            f"{total_inserted} new, "
                            f"{total_duplicates} duplicates, "
                            f"{total_old_skipped} old"
                        )
                
                logger.info(
                    f"\nUpdate complete: {total_processed} African records processed, "
                    f"{total_inserted} new inserted, {total_duplicates} duplicates skipped, "
                    f"{total_old_skipped} old records skipped"
                )
                
            except Exception as e:
                logger.error(f"Error processing CSV: {e}")
                raise
                
        return total_inserted, total_duplicates, total_old_skipped
    
    def optimize_database(self):
        """Optimize database after update"""
        logger.info("Optimizing database...")
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Update statistics
            cur.execute("ANALYZE")
            
            # Ensure all dates are normalized
            cur.execute('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    substr(PostedDate, 7, 4) || '-' || 
                    substr(PostedDate, 1, 2) || '-' || 
                    substr(PostedDate, 4, 2)
                WHERE PostedDate LIKE '__/__/____'
                AND PostedDate_normalized IS NULL
            ''')
            
            conn.commit()
    
    def run(self):
        """Run the daily update process"""
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("SAM.gov Daily Update - Fixed Version")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Ensure database is ready
        self.ensure_database_ready()
        
        # Get initial statistics
        try:
            initial_stats = self.system.db_manager.get_statistics()
            logger.info(f"Initial records: {initial_stats['total_records']:,}")
            logger.info(f"Recent records (30d): {initial_stats['recent_records']:,}")
        except:
            initial_stats = {'total_records': 0}
        
        # Process incremental updates
        inserted, duplicates, old_skipped = self.process_incremental()
        
        # Optimize if we inserted records
        if inserted > 0:
            self.optimize_database()
        
        # Get final statistics
        try:
            final_stats = self.system.db_manager.get_statistics()
            
            logger.info("\n" + "="*60)
            logger.info("Daily Update Complete!")
            logger.info(f"Time elapsed: {datetime.now() - start_time}")
            logger.info(f"New records: {inserted}")
            logger.info(f"Duplicates skipped: {duplicates}")
            logger.info(f"Total records: {final_stats['total_records']:,}")
            logger.info(f"Database size: {final_stats['size_mb']:.1f} MB")
            
            if self.new_by_country:
                logger.info("\nNew records by country:")
                for country, count in sorted(self.new_by_country.items(), 
                                            key=lambda x: x[1], reverse=True)[:10]:
                    logger.info(f"  {country}: +{count}")
            
            logger.info("="*60)
        except Exception as e:
            logger.warning(f"Could not get final statistics: {e}")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daily SAM.gov data update")
    parser.add_argument(
        "--lookback-days", 
        type=int, 
        default=7,
        help="Number of days to look back (default: 7)"
    )
    
    args = parser.parse_args()
    
    # Run updater
    updater = DailyUpdater(lookback_days=args.lookback_days)
    updater.run()

if __name__ == "__main__":
    main()