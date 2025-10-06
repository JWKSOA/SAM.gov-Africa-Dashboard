#!/usr/bin/env python3
"""
download_and_update.py - Fixed version for daily updates
Handles both incremental updates and proper date storage
"""

import os
import sys
import logging
import tempfile
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple

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
        
    def ensure_database_schema(self):
        """Ensure database has all required columns"""
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Get current columns
            cur.execute("PRAGMA table_info(opportunities)")
            existing_columns = {row[1] for row in cur.fetchall()}
            
            # Required columns from config
            required_columns = self.system.config.keep_columns
            
            # Add missing columns
            for col in required_columns:
                if col not in existing_columns and col != 'NoticeID':  # NoticeID is special
                    try:
                        logger.info(f"Adding missing column: {col}")
                        cur.execute(f'ALTER TABLE opportunities ADD COLUMN "{col}" TEXT')
                        conn.commit()
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            logger.warning(f"Could not add column {col}: {e}")
            
            # Ensure PostedDate_normalized column exists for reliable date filtering
            if 'PostedDate_normalized' not in existing_columns:
                logger.info("Adding PostedDate_normalized column for date filtering")
                cur.execute('''
                    ALTER TABLE opportunities 
                    ADD COLUMN PostedDate_normalized DATE
                ''')
                conn.commit()
                
                # Normalize existing dates
                logger.info("Normalizing existing PostedDate values...")
                cur.execute('''
                    UPDATE opportunities 
                    SET PostedDate_normalized = date(
                        CASE 
                            WHEN PostedDate LIKE '%/%/%' THEN 
                                substr(PostedDate, 7, 4) || '-' || 
                                substr(PostedDate, 1, 2) || '-' || 
                                substr(PostedDate, 4, 2)
                            WHEN PostedDate LIKE '%-%-%' THEN 
                                substr(PostedDate, 1, 10)
                            ELSE PostedDate
                        END
                    )
                    WHERE PostedDate_normalized IS NULL
                    AND PostedDate IS NOT NULL
                ''')
                conn.commit()
                
                # Create index on normalized date
                cur.execute('''
                    CREATE INDEX IF NOT EXISTS idx_posted_date_normalized 
                    ON opportunities(PostedDate_normalized)
                ''')
                conn.commit()
    
    def get_cutoff_date(self) -> datetime:
        """Determine cutoff date for processing"""
        # Always look back at least lookback_days to catch any updates
        cutoff = datetime.now() - timedelta(days=self.lookback_days)
        logger.info(f"Processing records posted after {cutoff.date()}")
        return cutoff
    
    def normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize date to YYYY-MM-DD format"""
        if not date_str or date_str == 'nan':
            return None
            
        try:
            # Try parsing different formats
            for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%Y/%m/%d', '%d/%m/%Y']:
                try:
                    dt = datetime.strptime(date_str[:10], fmt)
                    return dt.strftime('%Y-%m-%d')
                except:
                    continue
                    
            # If nothing works, try pandas
            import pandas as pd
            dt = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(dt):
                return dt.strftime('%Y-%m-%d')
        except:
            pass
            
        return None
    
    def process_incremental(self) -> Tuple[int, int]:
        """Process only new/recent records"""
        url = self.system.get_current_csv_url()
        cutoff_date = self.get_cutoff_date()
        
        total_inserted = 0
        total_duplicates = 0
        total_processed = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            logger.info("Downloading current opportunities CSV...")
            if not self.system.http_client.download_file(url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0
            
            # Process in chunks
            logger.info("Processing CSV chunks...")
            
            try:
                import pandas as pd
                
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Process chunk (filter for African countries)
                    processed = self.system.data_processor.process_chunk(chunk)
                    
                    if processed.empty:
                        continue
                    
                    # Normalize dates before inserting
                    if 'PostedDate' in processed.columns:
                        processed['PostedDate_normalized'] = processed['PostedDate'].apply(self.normalize_date)
                    
                    total_processed += len(processed)
                    
                    # Insert to database with normalized dates
                    with self.system.db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        
                        for _, row in processed.iterrows():
                            notice_id = str(row.get('NoticeID', '')).strip()
                            if not notice_id or notice_id.lower() == 'nan':
                                continue
                            
                            # Prepare column values
                            columns = ['NoticeID', 'PostedDate_normalized'] + self.system.config.keep_columns
                            values = [
                                notice_id,
                                row.get('PostedDate_normalized')
                            ] + [
                                str(row.get(col, '') or '') for col in self.system.config.keep_columns
                            ]
                            
                            # Build SQL with proper column names
                            columns_str = ','.join([f'"{col}"' for col in columns])
                            placeholders = ','.join(['?' for _ in columns])
                            
                            sql = f"""
                                INSERT OR IGNORE INTO opportunities ({columns_str}, updated_at)
                                VALUES ({placeholders}, CURRENT_TIMESTAMP)
                            """
                            
                            cur.execute(sql, values)
                            if cur.rowcount > 0:
                                total_inserted += 1
                            else:
                                total_duplicates += 1
                    
                    # Log progress
                    if chunk_num % 5 == 0:
                        logger.info(
                            f"Progress: Chunk {chunk_num}, "
                            f"{total_processed} African records found, "
                            f"{total_inserted} new"
                        )
                
                logger.info(
                    f"Update complete: {total_processed} African records processed, "
                    f"{total_inserted} new inserted, {total_duplicates} duplicates"
                )
                
            except Exception as e:
                logger.error(f"Error processing CSV: {e}")
                raise
                
        return total_inserted, total_duplicates
    
    def update_date_index(self):
        """Update the date index for faster queries"""
        logger.info("Updating date indexes...")
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Re-normalize any new dates
            cur.execute('''
                UPDATE opportunities 
                SET PostedDate_normalized = date(
                    CASE 
                        WHEN PostedDate LIKE '%/%/%' THEN 
                            substr(PostedDate, 7, 4) || '-' || 
                            substr(PostedDate, 1, 2) || '-' || 
                            substr(PostedDate, 4, 2)
                        WHEN PostedDate LIKE '%-%-%' THEN 
                            substr(PostedDate, 1, 10)
                        ELSE PostedDate
                    END
                )
                WHERE PostedDate_normalized IS NULL
                AND PostedDate IS NOT NULL
            ''')
            
            # Update statistics
            cur.execute("ANALYZE")
            conn.commit()
    
    def run(self, optimize: bool = True):
        """Run the daily update process"""
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("SAM.gov Daily Update - Fixed Version")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Ensure database schema is correct
        self.ensure_database_schema()
        
        # Get initial statistics
        try:
            initial_stats = self.system.db_manager.get_statistics()
            logger.info(f"Initial records: {initial_stats['total_records']:,}")
        except:
            initial_stats = {'total_records': 0}
        
        # Process incremental updates
        inserted, duplicates = self.process_incremental()
        
        # Update date indexes
        self.update_date_index()
        
        # Optimize if needed
        if optimize and inserted > 100:
            logger.info("Optimizing database...")
            self.system.db_manager.optimize_database()
        
        # Get final statistics
        try:
            final_stats = self.system.db_manager.get_statistics()
            new_records = final_stats['total_records'] - initial_stats['total_records']
            
            logger.info("\n" + "="*60)
            logger.info("Daily Update Complete!")
            logger.info(f"Time elapsed: {datetime.now() - start_time}")
            logger.info(f"New records: {new_records:,}")
            logger.info(f"Total records: {final_stats['total_records']:,}")
            logger.info(f"Database size: {final_stats['size_mb']:.1f} MB")
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
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Skip database optimization"
    )
    
    args = parser.parse_args()
    
    # Run updater
    updater = DailyUpdater(lookback_days=args.lookback_days)
    updater.run(optimize=not args.no_optimize)

if __name__ == "__main__":
    main()