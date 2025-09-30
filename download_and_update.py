#!/usr/bin/env python3
"""
download_and_update.py - Optimized Daily Update
Efficient incremental updates with change detection
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# Add parent directory to path if running standalone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import get_system, Config, logger

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DailyUpdater:
    """Handles efficient daily incremental updates"""
    
    def __init__(self, lookback_days: int = 7):
        """Initialize updater with lookback period"""
        self.system = get_system()
        self.lookback_days = lookback_days
        
    def get_cutoff_date(self) -> datetime:
        """Determine cutoff date for processing"""
        # Get last update from database
        last_update = self.system.db_manager.get_last_update_date()
        
        if last_update:
            # Add buffer to catch any delayed postings
            cutoff = last_update - timedelta(days=self.lookback_days)
        else:
            # No data in DB, get last 30 days
            cutoff = datetime.now() - timedelta(days=30)
        
        logger.info(f"Processing records posted after {cutoff.date()}")
        return cutoff
    
    def process_incremental(self) -> tuple[int, int]:
        """Process only new/recent records"""
        url = self.system.get_current_csv_url()
        cutoff_date = self.get_cutoff_date()
        
        total_inserted = 0
        total_duplicates = 0
        total_processed = 0
        total_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            logger.info("Downloading current opportunities CSV...")
            if not self.system.http_client.download_file(url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0
            
            # Process in chunks with date filtering
            logger.info("Processing CSV chunks...")
            
            try:
                # Import pandas here
                import pandas as pd
                
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Early date filtering if PostedDate exists
                    if 'PostedDate' in chunk.columns:
                        try:
                            chunk['PostedDate_parsed'] = pd.to_datetime(
                                chunk['PostedDate'], 
                                errors='coerce'
                            )
                            
                            # Skip old records
                            before_filter = len(chunk)
                            chunk = chunk[
                                (chunk['PostedDate_parsed'] >= cutoff_date) | 
                                (chunk['PostedDate_parsed'].isna())
                            ]
                            
                            skipped = before_filter - len(chunk)
                            if skipped > 0:
                                total_skipped += skipped
                                
                            if chunk.empty:
                                continue
                                
                        except Exception as e:
                            logger.warning(f"Date filtering failed: {e}")
                    
                    # Process chunk (filter for African countries)
                    processed = self.system.data_processor.process_chunk(chunk)
                    
                    if processed.empty:
                        continue
                    
                    total_processed += len(processed)
                    
                    # Insert to database
                    inserted, duplicates = self.system.db_manager.insert_batch(
                        processed, 
                        self.system.country_manager
                    )
                    
                    total_inserted += inserted
                    total_duplicates += duplicates
                    
                    # Log progress every 5 chunks
                    if chunk_num % 5 == 0:
                        logger.info(
                            f"  Progress: Chunk {chunk_num}, "
                            f"{total_processed} African records found, "
                            f"{total_inserted} new, {total_skipped} old skipped"
                        )
                
                logger.info(
                    f"\nUpdate complete: "
                    f"{total_processed} African records processed, "
                    f"{total_inserted} new inserted, "
                    f"{total_duplicates} duplicates skipped, "
                    f"{total_skipped} old records skipped"
                )
                
            except Exception as e:
                logger.error(f"Error processing CSV: {e}")
                raise
                
        return total_inserted, total_duplicates
    
    def cleanup_old_data(self, days_to_keep: int = 1825):  # 5 years default
        """Remove very old data to prevent database bloat"""
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Count records to be deleted
            cur.execute("""
                SELECT COUNT(*) FROM opportunities 
                WHERE date(PostedDate) < date(?)
            """, (cutoff.isoformat(),))
            
            count = cur.fetchone()[0]
            
            if count > 0:
                logger.info(f"Removing {count} records older than {cutoff.date()}")
                
                cur.execute("""
                    DELETE FROM opportunities 
                    WHERE date(PostedDate) < date(?)
                """, (cutoff.isoformat(),))
                
                conn.commit()
    
    def run(self, optimize: bool = True, cleanup: bool = False):
        """Run the daily update process"""
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("SAM.gov Daily Update - Optimized Version")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Get initial statistics
        initial_stats = self.system.db_manager.get_statistics()
        logger.info(f"Initial records: {initial_stats['total_records']:,}")
        logger.info(f"Recent records (30d): {initial_stats['recent_records']:,}")
        
        # Check cache for recent run
        cache_key = f"daily_update_{datetime.now().date()}"
        cached = self.system.cache_manager.get(cache_key, max_age=timedelta(hours=20))
        
        if cached and not os.environ.get('FORCE_UPDATE'):
            logger.info("Already updated today (use FORCE_UPDATE=1 to override)")
            return
        
        # Process incremental updates
        inserted, duplicates = self.process_incremental()
        
        # Optional cleanup of old data
        if cleanup:
            self.cleanup_old_data()
        
        # Optimize database if needed
        if optimize and inserted > 100:
            logger.info("\nOptimizing database...")
            self.system.db_manager.optimize_database()
        
        # Get final statistics
        final_stats = self.system.db_manager.get_statistics()
        
        # Cache successful run
        self.system.cache_manager.set(cache_key, {
            'inserted': inserted,
            'duplicates': duplicates,
            'timestamp': datetime.now().isoformat()
        })
        
        # Report results
        elapsed = datetime.now() - start_time
        logger.info("\n" + "="*60)
        logger.info("Daily Update Complete!")
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"New records: {inserted:,}")
        logger.info(f"Duplicates skipped: {duplicates:,}")
        logger.info(f"Total records: {final_stats['total_records']:,}")
        logger.info(f"Database size: {final_stats['size_mb']:.1f} MB")
        
        # Show changes by country if any inserts
        if inserted > 0:
            logger.info("\nNew records by country:")
            
            # Compare before/after stats
            for country in final_stats['by_country']:
                before = initial_stats['by_country'].get(country, 0)
                after = final_stats['by_country'][country]
                diff = after - before
                if diff > 0:
                    logger.info(f"  {country}: +{diff}")
        
        logger.info("="*60)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daily SAM.gov data update")
    parser.add_argument(
        "--lookback-days", 
        type=int, 
        default=7,
        help="Number of days to look back for updates (default: 7)"
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Skip database optimization"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove records older than 5 years"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override data directory location"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if already run today"
    )
    
    args = parser.parse_args()
    
    # Override config if specified
    if args.data_dir:
        os.environ['SAM_DATA_DIR'] = args.data_dir
    
    if args.force:
        os.environ['FORCE_UPDATE'] = '1'
    
    # Run updater
    updater = DailyUpdater(lookback_days=args.lookback_days)
    updater.run(optimize=not args.no_optimize, cleanup=args.cleanup)

if __name__ == "__main__":
    main()