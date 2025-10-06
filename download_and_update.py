#!/usr/bin/env python3
"""
download_and_update.py - Automatic daily updater for SAM.gov data
Runs nightly to update database with new opportunities - no user intervention needed
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import get_system, logger

# Configure logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'daily_update_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class DailyUpdater:
    """Handles automatic daily updates from SAM.gov"""
    
    def __init__(self, lookback_days: int = 14):
        """
        Initialize daily updater
        
        Args:
            lookback_days: Number of days to look back for updates (default 14)
                          Set higher to catch any missed updates
        """
        self.system = get_system()
        self.lookback_days = lookback_days
        
        # Track statistics
        self.stats = {
            'total_processed': 0,
            'african_found': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'by_country': {}
        }
        
    def get_last_update_file(self) -> Path:
        """Get path to last update tracking file"""
        return self.system.config.data_dir / ".last_update.json"
    
    def get_last_update_time(self) -> Optional[datetime]:
        """Get timestamp of last successful update"""
        last_update_file = self.get_last_update_file()
        
        if last_update_file.exists():
            try:
                import json
                with open(last_update_file, 'r') as f:
                    data = json.load(f)
                    return datetime.fromisoformat(data['timestamp'])
            except:
                pass
        return None
    
    def save_update_time(self):
        """Save current time as last update"""
        import json
        
        last_update_file = self.get_last_update_file()
        
        try:
            with open(last_update_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'stats': self.stats
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save update time: {e}")
    
    def should_run_update(self) -> bool:
        """Check if update should run (hasn't run today)"""
        last_update = self.get_last_update_time()
        
        if last_update is None:
            logger.info("No previous update found - will run update")
            return True
        
        # Check if last update was today
        today = datetime.now().date()
        last_date = last_update.date()
        
        if last_date >= today:
            logger.info(f"Already updated today at {last_update.strftime('%H:%M:%S')}")
            return False
        
        logger.info(f"Last update was {last_date}, running new update")
        return True
    
    def process_current_csv(self) -> Tuple[int, int, int]:
        """
        Process the current opportunities CSV for updates
        Returns: (inserted, updated, skipped)
        """
        logger.info("="*60)
        logger.info("Processing Current Opportunities CSV")
        logger.info("="*60)
        
        # Get current CSV URL
        current_url = self.system.get_current_url()
        
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            logger.info("Downloading current opportunities CSV...")
            if not self.system.http_client.download_file(current_url, csv_path, show_progress=False):
                logger.error("Failed to download current CSV")
                
                # Try S3 fallback
                s3_url = current_url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback: {s3_url}")
                if not self.system.http_client.download_file(s3_url, csv_path, show_progress=False):
                    logger.error("Failed to download from both sources")
                    return 0, 0, 0
            
            # Check file size
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded {file_size_mb:.1f} MB file")
            
            # Determine cutoff date for processing
            cutoff_date = None
            if self.lookback_days > 0:
                cutoff_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
                logger.info(f"Processing records posted after {cutoff_date}")
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path, chunksize=5000):
                    chunk_num += 1
                    self.stats['total_processed'] += len(chunk)
                    
                    # Filter for recent records if cutoff specified
                    if cutoff_date and 'PostedDate' in chunk.columns:
                        # Normalize dates for comparison
                        chunk['PostedDate_check'] = chunk['PostedDate'].apply(
                            self.system.db_manager.normalize_posted_date
                        )
                        
                        # Filter for recent records
                        recent_mask = chunk['PostedDate_check'] >= cutoff_date
                        chunk = chunk[recent_mask]
                        
                        if chunk.empty:
                            continue
                    
                    # Filter for African countries
                    african_data = self.system.data_processor.process_chunk(chunk)
                    
                    if not african_data.empty:
                        self.stats['african_found'] += len(african_data)
                        
                        # Insert/update with deduplication
                        inserted, updated, skipped = self.system.db_manager.insert_or_update_batch(
                            african_data,
                            source="DAILY_UPDATE"
                        )
                        
                        total_inserted += inserted
                        total_updated += updated
                        total_skipped += skipped
                        
                        # Track by country
                        for country in african_data['PopCountry'].value_counts().index:
                            if country not in self.stats['by_country']:
                                self.stats['by_country'][country] = 0
                            self.stats['by_country'][country] += 1
                        
                        # Log progress every 10 chunks
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: Processed {self.stats['total_processed']:,} total, "
                                      f"found {self.stats['african_found']} African")
                
                # Update statistics
                self.stats['inserted'] = total_inserted
                self.stats['updated'] = total_updated
                self.stats['skipped'] = total_skipped
                
                logger.info(f"\n✅ Processing complete:")
                logger.info(f"  Total records processed: {self.stats['total_processed']:,}")
                logger.info(f"  African opportunities found: {self.stats['african_found']:,}")
                logger.info(f"  New records inserted: {total_inserted:,}")
                logger.info(f"  Existing records updated: {total_updated:,}")
                logger.info(f"  Duplicates/old skipped: {total_skipped:,}")
                
            except Exception as e:
                logger.error(f"Error processing current CSV: {e}", exc_info=True)
                
        return total_inserted, total_updated, total_skipped
    
    def optimize_database(self):
        """Quick database optimization after update"""
        logger.info("Optimizing database...")
        
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Update any missing normalized dates
            cur.execute("""
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    CASE 
                        WHEN PostedDate LIKE '____-__-__ __-__-__' 
                            THEN substr(PostedDate, 1, 10)
                        WHEN PostedDate LIKE '____-__-__' 
                            THEN PostedDate
                        ELSE PostedDate_normalized
                    END
                WHERE PostedDate_normalized IS NULL 
                  AND PostedDate IS NOT NULL
                  AND PostedDate >= date('now', '-30 days')
            """)
            
            if cur.rowcount > 0:
                logger.info(f"  Normalized {cur.rowcount} recent dates")
            
            # Update statistics for recent data
            cur.execute("ANALYZE")
            conn.commit()
        
        logger.info("✅ Database optimized")
    
    def run(self, force: bool = False) -> bool:
        """
        Run the daily update process
        
        Args:
            force: Force update even if already run today
            
        Returns:
            True if update was performed, False if skipped
        """
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("SAM.gov Daily Update Process")
        logger.info(f"Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Check if database exists
        if not self.system.config.db_path.exists():
            logger.error("Database not found! Run bootstrap_historical.py first.")
            return False
        
        # Check if should run
        if not force and not self.should_run_update():
            logger.info("Update not needed - skipping")
            return False
        
        # Get initial statistics
        initial_stats = self.system.db_manager.get_statistics()
        logger.info(f"Initial database state:")
        logger.info(f"  Total records: {initial_stats['total_records']:,}")
        logger.info(f"  Recent (7 days): {initial_stats['recent_7_days']:,}")
        logger.info(f"  Recent (30 days): {initial_stats['recent_30_days']:,}")
        
        # Process current CSV
        inserted, updated, skipped = self.process_current_csv()
        
        # Optimize if changes were made
        if inserted > 0 or updated > 0:
            self.optimize_database()
        
        # Get final statistics
        final_stats = self.system.db_manager.get_statistics()
        
        # Save update time
        self.save_update_time()
        
        # Generate summary
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("DAILY UPDATE COMPLETE")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Changes made:")
        logger.info(f"  New records: +{inserted:,}")
        logger.info(f"  Updated records: {updated:,}")
        logger.info(f"Final database state:")
        logger.info(f"  Total records: {final_stats['total_records']:,}")
        logger.info(f"  Recent (7 days): {final_stats['recent_7_days']:,}")
        logger.info(f"  Recent (30 days): {final_stats['recent_30_days']:,}")
        
        if self.stats['by_country']:
            logger.info("\nNew/updated records by country:")
            sorted_countries = sorted(
                self.stats['by_country'].items(),
                key=lambda x: x[1],
                reverse=True
            )
            for country, count in sorted_countries[:10]:
                logger.info(f"  {country}: {count:,}")
        
        logger.info("\n✅ Update completed successfully!")
        logger.info("Dashboard data is now up to date.")
        
        return True


def main():
    """Main entry point for daily updates"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Daily update for SAM.gov Africa opportunities"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Number of days to look back for updates (default: 14)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if already run today"
    )
    
    args = parser.parse_args()
    
    # Create updater instance
    updater = DailyUpdater(lookback_days=args.lookback_days)
    
    # Run update
    success = updater.run(force=args.force)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()