#!/usr/bin/env python3
"""
bootstrap_historical.py - Optimized Historical Data Bootstrap
Fetches all historical years with improved performance and error handling
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add parent directory to path if running standalone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import (
    get_system, Config, logger,
    DatabaseManager, DataProcessor, HTTPClient, CSVReader
)

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class HistoricalBootstrap:
    """Handles historical data bootstrap with resume capability"""
    
    def __init__(self, start_year: int = 1998, end_year: Optional[int] = None):
        """Initialize bootstrap with year range"""
        self.system = get_system()
        self.start_year = start_year
        
        # Default to current fiscal year
        if end_year is None:
            today = datetime.today()
            self.end_year = today.year if today.month < 10 else today.year + 1
        else:
            self.end_year = end_year
        
        # Track progress
        self.progress_file = self.system.config.data_dir / ".bootstrap_progress.txt"
        self.completed_years = self._load_progress()
        
    def _load_progress(self) -> set:
        """Load previously completed years"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return set(map(int, f.read().strip().split(',')))
            except:
                pass
        return set()
    
    def _save_progress(self, year: int):
        """Save progress after completing a year"""
        self.completed_years.add(year)
        with open(self.progress_file, 'w') as f:
            f.write(','.join(map(str, sorted(self.completed_years))))
    
    def process_archive_year(self, year: int) -> tuple[int, int]:
        """Process a single archive year"""
        if year in self.completed_years:
            logger.info(f"Skipping FY{year} (already completed)")
            return 0, 0
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing FY{year}")
        logger.info(f"{'='*60}")
        
        # Get archive URL
        url = self.system.get_archive_url(year)
        if not url:
            logger.warning(f"No archive found for FY{year}")
            return 0, 0
        
        total_inserted = 0
        total_duplicates = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download archive
            if not self.system.http_client.download_file(url, csv_path):
                logger.error(f"Failed to download FY{year}")
                return 0, 0
            
            # Process in chunks
            try:
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Process chunk
                    processed = self.system.data_processor.process_chunk(chunk)
                    
                    if processed.empty:
                        continue
                    
                    # Insert to database
                    inserted, duplicates = self.system.db_manager.insert_batch(
                        processed, 
                        self.system.country_manager
                    )
                    
                    total_inserted += inserted
                    total_duplicates += duplicates
                    
                    # Log progress every 10 chunks
                    if chunk_num % 10 == 0:
                        logger.info(f"  Chunk {chunk_num}: {total_inserted} inserted, "
                                  f"{total_duplicates} duplicates")
                
                # Mark year as complete
                self._save_progress(year)
                
                logger.info(f"FY{year} complete: {total_inserted} inserted, "
                          f"{total_duplicates} duplicates")
                
            except Exception as e:
                logger.error(f"Error processing FY{year}: {e}")
                
        return total_inserted, total_duplicates
    
    def process_current_full(self) -> tuple[int, int]:
        """Process current full CSV"""
        logger.info(f"\n{'='*60}")
        logger.info("Processing current full CSV")
        logger.info(f"{'='*60}")
        
        url = self.system.get_current_csv_url()
        total_inserted = 0
        total_duplicates = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            if not self.system.http_client.download_file(url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0
            
            # Process in chunks
            try:
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    # Process chunk
                    processed = self.system.data_processor.process_chunk(chunk)
                    
                    if processed.empty:
                        continue
                    
                    # Only insert records from last 90 days to avoid duplicates
                    if 'PostedDate' in processed.columns:
                        try:
                            processed['PostedDate_parsed'] = pd.to_datetime(
                                processed['PostedDate'], 
                                errors='coerce'
                            )
                            cutoff = datetime.now() - timedelta(days=90)
                            processed = processed[processed['PostedDate_parsed'] >= cutoff]
                        except:
                            pass  # Process all if date parsing fails
                    
                    if processed.empty:
                        continue
                    
                    # Insert to database
                    inserted, duplicates = self.system.db_manager.insert_batch(
                        processed, 
                        self.system.country_manager
                    )
                    
                    total_inserted += inserted
                    total_duplicates += duplicates
                
                logger.info(f"Current CSV complete: {total_inserted} inserted, "
                          f"{total_duplicates} duplicates")
                
            except Exception as e:
                logger.error(f"Error processing current CSV: {e}")
                
        return total_inserted, total_duplicates
    
    def run(self, skip_current: bool = False):
        """Run the complete bootstrap process"""
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("SAM.gov Historical Bootstrap - Optimized Version")
        logger.info(f"Year range: FY{self.start_year} to FY{self.end_year}")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Get initial statistics
        initial_stats = self.system.db_manager.get_statistics()
        logger.info(f"Initial records: {initial_stats['total_records']:,}")
        
        # Process historical archives
        total_inserted = 0
        total_duplicates = 0
        
        for year in range(self.start_year, self.end_year + 1):
            inserted, duplicates = self.process_archive_year(year)
            total_inserted += inserted
            total_duplicates += duplicates
        
        # Process current full CSV
        if not skip_current:
            inserted, duplicates = self.process_current_full()
            total_inserted += inserted
            total_duplicates += duplicates
        
        # Optimize database
        logger.info("\nOptimizing database...")
        self.system.db_manager.optimize_database()
        
        # Get final statistics
        final_stats = self.system.db_manager.get_statistics()
        
        # Clean up progress file if all years completed
        expected_years = set(range(self.start_year, self.end_year + 1))
        if self.completed_years >= expected_years:
            self.progress_file.unlink(missing_ok=True)
        
        # Report results
        elapsed = datetime.now() - start_time
        logger.info("\n" + "="*60)
        logger.info("Bootstrap Complete!")
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total inserted: {total_inserted:,}")
        logger.info(f"Total duplicates: {total_duplicates:,}")
        logger.info(f"Final records: {final_stats['total_records']:,}")
        logger.info(f"Database size: {final_stats['size_mb']:.1f} MB")
        logger.info("\nTop 10 countries by opportunities:")
        
        for country, count in list(final_stats['by_country'].items())[:10]:
            logger.info(f"  {country}: {count:,}")
        
        logger.info("="*60)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bootstrap SAM.gov historical data")
    parser.add_argument(
        "--start-year", 
        type=int, 
        default=1998,
        help="Start year for historical data (default: 1998)"
    )
    parser.add_argument(
        "--end-year", 
        type=int, 
        default=None,
        help="End year for historical data (default: current fiscal year)"
    )
    parser.add_argument(
        "--skip-current",
        action="store_true",
        help="Skip processing current full CSV"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override data directory location"
    )
    
    args = parser.parse_args()
    
    # Override config if data-dir specified
    if args.data_dir:
        os.environ['SAM_DATA_DIR'] = args.data_dir
    
    # Import pandas here to ensure it's available
    global pd, timedelta
    import pandas as pd
    from datetime import timedelta
    
    # Run bootstrap
    bootstrap = HistoricalBootstrap(args.start_year, args.end_year)
    bootstrap.run(skip_current=args.skip_current)

if __name__ == "__main__":
    main()