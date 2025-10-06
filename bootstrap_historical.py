#!/usr/bin/env python3
"""
bootstrap_historical.py - Complete historical data loader from FY1998 to current
Processes all SAM.gov archive files and current data with proper deduplication
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import json

import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import get_system, logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bootstrap.log'),
        logging.StreamHandler()
    ]
)

class HistoricalBootstrap:
    """Complete bootstrap of all SAM.gov historical data"""
    
    def __init__(self):
        """Initialize bootstrap system"""
        self.system = get_system()
        self.progress_file = self.system.config.data_dir / "bootstrap_progress.json"
        self.completed_sources = self._load_progress()
        
        # Statistics tracking
        self.total_inserted = 0
        self.total_updated = 0
        self.total_skipped = 0
        self.country_stats = {}
        
    def _load_progress(self) -> dict:
        """Load progress from file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_progress(self, source: str, status: str = "completed"):
        """Save progress after completing a source"""
        self.completed_sources[source] = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "records": {
                "inserted": self.total_inserted,
                "updated": self.total_updated,
                "skipped": self.total_skipped
            }
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.completed_sources, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def clear_database(self):
        """Clear and reinitialize database"""
        logger.warning("Clearing existing database...")
        
        if self.system.config.db_path.exists():
            self.system.config.db_path.unlink()
            
        # Reinitialize with proper schema
        self.system.db_manager.initialize_database()
        
        # Clear progress tracking
        if self.progress_file.exists():
            self.progress_file.unlink()
        self.completed_sources = {}
        
        logger.info("Database cleared and reinitialized")
    
    def process_archive_year(self, year: int) -> Tuple[int, int, int]:
        """
        Process a single fiscal year archive
        Returns: (inserted, updated, skipped)
        """
        source_key = f"FY{year}"
        
        # Check if already processed
        if source_key in self.completed_sources:
            status = self.completed_sources[source_key].get('status')
            if status == 'completed':
                logger.info(f"Skipping {source_key} - already completed")
                return 0, 0, 0
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {source_key} Archive")
        logger.info(f"{'='*60}")
        
        # Get archive URL
        archive_url = self.system.get_archive_url(year)
        
        year_inserted = 0
        year_updated = 0
        year_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download archive
            if not self.system.http_client.download_file(archive_url, csv_path):
                logger.warning(f"Could not download FY{year} archive - may not exist")
                self._save_progress(source_key, "not_found")
                return 0, 0, 0
            
            # Check file size
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"Processing {file_size_mb:.1f} MB file")
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Filter for African countries
                    african_data = self.system.data_processor.process_chunk(chunk)
                    
                    if not african_data.empty:
                        # Insert/update with deduplication
                        inserted, updated, skipped = self.system.db_manager.insert_or_update_batch(
                            african_data, 
                            source=source_key
                        )
                        
                        year_inserted += inserted
                        year_updated += updated
                        year_skipped += skipped
                        
                        # Update country statistics
                        for country in african_data['PopCountry'].value_counts().index:
                            if country not in self.country_stats:
                                self.country_stats[country] = 0
                            self.country_stats[country] += 1
                        
                        # Log progress every 10 chunks
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: {year_inserted} new, "
                                      f"{year_updated} updated, {year_skipped} skipped")
                
                # Update totals
                self.total_inserted += year_inserted
                self.total_updated += year_updated
                self.total_skipped += year_skipped
                
                # Save progress
                self._save_progress(source_key, "completed")
                
                logger.info(f"✅ {source_key} complete: {year_inserted} inserted, "
                          f"{year_updated} updated, {year_skipped} skipped")
                
            except Exception as e:
                logger.error(f"Error processing {source_key}: {e}")
                self._save_progress(source_key, "error")
                
        return year_inserted, year_updated, year_skipped
    
    def process_current_data(self) -> Tuple[int, int, int]:
        """
        Process current opportunities CSV
        Returns: (inserted, updated, skipped)
        """
        source_key = "CURRENT"
        
        logger.info(f"\n{'='*60}")
        logger.info("Processing Current Opportunities CSV")
        logger.info(f"{'='*60}")
        
        current_url = self.system.get_current_url()
        
        current_inserted = 0
        current_updated = 0
        current_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            if not self.system.http_client.download_file(current_url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0, 0
            
            # Check file size
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"Processing {file_size_mb:.1f} MB current file")
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in self.system.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    
                    # Filter for African countries
                    african_data = self.system.data_processor.process_chunk(chunk)
                    
                    if not african_data.empty:
                        # Insert/update with deduplication
                        inserted, updated, skipped = self.system.db_manager.insert_or_update_batch(
                            african_data,
                            source=source_key
                        )
                        
                        current_inserted += inserted
                        current_updated += updated
                        current_skipped += skipped
                        
                        # Update country statistics
                        for country in african_data['PopCountry'].value_counts().index:
                            if country not in self.country_stats:
                                self.country_stats[country] = 0
                            self.country_stats[country] += 1
                        
                        # Log progress
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: {current_inserted} new, "
                                      f"{current_updated} updated, {current_skipped} skipped")
                
                # Update totals
                self.total_inserted += current_inserted
                self.total_updated += current_updated
                self.total_skipped += current_skipped
                
                # Save progress
                self._save_progress(source_key, "completed")
                
                logger.info(f"✅ Current data complete: {current_inserted} inserted, "
                          f"{current_updated} updated, {current_skipped} skipped")
                
            except Exception as e:
                logger.error(f"Error processing current data: {e}")
                self._save_progress(source_key, "error")
                
        return current_inserted, current_updated, current_skipped
    
    def optimize_database(self):
        """Optimize database after loading all data"""
        logger.info("\n🔧 Optimizing database...")
        
        with self.system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Ensure all dates are normalized
            logger.info("Normalizing any remaining dates...")
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
            """)
            
            normalized = cur.rowcount
            if normalized > 0:
                logger.info(f"  Normalized {normalized} dates")
            
            # Remove any non-African countries that might have slipped through
            logger.info("Verifying all records are African countries...")
            
            # Get all unique countries
            cur.execute("SELECT DISTINCT PopCountry FROM opportunities WHERE PopCountry IS NOT NULL")
            all_countries = [row[0] for row in cur.fetchall()]
            
            non_african = []
            for country in all_countries:
                if not self.system.country_manager.is_african_country(country):
                    non_african.append(country)
            
            if non_african:
                logger.warning(f"Found {len(non_african)} non-African countries, removing...")
                for country in non_african:
                    cur.execute("DELETE FROM opportunities WHERE PopCountry = ?", (country,))
                    removed = cur.rowcount
                    if removed > 0:
                        logger.info(f"  Removed {removed} records for {country}")
            
            # Update statistics
            cur.execute("ANALYZE")
            
            # Vacuum to reclaim space
            conn.commit()
            
        # Run VACUUM in separate connection
        conn = self.system.db_manager.get_connection().__enter__()
        conn.execute("VACUUM")
        conn.close()
        
        logger.info("✅ Database optimized")
    
    def run(self, start_year: int = 1998, end_year: Optional[int] = None, 
            clear_first: bool = False, skip_current: bool = False):
        """
        Run complete bootstrap process
        
        Args:
            start_year: First fiscal year to process (default 1998)
            end_year: Last fiscal year to process (default current FY)
            clear_first: Whether to clear database first
            skip_current: Whether to skip current data
        """
        start_time = datetime.now()
        
        # Determine end year
        if end_year is None:
            today = datetime.today()
            end_year = today.year if today.month < 10 else today.year + 1
        
        logger.info("="*60)
        logger.info("SAM.gov Complete Historical Bootstrap")
        logger.info(f"Processing FY{start_year} through FY{end_year}")
        logger.info(f"Database: {self.system.config.db_path}")
        logger.info("="*60)
        
        # Clear if requested
        if clear_first:
            self.clear_database()
        
        # Get initial statistics
        initial_stats = self.system.db_manager.get_statistics()
        logger.info(f"Starting with {initial_stats['total_records']:,} records")
        
        # Process all archive years
        years_to_process = list(range(start_year, end_year + 1))
        
        logger.info(f"Processing {len(years_to_process)} archive years...")
        
        for year in years_to_process:
            self.process_archive_year(year)
        
        # Process current data
        if not skip_current:
            self.process_current_data()
        
        # Optimize database
        self.optimize_database()
        
        # Get final statistics
        final_stats = self.system.db_manager.get_statistics()
        
        # Generate report
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("BOOTSTRAP COMPLETE!")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total inserted: {self.total_inserted:,}")
        logger.info(f"Total updated: {self.total_updated:,}")
        logger.info(f"Total skipped: {self.total_skipped:,}")
        logger.info(f"Final database records: {final_stats['total_records']:,}")
        logger.info(f"Active opportunities: {final_stats['active_records']:,}")
        logger.info(f"Database size: {final_stats['size_mb']:.1f} MB")
        
        logger.info("\n📊 Records by time period:")
        logger.info(f"  Last 7 days: {final_stats['recent_7_days']:,}")
        logger.info(f"  Last 30 days: {final_stats['recent_30_days']:,}")
        logger.info(f"  Last year: {final_stats['recent_year']:,}")
        logger.info(f"  Last 5 years: {final_stats['recent_5_years']:,}")
        
        logger.info("\n🌍 Top African countries:")
        for country, count in list(final_stats['by_country'].items())[:15]:
            logger.info(f"  {country}: {count:,}")
        
        logger.info("\n📅 Records by year:")
        for year, count in sorted(final_stats['by_year'].items(), reverse=True)[:10]:
            logger.info(f"  {year}: {count:,}")
        
        # Clean up progress file if complete
        if self.progress_file.exists():
            self.progress_file.unlink()
        
        logger.info("\n✅ Bootstrap completed successfully!")
        logger.info("Your dashboard now has complete historical data from FY1998 to current!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Bootstrap complete SAM.gov historical data (FY1998-current)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1998,
        help="Start fiscal year (default: 1998)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End fiscal year (default: current FY)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing database before loading"
    )
    parser.add_argument(
        "--skip-current",
        action="store_true",
        help="Skip loading current opportunities"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous progress"
    )
    
    args = parser.parse_args()
    
    # Print startup message
    print("="*60)
    print("Starting SAM.gov Bootstrap Script")
    print(f"Time: {datetime.now()}")
    print(f"Arguments: {args}")
    print("="*60)
    
    # Create bootstrap instance
    bootstrap = HistoricalBootstrap()
    
    # Run bootstrap
    bootstrap.run(
        start_year=args.start_year,
        end_year=args.end_year,
        clear_first=args.clear and not args.resume,
        skip_current=args.skip_current
    )


if __name__ == "__main__":
    main()