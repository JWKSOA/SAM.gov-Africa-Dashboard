#!/usr/bin/env python3
"""
migrate_database.py - Migrate existing database to optimized structure
Preserves all data while adding indexes and optimizations
"""

import os
import sys
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def migrate_database():
    """Migrate existing database to optimized structure"""
    
    print("="*60)
    print("SAM.gov Database Migration Tool")
    print("="*60)
    
    # Determine database location
    sam_data_dir = os.environ.get('SAM_DATA_DIR')
    if sam_data_dir:
        data_dir = Path(sam_data_dir).expanduser().resolve()
    else:
        # Check common locations
        repo_db = Path.cwd() / "data" / "opportunities.db"
        home_db = Path.home() / "sam_africa_data" / "opportunities.db"
        
        if repo_db.exists():
            data_dir = repo_db.parent
        elif home_db.exists():
            data_dir = home_db.parent
        else:
            print("❌ No existing database found to migrate")
            return False
    
    db_path = data_dir / "opportunities.db"
    
    if not db_path.exists():
        print(f"❌ Database not found at {db_path}")
        return False
    
    print(f"Found database at: {db_path}")
    
    # Create backup
    backup_path = data_dir / f"opportunities_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    print(f"Creating backup at: {backup_path}")
    shutil.copy2(db_path, backup_path)
    print("✅ Backup created")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Get initial statistics
        cur.execute("SELECT COUNT(*) FROM opportunities")
        initial_count = cur.fetchone()[0]
        print(f"\nInitial record count: {initial_count:,}")
        
        # Check current structure
        cur.execute("PRAGMA table_info(opportunities)")
        columns = {row[1]: row[2] for row in cur.fetchall()}
        print(f"Current columns: {len(columns)}")
        
        # Add missing columns if needed
        required_columns = [
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]
        
        for col_name, col_def in required_columns:
            if col_name not in columns:
                print(f"Adding column: {col_name}")
                try:
                    cur.execute(f"ALTER TABLE opportunities ADD COLUMN {col_name} {col_def}")
                    conn.commit()
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        raise
        
        # Clean up data
        print("\nCleaning data...")
        
        # Remove completely empty rows
        cur.execute("""
            DELETE FROM opportunities 
            WHERE (Title IS NULL OR Title = '' OR Title = 'nan')
            AND (PostedDate IS NULL OR PostedDate = '' OR PostedDate = 'nan')
            AND (Link IS NULL OR Link = '' OR Link = 'nan')
        """)
        empty_deleted = cur.rowcount
        if empty_deleted > 0:
            print(f"  Removed {empty_deleted} empty rows")
        
        # Deduplicate by NoticeID
        cur.execute("""
            DELETE FROM opportunities 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM opportunities 
                GROUP BY NoticeID
            )
        """)
        dupes_deleted = cur.rowcount
        if dupes_deleted > 0:
            print(f"  Removed {dupes_deleted} duplicate rows")
        
        conn.commit()
        
        # Standardize country codes
        print("\nStandardizing country codes...")
        
        # Import country manager
        try:
            from sam_utils import CountryManager
            country_mgr = CountryManager()
            
            # Get all unique country values
            cur.execute("SELECT DISTINCT PopCountry FROM opportunities WHERE PopCountry IS NOT NULL")
            countries = [row[0] for row in cur.fetchall()]
            
            updates = 0
            for old_value in countries:
                if old_value and not '(' in str(old_value):  # Not already formatted
                    new_value = country_mgr.standardize_country_code(old_value)
                    if new_value != old_value:
                        cur.execute(
                            "UPDATE opportunities SET PopCountry = ? WHERE PopCountry = ?",
                            (new_value, old_value)
                        )
                        updates += cur.rowcount
            
            if updates > 0:
                print(f"  Updated {updates} country values")
                conn.commit()
                
        except ImportError:
            print("  ⚠️  sam_utils not found, skipping country standardization")
        
        # Create indexes
        print("\nCreating optimized indexes...")
        
        indexes = [
            ("idx_notice_id", "NoticeID"),
            ("idx_posted_date", "PostedDate"),
            ("idx_pop_country", "PopCountry"),
            ("idx_country_code", "CountryCode"),
            ("idx_department", '"Department/Ind.Agency"'),
            ("idx_created_at", "created_at"),
            ("idx_updated_at", "updated_at"),
        ]
        
        # Composite indexes
        composite_indexes = [
            ("idx_country_date", "(PopCountry, PostedDate DESC)"),
            ("idx_dept_date", '("Department/Ind.Agency", PostedDate DESC)'),
        ]
        
        for idx_name, column in indexes:
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON opportunities({column})")
                print(f"  ✅ Created index: {idx_name}")
            except sqlite3.OperationalError as e:
                if "already exists" not in str(e).lower():
                    print(f"  ⚠️  Failed to create {idx_name}: {e}")
        
        for idx_name, columns in composite_indexes:
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON opportunities{columns}")
                print(f"  ✅ Created composite index: {idx_name}")
            except sqlite3.OperationalError as e:
                if "already exists" not in str(e).lower():
                    print(f"  ⚠️  Failed to create {idx_name}: {e}")
        
        conn.commit()
        
        # Optimize database
        print("\nOptimizing database...")
        
        # Update statistics
        cur.execute("ANALYZE")
        print("  ✅ Updated statistics")
        
        # Optimize query planner
        cur.execute("PRAGMA optimize")
        print("  ✅ Optimized query planner")
        
        # Check if VACUUM needed
        cur.execute("PRAGMA page_count")
        page_count = cur.fetchone()[0]
        cur.execute("PRAGMA freelist_count") 
        freelist_count = cur.fetchone()[0]
        
        fragmentation = (freelist_count / page_count) * 100 if page_count > 0 else 0
        db_size_mb = (page_count * 4096) / (1024 * 1024)
        
        print(f"\nDatabase statistics:")
        print(f"  Size: {db_size_mb:.1f} MB")
        print(f"  Fragmentation: {fragmentation:.1f}%")
        
        if fragmentation > 20:
            print("  Running VACUUM to defragment...")
            conn.execute("VACUUM")
            print("  ✅ Database defragmented")
        
        # Final statistics
        cur.execute("SELECT COUNT(*) FROM opportunities")
        final_count = cur.fetchone()[0]
        
        # Performance check
        print("\nPerformance check...")
        
        # Test query speed
        import time
        
        test_queries = [
            ("Recent records", "SELECT COUNT(*) FROM opportunities WHERE date(PostedDate) >= date('now', '-30 days')"),
            ("Country summary", "SELECT PopCountry, COUNT(*) FROM opportunities GROUP BY PopCountry LIMIT 10"),
            ("Agency summary", 'SELECT "Department/Ind.Agency", COUNT(*) FROM opportunities GROUP BY "Department/Ind.Agency" LIMIT 10'),
        ]
        
        for name, query in test_queries:
            start = time.time()
            cur.execute(query)
            _ = cur.fetchall()
            elapsed = (time.time() - start) * 1000
            print(f"  {name}: {elapsed:.1f}ms")
        
        print("\n" + "="*60)
        print("Migration Complete!")
        print(f"Initial records: {initial_count:,}")
        print(f"Final records: {final_count:,}")
        print(f"Records removed: {initial_count - final_count:,}")
        print(f"Backup saved at: {backup_path}")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print(f"Backup preserved at: {backup_path}")
        return False
        
    finally:
        conn.close()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate SAM.gov database to optimized structure")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override data directory location"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup (not recommended)"
    )
    
    args = parser.parse_args()
    
    if args.data_dir:
        os.environ['SAM_DATA_DIR'] = args.data_dir
    
    success = migrate_database()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()