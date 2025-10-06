#!/usr/bin/env python3
"""
fix_database.py - Fix the existing database for proper date filtering
Run this ONCE to fix your database
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import sys

def fix_database():
    """Fix the database to enable proper date filtering"""
    
    print("="*60)
    print("DATABASE FIX SCRIPT")
    print("="*60)
    
    # Find database
    db_paths = [
        Path("data/opportunities.db"),
        Path("opportunities.db"),
        Path.home() / "sam_africa_data" / "opportunities.db"
    ]
    
    db_path = None
    for path in db_paths:
        if path.exists():
            db_path = path
            break
    
    if not db_path:
        print("❌ No database found!")
        return False
    
    print(f"Found database at: {db_path}")
    
    # Check if it's a real database or LFS pointer
    if db_path.stat().st_size < 1000:
        print("❌ Database appears to be a Git LFS pointer file")
        print("Run: git lfs pull")
        return False
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Check current state
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total_records = cur.fetchone()[0]
        print(f"Total records: {total_records:,}")
        
        # Check if PostedDate_normalized already exists
        cur.execute("PRAGMA table_info(opportunities)")
        columns = {row[1] for row in cur.fetchall()}
        
        if 'PostedDate_normalized' in columns:
            print("✅ PostedDate_normalized column already exists")
            
            # Check how many are normalized
            cur.execute("SELECT COUNT(*) FROM opportunities WHERE PostedDate_normalized IS NOT NULL")
            normalized_count = cur.fetchone()[0]
            print(f"Normalized dates: {normalized_count:,}/{total_records:,}")
            
            if normalized_count < total_records:
                print("Normalizing remaining dates...")
        else:
            print("Adding PostedDate_normalized column...")
            cur.execute('''
                ALTER TABLE opportunities 
                ADD COLUMN PostedDate_normalized DATE
            ''')
            conn.commit()
            print("✅ Added PostedDate_normalized column")
        
        # Normalize dates
        print("\nNormalizing date formats...")
        
        # Handle different date formats
        date_updates = [
            # MM/DD/YYYY format
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    substr(PostedDate, 7, 4) || '-' || 
                    substr(PostedDate, 1, 2) || '-' || 
                    substr(PostedDate, 4, 2)
                WHERE PostedDate LIKE '__/__/____'
                AND PostedDate_normalized IS NULL
            ''', "MM/DD/YYYY format"),
            
            # YYYY-MM-DD format (already normalized)
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = substr(PostedDate, 1, 10)
                WHERE PostedDate LIKE '____-__-__'
                AND PostedDate_normalized IS NULL
            ''', "YYYY-MM-DD format"),
            
            # MM/DD/YY format
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    '20' || substr(PostedDate, 7, 2) || '-' || 
                    substr(PostedDate, 1, 2) || '-' || 
                    substr(PostedDate, 4, 2)
                WHERE PostedDate LIKE '__/__/__'
                AND PostedDate_normalized IS NULL
            ''', "MM/DD/YY format"),
        ]
        
        for query, format_name in date_updates:
            cur.execute(query)
            updated = cur.rowcount
            if updated > 0:
                print(f"  Normalized {updated:,} dates in {format_name}")
                conn.commit()
        
        # Check for any remaining unnormalized dates
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            AND PostedDate != ''
            AND PostedDate_normalized IS NULL
        """)
        remaining = cur.fetchone()[0]
        
        if remaining > 0:
            print(f"\n⚠️  {remaining:,} dates could not be normalized")
            
            # Sample the problematic dates
            cur.execute("""
                SELECT DISTINCT PostedDate 
                FROM opportunities 
                WHERE PostedDate IS NOT NULL 
                AND PostedDate != ''
                AND PostedDate_normalized IS NULL
                LIMIT 10
            """)
            
            print("Sample of problematic dates:")
            for row in cur.fetchall():
                print(f"  '{row[0]}'")
        
        # Create index on normalized dates
        print("\nCreating index on normalized dates...")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_posted_date_normalized 
            ON opportunities(PostedDate_normalized)
        ''')
        conn.commit()
        print("✅ Index created")
        
        # Add missing columns that scripts expect
        print("\nChecking for missing columns...")
        required_columns = [
            'Title', 'Department/Ind.Agency', 'Sub-Tier', 'Office', 
            'PostedDate', 'Type', 'PopCountry', 'AwardNumber', 
            'AwardDate', 'Award$', 'Awardee', 'PrimaryContactTitle',
            'PrimaryContactFullName', 'PrimaryContactEmail', 
            'PrimaryContactPhone', 'OrganizationType', 'CountryCode',
            'Link', 'Description'
        ]
        
        for col in required_columns:
            if col not in columns:
                print(f"  Adding missing column: {col}")
                cur.execute(f'ALTER TABLE opportunities ADD COLUMN "{col}" TEXT')
                conn.commit()
        
        # Test date filtering
        print("\nTesting date filtering...")
        
        # Last 7 days
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate_normalized >= date('now', '-7 days')
        """)
        last_7 = cur.fetchone()[0]
        print(f"  Last 7 days: {last_7:,} records")
        
        # Last 30 days
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate_normalized >= date('now', '-30 days')
        """)
        last_30 = cur.fetchone()[0]
        print(f"  Last 30 days: {last_30:,} records")
        
        # Last year
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate_normalized >= date('now', '-365 days')
        """)
        last_year = cur.fetchone()[0]
        print(f"  Last year: {last_year:,} records")
        
        # Optimize database
        print("\nOptimizing database...")
        cur.execute("VACUUM")
        cur.execute("ANALYZE")
        conn.commit()
        
        print("\n" + "="*60)
        print("DATABASE FIX COMPLETE!")
        print("="*60)
        print(f"✅ Database is now ready for proper date filtering")
        print(f"✅ Total records: {total_records:,}")
        
        cur.execute("SELECT MIN(PostedDate_normalized), MAX(PostedDate_normalized) FROM opportunities")
        min_date, max_date = cur.fetchone()
        if min_date and max_date:
            print(f"✅ Date range: {min_date} to {max_date}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    success = fix_database()
    sys.exit(0 if success else 1)