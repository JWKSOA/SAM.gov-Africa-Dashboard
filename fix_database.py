#!/usr/bin/env python3
"""
fix_database.py - Ensures database has proper date normalization for accurate filtering
Run this once to fix your database permanently
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import sys

def fix_database():
    """Fix the database to enable proper date filtering"""
    
    print("="*60)
    print("DATABASE FIX SCRIPT - Date Normalization")
    print("="*60)
    
    # Find database - check multiple locations
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
        print("Please run: python download_and_update.py first")
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
        
        normalized_exists = 'PostedDate_normalized' in columns
        
        if normalized_exists:
            print("✅ PostedDate_normalized column already exists")
            
            # Check how many are normalized
            cur.execute("SELECT COUNT(*) FROM opportunities WHERE PostedDate_normalized IS NOT NULL")
            normalized_count = cur.fetchone()[0]
            print(f"Normalized dates: {normalized_count:,}/{total_records:,}")
            
            if normalized_count >= total_records * 0.95:  # If 95% are normalized, we're good
                print("✅ Database already has normalized dates")
                conn.close()
                return True
            else:
                print("⚠️  Some dates need normalization")
        else:
            print("Adding PostedDate_normalized column...")
            cur.execute('''
                ALTER TABLE opportunities 
                ADD COLUMN PostedDate_normalized DATE
            ''')
            conn.commit()
            print("✅ Added PostedDate_normalized column")
        
        # Normalize dates with various format handlers
        print("\nNormalizing date formats...")
        
        # Sample some dates to understand formats
        cur.execute("""
            SELECT DISTINCT PostedDate 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            AND PostedDate != ''
            LIMIT 20
        """)
        
        sample_dates = [row[0] for row in cur.fetchall()]
        print(f"Sample date formats found:")
        for date_str in sample_dates[:5]:
            print(f"  '{date_str}'")
        
        # Handle different date formats
        normalization_queries = [
            # ISO format with time (2024-10-06T12:00:00)
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = date(substr(PostedDate, 1, 10))
                WHERE PostedDate LIKE '____-__-__T%'
                AND PostedDate_normalized IS NULL
            ''', "ISO with time"),
            
            # Standard YYYY-MM-DD
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = date(PostedDate)
                WHERE PostedDate LIKE '____-__-__'
                AND PostedDate_normalized IS NULL
            ''', "YYYY-MM-DD"),
            
            # MM/DD/YYYY format
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    substr(PostedDate, 7, 4) || '-' || 
                    substr(PostedDate, 1, 2) || '-' || 
                    substr(PostedDate, 4, 2)
                WHERE PostedDate LIKE '__/__/____'
                AND PostedDate_normalized IS NULL
            ''', "MM/DD/YYYY"),
            
            # DD/MM/YYYY format (less common but possible)
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    substr(PostedDate, 7, 4) || '-' || 
                    substr(PostedDate, 4, 2) || '-' || 
                    substr(PostedDate, 1, 2)
                WHERE PostedDate LIKE '__/__/____'
                AND PostedDate_normalized IS NULL
                AND CAST(substr(PostedDate, 4, 2) AS INTEGER) <= 12
                AND CAST(substr(PostedDate, 1, 2) AS INTEGER) > 12
            ''', "DD/MM/YYYY"),
            
            # Try to normalize any remaining dates
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = date(PostedDate)
                WHERE PostedDate_normalized IS NULL
                AND PostedDate IS NOT NULL
                AND PostedDate != ''
            ''', "Remaining dates")
        ]
        
        total_normalized = 0
        for query, format_name in normalization_queries:
            cur.execute(query)
            updated = cur.rowcount
            if updated > 0:
                print(f"  Normalized {updated:,} dates in {format_name} format")
                total_normalized += updated
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
            print(f"\n⚠️  {remaining:,} dates could not be normalized (may be invalid)")
        
        # Create or recreate index on normalized dates
        print("\nCreating/updating indexes...")
        cur.execute("DROP INDEX IF EXISTS idx_posted_date_normalized")
        cur.execute('''
            CREATE INDEX idx_posted_date_normalized 
            ON opportunities(PostedDate_normalized)
        ''')
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_posted_date 
            ON opportunities(PostedDate)
        ''')
        conn.commit()
        print("✅ Indexes created")
        
        # Test date filtering
        print("\nTesting date filtering...")
        today = datetime.now().date().isoformat()
        
        test_periods = [
            ("Today", today, today),
            ("Last 7 days", (datetime.now().date() - timedelta(days=7)).isoformat(), today),
            ("Last 30 days", (datetime.now().date() - timedelta(days=30)).isoformat(), today),
            ("Last year", (datetime.now().date() - timedelta(days=365)).isoformat(), today),
        ]
        
        for period_name, start_date, end_date in test_periods:
            cur.execute("""
                SELECT COUNT(*) FROM opportunities 
                WHERE PostedDate_normalized >= ?
                AND PostedDate_normalized <= ?
            """, (start_date, end_date))
            count = cur.fetchone()[0]
            print(f"  {period_name}: {count:,} records")
        
        # Get date range
        cur.execute("""
            SELECT MIN(PostedDate_normalized), MAX(PostedDate_normalized) 
            FROM opportunities 
            WHERE PostedDate_normalized IS NOT NULL
        """)
        min_date, max_date = cur.fetchone()
        
        # Verify column presence for dashboard
        print("\nVerifying required columns for dashboard...")
        required_columns = [
            'NoticeID', 'Title', 'PostedDate', 'PostedDate_normalized',
            'PopCountry', 'CountryCode', 'Type', 'Link', 'Description'
        ]
        
        # Check if Department column exists (might be Department/Ind.Agency)
        if 'Department/Ind.Agency' in columns:
            print("  ✅ Found 'Department/Ind.Agency' column")
        elif 'Department' in columns:
            print("  ✅ Found 'Department' column")
        else:
            print("  ⚠️  No Department column found")
        
        missing = []
        for col in required_columns:
            if col not in columns:
                missing.append(col)
        
        if missing:
            print(f"  ⚠️  Missing columns: {missing}")
        else:
            print("  ✅ All required columns present")
        
        # Optimize database
        print("\nOptimizing database...")
        cur.execute("ANALYZE")
        conn.commit()
        
        print("\n" + "="*60)
        print("DATABASE FIX COMPLETE!")
        print("="*60)
        print(f"✅ Database is now ready for proper date filtering")
        print(f"✅ Total records: {total_records:,}")
        print(f"✅ Normalized dates: {total_normalized:,}")
        
        if min_date and max_date:
            print(f"✅ Date range: {min_date} to {max_date}")
        
        print("\n✨ Your dashboard will now show accurate date ranges!")
        print("Next step: Restart your Streamlit dashboard")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    success = fix_database()
    sys.exit(0 if success else 1)