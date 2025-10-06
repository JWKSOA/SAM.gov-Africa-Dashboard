#!/usr/bin/env python3
"""
fix_all_data_issues.py - Master script to fix all data issues
Run this ONCE to fix your database completely
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import shutil
import sys

def fix_database_completely():
    """Complete database fix - handles all issues"""
    
    print("="*60)
    print("COMPLETE DATABASE FIX - MASTER SCRIPT")
    print("="*60)
    
    # Find database
    db_path = Path("data/opportunities.db")
    
    if not db_path.exists():
        print("❌ Database not found at data/opportunities.db")
        return False
    
    # Create backup
    print("\n📦 Creating backup...")
    backup_path = db_path.parent / f"opportunities_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(db_path, backup_path)
    print(f"✅ Backup saved to {backup_path}")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Get initial stats
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total_records = cur.fetchone()[0]
        print(f"\nTotal records: {total_records:,}")
        
        # Step 1: Add PostedDate_normalized column if missing
        print("\n🔧 Step 1: Adding normalized date column...")
        cur.execute("PRAGMA table_info(opportunities)")
        columns = {row[1] for row in cur.fetchall()}
        
        if 'PostedDate_normalized' not in columns:
            cur.execute('''
                ALTER TABLE opportunities 
                ADD COLUMN PostedDate_normalized DATE
            ''')
            conn.commit()
            print("✅ Added PostedDate_normalized column")
        else:
            print("✓ PostedDate_normalized already exists")
        
        # Step 2: Normalize all dates
        print("\n🔧 Step 2: Normalizing all dates...")
        
        # Get sample of dates to understand formats
        cur.execute("""
            SELECT DISTINCT PostedDate 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            AND PostedDate != ''
            LIMIT 50
        """)
        sample_dates = [row[0] for row in cur.fetchall()]
        
        print(f"Sample date formats found:")
        for i, date_str in enumerate(sample_dates[:5]):
            print(f"  {date_str}")
        
        # Normalize dates using multiple strategies
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
            
            # MM/DD/YYYY format (most common in SAM.gov)
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    substr(PostedDate, 7, 4) || '-' || 
                    substr(PostedDate, 1, 2) || '-' || 
                    substr(PostedDate, 4, 2)
                WHERE PostedDate LIKE '__/__/____'
                AND PostedDate_normalized IS NULL
            ''', "MM/DD/YYYY"),
            
            # M/D/YYYY or MM/D/YYYY formats
            ('''
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    printf('%04d-%02d-%02d',
                        CAST(substr(PostedDate, -4) AS INTEGER),
                        CAST(substr(PostedDate, 1, instr(PostedDate, '/') - 1) AS INTEGER),
                        CAST(substr(PostedDate, 
                            instr(PostedDate, '/') + 1,
                            instr(substr(PostedDate, instr(PostedDate, '/') + 1), '/') - 1
                        ) AS INTEGER)
                    )
                WHERE PostedDate LIKE '%/%/____'
                AND PostedDate_normalized IS NULL
                AND LENGTH(PostedDate) >= 8
            ''', "Variable M/D/YYYY"),
        ]
        
        total_normalized = 0
        for query, format_name in normalization_queries:
            try:
                cur.execute(query)
                updated = cur.rowcount
                if updated > 0:
                    print(f"  Normalized {updated:,} dates in {format_name} format")
                    total_normalized += updated
                    conn.commit()
            except Exception as e:
                print(f"  Warning in {format_name}: {e}")
        
        # Handle any remaining dates with pandas
        print("\n🔧 Step 3: Processing remaining dates with pandas...")
        cur.execute("""
            SELECT NoticeID, PostedDate 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            AND PostedDate != ''
            AND PostedDate_normalized IS NULL
            LIMIT 10000
        """)
        
        remaining = cur.fetchall()
        if remaining:
            print(f"  Processing {len(remaining)} remaining dates...")
            updates = []
            for notice_id, date_str in remaining:
                try:
                    # Try pandas date parsing
                    parsed = pd.to_datetime(date_str, errors='coerce')
                    if pd.notna(parsed):
                        normalized = parsed.strftime('%Y-%m-%d')
                        updates.append((normalized, notice_id))
                except:
                    pass
            
            if updates:
                cur.executemany("""
                    UPDATE opportunities 
                    SET PostedDate_normalized = ? 
                    WHERE NoticeID = ?
                """, updates)
                conn.commit()
                print(f"  ✅ Normalized {len(updates)} additional dates")
        
        # Step 4: Create/recreate indexes
        print("\n🔧 Step 4: Creating optimized indexes...")
        
        # Drop old indexes if they exist
        cur.execute("DROP INDEX IF EXISTS idx_posted_date_normalized")
        cur.execute("DROP INDEX IF EXISTS idx_posted_date")
        cur.execute("DROP INDEX IF EXISTS idx_country_date")
        
        # Create new indexes
        cur.execute('''
            CREATE INDEX idx_posted_date_normalized 
            ON opportunities(PostedDate_normalized)
        ''')
        cur.execute('''
            CREATE INDEX idx_posted_date 
            ON opportunities(PostedDate)
        ''')
        cur.execute('''
            CREATE INDEX idx_country_date 
            ON opportunities(PopCountry, PostedDate_normalized DESC)
        ''')
        conn.commit()
        print("✅ Indexes created")
        
        # Step 5: Verify the fix
        print("\n🔍 Step 5: Verifying the fix...")
        
        # Check how many dates were normalized
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate_normalized IS NOT NULL
        """)
        normalized_count = cur.fetchone()[0]
        
        # Test date queries
        today = datetime.now().date()
        test_periods = [
            ("Last 7 days", (today - timedelta(days=7)).isoformat(), today.isoformat()),
            ("Last 30 days", (today - timedelta(days=30)).isoformat(), today.isoformat()),
            ("Last year", (today - timedelta(days=365)).isoformat(), today.isoformat()),
            ("Last 5 years", (today - timedelta(days=1825)).isoformat(), today.isoformat()),
        ]
        
        print(f"\n✅ Normalized {normalized_count:,}/{total_records:,} dates ({normalized_count/total_records*100:.1f}%)")
        print("\nRecords by time period:")
        
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
        
        # Optimize database
        print("\n🔧 Step 6: Optimizing database...")
        cur.execute("ANALYZE")
        conn.commit()
        
        print("\n" + "="*60)
        print("DATABASE FIX COMPLETE!")
        print("="*60)
        print(f"✅ Total records: {total_records:,}")
        print(f"✅ Normalized dates: {normalized_count:,}")
        print(f"✅ Date range: {min_date} to {max_date}")
        print(f"✅ Backup saved at: {backup_path}")
        print("\n✨ Your dashboard will now show data in all tabs!")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    success = fix_database_completely()
    sys.exit(0 if success else 1)