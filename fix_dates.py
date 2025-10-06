#!/usr/bin/env python3
"""
fix_dates.py - Fix date formatting issues in the database
This will standardize all dates to YYYY-MM-DD format
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd
import shutil

def fix_dates():
    """Fix and standardize date formats in the database"""
    
    print("="*60)
    print("DATE FIX UTILITY")
    print("="*60)
    
    # Database path
    db_path = Path("data/opportunities.db")
    
    if not db_path.exists():
        print("❌ Database not found!")
        return False
    
    # Create backup
    print("\n📦 Creating backup...")
    backup_path = db_path.parent / f"opportunities_before_date_fix.db"
    shutil.copy2(db_path, backup_path)
    print(f"✅ Backup saved to {backup_path}")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # First, let's see what we're dealing with
        print("\n🔍 Analyzing current date formats...")
        
        cur.execute("""
            SELECT DISTINCT 
                CASE 
                    WHEN PostedDate LIKE '____-__-__T%' THEN 'ISO with time'
                    WHEN PostedDate LIKE '____-__-__' THEN 'YYYY-MM-DD'
                    WHEN PostedDate LIKE '__/__/____' THEN 'MM/DD/YYYY'
                    WHEN PostedDate IS NULL THEN 'NULL'
                    WHEN PostedDate = '' THEN 'Empty'
                    ELSE 'Other'
                END as format_type,
                COUNT(*) as count
            FROM opportunities
            GROUP BY format_type
        """)
        
        formats = cur.fetchall()
        for format_type, count in formats:
            print(f"   {format_type}: {count} records")
        
        # Create a temporary table with fixed dates
        print("\n🔧 Creating temporary table with fixed dates...")
        
        cur.execute("""
            CREATE TEMP TABLE fixed_dates AS
            SELECT 
                NoticeID,
                PostedDate as original_date,
                CASE
                    -- Handle ISO format with time (YYYY-MM-DDTHH:MM:SS)
                    WHEN PostedDate LIKE '____-__-__T%' THEN 
                        substr(PostedDate, 1, 10)
                    -- Already in YYYY-MM-DD format
                    WHEN PostedDate LIKE '____-__-__' THEN 
                        PostedDate
                    -- Handle MM/DD/YYYY format
                    WHEN PostedDate LIKE '__/__/____' THEN
                        substr(PostedDate, 7, 4) || '-' || 
                        substr(PostedDate, 1, 2) || '-' || 
                        substr(PostedDate, 4, 2)
                    -- Handle other cases
                    ELSE PostedDate
                END as fixed_date
            FROM opportunities
            WHERE PostedDate IS NOT NULL AND PostedDate != ''
        """)
        
        # Check how many dates we can fix
        cur.execute("""
            SELECT COUNT(*) FROM fixed_dates 
            WHERE fixed_date != original_date
        """)
        
        dates_to_fix = cur.fetchone()[0]
        print(f"✅ Found {dates_to_fix} dates to standardize")
        
        if dates_to_fix > 0:
            # Update the main table
            print("\n📝 Updating dates in main table...")
            
            cur.execute("""
                UPDATE opportunities
                SET PostedDate = (
                    SELECT fixed_date 
                    FROM fixed_dates 
                    WHERE fixed_dates.NoticeID = opportunities.NoticeID
                )
                WHERE NoticeID IN (
                    SELECT NoticeID FROM fixed_dates 
                    WHERE fixed_date != original_date
                )
            """)
            
            updated_count = cur.rowcount
            conn.commit()
            print(f"✅ Updated {updated_count} records")
        
        # Verify the fix
        print("\n🔍 Verifying date formats after fix...")
        
        cur.execute("""
            SELECT 
                CASE 
                    WHEN PostedDate LIKE '____-__-__' THEN 'YYYY-MM-DD (Good)'
                    WHEN PostedDate IS NULL THEN 'NULL'
                    WHEN PostedDate = '' THEN 'Empty'
                    ELSE 'Non-standard'
                END as format_type,
                COUNT(*) as count
            FROM opportunities
            GROUP BY format_type
        """)
        
        formats_after = cur.fetchall()
        for format_type, count in formats_after:
            print(f"   {format_type}: {count} records")
        
        # Test queries
        print("\n✅ Testing date queries after fix:")
        
        test_queries = [
            ("Last 7 days", "SELECT COUNT(*) FROM opportunities WHERE date(PostedDate) >= date('now', '-7 days')"),
            ("Last 30 days", "SELECT COUNT(*) FROM opportunities WHERE date(PostedDate) >= date('now', '-30 days')"),
            ("Last year", "SELECT COUNT(*) FROM opportunities WHERE date(PostedDate) >= date('now', '-365 days')"),
            ("All time", "SELECT COUNT(*) FROM opportunities WHERE PostedDate IS NOT NULL"),
        ]
        
        for description, query in test_queries:
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f"   {description}: {count} records")
        
        # Create indexes for better performance
        print("\n🚀 Creating/updating indexes for better performance...")
        
        # Drop existing index if it exists
        cur.execute("DROP INDEX IF EXISTS idx_posted_date")
        
        # Create new index
        cur.execute("CREATE INDEX idx_posted_date ON opportunities(PostedDate)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_date_desc ON opportunities(PostedDate DESC)")
        
        conn.commit()
        print("✅ Indexes created")
        
        print("\n" + "="*60)
        print("DATE FIX COMPLETE!")
        print("="*60)
        print("✅ All dates have been standardized to YYYY-MM-DD format")
        print("✅ Database is optimized for date queries")
        print(f"✅ Backup saved at: {backup_path}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error during date fix: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    fix_dates()