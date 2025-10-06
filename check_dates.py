#!/usr/bin/env python3
"""
check_dates.py - Diagnostic script to check date formatting in database
This will help us understand why dates aren't working properly
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

def check_dates():
    """Check date formats and data in the database"""
    
    print("="*60)
    print("DATE DIAGNOSTIC CHECK")
    print("="*60)
    
    # Database path
    db_path = Path("data/opportunities.db")
    
    if not db_path.exists():
        print("❌ Database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # 1. Check sample of PostedDate values
        print("\n1. Sample PostedDate values from database:")
        print("-" * 40)
        cur.execute("""
            SELECT PostedDate, COUNT(*) as count 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            GROUP BY PostedDate 
            ORDER BY PostedDate DESC 
            LIMIT 10
        """)
        
        for date_val, count in cur.fetchall():
            print(f"   {date_val}: {count} records")
        
        # 2. Check date format patterns
        print("\n2. Date format patterns:")
        print("-" * 40)
        cur.execute("""
            SELECT PostedDate 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            LIMIT 100
        """)
        
        dates = [row[0] for row in cur.fetchall()]
        formats_found = set()
        
        for date_str in dates[:20]:  # Check first 20
            if '/' in str(date_str):
                formats_found.add("MM/DD/YYYY format")
            elif '-' in str(date_str):
                formats_found.add("YYYY-MM-DD format")
            if 'T' in str(date_str):
                formats_found.add("ISO format with time")
            if len(str(date_str)) > 10:
                formats_found.add("Includes time component")
        
        for format_type in formats_found:
            print(f"   Found: {format_type}")
        
        # 3. Test date filtering
        print("\n3. Testing date queries:")
        print("-" * 40)
        
        # Get today's date
        today = datetime.now()
        
        # Test different date query methods
        queries = [
            ("Last 7 days (date function)", """
                SELECT COUNT(*) FROM opportunities 
                WHERE date(PostedDate) >= date('now', '-7 days')
            """),
            ("Last 30 days (date function)", """
                SELECT COUNT(*) FROM opportunities 
                WHERE date(PostedDate) >= date('now', '-30 days')
            """),
            ("Last year (date function)", """
                SELECT COUNT(*) FROM opportunities 
                WHERE date(PostedDate) >= date('now', '-365 days')
            """),
            ("Last 7 days (string comparison)", f"""
                SELECT COUNT(*) FROM opportunities 
                WHERE PostedDate >= '{(today - timedelta(days=7)).strftime('%Y-%m-%d')}'
            """),
            ("Last 30 days (string comparison)", f"""
                SELECT COUNT(*) FROM opportunities 
                WHERE PostedDate >= '{(today - timedelta(days=30)).strftime('%Y-%m-%d')}'
            """),
        ]
        
        for description, query in queries:
            try:
                cur.execute(query)
                count = cur.fetchone()[0]
                print(f"   {description}: {count} records")
            except Exception as e:
                print(f"   {description}: ERROR - {e}")
        
        # 4. Check for NULL or invalid dates
        print("\n4. Data quality check:")
        print("-" * 40)
        
        cur.execute("SELECT COUNT(*) FROM opportunities WHERE PostedDate IS NULL")
        null_count = cur.fetchone()[0]
        print(f"   NULL PostedDate: {null_count} records")
        
        cur.execute("SELECT COUNT(*) FROM opportunities WHERE PostedDate = ''")
        empty_count = cur.fetchone()[0]
        print(f"   Empty PostedDate: {empty_count} records")
        
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total_count = cur.fetchone()[0]
        print(f"   Total records: {total_count}")
        
        # 5. Get most recent and oldest dates
        print("\n5. Date range in database:")
        print("-" * 40)
        
        cur.execute("""
            SELECT MIN(PostedDate), MAX(PostedDate) 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL AND PostedDate != ''
        """)
        
        min_date, max_date = cur.fetchone()
        print(f"   Oldest date: {min_date}")
        print(f"   Newest date: {max_date}")
        
        # 6. Try parsing with pandas
        print("\n6. Testing pandas date parsing:")
        print("-" * 40)
        
        # Get a sample of dates
        cur.execute("""
            SELECT PostedDate 
            FROM opportunities 
            WHERE PostedDate IS NOT NULL 
            LIMIT 10
        """)
        
        sample_dates = [row[0] for row in cur.fetchall()]
        
        for date_str in sample_dates[:5]:
            try:
                parsed = pd.to_datetime(date_str)
                print(f"   '{date_str}' -> {parsed}")
            except Exception as e:
                print(f"   '{date_str}' -> PARSE ERROR: {e}")
        
        # 7. Distribution by time period
        print("\n7. Records by time period:")
        print("-" * 40)
        
        periods = [
            ("Today", "0"),
            ("Last 7 days", "-7"),
            ("Last 30 days", "-30"),
            ("Last 90 days", "-90"),
            ("Last 365 days", "-365"),
            ("Last 5 years", "-1825"),
        ]
        
        for period_name, days in periods:
            cur.execute(f"""
                SELECT COUNT(*) FROM opportunities 
                WHERE date(PostedDate) >= date('now', '{days} days')
                AND PostedDate IS NOT NULL
            """)
            count = cur.fetchone()[0]
            print(f"   {period_name}: {count} records")
        
        print("\n" + "="*60)
        print("DIAGNOSTIC COMPLETE")
        print("="*60)
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error during diagnostic: {e}")
        conn.close()

if __name__ == "__main__":
    check_dates()