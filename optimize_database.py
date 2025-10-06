#!/usr/bin/env python3
"""
optimize_database.py - Quick script to optimize the SAM.gov database
Run this after bootstrap to optimize and get statistics
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

def optimize_database():
    """Optimize and analyze the database"""
    db_path = Path("data/opportunities.db")
    
    if not db_path.exists():
        print("❌ Database not found at data/opportunities.db")
        return False
    
    print("🔧 Optimizing database...")
    
    # First connection for ANALYZE
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Normalize any remaining dates
    print("  Normalizing dates...")
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
    
    if cur.rowcount > 0:
        print(f"  ✅ Normalized {cur.rowcount} dates")
    
    # Update statistics
    print("  Analyzing tables...")
    cur.execute("ANALYZE")
    conn.commit()
    conn.close()
    
    # Separate connection for VACUUM (requires exclusive access)
    print("  Vacuuming database...")
    conn = sqlite3.connect(str(db_path))
    conn.execute("VACUUM")
    conn.close()
    
    print("✅ Database optimized!")
    
    # Get and display statistics
    print("\n📊 Database Statistics:")
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Total records
    cur.execute("SELECT COUNT(*) FROM opportunities")
    total = cur.fetchone()[0]
    print(f"  Total records: {total:,}")
    
    # Active records
    cur.execute("SELECT COUNT(*) FROM opportunities WHERE Active = 'Yes'")
    active = cur.fetchone()[0]
    print(f"  Active opportunities: {active:,}")
    
    # Recent records
    today = datetime.now().date().isoformat()
    
    for days, label in [(7, "Last 7 days"), (30, "Last 30 days"), 
                        (365, "Last year")]:
        cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
        cur.execute("""
            SELECT COUNT(*) FROM opportunities 
            WHERE PostedDate_normalized >= ? AND PostedDate_normalized <= ?
        """, (cutoff, today))
        count = cur.fetchone()[0]
        print(f"  {label}: {count:,}")
    
    # Top countries
    print("\n🌍 Top 10 African Countries:")
    cur.execute("""
        SELECT PopCountry, COUNT(*) as cnt 
        FROM opportunities 
        WHERE PopCountry IS NOT NULL 
        GROUP BY PopCountry 
        ORDER BY cnt DESC 
        LIMIT 10
    """)
    
    for country, count in cur.fetchall():
        print(f"  {country}: {count:,}")
    
    # Database size
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"\n💾 Database size: {size_mb:.1f} MB")
    
    conn.close()
    return True

if __name__ == "__main__":
    print("="*60)
    print("SAM.gov Database Optimizer")
    print("="*60)
    
    if optimize_database():
        print("\n✅ Optimization complete! Your database is ready to use.")
        print("\nNext steps:")
        print("1. Test locally: streamlit run streamlit_dashboard.py")
        print("2. Push to GitHub: git add -A && git commit -m 'Add historical data' && git push")
    else:
        print("\n❌ Optimization failed. Check the error messages above.")