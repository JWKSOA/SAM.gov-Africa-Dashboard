#!/usr/bin/env python3
"""
cleanup_non_african.py - Remove non-African countries from the existing database
This script will clean up the database without re-downloading all the data
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import get_system, CountryManager

def cleanup_non_african_countries():
    """Remove all non-African countries from the database"""
    
    print("="*60)
    print("SAM.gov Database Cleanup - Removing Non-African Countries")
    print("="*60)
    
    # Initialize system
    system = get_system()
    country_manager = CountryManager()
    
    if not system.config.db_path.exists():
        print("❌ Database not found!")
        return False
    
    # Get initial statistics
    with system.db_manager.get_connection() as conn:
        cur = conn.cursor()
        
        # Get total records before cleanup
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total_before = cur.fetchone()[0]
        print(f"📊 Total records before cleanup: {total_before:,}")
        
        # Get all unique countries in database
        cur.execute("SELECT DISTINCT PopCountry FROM opportunities WHERE PopCountry IS NOT NULL")
        all_countries = [row[0] for row in cur.fetchall()]
        
        print(f"📍 Found {len(all_countries)} unique country values")
        
        # Identify non-African countries
        non_african = []
        african = []
        
        for country_value in all_countries:
            if country_manager.is_african_country(country_value):
                african.append(country_value)
            else:
                non_african.append(country_value)
        
        print(f"\n✅ African countries: {len(african)}")
        print(f"❌ Non-African countries to remove: {len(non_african)}")
        
        if non_african:
            print("\nNon-African countries found:")
            for country in sorted(non_african)[:20]:  # Show first 20
                # Count records for this country
                cur.execute("SELECT COUNT(*) FROM opportunities WHERE PopCountry = ?", (country,))
                count = cur.fetchone()[0]
                print(f"  - {country}: {count:,} records")
            
            if len(non_african) > 20:
                print(f"  ... and {len(non_african) - 20} more")
            
            # Remove non-African countries
            print("\n🗑️  Removing non-African countries...")
            
            total_removed = 0
            for country in non_african:
                cur.execute("DELETE FROM opportunities WHERE PopCountry = ?", (country,))
                removed = cur.rowcount
                total_removed += removed
                if removed > 0:
                    print(f"  Removed {removed:,} records for {country}")
            
            # Commit changes
            conn.commit()
            
            # Get final count
            cur.execute("SELECT COUNT(*) FROM opportunities")
            total_after = cur.fetchone()[0]
            
            print(f"\n✅ Cleanup complete!")
            print(f"  Records before: {total_before:,}")
            print(f"  Records removed: {total_removed:,}")
            print(f"  Records after: {total_after:,}")
            
            # Show final African countries
            print("\n🌍 Remaining African countries in database:")
            cur.execute("""
                SELECT PopCountry, COUNT(*) as cnt 
                FROM opportunities 
                WHERE PopCountry IS NOT NULL 
                GROUP BY PopCountry 
                ORDER BY cnt DESC
                LIMIT 20
            """)
            
            for country, count in cur.fetchall():
                print(f"  {country}: {count:,}")
            
            # Optimize database
            print("\n🔧 Optimizing database...")
            cur.execute("ANALYZE")
            conn.commit()
            
        else:
            print("\n✅ No non-African countries found - database is already clean!")
    
    # Vacuum database (requires separate connection)
    print("📦 Vacuuming database to reclaim space...")
    conn = sqlite3.connect(str(system.config.db_path))
    conn.execute("VACUUM")
    conn.close()
    
    # Show database size
    size_mb = system.config.db_path.stat().st_size / (1024 * 1024)
    print(f"\n💾 Database size: {size_mb:.1f} MB")
    
    print("\n✅ All done! Your database now contains only African countries.")
    return True

if __name__ == "__main__":
    print(f"Starting cleanup at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if cleanup_non_african_countries():
        print("\n✨ Success! You can now refresh your dashboard.")
        print("\nNext steps:")
        print("1. Test locally: streamlit run streamlit_dashboard.py")
        print("2. Commit changes: git add -A && git commit -m 'Fix: Remove non-African countries'")
        print("3. Push to GitHub: git push")
    else:
        print("\n❌ Cleanup failed. Check the error messages above.")