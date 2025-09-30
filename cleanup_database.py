#!/usr/bin/env python3
"""
cleanup_database.py - Remove all non-African country entries from database
Fixed version that properly handles VACUUM command
"""

import sqlite3
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sam_utils import CountryManager

def cleanup_database():
    """Remove all non-African country entries"""
    
    print("="*60)
    print("DATABASE CLEANUP - REMOVING NON-AFRICAN COUNTRIES")
    print("="*60)
    
    # Database path
    db_path = Path("data/opportunities.db")
    
    if not db_path.exists():
        print("❌ Database not found!")
        return False
    
    # Initialize country manager
    country_mgr = CountryManager()
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    try:
        # Get initial count
        cur.execute("SELECT COUNT(*) FROM opportunities")
        initial_count = cur.fetchone()[0]
        print(f"Initial records: {initial_count:,}")
        
        # Get all valid African country codes with proper format
        valid_countries = set()
        for country_name, iso_code in country_mgr.AFRICAN_COUNTRIES.items():
            # Add the formatted version: "COUNTRY NAME (ISO)"
            valid_countries.add(f"{country_name} ({iso_code})")
            # Also add just the ISO code for safety
            valid_countries.add(iso_code)
        
        print(f"\nValid African countries: {len(country_mgr.AFRICAN_COUNTRIES)}")
        
        # First, let's see what countries are in the database
        cur.execute("""
            SELECT DISTINCT PopCountry, COUNT(*) as cnt 
            FROM opportunities 
            WHERE PopCountry IS NOT NULL 
            GROUP BY PopCountry 
            ORDER BY cnt DESC
        """)
        
        all_countries = cur.fetchall()
        
        print(f"\nFound {len(all_countries)} unique country values in database")
        
        # Identify non-African countries
        non_african = []
        african = []
        
        for country, count in all_countries:
            if country and str(country).strip():
                # Check if it's a valid African country
                is_african = False
                
                # Check exact match first
                if country in valid_countries:
                    is_african = True
                else:
                    # Check if it contains an African country ISO code
                    for iso in country_mgr.iso3_codes:
                        if iso in str(country).upper():
                            is_african = True
                            break
                    
                    # Check using the country manager's method
                    if not is_african:
                        is_african = country_mgr.is_african_country(country)
                
                if is_african:
                    african.append((country, count))
                else:
                    non_african.append((country, count))
        
        print(f"\n✅ African countries: {len(african)}")
        for country, count in african[:10]:  # Show first 10
            print(f"   {country}: {count} records")
        if len(african) > 10:
            print(f"   ... and {len(african)-10} more")
        
        print(f"\n❌ Non-African countries to remove: {len(non_african)}")
        for country, count in non_african[:10]:  # Show first 10
            print(f"   {country}: {count} records")
        if len(non_african) > 10:
            print(f"   ... and {len(non_african)-10} more")
        
        if non_african:
            # Create a list of countries to remove
            countries_to_remove = [country for country, _ in non_african]
            
            # Create backup first
            print("\n📦 Creating backup...")
            backup_path = db_path.parent / f"opportunities_before_cleanup.db"
            import shutil
            shutil.copy2(db_path, backup_path)
            print(f"✅ Backup saved to {backup_path}")
            
            # Remove non-African countries
            print("\n🧹 Removing non-African countries...")
            
            # Use parameterized query with placeholders
            placeholders = ','.join(['?' for _ in countries_to_remove])
            delete_query = f"""
                DELETE FROM opportunities 
                WHERE PopCountry IN ({placeholders})
            """
            
            cur.execute(delete_query, countries_to_remove)
            deleted_count = cur.rowcount
            conn.commit()
            
            print(f"✅ Removed {deleted_count:,} records")
            
            # Also remove records with NULL or empty PopCountry
            cur.execute("""
                DELETE FROM opportunities 
                WHERE PopCountry IS NULL 
                   OR PopCountry = '' 
                   OR PopCountry = 'nan'
                   OR PopCountry = 'None'
            """)
            
            null_deleted = cur.rowcount
            if null_deleted > 0:
                conn.commit()
                print(f"✅ Removed {null_deleted:,} records with empty country")
            
            # Get final count
            cur.execute("SELECT COUNT(*) FROM opportunities")
            final_count = cur.fetchone()[0]
            
            # Close current connection before VACUUM
            conn.close()
            
            # Optimize database with new connection (VACUUM requires this)
            print("\n🔧 Optimizing database...")
            try:
                # Open new connection for VACUUM
                conn_vacuum = sqlite3.connect(db_path)
                conn_vacuum.execute("VACUUM")
                conn_vacuum.execute("ANALYZE")
                conn_vacuum.close()
                print("✅ Database optimized")
            except Exception as e:
                print(f"⚠️  Optimization skipped (non-critical): {e}")
            
            # Reopen connection for final statistics
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            
            print("\n" + "="*60)
            print("CLEANUP COMPLETE!")
            print("="*60)
            print(f"Initial records: {initial_count:,}")
            print(f"Records removed: {initial_count - final_count:,}")
            print(f"Final records: {final_count:,}")
            print(f"Backup saved at: {backup_path}")
            
            # Show final country distribution
            print("\n📊 Final country distribution (Top 10):")
            cur.execute("""
                SELECT PopCountry, COUNT(*) as cnt 
                FROM opportunities 
                WHERE PopCountry IS NOT NULL 
                GROUP BY PopCountry 
                ORDER BY cnt DESC 
                LIMIT 10
            """)
            
            for country, count in cur.fetchall():
                print(f"   {country}: {count:,} records")
            
        else:
            print("\n✅ Database already clean - contains only African countries!")
            
            # Get final count
            cur.execute("SELECT COUNT(*) FROM opportunities")
            final_count = cur.fetchone()[0]
            print(f"Total records: {final_count:,}")
        
        conn.close()
        print("\n✅ Database cleanup successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    cleanup_database()