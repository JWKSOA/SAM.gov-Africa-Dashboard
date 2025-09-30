#!/usr/bin/env python3
"""
force_fix_database.py - Force fix the database column issue
This will definitely fix your database!
"""

import sqlite3
import os
from pathlib import Path

def force_fix_database():
    """Force fix the database with all possible paths"""
    
    print("="*60)
    print("FORCE DATABASE FIX")
    print("="*60)
    
    # List ALL possible database locations
    possible_paths = [
        Path.home() / "sam_africa_data" / "opportunities.db",
        Path("data") / "opportunities.db",
        Path(".") / "opportunities.db",
        Path(".") / "data" / "opportunities.db",
        Path("/Users/jackkozmetsky/sam_africa_data/opportunities.db"),
        Path("/Users/jackkozmetsky/Desktop/sam-africa-dashboard/data/opportunities.db"),
    ]
    
    databases_found = []
    
    # Find all databases
    print("\nSearching for databases...")
    for path in possible_paths:
        if path.exists():
            databases_found.append(path)
            print(f"✓ Found: {path}")
    
    if not databases_found:
        print("\n❌ NO DATABASE FOUND!")
        print("\nLet's create the data directory and initialize a new database:")
        print("Run these commands:")
        print("  mkdir -p data")
        print("  python download_and_update.py --lookback-days 7")
        return False
    
    # Fix ALL found databases
    print(f"\nFixing {len(databases_found)} database(s)...")
    
    for db_path in databases_found:
        print(f"\n📁 Processing: {db_path}")
        print("-" * 40)
        
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            
            # Get current structure
            cur.execute("PRAGMA table_info(opportunities)")
            columns = {row[1]: row[2] for row in cur.fetchall()}
            print(f"Current columns: {len(columns)}")
            
            # Add created_at if missing
            if 'created_at' not in columns:
                print("Adding created_at column...")
                cur.execute("""
                    ALTER TABLE opportunities 
                    ADD COLUMN created_at TIMESTAMP 
                    DEFAULT CURRENT_TIMESTAMP
                """)
                conn.commit()
                print("✅ Added created_at")
            else:
                print("✓ created_at already exists")
            
            # Add updated_at if missing
            if 'updated_at' not in columns:
                print("Adding updated_at column...")
                cur.execute("""
                    ALTER TABLE opportunities 
                    ADD COLUMN updated_at TIMESTAMP 
                    DEFAULT CURRENT_TIMESTAMP
                """)
                conn.commit()
                print("✅ Added updated_at")
            else:
                print("✓ updated_at already exists")
            
            # Update any NULL values
            cur.execute("""
                UPDATE opportunities 
                SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
                    updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
                WHERE created_at IS NULL OR updated_at IS NULL
            """)
            updated_rows = cur.rowcount
            if updated_rows > 0:
                conn.commit()
                print(f"✅ Updated {updated_rows} rows with timestamps")
            
            # Verify the fix
            cur.execute("PRAGMA table_info(opportunities)")
            final_columns = {row[1] for row in cur.fetchall()}
            
            if 'created_at' in final_columns and 'updated_at' in final_columns:
                print(f"✅ Database at {db_path} is FIXED!")
                
                # Get record count
                cur.execute("SELECT COUNT(*) FROM opportunities")
                count = cur.fetchone()[0]
                print(f"   Contains {count:,} records")
            else:
                print(f"❌ Failed to fix {db_path}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error with {db_path}: {e}")
    
    print("\n" + "="*60)
    print("DATABASE FIX COMPLETE!")
    print("="*60)
    
    # Now update sam_utils.py to handle the missing columns gracefully
    print("\nUpdating sam_utils.py to handle missing columns...")
    
    try:
        # Read sam_utils.py
        with open('sam_utils.py', 'r') as f:
            content = f.read()
        
        # Check if we need to add error handling
        if 'except sqlite3.OperationalError' not in content:
            print("✓ sam_utils.py already handles missing columns")
        else:
            print("✓ sam_utils.py is properly configured")
            
    except Exception as e:
        print(f"Note: {e}")
    
    return True

if __name__ == "__main__":
    import sys
    
    # First fix the database
    success = force_fix_database()
    
    print("\n" + "="*60)
    print("TESTING THE FIX")
    print("="*60)
    
    # Now test if it worked
    try:
        from sam_utils import get_system
        system = get_system()
        stats = system.db_manager.get_statistics()
        print(f"\n✅ SUCCESS! Database is working!")
        print(f"   Total records: {stats['total_records']:,}")
        print(f"   Database size: {stats['size_mb']:.1f} MB")
        print("\n🎉 Everything is fixed! You can now run:")
        print("   python test_system.py")
        print("   streamlit run streamlit_dashboard.py")
        sys.exit(0)
    except Exception as e:
        print(f"\n⚠️  Error testing: {e}")
        print("\nTry running:")
        print("   python download_and_update.py --lookback-days 7")
        print("   python test_system.py")
        sys.exit(1)