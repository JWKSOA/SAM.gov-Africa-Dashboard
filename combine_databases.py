#!/usr/bin/env python3
"""
combine_databases.py - Combine split databases back into one
Fixed version that properly handles column names with special characters
"""

import sqlite3
import os
from pathlib import Path
import time

def combine_databases():
    """Combine archive and recent databases into one"""
    
    print("="*60)
    print("DATABASE COMBINER - FIXED VERSION")
    print("="*60)
    
    # Paths
    archive_db = Path("data/opportunities_archive.db")
    recent_db = Path("data/opportunities_recent.db")
    combined_db = Path("data/opportunities.db")
    
    # Check if already combined
    if combined_db.exists():
        # Check if it's a real database (not Git LFS pointer)
        if combined_db.stat().st_size > 1000000:  # > 1MB
            print(f"✅ Combined database already exists ({combined_db.stat().st_size / 1024 / 1024:.1f} MB)")
            
            # Verify it's valid
            try:
                conn = sqlite3.connect(combined_db)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM opportunities")
                count = cur.fetchone()[0]
                conn.close()
                print(f"   Contains {count:,} records")
                return True
            except:
                print("   But it appears to be invalid, recreating...")
    
    # Check if split databases exist
    if not archive_db.exists() or not recent_db.exists():
        print("❌ Split databases not found!")
        print(f"   Archive exists: {archive_db.exists()}")
        print(f"   Recent exists: {recent_db.exists()}")
        return False
    
    print(f"Archive database: {archive_db.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"Recent database: {recent_db.stat().st_size / 1024 / 1024:.1f} MB")
    
    start_time = time.time()
    
    # Create combined database
    print("\n🔄 Combining databases...")
    
    # Connect to all databases
    conn_archive = sqlite3.connect(archive_db)
    conn_recent = sqlite3.connect(recent_db)
    conn_combined = sqlite3.connect(combined_db)
    
    cur_archive = conn_archive.cursor()
    cur_recent = conn_recent.cursor()
    cur_combined = conn_combined.cursor()
    
    # Get schema from archive (they should be identical)
    cur_archive.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='opportunities'")
    table_schema = cur_archive.fetchone()[0]
    
    # Create table in combined database
    conn_combined.execute(table_schema)
    
    # Get column names properly
    cur_archive.execute("PRAGMA table_info(opportunities)")
    columns_info = cur_archive.fetchall()
    columns = [col[1] for col in columns_info]
    
    # Properly quote column names (especially those with special characters)
    columns_quoted = [f'"{col}"' for col in columns]
    columns_str = ','.join(columns_quoted)
    placeholders = ','.join(['?' for _ in columns])
    
    print(f"Found {len(columns)} columns in the database")
    
    # Copy archive data
    print("📂 Copying archive records...")
    cur_archive.execute(f"SELECT {columns_str} FROM opportunities")
    
    batch_size = 5000
    inserted_archive = 0
    
    while True:
        rows = cur_archive.fetchmany(batch_size)
        if not rows:
            break
        
        cur_combined.executemany(
            f"INSERT OR IGNORE INTO opportunities ({columns_str}) VALUES ({placeholders})", 
            rows
        )
        inserted_archive += len(rows)
        if inserted_archive % 10000 == 0:
            print(f"   Copied {inserted_archive:,} archive records...")
            conn_combined.commit()  # Commit periodically
    
    conn_combined.commit()
    print(f"✅ Copied {inserted_archive:,} archive records")
    
    # Copy recent data
    print("📂 Copying recent records...")
    cur_recent.execute(f"SELECT {columns_str} FROM opportunities")
    
    inserted_recent = 0
    
    while True:
        rows = cur_recent.fetchmany(batch_size)
        if not rows:
            break
        
        cur_combined.executemany(
            f"INSERT OR IGNORE INTO opportunities ({columns_str}) VALUES ({placeholders})", 
            rows
        )
        inserted_recent += len(rows)
        if inserted_recent % 10000 == 0:
            print(f"   Copied {inserted_recent:,} recent records...")
            conn_combined.commit()  # Commit periodically
    
    conn_combined.commit()
    print(f"✅ Copied {inserted_recent:,} recent records")
    
    # Copy all indexes
    print("📂 Creating indexes...")
    cur_archive.execute("SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
    indexes = cur_archive.fetchall()
    for index in indexes:
        try:
            conn_combined.execute(index[0])
        except sqlite3.OperationalError:
            pass  # Index might already exist
    
    conn_combined.commit()
    
    # Get final count
    cur_combined.execute("SELECT COUNT(*) FROM opportunities")
    total_records = cur_combined.fetchone()[0]
    
    # Close all connections
    conn_archive.close()
    conn_recent.close()
    conn_combined.close()
    
    elapsed = time.time() - start_time
    combined_size = combined_db.stat().st_size / (1024 * 1024)
    
    print("\n" + "="*60)
    print("COMBINATION COMPLETE!")
    print("="*60)
    print(f"Total records: {total_records:,}")
    print(f"Combined size: {combined_size:.1f} MB")
    print(f"Time elapsed: {elapsed:.1f} seconds")
    print(f"✅ Database ready at: {combined_db}")
    
    return True

if __name__ == "__main__":
    success = combine_databases()
    if not success:
        print("\n❌ Failed to combine databases!")
        import sys
        sys.exit(1)