#!/usr/bin/env python3
"""
split_database.py - Split large database into two manageable parts
Fixed version that properly handles databases with only recent data
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timedelta

def split_database():
    """Split database into two parts: archive and recent"""
    
    print("="*60)
    print("DATABASE SPLITTER - FIXED VERSION")
    print("="*60)
    
    # Paths
    original_db = Path("data/opportunities.db")
    archive_db = Path("data/opportunities_archive.db")
    recent_db = Path("data/opportunities_recent.db")
    backup_db = Path("data/opportunities_backup.db")
    
    # First, restore from backup if original is corrupted
    if backup_db.exists() and original_db.exists():
        conn_test = sqlite3.connect(original_db)
        cur_test = conn_test.cursor()
        cur_test.execute("SELECT COUNT(*) FROM opportunities")
        test_count = cur_test.fetchone()[0]
        conn_test.close()
        
        if test_count == 0:
            print("⚠️  Original database is empty, restoring from backup...")
            shutil.copy2(backup_db, original_db)
            print("✅ Restored from backup")
    
    if not original_db.exists():
        print(f"❌ Database not found at {original_db}")
        return False
    
    # Get original size
    original_size = original_db.stat().st_size / (1024 * 1024)
    print(f"Original database size: {original_size:.1f} MB")
    
    # Connect to original database
    conn_orig = sqlite3.connect(original_db)
    cur_orig = conn_orig.cursor()
    
    # Get total record count
    cur_orig.execute("SELECT COUNT(*) FROM opportunities")
    total_records = cur_orig.fetchone()[0]
    print(f"Total records: {total_records:,}")
    
    if total_records == 0:
        print("❌ Database has no records!")
        conn_orig.close()
        return False
    
    # Get date range of data
    cur_orig.execute("""
        SELECT MIN(PostedDate), MAX(PostedDate) 
        FROM opportunities 
        WHERE PostedDate IS NOT NULL AND PostedDate != ''
    """)
    min_date, max_date = cur_orig.fetchone()
    print(f"Date range: {min_date} to {max_date}")
    
    # Strategy: Split by record count to ensure both files are under 100MB
    # We'll put approximately half the records in each file
    target_archive_count = total_records // 2
    
    print(f"\nSplitting into two roughly equal parts...")
    print(f"Target: ~{target_archive_count:,} records in each database")
    
    # Find the median date to use as split point
    cur_orig.execute("""
        SELECT PostedDate 
        FROM opportunities 
        WHERE PostedDate IS NOT NULL AND PostedDate != ''
        ORDER BY PostedDate
        LIMIT 1 OFFSET ?
    """, (target_archive_count,))
    
    result = cur_orig.fetchone()
    if result:
        split_date_str = result[0]
    else:
        # Fallback: use a date 1 year ago
        split_date = datetime.now() - timedelta(days=365)
        split_date_str = split_date.strftime('%Y-%m-%d')
    
    print(f"Split date: {split_date_str}")
    
    # Count records in each part
    cur_orig.execute("""
        SELECT COUNT(*) FROM opportunities 
        WHERE PostedDate < ? OR PostedDate IS NULL OR PostedDate = ''
    """, (split_date_str,))
    archive_count = cur_orig.fetchone()[0]
    
    cur_orig.execute("""
        SELECT COUNT(*) FROM opportunities 
        WHERE PostedDate >= ? AND PostedDate IS NOT NULL AND PostedDate != ''
    """, (split_date_str,))
    recent_count = cur_orig.fetchone()[0]
    
    print(f"Archive records (before {split_date_str}): {archive_count:,}")
    print(f"Recent records (from {split_date_str}): {recent_count:,}")
    
    # Get column info
    cur_orig.execute("PRAGMA table_info(opportunities)")
    columns_info = cur_orig.fetchall()
    columns = [col[1] for col in columns_info]
    columns_str = ','.join([f'"{col}"' for col in columns])
    placeholders = ','.join(['?' for _ in columns])
    
    # Create ARCHIVE database
    print("\n📁 Creating archive database...")
    if archive_db.exists():
        archive_db.unlink()
    
    conn_archive = sqlite3.connect(archive_db)
    cur_archive = conn_archive.cursor()
    
    # Create table with same structure
    cur_orig.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name='opportunities'
    """)
    create_table_sql = cur_orig.fetchone()[0]
    cur_archive.execute(create_table_sql)
    
    # Copy archive data
    print("Copying archive data...")
    cur_orig.execute(f"""
        SELECT {columns_str} FROM opportunities 
        WHERE PostedDate < ? OR PostedDate IS NULL OR PostedDate = ''
        ORDER BY PostedDate
    """, (split_date_str,))
    
    # Insert in batches
    batch_size = 1000
    inserted = 0
    
    while True:
        rows = cur_orig.fetchmany(batch_size)
        if not rows:
            break
        
        cur_archive.executemany(
            f"INSERT INTO opportunities ({columns_str}) VALUES ({placeholders})", 
            rows
        )
        inserted += len(rows)
        if inserted % 10000 == 0:
            print(f"  Inserted {inserted:,} records into archive...")
    
    conn_archive.commit()
    
    # Add indexes to archive
    print("Creating indexes on archive...")
    cur_orig.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='index' AND tbl_name='opportunities' AND sql IS NOT NULL
    """)
    for index in cur_orig.fetchall():
        try:
            cur_archive.execute(index[0])
        except:
            pass
    
    conn_archive.commit()
    conn_archive.close()
    
    archive_size = archive_db.stat().st_size / (1024 * 1024)
    print(f"✅ Archive database created: {archive_size:.1f} MB with {inserted:,} records")
    
    # Create RECENT database
    print("\n📁 Creating recent database...")
    if recent_db.exists():
        recent_db.unlink()
    
    conn_recent = sqlite3.connect(recent_db)
    cur_recent = conn_recent.cursor()
    
    # Create table
    cur_recent.execute(create_table_sql)
    
    # Copy recent data
    print("Copying recent data...")
    cur_orig.execute(f"""
        SELECT {columns_str} FROM opportunities 
        WHERE PostedDate >= ? AND PostedDate IS NOT NULL AND PostedDate != ''
        ORDER BY PostedDate DESC
    """, (split_date_str,))
    
    inserted = 0
    
    while True:
        rows = cur_orig.fetchmany(batch_size)
        if not rows:
            break
        
        cur_recent.executemany(
            f"INSERT INTO opportunities ({columns_str}) VALUES ({placeholders})", 
            rows
        )
        inserted += len(rows)
        if inserted % 10000 == 0:
            print(f"  Inserted {inserted:,} records into recent...")
    
    conn_recent.commit()
    
    # Add indexes to recent
    print("Creating indexes on recent...")
    cur_orig.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='index' AND tbl_name='opportunities' AND sql IS NOT NULL
    """)
    for index in cur_orig.fetchall():
        try:
            cur_recent.execute(index[0])
        except:
            pass
    
    conn_recent.commit()
    conn_recent.close()
    
    recent_size = recent_db.stat().st_size / (1024 * 1024)
    print(f"✅ Recent database created: {recent_size:.1f} MB with {inserted:,} records")
    
    # Close original connection
    conn_orig.close()
    
    # Verify the split
    print("\n🔍 Verifying split databases...")
    
    # Check archive
    conn = sqlite3.connect(archive_db)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM opportunities")
    archive_final = cur.fetchone()[0]
    conn.close()
    
    # Check recent
    conn = sqlite3.connect(recent_db)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM opportunities")
    recent_final = cur.fetchone()[0]
    conn.close()
    
    # Summary
    print("\n" + "="*60)
    print("SPLIT COMPLETE!")
    print("="*60)
    print(f"Original: {original_size:.1f} MB with {total_records:,} records")
    print(f"Archive:  {archive_size:.1f} MB with {archive_final:,} records (verified)")
    print(f"Recent:   {recent_size:.1f} MB with {recent_final:,} records (verified)")
    print(f"Total:    {archive_final + recent_final:,} records")
    
    if archive_final + recent_final != total_records:
        print(f"⚠️  Warning: Total doesn't match! Missing {total_records - (archive_final + recent_final)} records")
    
    print("\nFiles created:")
    print(f"  ✅ {archive_db}")
    print(f"  ✅ {recent_db}")
    print(f"  ✅ {backup_db} (backup)")
    
    return True

if __name__ == "__main__":
    success = split_database()
    if success:
        print("\n✅ Database split successfully!")
        print("\nNext steps:")
        print("1. Run: python combine_databases.py (to test combining)")
        print("2. Add both database files to Git")
        print("3. Push to GitHub")
    else:
        print("\n❌ Database split failed!")