#!/usr/bin/env python3
"""
optimize_database.py - Compress and optimize the database
"""

import sqlite3
import os
from pathlib import Path

# DB path
SAM_DATA_DIR = os.environ.get("SAM_DATA_DIR")
LOCAL_DATA_DIR = Path(SAM_DATA_DIR).expanduser().resolve() if SAM_DATA_DIR else (Path.home() / "sam_africa_data")
DB_PATH = LOCAL_DATA_DIR / "opportunities.db"

def optimize():
    if not DB_PATH.exists():
        print("Database not found")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Get initial size
    initial_size = DB_PATH.stat().st_size / (1024*1024)
    print(f"Initial size: {initial_size:.2f} MB")
    
    # Remove unnecessary columns to save space
    try:
        # Drop rarely used columns
        cur.execute('ALTER TABLE opportunities DROP COLUMN "PrimaryContactFullName"')
        cur.execute('ALTER TABLE opportunities DROP COLUMN "PrimaryContactTitle"')
        cur.execute('ALTER TABLE opportunities DROP COLUMN "Sub-Tier"')
        cur.execute('ALTER TABLE opportunities DROP COLUMN "OrganizationType"')
    except:
        pass  # Columns may already be dropped
    
    # Clean up empty values
    cur.execute("""
        UPDATE opportunities 
        SET Description = NULL 
        WHERE Description = '' OR Description = 'nan'
    """)
    
    # Remove old duplicates
    cur.execute("""
        DELETE FROM opportunities 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM opportunities 
            GROUP BY "NoticeID"
        )
    """)
    
    conn.commit()
    
    # Optimize database
    cur.execute('PRAGMA optimize')
    cur.execute('VACUUM')
    cur.execute('REINDEX')
    cur.execute('ANALYZE')
    
    conn.commit()
    conn.close()
    
    # Final size
    final_size = DB_PATH.stat().st_size / (1024*1024)
    print(f"Final size: {final_size:.2f} MB")
    print(f"Saved: {initial_size - final_size:.2f} MB")

if __name__ == "__main__":
    optimize()