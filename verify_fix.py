#!/usr/bin/env python3
"""
verify_fix.py - Verify that all fixes have been applied successfully
Run this after implementing the fixes to ensure everything works
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

# Color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_status(message, status="info"):
    """Print colored status message"""
    if status == "success":
        print(f"{GREEN}✅ {message}{RESET}")
    elif status == "error":
        print(f"{RED}❌ {message}{RESET}")
    elif status == "warning":
        print(f"{YELLOW}⚠️  {message}{RESET}")
    else:
        print(f"{BLUE}ℹ️  {message}{RESET}")

def verify_database():
    """Verify database has been fixed properly"""
    print("\n" + "="*60)
    print("VERIFYING DATABASE FIX")
    print("="*60)
    
    # Find database
    db_paths = [
        Path("data/opportunities.db"),
        Path("opportunities.db"),
        Path.home() / "sam_africa_data" / "opportunities.db"
    ]
    
    db_path = None
    for path in db_paths:
        if path.exists() and path.stat().st_size > 1000:
            db_path = path
            break
    
    if not db_path:
        print_status("Database not found!", "error")
        return False
    
    print_status(f"Database found at: {db_path}", "success")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    all_good = True
    
    try:
        # Check total records
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total = cur.fetchone()[0]
        print_status(f"Total records: {total:,}", "success" if total > 0 else "error")
        
        # Check columns
        cur.execute("PRAGMA table_info(opportunities)")
        columns = {row[1] for row in cur.fetchall()}
        
        # Check for PostedDate_normalized
        if 'PostedDate_normalized' in columns:
            print_status("PostedDate_normalized column exists", "success")
            
            # Check how many dates are normalized
            cur.execute("""
                SELECT COUNT(*) FROM opportunities 
                WHERE PostedDate_normalized IS NOT NULL
            """)
            normalized = cur.fetchone()[0]
            percentage = (normalized / total * 100) if total > 0 else 0
            
            if percentage > 90:
                print_status(f"Dates normalized: {normalized:,}/{total:,} ({percentage:.1f}%)", "success")
            else:
                print_status(f"Only {percentage:.1f}% of dates normalized", "warning")
                all_good = False
        else:
            print_status("PostedDate_normalized column missing", "error")
            all_good = False
        
        # Check for Department column (various forms)
        if 'Department/Ind.Agency' in columns:
            print_status("Department/Ind.Agency column found", "success")
        elif 'Department' in columns:
            print_status("Department column found", "success")
        else:
            print_status("No Department column found", "warning")
        
        # Test date queries
        print("\nTesting date queries:")
        today = datetime.now().date()
        
        test_queries = [
            ("Last 7 days", (today - timedelta(days=7)).isoformat(), today.isoformat()),
            ("Last 30 days", (today - timedelta(days=30)).isoformat(), today.isoformat()),
            ("Last year", (today - timedelta(days=365)).isoformat(), today.isoformat()),
        ]
        
        for period, start, end in test_queries:
            if 'PostedDate_normalized' in columns:
                cur.execute("""
                    SELECT COUNT(*) FROM opportunities 
                    WHERE PostedDate_normalized >= ? 
                    AND PostedDate_normalized <= ?
                """, (start, end))
            else:
                # Fallback query
                cur.execute("""
                    SELECT COUNT(*) FROM opportunities 
                    WHERE date(PostedDate) >= date(?)
                    AND date(PostedDate) <= date(?)
                """, (start, end))
            
            count = cur.fetchone()[0]
            print(f"  {period}: {count:,} records")
        
        # Check indexes
        cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cur.fetchall()]
        
        important_indexes = ['idx_posted_date_normalized', 'idx_posted_date']
        for idx in important_indexes:
            if idx in indexes:
                print_status(f"Index {idx} exists", "success")
        
        conn.close()
        return all_good
        
    except Exception as e:
        print_status(f"Error checking database: {e}", "error")
        conn.close()
        return False

def verify_dashboard_script():
    """Verify the dashboard script has been updated"""
    print("\n" + "="*60)
    print("VERIFYING DASHBOARD SCRIPT")
    print("="*60)
    
    dashboard_path = Path("streamlit_dashboard.py")
    
    if not dashboard_path.exists():
        print_status("streamlit_dashboard.py not found!", "error")
        return False
    
    print_status("Dashboard script found", "success")
    
    # Check if the script has the fixes
    with open(dashboard_path, 'r') as f:
        content = f.read()
    
    # Check for key fixes
    checks = [
        ("Column mapping fix", '"Department/Ind.Agency" as Department'),
        ("Date normalization handling", "PostedDate_normalized"),
        ("Proper error handling", "if 'Department' in df.columns"),
        ("Date range display", "Showing data from"),
    ]
    
    all_good = True
    for check_name, check_string in checks:
        if check_string in content:
            print_status(f"{check_name} found", "success")
        else:
            print_status(f"{check_name} not found", "warning")
            all_good = False
    
    return all_good

def verify_fix_script():
    """Verify the fix_database.py script exists"""
    print("\n" + "="*60)
    print("VERIFYING FIX SCRIPT")
    print("="*60)
    
    fix_path = Path("fix_database.py")
    
    if fix_path.exists():
        print_status("fix_database.py exists", "success")
        return True
    else:
        print_status("fix_database.py not found", "warning")
        print("  This script helps fix the database if needed")
        return False

def test_import():
    """Test that the dashboard can be imported without errors"""
    print("\n" + "="*60)
    print("TESTING IMPORTS")
    print("="*60)
    
    try:
        # Test sam_utils import
        from sam_utils import get_system
        print_status("sam_utils imports successfully", "success")
        
        # Test system initialization
        system = get_system()
        print_status("System initializes successfully", "success")
        
        # Test pandas
        import pandas as pd
        print_status("pandas imports successfully", "success")
        
        # Test plotly
        import plotly.express as px
        print_status("plotly imports successfully", "success")
        
        return True
        
    except Exception as e:
        print_status(f"Import error: {e}", "error")
        return False

def main():
    """Run all verification checks"""
    print("="*60)
    print("SAM.GOV DASHBOARD FIX VERIFICATION")
    print("="*60)
    print(f"Verification Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # Run all checks
    checks = [
        ("Database Fixed", verify_database),
        ("Dashboard Updated", verify_dashboard_script),
        ("Fix Script Present", verify_fix_script),
        ("Imports Working", test_import),
    ]
    
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print_status(f"Check '{name}' failed: {e}", "error")
            results.append((name, False))
    
    # Summary
    print("\n" + "="*60)
    print("VERIFICATION SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "success" if result else "error"
        symbol = "✅" if result else "❌"
        print(f"{symbol} {name}: {'PASSED' if result else 'NEEDS ATTENTION'}")
    
    print("\n" + "="*60)
    if passed == total:
        print_status("All checks passed! Your dashboard is ready to use.", "success")
        print("\nNext steps:")
        print("1. Commit and push changes to GitHub:")
        print("   git add -A")
        print("   git commit -m 'Fix dashboard column and date issues'")
        print("   git push origin main")
        print("\n2. Your Streamlit Cloud app will auto-update in 2-3 minutes")
        print("\n3. Visit your dashboard and verify everything works!")
        return 0
    else:
        print_status(f"{passed}/{total} checks passed.", "warning")
        print("\nTo fix remaining issues:")
        print("1. Run: python fix_database.py")
        print("2. Make sure you copied the new streamlit_dashboard.py")
        print("3. Run this verification again")
        return 1

if __name__ == "__main__":
    sys.exit(main())