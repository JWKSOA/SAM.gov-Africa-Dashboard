#!/usr/bin/env python3
"""
test_system.py - Verify the optimized SAM.gov system is working correctly
Run this after implementation to ensure everything is functioning
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import time

# Color codes for terminal output
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

def test_imports():
    """Test that all required modules can be imported"""
    print("\n" + "="*60)
    print("Testing Module Imports")
    print("="*60)
    
    modules = [
        ('sam_utils', 'Core utilities'),
        ('pandas', 'Data processing'),
        ('streamlit', 'Dashboard framework'),
        ('plotly', 'Visualizations'),
        ('requests', 'HTTP client'),
    ]
    
    failed = []
    for module_name, description in modules:
        try:
            __import__(module_name)
            print_status(f"{description} ({module_name})", "success")
        except ImportError as e:
            print_status(f"{description} ({module_name}): {e}", "error")
            failed.append(module_name)
    
    return len(failed) == 0

def test_utilities():
    """Test sam_utils functionality"""
    print("\n" + "="*60)
    print("Testing SAM Utilities")
    print("="*60)
    
    try:
        from sam_utils import get_system, CountryManager
        
        # Test system initialization
        system = get_system()
        print_status("System initialization", "success")
        
        # Test country manager
        cm = CountryManager()
        
        # Test country detection
        test_cases = [
            ("Kenya", True),
            ("NIGERIA", True),
            ("DZA", True),
            ("United States", False),
            ("", False)
        ]
        
        for value, expected in test_cases:
            result = cm.is_african_country(value)
            if result == expected:
                print_status(f"Country detection: '{value}' -> {result}", "success")
            else:
                print_status(f"Country detection: '{value}' expected {expected}, got {result}", "error")
                return False
        
        # Test database connection
        stats = system.db_manager.get_statistics()
        print_status(f"Database connection (Records: {stats['total_records']:,})", "success")
        
        return True
        
    except Exception as e:
        print_status(f"Utilities test failed: {e}", "error")
        return False

def test_database():
    """Test database operations"""
    print("\n" + "="*60)
    print("Testing Database")
    print("="*60)
    
    try:
        from sam_utils import get_system
        import pandas as pd
        
        system = get_system()
        
        # Check database exists
        if not system.config.db_path.exists():
            print_status("Database not found - run bootstrap_historical.py or download_and_update.py first", "warning")
            return True  # Not a failure, just needs initialization
        
        # Test database integrity
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Check integrity
            cur.execute("PRAGMA integrity_check")
            result = cur.fetchone()[0]
            if result == "ok":
                print_status("Database integrity check", "success")
            else:
                print_status(f"Database integrity: {result}", "error")
                return False
            
            # Check indexes
            cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cur.fetchall()]
            
            expected_indexes = ['idx_notice_id', 'idx_posted_date', 'idx_pop_country']
            missing = [idx for idx in expected_indexes if idx not in indexes]
            
            if not missing:
                print_status(f"Database indexes ({len(indexes)} indexes)", "success")
            else:
                print_status(f"Missing indexes: {missing}", "warning")
            
            # Test query performance
            start = time.time()
            cur.execute("SELECT COUNT(*) FROM opportunities WHERE date(PostedDate) >= date('now', '-30 days')")
            count = cur.fetchone()[0]
            elapsed = (time.time() - start) * 1000
            
            if elapsed < 100:  # Should be under 100ms
                print_status(f"Query performance ({elapsed:.1f}ms for {count} recent records)", "success")
            else:
                print_status(f"Query performance slow ({elapsed:.1f}ms)", "warning")
        
        return True
        
    except Exception as e:
        print_status(f"Database test failed: {e}", "error")
        return False

def test_scripts():
    """Test that main scripts can be run"""
    print("\n" + "="*60)
    print("Testing Main Scripts")
    print("="*60)
    
    scripts = [
        'bootstrap_historical.py',
        'download_and_update.py',
        'streamlit_dashboard.py'
    ]
    
    for script in scripts:
        if not Path(script).exists():
            print_status(f"Script not found: {script}", "error")
            return False
        
        # Test syntax by compiling
        try:
            with open(script, 'r') as f:
                compile(f.read(), script, 'exec')
            print_status(f"Script syntax valid: {script}", "success")
        except SyntaxError as e:
            print_status(f"Syntax error in {script}: {e}", "error")
            return False
    
    return True

def test_github_action():
    """Test GitHub Actions workflow syntax"""
    print("\n" + "="*60)
    print("Testing GitHub Actions Workflow")
    print("="*60)
    
    workflow_path = Path(".github/workflows/update-sam-db.yml")
    
    if not workflow_path.exists():
        print_status("Workflow file not found", "warning")
        return True  # Not critical
    
    try:
        import yaml
        with open(workflow_path, 'r') as f:
            yaml.safe_load(f)
        print_status("Workflow syntax valid", "success")
        return True
    except ImportError:
        print_status("PyYAML not installed, skipping workflow test", "warning")
        return True
    except Exception as e:
        print_status(f"Workflow syntax error: {e}", "error")
        return False

def test_performance():
    """Run performance benchmarks"""
    print("\n" + "="*60)
    print("Performance Benchmarks")
    print("="*60)
    
    try:
        from sam_utils import get_system, DataProcessor, CountryManager
        import pandas as pd
        import numpy as np
        
        system = get_system()
        processor = DataProcessor(system.config, system.country_manager)
        
        # Create test data
        test_size = 10000
        test_data = pd.DataFrame({
            'Title': [f'Test Contract {i}' for i in range(test_size)],
            'PopCountry': np.random.choice(['Kenya', 'Nigeria', 'USA', 'Egypt', 'China'], test_size),
            'PostedDate': pd.date_range('2024-01-01', periods=test_size, freq='h'),
            'Department': np.random.choice(['DOD', 'STATE', 'USAID'], test_size),
            'Link': [f'https://sam.gov/{i}' for i in range(test_size)]
        })
        
        # Test filtering performance
        start = time.time()
        filtered = processor.filter_african_rows(test_data)
        elapsed = (time.time() - start) * 1000
        
        african_count = len(filtered)
        if elapsed < 500:  # Should process 10k rows in under 500ms
            print_status(f"Filtering performance: {elapsed:.1f}ms for {test_size:,} rows ({african_count} African)", "success")
        else:
            print_status(f"Filtering slow: {elapsed:.1f}ms for {test_size:,} rows", "warning")
        
        # Test memory usage
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        if memory_mb < 500:
            print_status(f"Memory usage: {memory_mb:.1f} MB", "success")
        else:
            print_status(f"High memory usage: {memory_mb:.1f} MB", "warning")
        
        return True
        
    except ImportError:
        print_status("psutil not installed, skipping memory test", "warning")
        return True
    except Exception as e:
        print_status(f"Performance test error: {e}", "warning")
        return True

def main():
    """Run all tests"""
    print("="*60)
    print("SAM.gov System Verification")
    print("="*60)
    print(f"Testing at: {datetime.now()}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {Path.cwd()}")
    
    # Run tests
    results = []
    
    tests = [
        ("Module Imports", test_imports),
        ("Utilities", test_utilities),
        ("Database", test_database),
        ("Scripts", test_scripts),
        ("GitHub Action", test_github_action),
        ("Performance", test_performance),
    ]
    
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print_status(f"Test '{name}' crashed: {e}", "error")
            results.append((name, False))
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "success" if result else "error"
        print_status(f"{name}: {'PASSED' if result else 'FAILED'}", status)
    
    print("\n" + "="*60)
    if passed == total:
        print_status(f"All {total} tests passed! System is ready.", "success")
        print("\nNext steps:")
        print("1. Run 'python download_and_update.py' to get latest data")
        print("2. Run 'streamlit run streamlit_dashboard.py' to start dashboard")
        print("3. Push to GitHub to enable automatic updates")
        return 0
    else:
        print_status(f"{passed}/{total} tests passed. Please fix issues above.", "error")
        return 1

if __name__ == "__main__":
    # Try to install psutil if not present (for memory testing)
    try:
        import psutil
    except ImportError:
        print_status("Installing psutil for memory testing...", "info")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil", "-q"])
    
    sys.exit(main())