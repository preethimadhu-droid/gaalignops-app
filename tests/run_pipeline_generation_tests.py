#!/usr/bin/env python3
"""
Test Runner for Pipeline Generation Test Suite
==============================================

This script runs the comprehensive pipeline generation tests and provides
detailed reporting on the functionality.

CRITICAL: These tests validate the pipeline generation functionality without
modifying any core logic. Run these tests before any changes to ensure
functionality remains intact.
"""

import os
import sys
import subprocess
import time
from datetime import datetime

def run_test_suite():
    """Run the complete pipeline generation test suite"""
    print("=" * 70)
    print("🧪 PIPELINE GENERATION TEST SUITE")
    print("=" * 70)
    print(f"📅 Test Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌍 Environment: Development")
    print("=" * 70)
    
    # List of test files to run
    test_files = [
        'tests/test_staffing_plan_pipeline_generation.py',
        'tests/test_pipeline_generation_integration.py'
    ]
    
    overall_success = True
    test_results = {}
    
    for test_file in test_files:
        print(f"\n🚀 Running {test_file}")
        print("-" * 50)
        
        start_time = time.time()
        
        try:
            # Run pytest for each test file
            result = subprocess.run([
                sys.executable, '-m', 'pytest', 
                test_file, 
                '-v', 
                '--tb=short',
                '--color=yes'
            ], capture_output=True, text=True, timeout=300)
            
            end_time = time.time()
            duration = end_time - start_time
            
            test_results[test_file] = {
                'success': result.returncode == 0,
                'duration': duration,
                'output': result.stdout,
                'error': result.stderr
            }
            
            if result.returncode == 0:
                print(f"✅ PASSED in {duration:.2f}s")
            else:
                print(f"❌ FAILED in {duration:.2f}s")
                overall_success = False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ TIMEOUT after 300s")
            test_results[test_file] = {
                'success': False,
                'duration': 300,
                'output': '',
                'error': 'Test timeout'
            }
            overall_success = False
            
        except Exception as e:
            print(f"💥 ERROR: {str(e)}")
            test_results[test_file] = {
                'success': False,
                'duration': 0,
                'output': '',
                'error': str(e)
            }
            overall_success = False
    
    # Print summary report
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY REPORT")
    print("=" * 70)
    
    total_duration = sum(result['duration'] for result in test_results.values())
    passed_tests = sum(1 for result in test_results.values() if result['success'])
    total_tests = len(test_results)
    
    print(f"📈 Tests Passed: {passed_tests}/{total_tests}")
    print(f"⏱️  Total Duration: {total_duration:.2f}s")
    print(f"🎯 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    print("\n📋 Detailed Results:")
    for test_file, result in test_results.items():
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        print(f"   {status} {test_file} ({result['duration']:.2f}s)")
        
        if not result['success'] and result['error']:
            print(f"      Error: {result['error']}")
    
    # Show detailed output for failed tests
    failed_tests = [tf for tf, result in test_results.items() if not result['success']]
    if failed_tests:
        print("\n" + "=" * 70)
        print("❌ FAILED TEST DETAILS")
        print("=" * 70)
        
        for test_file in failed_tests:
            result = test_results[test_file]
            print(f"\n🔍 {test_file}")
            print("-" * 50)
            if result['output']:
                print("STDOUT:")
                print(result['output'])
            if result['error']:
                print("STDERR:")
                print(result['error'])
    
    print("\n" + "=" * 70)
    if overall_success:
        print("🎉 ALL PIPELINE GENERATION TESTS PASSED!")
        print("✅ Pipeline functionality is working correctly")
    else:
        print("⚠️  SOME TESTS FAILED")
        print("❌ Please review failed tests before making changes")
    print("=" * 70)
    
    return overall_success

def run_specific_test(test_name):
    """Run a specific test function"""
    print(f"🎯 Running specific test: {test_name}")
    
    # Find which file contains the test
    test_files = [
        'tests/test_staffing_plan_pipeline_generation.py',
        'tests/test_pipeline_generation_integration.py'
    ]
    
    for test_file in test_files:
        try:
            result = subprocess.run([
                sys.executable, '-m', 'pytest', 
                f"{test_file}::{test_name}",
                '-v', 
                '--tb=long',
                '--color=yes'
            ], timeout=60)
            
            if result.returncode == 0:
                print(f"✅ Test {test_name} passed")
                return True
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Test {test_name} timed out")
        except Exception as e:
            print(f"💥 Error running {test_name}: {e}")
    
    print(f"❌ Test {test_name} failed or not found")
    return False

def check_environment():
    """Check if the test environment is properly set up"""
    print("🔧 Checking test environment...")
    
    # Check if we're in development environment
    try:
        from utils.environment_manager import EnvironmentManager
        env_manager = EnvironmentManager()
        
        if env_manager.environment != 'development':
            print("❌ ERROR: Tests must run in development environment")
            return False
        
        print("✅ Development environment confirmed")
        
        # Check database connection
        try:
            import psycopg2
            conn = psycopg2.connect(env_manager.get_database_url())
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            print("✅ Database connection working")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def main():
    """Main test runner function"""
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        if not check_environment():
            sys.exit(1)
        success = run_specific_test(test_name)
        sys.exit(0 if success else 1)
    else:
        # Run full test suite
        if not check_environment():
            sys.exit(1)
        success = run_test_suite()
        sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()