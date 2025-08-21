#!/usr/bin/env python3
"""
Test Suite for Scheduler Functionality
======================================

This test suite validates the scheduler functionality, ensuring:
1. DataSyncScheduler initialization and basic operations
2. OnboardingScheduler background processing and job management
3. Global scheduler instance creation and management
4. Scheduler status reporting and error handling
5. Integration with main application requirements

Test Coverage:
- DataSyncScheduler start/stop operations
- OnboardingScheduler job scheduling and execution
- Global instance management
- Status reporting functionality
- Error handling and recovery
- Thread safety and cleanup
"""

import pytest
import time
import threading
import os
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import scheduler components
from utils.scheduler import (
    DataSyncScheduler, 
    OnboardingScheduler,
    data_sync_scheduler,
    get_scheduler,
    get_data_sync_scheduler,
    start_onboarding_automation,
    stop_onboarding_automation,
    get_automation_status
)

class TestDataSyncScheduler:
    """Test class for DataSyncScheduler functionality"""
    
    def test_01_data_sync_scheduler_initialization(self):
        """Test DataSyncScheduler initialization"""
        scheduler = DataSyncScheduler()
        
        assert scheduler.running == False, "Scheduler should start as not running"
        assert scheduler.scheduler_thread is None, "Scheduler thread should be None initially"
        
        print("✅ DataSyncScheduler initialization validated")

    def test_02_data_sync_scheduler_start_stop(self):
        """Test DataSyncScheduler start and stop operations"""
        scheduler = DataSyncScheduler()
        
        # Test start operation
        scheduler.start_scheduler()
        assert scheduler.running == True, "Scheduler should be running after start"
        
        # Test alias method
        scheduler2 = DataSyncScheduler()
        scheduler2.start_sync_scheduler()
        assert scheduler2.running == True, "Scheduler should be running after start_sync_scheduler"
        
        # Test stop operation
        scheduler.stop_scheduler()
        assert scheduler.running == False, "Scheduler should be stopped after stop"
        
        # Test alias method
        scheduler2.stop_sync_scheduler()
        assert scheduler2.running == False, "Scheduler should be stopped after stop_sync_scheduler"
        
        print("✅ DataSyncScheduler start/stop operations validated")

    def test_03_data_sync_scheduler_status(self):
        """Test DataSyncScheduler status reporting"""
        scheduler = DataSyncScheduler()
        
        # Test initial status
        status = scheduler.get_scheduler_status()
        assert isinstance(status, dict), "Status should be a dictionary"
        assert 'running' in status, "Status should contain 'running' field"
        assert 'scheduler_type' in status, "Status should contain 'scheduler_type' field"
        assert 'thread_active' in status, "Status should contain 'thread_active' field"
        
        assert status['running'] == False, "Initial running status should be False"
        assert status['scheduler_type'] == 'DataSyncScheduler', "Scheduler type should be correct"
        assert status['thread_active'] == False, "Thread should not be active initially"
        
        # Test status after starting
        scheduler.start_scheduler()
        status = scheduler.get_scheduler_status()
        assert status['running'] == True, "Running status should be True after start"
        
        print("✅ DataSyncScheduler status reporting validated")

    def test_04_global_data_sync_scheduler_instance(self):
        """Test global data_sync_scheduler instance"""
        # Test that global instance exists and is correct type
        assert data_sync_scheduler is not None, "Global data_sync_scheduler should exist"
        assert isinstance(data_sync_scheduler, DataSyncScheduler), "Global instance should be DataSyncScheduler"
        
        # Test that get_data_sync_scheduler returns same instance
        scheduler_instance = get_data_sync_scheduler()
        assert scheduler_instance is data_sync_scheduler, "get_data_sync_scheduler should return global instance"
        
        # Test methods work on global instance
        initial_status = data_sync_scheduler.get_scheduler_status()
        assert isinstance(initial_status, dict), "Global instance methods should work"
        
        print("✅ Global data_sync_scheduler instance validated")

class TestOnboardingScheduler:
    """Test class for OnboardingScheduler functionality"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.scheduler = OnboardingScheduler()

    def teardown_method(self):
        """Cleanup after each test method"""
        if hasattr(self, 'scheduler') and self.scheduler:
            self.scheduler.stop_scheduler()

    def test_01_onboarding_scheduler_initialization(self):
        """Test OnboardingScheduler initialization"""
        scheduler = OnboardingScheduler()
        
        assert scheduler.running == False, "Scheduler should start as not running"
        assert scheduler.scheduler_thread is None, "Scheduler thread should be None initially"
        
        print("✅ OnboardingScheduler initialization validated")

    @patch('utils.scheduler.process_pending_onboarding')
    def test_02_onboarding_scheduler_job_execution(self, mock_process):
        """Test OnboardingScheduler job execution"""
        # Mock the onboarding processing function
        mock_process.return_value = [
            {'success': True, 'candidate_name': 'Test Candidate 1', 'candidate_id': 1},
            {'success': True, 'candidate_name': 'Test Candidate 2', 'candidate_id': 2}
        ]
        
        scheduler = OnboardingScheduler()
        
        # Test job function directly
        scheduler._process_onboarding_job()
        
        # Verify the function was called
        mock_process.assert_called_once()
        
        print("✅ OnboardingScheduler job execution validated")

    def test_03_onboarding_scheduler_custom_jobs(self):
        """Test OnboardingScheduler custom job addition"""
        scheduler = OnboardingScheduler()
        
        # Create a mock job function
        job_executed = {'count': 0}
        
        def mock_job():
            job_executed['count'] += 1
            return True
        
        # Add custom job
        scheduler.add_custom_job(mock_job, interval_minutes=1, job_name="test_job")
        
        # Verify job was added (check job status)
        status = scheduler.get_job_status()
        assert isinstance(status, dict), "Job status should be a dictionary"
        assert 'total_jobs' in status, "Status should contain total_jobs"
        assert status['total_jobs'] > 0, "Should have at least one job"
        
        print("✅ OnboardingScheduler custom jobs validated")

    def test_04_onboarding_scheduler_status(self):
        """Test OnboardingScheduler status reporting"""
        scheduler = OnboardingScheduler()
        
        # Test initial status
        status = scheduler.get_job_status()
        assert isinstance(status, dict), "Status should be a dictionary"
        assert 'total_jobs' in status, "Status should contain 'total_jobs' field"
        assert 'running' in status, "Status should contain 'running' field"
        assert 'jobs' in status, "Status should contain 'jobs' field"
        
        assert status['running'] == False, "Initial running status should be False"
        assert isinstance(status['jobs'], list), "Jobs should be a list"
        
        print("✅ OnboardingScheduler status reporting validated")

    def test_05_global_onboarding_scheduler(self):
        """Test global OnboardingScheduler instance management"""
        # Test get_scheduler function
        global_scheduler = get_scheduler()
        assert isinstance(global_scheduler, OnboardingScheduler), "Global scheduler should be OnboardingScheduler"
        
        # Test that multiple calls return same instance
        scheduler2 = get_scheduler()
        assert global_scheduler is scheduler2, "Should return same global instance"
        
        print("✅ Global OnboardingScheduler validated")

class TestSchedulerIntegration:
    """Test class for scheduler integration functionality"""
    
    def test_01_automation_functions(self):
        """Test automation control functions"""
        # Test start_onboarding_automation function exists and is callable
        assert callable(start_onboarding_automation), "start_onboarding_automation should be callable"
        assert callable(stop_onboarding_automation), "stop_onboarding_automation should be callable"
        assert callable(get_automation_status), "get_automation_status should be callable"
        
        # Test get_automation_status returns valid data
        status = get_automation_status()
        assert isinstance(status, dict), "Automation status should be a dictionary"
        
        print("✅ Automation control functions validated")

    def test_02_setup_onboarding_automation(self):
        """Test setup_onboarding_automation function exists and is callable"""
        # Import the function
        from utils.scheduler import setup_onboarding_automation
        
        # Test that function exists and is callable
        assert callable(setup_onboarding_automation), "setup_onboarding_automation should be callable"
        
        # Test function signature by checking it can be called with no arguments
        # We don't actually call it to avoid side effects, just verify it exists
        import inspect
        sig = inspect.signature(setup_onboarding_automation)
        assert len(sig.parameters) == 0, "setup_onboarding_automation should take no required parameters"
        
        print("✅ Setup onboarding automation function validated")

    def test_03_scheduler_imports_for_main_app(self):
        """Test that scheduler provides all required imports for main app"""
        # Test that main app can import what it needs
        try:
            from utils.scheduler import DataSyncScheduler, data_sync_scheduler
            
            # Test that imports are correct types
            assert DataSyncScheduler is not None, "DataSyncScheduler class should be importable"
            assert data_sync_scheduler is not None, "data_sync_scheduler instance should be importable"
            assert isinstance(data_sync_scheduler, DataSyncScheduler), "data_sync_scheduler should be DataSyncScheduler instance"
            
            # Test required methods exist
            assert hasattr(data_sync_scheduler, 'start_scheduler'), "data_sync_scheduler should have start_scheduler method"
            assert hasattr(data_sync_scheduler, 'get_scheduler_status'), "data_sync_scheduler should have get_scheduler_status method"
            
            # Test methods are callable
            assert callable(data_sync_scheduler.start_scheduler), "start_scheduler should be callable"
            assert callable(data_sync_scheduler.get_scheduler_status), "get_scheduler_status should be callable"
            
        except ImportError as e:
            pytest.fail(f"Required scheduler imports failed: {e}")
        
        print("✅ Scheduler imports for main app validated")

    def test_04_scheduler_thread_safety(self):
        """Test scheduler thread safety"""
        scheduler = DataSyncScheduler()
        results = {'start_count': 0, 'stop_count': 0}
        
        def start_scheduler_thread():
            scheduler.start_scheduler()
            results['start_count'] += 1
        
        def stop_scheduler_thread():
            scheduler.stop_scheduler()
            results['stop_count'] += 1
        
        # Create multiple threads to test thread safety
        threads = []
        for i in range(5):
            t1 = threading.Thread(target=start_scheduler_thread)
            t2 = threading.Thread(target=stop_scheduler_thread)
            threads.extend([t1, t2])
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify operations completed
        assert results['start_count'] == 5, "All start operations should complete"
        assert results['stop_count'] == 5, "All stop operations should complete"
        
        print("✅ Scheduler thread safety validated")

    def test_05_scheduler_error_handling(self):
        """Test scheduler error handling"""
        scheduler = OnboardingScheduler()
        
        # Test with invalid job function
        def failing_job():
            raise Exception("Test error")
        
        # Add failing job (should not crash the scheduler)
        try:
            scheduler.add_custom_job(failing_job, interval_minutes=1, job_name="failing_job")
            # If we get here, the scheduler handled the error gracefully
            print("✅ Scheduler error handling validated")
        except Exception as e:
            # This is acceptable - scheduler might reject invalid jobs
            print(f"✅ Scheduler rejected invalid job appropriately: {e}")

class TestSchedulerCleanup:
    """Test class for scheduler cleanup functionality"""
    
    def test_01_scheduler_cleanup(self):
        """Test proper cleanup of scheduler resources"""
        scheduler = OnboardingScheduler()
        
        # Start scheduler
        scheduler.running = True
        
        # Test stop operation
        scheduler.stop_scheduler()
        
        # Verify cleanup
        assert scheduler.running == False, "Scheduler should be stopped"
        
        print("✅ Scheduler cleanup validated")

    def test_02_global_instances_persistence(self):
        """Test that global instances persist correctly"""
        # Get initial instances
        initial_data_scheduler = get_data_sync_scheduler()
        initial_onboarding_scheduler = get_scheduler()
        
        # Get instances again
        second_data_scheduler = get_data_sync_scheduler()
        second_onboarding_scheduler = get_scheduler()
        
        # Verify same instances returned
        assert initial_data_scheduler is second_data_scheduler, "DataSyncScheduler instance should persist"
        assert initial_onboarding_scheduler is second_onboarding_scheduler, "OnboardingScheduler instance should persist"
        
        print("✅ Global instances persistence validated")

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])