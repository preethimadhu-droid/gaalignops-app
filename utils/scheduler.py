"""
Scheduler for Automated Onboarding Tasks

Provides scheduling and background processing for onboarding automation
"""

import logging
import schedule
import time
import threading
from datetime import datetime
from typing import Optional, Callable

from .status_change_monitor import process_pending_onboarding

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSyncScheduler:
    """
    Scheduler for data synchronization tasks
    """
    
    def __init__(self):
        self.running = False
        self.scheduler_thread = None
        
    def start_scheduler(self):
        """Start data sync scheduler"""
        logger.info("Data sync scheduler started")
        self.running = True
        
    def start_sync_scheduler(self):
        """Start data sync scheduler (alias)"""
        return self.start_scheduler()
        
    def stop_sync_scheduler(self):
        """Stop data sync scheduler"""
        logger.info("Data sync scheduler stopped")
        self.running = False
        
    def stop_scheduler(self):
        """Stop data sync scheduler (alias)"""
        return self.stop_sync_scheduler()
    
    def get_scheduler_status(self):
        """Get status of the data sync scheduler"""
        return {
            'running': self.running,
            'scheduler_type': 'DataSyncScheduler',
            'thread_active': self.scheduler_thread is not None and self.scheduler_thread.is_alive() if self.scheduler_thread else False
        }

class OnboardingScheduler:
    """
    Scheduler for automating onboarding tasks
    """
    
    def __init__(self):
        self.running = False
        self.scheduler_thread = None
        
    def start_background_scheduler(self, check_interval_minutes: int = 15):
        """
        Start background scheduler to process onboarding tasks
        
        Args:
            check_interval_minutes: How often to check for new onboarded candidates
        """
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        # Schedule the job
        schedule.every(check_interval_minutes).minutes.do(self._process_onboarding_job)
        
        # Also run once immediately
        schedule.every().second.do(self._initial_run).tag('initial')
        
        self.running = True
        
        def run_scheduler():
            logger.info(f"Started onboarding scheduler (checking every {check_interval_minutes} minutes)")
            
            while self.running:
                try:
                    schedule.run_pending()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Error in scheduler: {e}")
                    
            logger.info("Onboarding scheduler stopped")
        
        # Start in background thread
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
    
    def _initial_run(self):
        """Run the job once immediately and remove the initial tag"""
        self._process_onboarding_job()
        schedule.clear('initial')
        return schedule.CancelJob
    
    def _process_onboarding_job(self):
        """Job function to process pending onboarding candidates"""
        try:
            logger.info("Running scheduled onboarding processing...")
            results = process_pending_onboarding()
            
            if results:
                successful = sum(1 for r in results if r['success'])
                total = len(results)
                logger.info(f"Processed {successful}/{total} candidates successfully")
                
                # Log failed ones for debugging
                failed = [r for r in results if not r['success']]
                for failure in failed:
                    logger.warning(f"Failed to process candidate {failure['candidate_name']} (ID: {failure['candidate_id']}): {failure.get('notes', 'Unknown error')}")
            else:
                logger.debug("No pending candidates to process")
                
        except Exception as e:
            logger.error(f"Error in onboarding processing job: {e}")
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        self.running = False
        schedule.clear()
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            logger.info("Stopping onboarding scheduler...")
    
    def add_custom_job(self, job_func: Callable, interval_minutes: int, job_name: str = "custom"):
        """
        Add a custom job to the scheduler
        
        Args:
            job_func: Function to execute
            interval_minutes: How often to run the job
            job_name: Name for the job (for tagging)
        """
        try:
            schedule.every(interval_minutes).minutes.do(job_func).tag(job_name)
            logger.info(f"Added custom job '{job_name}' to run every {interval_minutes} minutes")
        except Exception as e:
            logger.error(f"Error adding custom job '{job_name}': {e}")
    
    def get_job_status(self):
        """Get status of scheduled jobs"""
        jobs = schedule.get_jobs()
        return {
            'total_jobs': len(jobs),
            'running': self.running,
            'jobs': [
                {
                    'job': str(job.job_func),
                    'interval': str(job.interval),
                    'next_run': job.next_run,
                    'tags': list(job.tags) if hasattr(job, 'tags') else []
                }
                for job in jobs
            ]
        }

# Global scheduler instances
_global_scheduler = None
_data_sync_scheduler = None

def get_scheduler() -> OnboardingScheduler:
    """Get the global scheduler instance"""
    global _global_scheduler
    if _global_scheduler is None:
        _global_scheduler = OnboardingScheduler()
    return _global_scheduler

def get_data_sync_scheduler() -> DataSyncScheduler:
    """Get the global data sync scheduler instance"""
    global _data_sync_scheduler
    if _data_sync_scheduler is None:
        _data_sync_scheduler = DataSyncScheduler()
    return _data_sync_scheduler

# Create global instance for direct import
data_sync_scheduler = get_data_sync_scheduler()

def start_onboarding_automation(check_interval_minutes: int = 15):
    """
    Start the onboarding automation scheduler
    
    Args:
        check_interval_minutes: How often to check for new candidates
    """
    scheduler = get_scheduler()
    scheduler.start_background_scheduler(check_interval_minutes)

def stop_onboarding_automation():
    """Stop the onboarding automation scheduler"""
    scheduler = get_scheduler()
    scheduler.stop_scheduler()

def get_automation_status():
    """Get the current status of automation"""
    scheduler = get_scheduler()
    return scheduler.get_job_status()

# One-time setup function
def setup_onboarding_automation():
    """
    Set up the complete onboarding automation system
    
    This function:
    1. Sets up database infrastructure
    2. Processes any existing unprocessed candidates
    3. Starts the scheduler
    """
    try:
        # Import required modules
        from .status_change_monitor import setup_status_monitoring, process_pending_onboarding
        
        logger.info("Setting up onboarding automation system...")
        
        # Step 1: Set up database infrastructure
        logger.info("Setting up status monitoring infrastructure...")
        setup_success = setup_status_monitoring()
        if not setup_success:
            logger.error("Failed to set up status monitoring infrastructure")
            return False
        
        # Step 2: Process any existing unprocessed candidates
        logger.info("Processing any existing unprocessed candidates...")
        initial_results = process_pending_onboarding()
        if initial_results:
            successful = sum(1 for r in initial_results if r['success'])
            logger.info(f"Initially processed {successful}/{len(initial_results)} existing candidates")
        
        # Step 3: Start the scheduler
        logger.info("Starting background scheduler...")
        start_onboarding_automation(check_interval_minutes=15)
        
        logger.info("✅ Onboarding automation system setup complete!")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up onboarding automation: {e}")
        return False