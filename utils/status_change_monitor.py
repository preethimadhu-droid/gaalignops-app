"""
Status Change Monitor

Monitors candidate status changes and triggers appropriate automation workflows.
This module provides both real-time monitoring and batch processing capabilities.
"""

import logging
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
import threading
import time

from .candidate_onboarding_automation import CandidateOnboardingAutomation

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StatusChangeMonitor:
    """
    Monitors status changes in candidate_data table and triggers automation
    """
    
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
        self.automation = CandidateOnboardingAutomation()
        self.monitoring = False
        
    def get_db_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(self.db_url)
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def setup_status_change_tracking(self):
        """
        Set up database infrastructure for tracking status changes
        Creates necessary tables and triggers
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Create status change log table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS candidate_status_changes (
                    id SERIAL PRIMARY KEY,
                    candidate_id INTEGER REFERENCES candidate_data(id),
                    old_status VARCHAR(100),
                    new_status VARCHAR(100),
                    changed_at TIMESTAMP DEFAULT NOW(),
                    processed BOOLEAN DEFAULT FALSE,
                    processing_notes TEXT,
                    created_by VARCHAR(100)
                );
            """)
            
            # Create index for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_status_changes_candidate_id 
                ON candidate_status_changes(candidate_id);
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_status_changes_processed 
                ON candidate_status_changes(processed, new_status);
            """)
            
            # Add status_last_changed column to candidate_data if it doesn't exist
            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='candidate_data' AND column_name='status_last_changed') THEN
                        ALTER TABLE candidate_data ADD COLUMN status_last_changed TIMESTAMP;
                    END IF;
                END $$;
            """)
            
            # Create trigger function for automatic status change logging
            cursor.execute("""
                CREATE OR REPLACE FUNCTION log_candidate_status_change()
                RETURNS TRIGGER AS $$
                BEGIN
                    -- Only log if status actually changed
                    IF OLD.status IS DISTINCT FROM NEW.status THEN
                        INSERT INTO candidate_status_changes (
                            candidate_id, old_status, new_status, changed_at
                        ) VALUES (
                            NEW.id, OLD.status, NEW.status, NOW()
                        );
                        
                        -- Update the status_last_changed timestamp
                        NEW.status_last_changed = NOW();
                    END IF;
                    
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # Create trigger if it doesn't exist
            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'candidate_status_change_trigger') THEN
                        CREATE TRIGGER candidate_status_change_trigger
                            BEFORE UPDATE ON candidate_data
                            FOR EACH ROW
                            EXECUTE FUNCTION log_candidate_status_change();
                    END IF;
                END $$;
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("Status change tracking infrastructure set up successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up status change tracking: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False
    
    def get_recent_status_changes(self, hours: int = 24) -> List[Dict]:
        """
        Get recent status changes within specified hours
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of status change records
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            since_time = datetime.now() - timedelta(hours=hours)
            
            query = """
            SELECT 
                csc.id,
                csc.candidate_id,
                cd.candidate_name,
                csc.old_status,
                csc.new_status,
                csc.changed_at,
                csc.processed,
                csc.processing_notes
            FROM candidate_status_changes csc
            JOIN candidate_data cd ON csc.candidate_id = cd.id
            WHERE csc.changed_at >= %s
            ORDER BY csc.changed_at DESC
            """
            
            cursor.execute(query, (since_time,))
            results = cursor.fetchall()
            
            changes = []
            for row in results:
                changes.append({
                    'change_id': row[0],
                    'candidate_id': row[1],
                    'candidate_name': row[2],
                    'old_status': row[3],
                    'new_status': row[4],
                    'changed_at': row[5],
                    'processed': row[6],
                    'processing_notes': row[7]
                })
            
            cursor.close()
            conn.close()
            
            return changes
            
        except Exception as e:
            logger.error(f"Error getting recent status changes: {e}")
            return []
    
    def process_unprocessed_onboarding_changes(self) -> List[Dict]:
        """
        Process all unprocessed status changes to "On Boarded"
        
        Returns:
            List of processing results
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get unprocessed "On Boarded" status changes
            query = """
            SELECT 
                csc.id as change_id,
                csc.candidate_id,
                cd.candidate_name,
                csc.changed_at
            FROM candidate_status_changes csc
            JOIN candidate_data cd ON csc.candidate_id = cd.id
            WHERE csc.new_status = 'On Boarded'
            AND csc.processed = FALSE
            ORDER BY csc.changed_at ASC
            """
            
            cursor.execute(query)
            unprocessed_changes = cursor.fetchall()
            
            results = []
            
            for change_id, candidate_id, candidate_name, changed_at in unprocessed_changes:
                try:
                    # Process the onboarding
                    success = self.automation.process_onboarded_candidate(candidate_id, changed_at)
                    
                    # Mark as processed
                    processing_notes = "Successfully processed" if success else "Processing failed"
                    
                    cursor.execute("""
                        UPDATE candidate_status_changes 
                        SET processed = TRUE, processing_notes = %s
                        WHERE id = %s
                    """, (processing_notes, change_id))
                    
                    results.append({
                        'change_id': change_id,
                        'candidate_id': candidate_id,
                        'candidate_name': candidate_name,
                        'success': success,
                        'notes': processing_notes
                    })
                    
                    logger.info(f"Processed onboarding for {candidate_name} (ID: {candidate_id}): {processing_notes}")
                    
                except Exception as e:
                    error_msg = f"Error processing candidate {candidate_id}: {str(e)}"
                    logger.error(error_msg)
                    
                    cursor.execute("""
                        UPDATE candidate_status_changes 
                        SET processed = TRUE, processing_notes = %s
                        WHERE id = %s
                    """, (error_msg, change_id))
                    
                    results.append({
                        'change_id': change_id,
                        'candidate_id': candidate_id,
                        'candidate_name': candidate_name,
                        'success': False,
                        'notes': error_msg
                    })
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing unprocessed changes: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return []
    
    def start_monitoring(self, check_interval: int = 300):
        """
        Start continuous monitoring for status changes
        
        Args:
            check_interval: Seconds between checks (default 5 minutes)
        """
        if self.monitoring:
            logger.warning("Monitoring already started")
            return
        
        self.monitoring = True
        
        def monitor_loop():
            logger.info(f"Started status change monitoring (checking every {check_interval} seconds)")
            
            while self.monitoring:
                try:
                    results = self.process_unprocessed_onboarding_changes()
                    if results:
                        logger.info(f"Processed {len(results)} status changes")
                        
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}")
                
                # Wait for next check
                for _ in range(check_interval):
                    if not self.monitoring:
                        break
                    time.sleep(1)
            
            logger.info("Status change monitoring stopped")
        
        # Start monitoring in background thread
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.monitoring = False
        logger.info("Stopping status change monitoring...")

# Convenience functions
def setup_status_monitoring():
    """Set up status change monitoring infrastructure"""
    monitor = StatusChangeMonitor()
    return monitor.setup_status_change_tracking()

def process_pending_onboarding():
    """Process all pending onboarding status changes"""
    monitor = StatusChangeMonitor()
    return monitor.process_unprocessed_onboarding_changes()

def get_recent_status_changes(hours: int = 24):
    """Get recent status changes"""
    monitor = StatusChangeMonitor()
    return monitor.get_recent_status_changes(hours)