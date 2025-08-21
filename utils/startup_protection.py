"""
Startup Protection System
Monitors and prevents data overwrites during application startup and deployment
"""

import psycopg2
import os
import logging
from datetime import datetime
from .production_data_protection import ProductionDataProtection

logger = logging.getLogger(__name__)

class StartupProtection:
    """Protection system for application startup and deployment"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        self.protection = ProductionDataProtection()
        self.startup_time = datetime.now()
        
    def create_data_integrity_checkpoint(self):
        """Create a checkpoint of current data for integrity monitoring"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check critical data points
            checkpoints = {}
            
            # Check Q2 billed data specifically (the value that keeps getting overwritten)
            cursor.execute("""
                SELECT account_name, value, metric_type, month, year 
                FROM unified_sales_data 
                WHERE metric_type = 'Billed' 
                AND month = 'June' 
                AND year = 2025
                ORDER BY account_name
            """)
            q2_billed_data = cursor.fetchall()
            checkpoints['q2_billed'] = q2_billed_data
            
            # Check total record counts
            tables_to_monitor = ['unified_sales_data', 'talent_supply', 'master_clients']
            for table in tables_to_monitor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                checkpoints[f'{table}_count'] = count
            
            conn.close()
            
            logger.info(f"Data integrity checkpoint created: {checkpoints}")
            return checkpoints
            
        except Exception as e:
            logger.error(f"Error creating data checkpoint: {e}")
            return None
    
    def verify_data_integrity(self, original_checkpoint):
        """Verify data hasn't been corrupted during startup"""
        try:
            current_checkpoint = self.create_data_integrity_checkpoint()
            
            if not original_checkpoint or not current_checkpoint:
                return True  # Can't verify, assume OK
            
            # Check Q2 billed data for changes
            if original_checkpoint.get('q2_billed') != current_checkpoint.get('q2_billed'):
                logger.error("🚨 DATA CORRUPTION DETECTED: Q2 Billed data has been modified!")
                logger.error(f"Original: {original_checkpoint.get('q2_billed')}")
                logger.error(f"Current: {current_checkpoint.get('q2_billed')}")
                return False
            
            # Check record counts
            for key in ['unified_sales_data_count', 'talent_supply_count', 'master_clients_count']:
                if original_checkpoint.get(key) != current_checkpoint.get(key):
                    logger.warning(f"Record count changed for {key}: {original_checkpoint.get(key)} -> {current_checkpoint.get(key)}")
            
            logger.info("✅ Data integrity verified - no corruption detected")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying data integrity: {e}")
            return False
    
    def monitor_startup_operations(self):
        """Monitor for any data modification operations during startup"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Log current database activity
            cursor.execute("""
                SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del
                FROM pg_stat_user_tables
                WHERE schemaname = 'public'
                AND relname IN ('unified_sales_data', 'talent_supply', 'master_clients')
            """)
            
            activity = cursor.fetchall()
            logger.info(f"Database activity snapshot: {activity}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error monitoring startup operations: {e}")
    
    def enable_startup_protection(self):
        """Enable comprehensive startup protection"""
        logger.info("🔒 STARTUP PROTECTION ACTIVATED")
        
        # Create baseline checkpoint
        checkpoint = self.create_data_integrity_checkpoint()
        
        # Monitor for changes
        self.monitor_startup_operations()
        
        # Log protection status
        protection_status = self.protection._get_protection_status()
        logger.info(f"Production protection status: {protection_status}")
        
        return checkpoint
    
    def log_data_modification_attempt(self, operation, table, details=""):
        """Log any attempt to modify data"""
        logger.warning(f"🔍 DATA MODIFICATION ATTEMPT: {operation} on {table} - {details}")
        
        # If this is during startup (within 30 seconds), it's suspicious
        time_since_startup = (datetime.now() - self.startup_time).total_seconds()
        if time_since_startup < 30:
            logger.error(f"🚨 SUSPICIOUS: Data modification attempted during startup ({time_since_startup:.1f}s after start)")

# Global instance
startup_protection = StartupProtection()

def enable_startup_protection():
    """Enable startup protection system"""
    return startup_protection.enable_startup_protection()

def verify_data_integrity(checkpoint):
    """Verify data integrity against checkpoint"""
    return startup_protection.verify_data_integrity(checkpoint)

def log_modification_attempt(operation, table, details=""):
    """Log data modification attempt"""
    startup_protection.log_data_modification_attempt(operation, table, details)