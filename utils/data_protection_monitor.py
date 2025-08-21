"""
Data Protection Monitor - Additional safeguards for development/production data segregation
"""
import os
import logging
import psycopg2
from datetime import datetime
import streamlit as st

class DataProtectionMonitor:
    """Monitor and enforce data protection rules"""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self.setup_logging()
        
    def setup_logging(self):
        """Setup data protection logging"""
        self.logger = logging.getLogger('data_protection')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - PROTECTION - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def validate_environment(self):
        """Validate current environment and log status"""
        status = {
            'environment': self.environment,
            'is_production': self.environment == 'production',
            'table_prefix': 'dev_' if self.environment == 'development' else '',
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Environment validation: {status}")
        return status
    
    def check_table_access(self, table_name, operation='SELECT'):
        """Check if table access is allowed in current environment"""
        is_dev_table = table_name.startswith('dev_')
        is_production = self.environment == 'production'
        
        # In production, only allow access to non-dev tables
        if is_production and is_dev_table:
            error_msg = f"BLOCKED: Cannot access dev table '{table_name}' in production environment"
            self.logger.error(error_msg)
            return False, error_msg
            
        # In development, only allow access to dev tables for main operations
        if not is_production and not is_dev_table and operation in ['INSERT', 'UPDATE', 'DELETE']:
            warning_msg = f"WARNING: Attempting {operation} on production table '{table_name}' in development"
            self.logger.warning(warning_msg)
            # Allow but log warning
            
        success_msg = f"ALLOWED: {operation} on '{table_name}' in {self.environment} environment"
        self.logger.info(success_msg)
        return True, success_msg
    
    def validate_migration_safety(self, migration_name):
        """Validate if migration is safe to run in current environment"""
        dangerous_migrations = [
            'migrate_to_postgresql.py',
            'complete_data_migration_from_sqlite.py',
            'migrate_complete_demand_data.py',
            'migrate_target_data.py',
            'complete_postgresql_migration.py',
            'fix_all_errors.py'
        ]
        
        if migration_name in dangerous_migrations and self.environment == 'production':
            error_msg = f"BLOCKED: Migration '{migration_name}' not allowed in production"
            self.logger.error(error_msg)
            return False, error_msg
            
        return True, f"Migration '{migration_name}' validated for {self.environment}"
    
    def get_protection_status(self):
        """Get current protection status for UI display"""
        status = self.validate_environment()
        
        if status['is_production']:
            return {
                'environment': 'Production',
                'color': '🔴',
                'warning': 'LIVE BUSINESS DATA - Exercise extreme caution',
                'table_info': 'Using production tables (no prefix)',
                'protection_level': 'MAXIMUM'
            }
        else:
            return {
                'environment': 'Development',
                'color': '🟢',
                'warning': 'Safe development environment',
                'table_info': 'Using development tables (dev_ prefix)',
                'protection_level': 'STANDARD'
            }
    
    def log_database_operation(self, table_name, operation, user_id=None):
        """Log all database operations for audit trail"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'environment': self.environment,
            'table': table_name,
            'operation': operation,
            'user': user_id or 'unknown',
            'session_id': getattr(st.session_state, 'user_email', 'no_session')
        }
        
        self.logger.info(f"DB_OPERATION: {log_entry}")
        return log_entry
    
    def verify_table_segregation(self):
        """Verify that development and production tables are properly segregated"""
        try:
            database_url = os.getenv('DATABASE_URL')
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            all_tables = [row[0] for row in cursor.fetchall()]
            dev_tables = [t for t in all_tables if t.startswith('dev_')]
            prod_tables = [t for t in all_tables if not t.startswith('dev_')]
            
            segregation_report = {
                'total_tables': len(all_tables),
                'dev_tables': len(dev_tables),
                'prod_tables': len(prod_tables),
                'properly_segregated': len(dev_tables) > 0 and len(prod_tables) > 0,
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Table segregation verification: {segregation_report}")
            
            conn.close()
            return segregation_report
            
        except Exception as e:
            error_msg = f"Error verifying table segregation: {str(e)}"
            self.logger.error(error_msg)
            return {'error': error_msg}
    
    def display_protection_banner(self):
        """Display protection status banner in Streamlit UI"""
        status = self.get_protection_status()
        
        if status['environment'] == 'Production':
            st.error(f"🔴 **PRODUCTION ENVIRONMENT** - {status['warning']}")
        else:
            st.success(f"🟢 **DEVELOPMENT ENVIRONMENT** - {status['warning']}")
        
        # Display in sidebar as well for constant visibility
        with st.sidebar:
            st.markdown(f"### {status['color']} {status['environment']} Mode")
            st.caption(status['table_info'])

# Global instance for easy access
data_protection_monitor = DataProtectionMonitor()