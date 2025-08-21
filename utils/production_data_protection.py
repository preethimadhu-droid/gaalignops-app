"""
Production Data Protection System
Prevents data loss and overwrites in production environment
"""

import psycopg2
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ProductionDataProtection:
    """Centralized production data protection system"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        self.production_mode = self._detect_production_mode()
    
    def _detect_production_mode(self):
        """Detect if we're in production mode based on environment variable first, then data presence"""
        # Check environment variable first - this takes priority
        env_mode = os.environ.get('ENVIRONMENT', 'development')
        if env_mode == 'development':
            return False
        elif env_mode == 'production':
            return True
            
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check for key production tables and data only if no environment variable set
            production_indicators = [
                ("unified_sales_data", 1000),  # Increased threshold to 1000
                ("talent_supply", 50),         # Increased threshold to 50
                ("master_clients", 30),        # Increased threshold to 30
                ("users", 5),                  # Increased threshold to 5
                ("roles", 5)                   # Increased threshold to 5
            ]
            
            for table_name, threshold in production_indicators:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    );
                """)
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    record_count = cursor.fetchone()[0]
                    
                    if record_count >= threshold:
                        logger.info(f"Production mode detected: {record_count} records in {table_name}")
                        conn.close()
                        return True
            
            conn.close()
            return False
            
        except Exception as e:
            logger.warning(f"Could not detect production mode: {e}")
            return True  # Default to safe mode if unsure
    
    def check_data_protection(self, table_name, operation="DELETE", force_override=False, record_count=None):
        """
        Check if a potentially destructive operation should be allowed
        
        Args:
            table_name: Name of table being affected
            operation: Type of operation (DELETE, TRUNCATE, DROP, UPDATE, BULK_UPDATE, DATA_LOAD)
            force_override: Allow operation even in production mode
            record_count: Number of records being affected
            
        Returns:
            tuple: (allowed, message)
        """
        if not self.production_mode:
            return True, "Development mode - operation allowed"
        
        if force_override:
            logger.warning(f"⚠️  FORCE OVERRIDE: {operation} operation on {table_name} allowed by override")
            return True, f"Force override enabled - {operation} allowed"
        
        # Block all potentially destructive operations in production
        destructive_operations = ["DELETE", "TRUNCATE", "DROP", "DATA_LOAD", "BULK_UPDATE", "CSV_OVERWRITE", "MASS_UPDATE"]
        
        # Special protection for Q2 Billed data (the specific value that keeps getting overwritten)
        if operation == "UPDATE" and table_name == "unified_sales_data" and record_count and record_count > 5:
            logger.warning(f"Large UPDATE operation detected on {table_name}: {record_count} records")
            operation = "MASS_UPDATE"  # Treat large updates as potentially destructive
        
        if operation in destructive_operations:
            message = f"""
            🔒 PRODUCTION DATA PROTECTION ACTIVE
            
            Operation blocked: {operation} on table '{table_name}'
            Reason: Production environment detected with existing data
            Records affected: {record_count if record_count else "Unknown"}
            
            To override (DANGEROUS):
            - Use force_override=True parameter
            - Or set PRODUCTION_MODE=false environment variable
            
            Current protection status: {self._get_protection_status()}
            """
            
            logger.error(f"Blocked {operation} operation on {table_name} - production protection active")
            return False, message.strip()
        
        # Allow safe operations like single record updates
        return True, f"Safe {operation} operation allowed"
    
    def _get_protection_status(self):
        """Get detailed protection status"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            status = {}
            tables_to_check = [
                "unified_sales_data", "talent_supply", "master_clients", 
                "users", "roles", "demand_supply_assignments"
            ]
            
            for table in tables_to_check:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table}'
                    );
                """)
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    status[table] = count
                else:
                    status[table] = 0
            
            conn.close()
            return status
            
        except Exception as e:
            return {"error": str(e)}
    
    def safe_table_creation(self, cursor, create_sql, table_name):
        """Safely create table only if it doesn't exist"""
        try:
            # Check if table exists
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                logger.info(f"Table {table_name} already exists - skipping creation")
                return True
            
            # Create table with IF NOT EXISTS
            safe_sql = create_sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            cursor.execute(safe_sql)
            logger.info(f"Created table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return False
    
    def safe_data_load(self, table_name, data_loader_func, force_overwrite=False):
        """Safely load data with protection checks"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check existing data
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            existing_records = cursor.fetchone()[0]
            
            if existing_records > 0:
                allowed, message = self.check_data_protection(
                    table_name, "DATA_LOAD", force_overwrite
                )
                
                if not allowed:
                    conn.close()
                    return False, message
                
                logger.warning(f"Overwriting {existing_records} records in {table_name}")
            
            # Execute data loading function
            result = data_loader_func(cursor)
            conn.commit()
            conn.close()
            
            return True, f"Data loaded successfully into {table_name}"
            
        except Exception as e:
            logger.error(f"Error in safe data load for {table_name}: {e}")
            return False, str(e)
    
    def backup_table(self, table_name):
        """Create a backup of a table before destructive operations"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            backup_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            cursor.execute(f"""
                CREATE TABLE {backup_name} AS 
                SELECT * FROM {table_name}
            """)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Created backup table: {backup_name}")
            return backup_name
            
        except Exception as e:
            logger.error(f"Error creating backup for {table_name}: {e}")
            return None

# Singleton instance
production_protection = ProductionDataProtection()

def check_production_safety(table_name, operation="DELETE", force_override=False):
    """Convenience function for production safety checks"""
    return production_protection.check_data_protection(table_name, operation, force_override)

def safe_table_create(cursor, create_sql, table_name):
    """Convenience function for safe table creation"""
    return production_protection.safe_table_creation(cursor, create_sql, table_name)

def safe_data_load(table_name, data_loader_func, force_overwrite=False):
    """Convenience function for safe data loading"""
    return production_protection.safe_data_load(table_name, data_loader_func, force_overwrite)