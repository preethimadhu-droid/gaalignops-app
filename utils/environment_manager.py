"""
Environment Manager - Database Separation for Development and Production
"""
import os
import psycopg2
import pandas as pd
from datetime import datetime

class EnvironmentManager:
    def __init__(self):
        # Auto-detect environment based on URL context if ENVIRONMENT not explicitly set
        env_from_var = os.getenv('ENVIRONMENT')
        if not env_from_var:
            # Check if we're in production by looking for production indicators
            replit_slug = os.getenv('REPL_SLUG', '')
            replit_owner = os.getenv('REPL_OWNER', '')
            # Production is ONLY when deployed to replit.app domain AND owned by greyampmanagement
            if '.replit.app' in replit_slug and replit_owner == 'greyampmanagement':
                self.environment = 'production'
            else:
                self.environment = 'development'
        else:
            self.environment = env_from_var
        
        self.setup_database_connection()
    
    def setup_database_connection(self):
        """Setup database connection based on environment - COMPLETELY SEPARATE"""
        if self.environment == 'production':
            # Production uses clean table names and production database
            self.database_url = os.getenv('DATABASE_URL')
            self.db_prefix = ''
            self.environment_note = 'PRODUCTION - Clean table names'
        else:
            # Development uses dev_ prefixed tables and same database but ISOLATED
            self.database_url = os.getenv('DATABASE_URL')
            self.db_prefix = 'dev_'
            self.environment_note = 'DEVELOPMENT - dev_ prefixed tables, completely isolated'
    
    def get_database_url(self):
        """Get appropriate database URL for current environment"""
        return self.database_url
    
    def get_table_name(self, base_table_name):
        """Get environment-specific table name - STRICT SEPARATION"""
        table_name = f"{self.db_prefix}{base_table_name}"
        # Log for debugging to ensure proper separation
        print(f"[{self.environment.upper()}] Table mapping: {base_table_name} -> {table_name}")
        return table_name
    
    def get_table_prefix(self):
        """Get the table prefix for current environment"""
        return self.db_prefix
    
    def is_production(self):
        """Check if running in production environment"""
        return self.environment == 'production'
    
    def is_development(self):
        """Check if running in development environment"""
        return self.environment == 'development'
    
    def get_environment_info(self):
        """Get detailed environment information"""
        return {
            'environment': self.environment,
            'db_prefix': self.db_prefix,
            'note': self.environment_note,
            'database_url': self.database_url[:50] + '...' if len(self.database_url) > 50 else self.database_url
        }
    
    def create_development_tables(self):
        """Create development tables by copying production structure"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # List of tables to copy
            tables_to_copy = [
                'unified_sales_data',
                'master_clients', 
                'talent_supply',
                'demand_supply_assignments',
                'annual_targets',
                'owner_targets',
                'users',
                'roles',
                'role_groups',
                'role_group_mappings',
                'pipeline_stages',
                'talent_pipelines'
            ]
            
            for table in tables_to_copy:
                dev_table = self.get_table_name(table)
                
                # Create development table structure
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {dev_table} 
                    (LIKE {table} INCLUDING ALL)
                """)
                
                # Check if table has id column for data copying
                cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = '{table}' AND column_name = 'id'
                """)
                has_id = cursor.fetchone() is not None
                
                # Check if table has data and copy it
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # Copy ALL data to development (full dataset for proper testing)
                    try:
                        cursor.execute(f"""
                            INSERT INTO {dev_table} 
                            SELECT * FROM {table}
                            ON CONFLICT DO NOTHING
                        """)
                        print(f"Copied {count} records to {dev_table}")
                    except Exception as e:
                        print(f"Warning: Could not copy data to {dev_table}: {str(e)}")
                        # Continue with other tables
                else:
                    print(f"No data to copy for {dev_table}")
                
                print(f"Created development table: {dev_table}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error creating development tables: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return False
    
    def sync_production_to_development(self):
        """Safely sync production data to development (one-way only)"""
        if self.is_production():
            print("Cannot sync from production environment")
            return False
            
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Clear and refresh development data
            tables_to_sync = ['unified_sales_data', 'master_clients', 'talent_supply']
            
            for table in tables_to_sync:
                dev_table = self.get_table_name(table)
                
                # Clear development table
                cursor.execute(f"TRUNCATE TABLE {dev_table} CASCADE")
                
                # Copy fresh production data
                cursor.execute(f"""
                    INSERT INTO {dev_table} 
                    SELECT * FROM {table}
                """)
                
                print(f"Synced {table} to {dev_table}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error syncing data: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return False
    
    def get_environment_info(self):
        """Get current environment information"""
        return {
            'environment': self.environment,
            'database_url': self.database_url[:50] + "..." if len(self.database_url) > 50 else self.database_url,
            'table_prefix': self.db_prefix,
            'is_production': self.is_production()
        }