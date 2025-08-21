"""
Confidence Consolidation System
Centralizes all confidence data in master_clients table as single source of truth
"""

import psycopg2
import pandas as pd
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfidenceConsolidator:
    def __init__(self, env_manager=None):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        self.env_manager = env_manager
    
    def consolidate_all_confidence_data(self):
        """Consolidate confidence data from all sources into master_clients table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Step 1: Ensure master_clients table structure is correct
            cursor.execute("""
                -- Check if master_clients table exists, if not create it
                CREATE TABLE IF NOT EXISTS master_clients (
                    master_client_id SERIAL PRIMARY KEY,
                    client_name VARCHAR(255) UNIQUE NOT NULL,
                    confidence_level INTEGER DEFAULT 50,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'Active'
                )
            """)
            
            # Step 2: Add any missing columns to existing table
            cursor.execute("""
                ALTER TABLE master_clients ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            """)
            
            # Step 3: Ensure all clients from unified_sales_data exist in master_clients
            cursor.execute("""
                INSERT INTO master_clients (client_name, confidence_level)
                SELECT DISTINCT account_name, 50.0
                FROM unified_sales_data 
                WHERE account_name IS NOT NULL
                AND account_name NOT IN (SELECT client_name FROM master_clients)
                ON CONFLICT (client_name) DO NOTHING
            """)
            
            # Step 4: Consolidate confidence from demand_metadata
            cursor.execute("""
                INSERT INTO master_clients (client_name, confidence_level)
                SELECT DISTINCT 
                    client_name,
                    COALESCE(confidence_pct, 50.0) as confidence_level
                FROM demand_metadata 
                WHERE client_name IS NOT NULL
                GROUP BY client_name, confidence_pct
                ON CONFLICT (client_name) 
                DO UPDATE SET 
                    confidence_level = GREATEST(EXCLUDED.confidence_level, master_clients.confidence_level),
                    updated_at = CURRENT_TIMESTAMP
            """)
            

            
            conn.commit()
            
            # Step 6: Get consolidation results
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_clients,
                    AVG(confidence_level) as avg_confidence,
                    COUNT(CASE WHEN confidence_level > 60 THEN 1 END) as high_confidence_clients
                FROM master_clients
            """)
            
            stats = cursor.fetchone()
            conn.close()
            
            logger.info(f"Confidence consolidation completed: {stats[0]} clients, avg confidence: {stats[1]:.1f}%, high confidence: {stats[2]}")
            return {
                'total_clients': stats[0],
                'avg_confidence': stats[1],
                'high_confidence_clients': stats[2]
            }
            
        except Exception as e:
            logger.error(f"Error consolidating confidence data: {str(e)}")
            return None
    
    def update_client_confidence(self, client_name, confidence_level):
        """Update confidence for a specific client and auto-create Ready for Staffing if needed - Legacy method using client_name"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get table names based on environment
            master_clients_table = self._get_table_name('master_clients')
            
            # Get old confidence level
            cursor.execute(f"SELECT confidence_level FROM {master_clients_table} WHERE client_name = %s", (client_name,))
            old_result = cursor.fetchone()
            old_confidence = old_result[0] if old_result else 0
            
            # Update confidence
            cursor.execute(f"""
                INSERT INTO {master_clients_table} (client_name, confidence_level) 
                VALUES (%s, %s)
                ON CONFLICT (client_name) 
                DO UPDATE SET 
                    confidence_level = EXCLUDED.confidence_level,
                    updated_at = CURRENT_TIMESTAMP
            """, (client_name, confidence_level))
            
            # Check if we crossed the 70% threshold (going from below to above)
            if old_confidence < 70 and confidence_level >= 70:
                self._auto_create_ready_for_staffing(cursor, client_name, confidence_level)
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating client confidence: {str(e)}")
            return False
    
    def update_client_confidence_by_id(self, client_id, confidence_level):
        """Update confidence for a specific client by ID and auto-create Ready for Staffing if needed - Preferred method"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get table names based on environment
            master_clients_table = self._get_table_name('master_clients')
            
            # Get client info and old confidence level
            cursor.execute(f"""
                SELECT master_client_id, client_name, confidence_level 
                FROM {master_clients_table} 
                WHERE master_client_id = %s
            """, (client_id,))
            client_result = cursor.fetchone()
            
            if not client_result:
                logger.error(f"Client ID {client_id} not found in master_clients table")
                return False
                
            master_client_id, client_name, old_confidence = client_result
            old_confidence = old_confidence or 0
            
            # Update confidence using client_id
            cursor.execute(f"""
                UPDATE {master_clients_table}
                SET confidence_level = %s, updated_at = CURRENT_TIMESTAMP
                WHERE master_client_id = %s
            """, (confidence_level, master_client_id))
            
            # Check if we crossed the 70% threshold (going from below to above)
            if old_confidence < 70 and confidence_level >= 70:
                self._auto_create_ready_for_staffing(cursor, client_name, confidence_level)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated confidence for client_id {client_id} ({client_name}) from {old_confidence}% to {confidence_level}%")
            return True
            
        except Exception as e:
            logger.error(f"Error updating client confidence by ID {client_id}: {str(e)}")
            return False
    
    def delete_demand_record_with_confidence_reduction(self, client_id, status=None):
        """
        Delete demand record and reduce confidence to 50%, remove assignments
        Used when deleting from Existing or Ready for Staffing views
        Uses client_id for all operations to ensure data consistency
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get table names based on environment  
            master_clients_table = self._get_table_name('master_clients')
            demand_metadata_table = self._get_table_name('demand_metadata')
            demand_supply_assignments_table = self._get_table_name('demand_supply_assignments')
            
            logger.info(f"Deleting demand record for client_id {client_id} with status {status}")
            
            # Step 1: Verify client exists and get client name for metadata operations
            cursor.execute(f"""
                SELECT master_client_id, client_name FROM {master_clients_table} 
                WHERE master_client_id = %s
            """, (client_id,))
            client_result = cursor.fetchone()
            
            if not client_result:
                return {
                    'success': False,
                    'error': f"Client ID {client_id} not found in master_clients table"
                }
            
            master_client_id, client_name = client_result
            
            # Step 2: Remove all assignments for this client using master_client_id
            cursor.execute(f"""
                DELETE FROM {demand_supply_assignments_table} 
                WHERE master_client_id = %s
            """, (master_client_id,))
            deleted_assignments = cursor.rowcount
            
            # Step 3: Delete demand metadata record using client_name (demand_metadata uses client_name as key)
            if status:
                cursor.execute(f"""
                    DELETE FROM {demand_metadata_table} 
                    WHERE client_name = %s AND status = %s
                """, (client_name, status))
            else:
                cursor.execute(f"""
                    DELETE FROM {demand_metadata_table} 
                    WHERE client_name = %s  
                """, (client_name,))
            deleted_demand_records = cursor.rowcount
            
            # Step 4: Reduce confidence to 50% in master_clients using master_client_id
            cursor.execute(f"""
                UPDATE {master_clients_table}
                SET confidence_level = 50, updated_at = CURRENT_TIMESTAMP
                WHERE master_client_id = %s
            """, (master_client_id,))
            updated_confidence = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Deletion completed for client_id {client_id} ({client_name}): {deleted_assignments} assignments removed, {deleted_demand_records} demand records deleted, confidence reduced to 50%")
            
            return {
                'success': True,
                'client_name': client_name,
                'assignments_removed': deleted_assignments,
                'demand_records_deleted': deleted_demand_records, 
                'confidence_updated': updated_confidence > 0
            }
            
        except Exception as e:
            logger.error(f"Error deleting demand record for client_id {client_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
            logger.info(f"Updated confidence for {client_name}: {confidence_level}%")
            return True
            
        except Exception as e:
            logger.error(f"Error updating confidence for {client_name}: {str(e)}")
            return False
            
    def _get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.env_manager and hasattr(self.env_manager, 'get_table_name'):
            return self.env_manager.get_table_name(table_name)
        # Fallback: check if we're in development environment
        env = os.getenv('ENVIRONMENT', 'development')
        if env == 'development':
            return f'dev_{table_name}'
        return table_name
    
    def _auto_create_ready_for_staffing(self, cursor, client_name, confidence_level):
        """Auto-create Ready for Staffing demand entry when confidence >= 70% - ONLY creates new records, never overwrites existing data"""
        try:
            demand_metadata_table = self._get_table_name('demand_metadata')
            
            # Check if record already exists
            cursor.execute(f"SELECT id, status FROM {demand_metadata_table} WHERE client_name = %s", (client_name,))
            existing = cursor.fetchone()
            
            if not existing:
                # Only create new record if none exists - get calculated metadata from unified sales data
                from utils.corrected_mapping_manager import CorrectedMappingManager
                from utils.environment_manager import EnvironmentManager
                env_manager = EnvironmentManager()
                mapping_manager = CorrectedMappingManager(env_manager)
                calculated_metadata = mapping_manager._calculate_metadata_from_unified_sales_data(cursor, client_name)
                
                if calculated_metadata:
                    # Insert new Ready for Staffing demand
                    cursor.execute(f"""
                        INSERT INTO {demand_metadata_table} 
                        (client_name, people_expected, confidence_pct, region, track, 
                         start_date, duration_months, status, is_auto_created, created_at, updated_at, leads)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        client_name,
                        calculated_metadata.get('people_expected', 1.0),
                        confidence_level,
                        calculated_metadata.get('region', 'India'),
                        calculated_metadata.get('track', 'Digital Engineering'),
                        calculated_metadata.get('start_date'),
                        calculated_metadata.get('duration_months', 1),
                        'Ready for Staffing',
                        True,
                        datetime.now(),
                        datetime.now(),
                        0.0
                    ))
                    logger.info(f"Auto-created Ready for Staffing demand for {client_name} (confidence: {confidence_level}%)")
            elif existing[1] != 'Ready for Staffing':
                # ONLY update status and confidence - NEVER overwrite duration, dates, or other existing data
                cursor.execute(f"""
                    UPDATE {demand_metadata_table} 
                    SET status = 'Ready for Staffing', confidence_pct = %s, updated_at = %s
                    WHERE client_name = %s
                """, (confidence_level, datetime.now(), client_name))
                logger.info(f"Updated {client_name} status to Ready for Staffing (confidence: {confidence_level}%) - preserved existing data")
            else:
                # Record exists and is already Ready for Staffing - only update confidence, preserve everything else
                cursor.execute(f"""
                    UPDATE {demand_metadata_table} 
                    SET confidence_pct = %s, updated_at = %s
                    WHERE client_name = %s
                """, (confidence_level, datetime.now(), client_name))
                logger.info(f"Updated {client_name} confidence to {confidence_level}% - preserved all existing data")
                
        except Exception as e:
            logger.error(f"Error auto-creating Ready for Staffing for {client_name}: {str(e)}")
    
    def get_client_confidence(self, client_name):
        """Get confidence for a specific client"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT confidence_level FROM master_clients 
                WHERE client_name = %s
            """, (client_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else 50.0
            
        except Exception as e:
            logger.error(f"Error getting confidence for {client_name}: {str(e)}")
            return 50.0
    
    def get_all_client_confidence(self):
        """Get all client confidence data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT client_name, confidence_level, updated_at
                FROM master_clients
                ORDER BY confidence_level DESC, client_name
            """)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting all client confidence: {str(e)}")
            return pd.DataFrame()
    
    def sync_confidence_across_tables(self):
        """Sync confidence from master_clients to other tables"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Update demand_metadata confidence from master_clients
            cursor.execute("""
                UPDATE demand_metadata 
                SET confidence_pct = mc.confidence_level
                FROM master_clients mc
                WHERE demand_metadata.client_name = mc.client_name
                AND (demand_metadata.confidence_pct IS NULL OR demand_metadata.confidence_pct != mc.confidence_level)
            """)
            
            conn.commit()
            conn.close()
            
            logger.info("Confidence synchronization completed across all tables")
            return True
            
        except Exception as e:
            logger.error(f"Error synchronizing confidence: {str(e)}")
            return False
    
    def get_confidence_report(self):
        """Generate comprehensive confidence report"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get confidence distribution
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN confidence_level >= 80 THEN '80-100%'
                        WHEN confidence_level >= 60 THEN '60-79%'
                        WHEN confidence_level >= 40 THEN '40-59%'
                        WHEN confidence_level >= 20 THEN '20-39%'
                        ELSE '0-19%'
                    END as confidence_range,
                    COUNT(*) as client_count,
                    ROUND(AVG(confidence_level), 1) as avg_confidence
                FROM master_clients
                GROUP BY 
                    CASE 
                        WHEN confidence_level >= 80 THEN '80-100%'
                        WHEN confidence_level >= 60 THEN '60-79%'
                        WHEN confidence_level >= 40 THEN '40-59%'
                        WHEN confidence_level >= 20 THEN '20-39%'
                        ELSE '0-19%'
                    END
                ORDER BY avg_confidence DESC
            """)
            
            distribution = cursor.fetchall()
            
            # Get top confidence clients
            cursor.execute("""
                SELECT client_name, confidence_level, updated_at
                FROM master_clients
                WHERE confidence_level > 60
                ORDER BY confidence_level DESC, client_name
                LIMIT 10
            """)
            
            top_clients = cursor.fetchall()
            
            conn.close()
            
            return {
                'distribution': distribution,
                'top_clients': top_clients,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error generating confidence report: {str(e)}")
            return None

def run_confidence_consolidation():
    """Standalone function to run confidence consolidation"""
    consolidator = ConfidenceConsolidator()
    result = consolidator.consolidate_all_confidence_data()
    
    if result:
        print(f"✓ Confidence consolidation completed:")
        print(f"  - Total clients: {result['total_clients']}")
        print(f"  - Average confidence: {result['avg_confidence']:.1f}%")
        print(f"  - High confidence clients (>60%): {result['high_confidence_clients']}")
        
        # Sync across tables
        if consolidator.sync_confidence_across_tables():
            print("✓ Confidence synchronized across all tables")
        else:
            print("✗ Failed to sync confidence across tables")
    else:
        print("✗ Failed to consolidate confidence data")

if __name__ == "__main__":
    run_confidence_consolidation()