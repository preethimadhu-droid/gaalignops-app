"""
Auto Demand Mapping Manager
Manages auto-created demand mapping records from database triggers
Works with PostgreSQL trigger system for automatic mapping creation
"""

import psycopg2
import os
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

class AutoDemandMappingManager:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
    
    def get_auto_created_mappings(self):
        """Get all auto-created demand mappings from database trigger system"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT dm.client_name, dm.status, dm.people_expected, dm.confidence_pct,
                       dm.region, dm.duration_months, dm.start_date, dm.created_at,
                       mc.confidence_level
                FROM demand_metadata dm
                JOIN master_clients mc ON dm.client_name = mc.client_name
                WHERE dm.is_auto_created = true
                ORDER BY dm.created_at DESC
            """)
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            return results
                
        except Exception as e:
            logger.error(f"Error getting auto-created mappings: {e}")
            return []
    
    def get_trigger_status(self):
        """Check if the auto-mapping trigger is active"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT trigger_name, event_manipulation, action_statement
                FROM information_schema.triggers 
                WHERE trigger_name = 'auto_demand_mapping_trigger'
                AND event_object_table = 'master_clients'
            """)
            
            result = cursor.fetchone()
            conn.close()
            return result is not None
                
        except Exception as e:
            logger.error(f"Error checking trigger status: {e}")
            return False
    
    def get_potential_auto_mappings(self):
        """Get clients with >60% confidence that could trigger auto-mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT mc.client_name, mc.confidence_level,
                       CASE 
                           WHEN dm.client_name IS NOT NULL THEN 'Already Mapped'
                           ELSE 'Ready for Auto-Mapping'
                       END as mapping_status
                FROM master_clients mc
                LEFT JOIN demand_metadata dm ON mc.client_name = dm.client_name
                WHERE mc.confidence_level > 60
                ORDER BY mc.confidence_level DESC
            """)
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            return results
                
        except Exception as e:
            logger.error(f"Error getting potential auto-mappings: {e}")
            return []
    
    def trigger_auto_mapping(self, client_name):
        """Manually trigger auto-mapping for a specific client by updating confidence"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get current confidence
            cursor.execute("""
                SELECT confidence_level FROM master_clients 
                WHERE client_name = %s
            """, (client_name,))
            
            result = cursor.fetchone()
            if result:
                current_confidence = result[0]
                if current_confidence > 60:
                    # Trigger by temporarily lowering and raising confidence
                    cursor.execute("""
                        UPDATE master_clients 
                        SET confidence_level = 50 
                        WHERE client_name = %s
                    """, (client_name,))
                    
                    cursor.execute("""
                        UPDATE master_clients 
                        SET confidence_level = %s 
                        WHERE client_name = %s
                    """, (current_confidence, client_name))
                    
                    conn.commit()
                    conn.close()
                    return True
            
            conn.close()
            return False
                
        except Exception as e:
            logger.error(f"Error triggering auto-mapping for {client_name}: {e}")
            return False
    
    def update_mapping_status(self, client_name, new_status):
        """Update status of an auto-created demand mapping record"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE demand_metadata 
                SET status = %s, updated_at = %s
                WHERE client_name = %s AND is_auto_created = true
            """, (new_status, datetime.now(), client_name))
            
            conn.commit()
            success = cursor.rowcount > 0
            conn.close()
            return success
                
        except Exception as e:
            logger.error(f"Error updating mapping status for {client_name}: {e}")
            return False
    
    def convert_to_manual_mapping(self, client_name):
        """Convert auto-created mapping to manual mapping (remove auto-created flag)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE demand_metadata 
                SET is_auto_created = false, updated_at = %s
                WHERE client_name = %s AND is_auto_created = true
            """, (datetime.now(), client_name))
            
            conn.commit()
            success = cursor.rowcount > 0
            conn.close()
            return success
                
        except Exception as e:
            logger.error(f"Error converting to manual mapping for {client_name}: {e}")
            return False
    
    def get_client_demand_details(self, client_name):
        """Get detailed demand information for a client from unified sales data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    month, year, value, metric_type, region, lob, owner
                FROM unified_sales_data
                WHERE account_name = %s
                AND value > 0
                ORDER BY year, 
                    CASE month 
                        WHEN 'January' THEN 1
                        WHEN 'February' THEN 2
                        WHEN 'March' THEN 3
                        WHEN 'April' THEN 4
                        WHEN 'May' THEN 5
                        WHEN 'June' THEN 6
                        WHEN 'July' THEN 7
                        WHEN 'August' THEN 8
                        WHEN 'September' THEN 9
                        WHEN 'October' THEN 10
                        WHEN 'November' THEN 11
                        WHEN 'December' THEN 12
                    END
            """, (client_name,))
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            return results
                
        except Exception as e:
            logger.error(f"Error getting client demand details: {e}")
            return []
    
    def get_system_status(self):
        """Get comprehensive status of the auto-mapping system"""
        try:
            auto_mappings = self.get_auto_created_mappings()
            potential_mappings = self.get_potential_auto_mappings()
            trigger_active = self.get_trigger_status()
            
            return {
                'trigger_active': trigger_active,
                'auto_mappings_count': len(auto_mappings),
                'auto_mappings': auto_mappings,
                'potential_mappings_count': len([m for m in potential_mappings if m['mapping_status'] == 'Ready for Auto-Mapping']),
                'potential_mappings': potential_mappings,
                'system_healthy': trigger_active and len(auto_mappings) >= 0
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                'trigger_active': False,
                'auto_mappings_count': 0,
                'auto_mappings': [],
                'potential_mappings_count': 0,
                'potential_mappings': [],
                'system_healthy': False
            }

# Global instance
auto_demand_manager = AutoDemandMappingManager()