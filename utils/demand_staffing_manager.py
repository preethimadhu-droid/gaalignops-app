"""
Demand-Staffing Manager for managing demand-supply mapping records
"""

import os
import psycopg2
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DemandStaffingManager:
    """Manage demand-staffing mapping data"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not found")
        self.create_tables()
    
    def create_tables(self):
        """Create demand-staffing mapping table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Create demand-staffing mapping table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demand_staffing (
                    id SERIAL PRIMARY KEY,
                    client_name VARCHAR(255) NOT NULL,
                    talent_name VARCHAR(255) NOT NULL,
                    role VARCHAR(255) NOT NULL,
                    assigned_percentage DECIMAL DEFAULT 0,
                    start_date VARCHAR(50),
                    end_date VARCHAR(50),
                    duration_months INTEGER DEFAULT 0,
                    people_expected DECIMAL DEFAULT 1,
                    confidence_pct DECIMAL DEFAULT 0,
                    region VARCHAR(255),
                    current_assignment VARCHAR(255),
                    status VARCHAR(100) DEFAULT 'Active',
                    notes VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("Demand-staffing table created successfully")
            
        except Exception as e:
            logger.error(f"Error creating demand-staffing table: {str(e)}")
            raise
    
    def save_mapping(self, mapping_data):
        """Save a demand-staffing mapping record"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO demand_staffing (
                    client_name, talent_name, role, assigned_percentage,
                    start_date, end_date, duration_months, people_expected,
                    confidence_pct, region, current_assignment, status, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                mapping_data.get('client_name', ''),
                mapping_data.get('talent_name', ''),
                mapping_data.get('role', ''),
                mapping_data.get('assigned_percentage', 0),
                mapping_data.get('start_date', ''),
                mapping_data.get('end_date', ''),
                mapping_data.get('duration_months', 0),
                mapping_data.get('people_expected', 1),
                mapping_data.get('confidence_pct', 0),
                mapping_data.get('region', ''),
                mapping_data.get('current_assignment', ''),
                mapping_data.get('status', 'Active'),
                mapping_data.get('notes', '')
            ))
            
            mapping_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            
            logger.info(f"Saved demand-staffing mapping with ID: {mapping_id}")
            return mapping_id
            
        except Exception as e:
            logger.error(f"Error saving mapping: {str(e)}")
            raise
    
    def get_all_mappings(self):
        """Get all demand-staffing mappings"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            query = """
                SELECT * FROM demand_staffing 
                ORDER BY created_at DESC
            """
            
            mappings_df = pd.read_sql_query(query, conn)
            conn.close()
            
            return mappings_df
            
        except Exception as e:
            logger.error(f"Error retrieving mappings: {str(e)}")
            return pd.DataFrame()
    
    def get_mappings_by_client(self, client_name):
        """Get mappings for a specific client"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            query = """
                SELECT * FROM demand_staffing 
                WHERE client_name = %s
                ORDER BY created_at DESC
            """
            
            mappings_df = pd.read_sql_query(query, conn, params=[client_name])
            conn.close()
            
            return mappings_df
            
        except Exception as e:
            logger.error(f"Error retrieving mappings for client {client_name}: {str(e)}")
            return pd.DataFrame()
    
    def update_mapping(self, mapping_id, updated_data):
        """Update an existing mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Build dynamic update query
            set_clauses = []
            values = []
            
            for key, value in updated_data.items():
                if key != 'id':
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
            
            values.append(mapping_id)
            
            query = f"""
                UPDATE demand_staffing 
                SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """
            
            cursor.execute(query, values)
            conn.commit()
            conn.close()
            
            logger.info(f"Updated mapping ID: {mapping_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating mapping: {str(e)}")
            return False
    
    def delete_mapping(self, mapping_id):
        """Delete a mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM demand_staffing WHERE id = %s", (mapping_id,))
            conn.commit()
            conn.close()
            
            logger.info(f"Deleted mapping ID: {mapping_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting mapping: {str(e)}")
            return False
    
    def get_mapping_summary(self):
        """Get summary statistics of mappings"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            query = """
                SELECT 
                    COUNT(*) as total_mappings,
                    COUNT(DISTINCT client_name) as unique_clients,
                    COUNT(DISTINCT talent_name) as unique_talents,
                    COUNT(DISTINCT role) as unique_roles,
                    AVG(assigned_percentage) as avg_assignment_pct,
                    COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_mappings
                FROM demand_staffing
            """
            
            summary = pd.read_sql_query(query, conn).iloc[0].to_dict()
            conn.close()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting mapping summary: {str(e)}")
            return {}