"""
Demand-Supply Mapping Manager
Handles assignment mappings between demand and supply
"""

import psycopg2
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class MappingManager:
    """Manage demand-supply assignment mappings"""
    
    def __init__(self, env_manager=None):
        self.database_url = os.environ.get('DATABASE_URL')
        
        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False
        
        self.create_table()
    
    def get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.use_dev_tables:
            return f"dev_{table_name}"
        return table_name
    
    def create_table(self):
        """Create mapping table if not exists"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Table already exists with correct structure - just ensure it's there
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demand_supply_assignments (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER,
                    talent_id INTEGER,
                    assignment_percentage REAL,
                    duration_months INTEGER,
                    start_date DATE,
                    end_date DATE,
                    status TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    master_client_id INTEGER,
                    start_month DATE,
                    end_month DATE
                )
            """)
            
            # Create demand metadata table for storing demand panel information
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demand_metadata (
                    id SERIAL PRIMARY KEY,
                    client_name TEXT UNIQUE NOT NULL,
                    duration_months INTEGER,
                    start_date DATE,
                    end_date DATE,
                    people_expected REAL,
                    confidence_pct REAL,
                    region TEXT,
                    track TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error creating mapping table: {str(e)}")
    
    def save_assignment(self, assignment_data):
        """Save a single assignment mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if mapping already exists and get current assignment percentage
            cursor.execute("""
                SELECT id, assigned_percentage FROM demand_supply_assignments 
                WHERE client_name = %s AND talent_name = %s
            """, (assignment_data['client_name'], assignment_data['talent_name']))
            
            existing = cursor.fetchone()
            previous_percentage = 0.0
            
            if existing:
                previous_percentage = float(existing[1])  # Get the current assigned percentage
                
                # Check if data actually changed before updating
                current_data = (
                    assignment_data['role'],
                    float(assignment_data['assigned_percentage']),
                    assignment_data['assignment_duration'],
                    assignment_data['assignment_start_date'],
                    assignment_data['assignment_end_date'],
                    assignment_data.get('skills', '')
                )
                
                # Get existing data to compare
                cursor.execute("""
                    SELECT role, assigned_percentage, assignment_duration, 
                           assignment_start_date, assignment_end_date, skills
                    FROM demand_supply_assignments 
                    WHERE client_name = %s AND talent_name = %s
                """, (assignment_data['client_name'], assignment_data['talent_name']))
                
                existing_data = cursor.fetchone()
                
                # Only update if data has actually changed
                data_changed = (
                    existing_data[0] != current_data[0] or  # role
                    float(existing_data[1]) != current_data[1] or  # assigned_percentage
                    existing_data[2] != current_data[2] or  # duration
                    existing_data[3] != current_data[3] or  # start_date
                    existing_data[4] != current_data[4] or  # end_date
                    (existing_data[5] or '') != current_data[5]  # skills
                )
                
                if data_changed:
                    # Update existing mapping
                    cursor.execute("""
                        UPDATE demand_supply_assignments 
                        SET role = %s, assigned_percentage = %s, assignment_duration = %s,
                            assignment_start_date = %s, assignment_end_date = %s, 
                            skills = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE client_name = %s AND talent_name = %s
                    """, (
                        assignment_data['role'],
                        assignment_data['assigned_percentage'],
                        assignment_data['assignment_duration'],
                        assignment_data['assignment_start_date'],
                        assignment_data['assignment_end_date'],
                        assignment_data.get('skills', ''),
                        assignment_data['client_name'],
                        assignment_data['talent_name']
                    ))
            else:
                # Insert new mapping
                cursor.execute("""
                    INSERT INTO demand_supply_assignments 
                    (client_name, talent_name, role, assigned_percentage, assignment_duration,
                     assignment_start_date, assignment_end_date, skills)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    assignment_data['client_name'],
                    assignment_data['talent_name'],
                    assignment_data['role'],
                    assignment_data['assigned_percentage'],
                    assignment_data['assignment_duration'],
                    assignment_data['assignment_start_date'],
                    assignment_data['assignment_end_date'],
                    assignment_data.get('skills', '')
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Assignment saved: {assignment_data['talent_name']} -> {assignment_data['client_name']}")
            
            # Return both success status and the percentage change information
            percentage_changed = previous_percentage != float(assignment_data['assigned_percentage'])
            
            return {
                'success': True,
                'previous_percentage': previous_percentage,
                'new_percentage': float(assignment_data['assigned_percentage']),
                'percentage_changed': percentage_changed,
                'data_updated': data_changed if existing else True  # New record is always considered updated
            }
            
        except Exception as e:
            logger.error(f"Error saving assignment: {str(e)}")
            return {
                'success': False,
                'previous_percentage': 0.0,
                'new_percentage': 0.0,
                'percentage_changed': False,
                'data_updated': False
            }
    
    def get_all_assignments(self):
        """Get all assignment mappings"""
        try:
            conn = psycopg2.connect(self.database_url)
            df = pd.read_sql_query("""
                SELECT * FROM demand_supply_assignments 
                ORDER BY created_at DESC
            """, conn)
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving assignments: {str(e)}")
            return pd.DataFrame()
    
    def get_assignments_by_client(self, client_name):
        """Get assignments for a specific client"""
        try:
            conn = psycopg2.connect(self.database_url)
            df = pd.read_sql_query("""
                SELECT * FROM demand_supply_assignments 
                WHERE client_name = %s
                ORDER BY created_at DESC
            """, conn, params=[client_name])
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving client assignments: {str(e)}")
            return pd.DataFrame()
    
    def delete_assignment(self, client_name, talent_name):
        """Delete a specific assignment"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM demand_supply_assignments 
                WHERE client_name = %s AND talent_name = %s
            """, (client_name, talent_name))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Assignment deleted: {talent_name} from {client_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting assignment: {str(e)}")
            return False
    
    def save_demand_metadata(self, client_name, demand_info):
        """Save demand panel metadata for a client"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if metadata already exists
            cursor.execute("""
                SELECT id FROM demand_metadata WHERE client_name = %s
            """, (client_name,))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing metadata
                cursor.execute("""
                    UPDATE demand_metadata 
                    SET duration_months = %s, start_date = %s, end_date = %s, 
                        people_expected = %s, confidence_pct = %s, region = %s, 
                        track = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE client_name = %s
                """, (
                    demand_info.get('duration_months'),
                    demand_info.get('start_date'),
                    demand_info.get('end_date'),
                    demand_info.get('people_expected'),
                    demand_info.get('confidence'),
                    demand_info.get('region'),
                    demand_info.get('track'),
                    client_name
                ))
            else:
                # Insert new metadata
                cursor.execute("""
                    INSERT INTO demand_metadata 
                    (client_name, duration_months, start_date, end_date, people_expected, 
                     confidence_pct, region, track)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    client_name,
                    demand_info.get('duration_months'),
                    demand_info.get('start_date'),
                    demand_info.get('end_date'),
                    demand_info.get('people_expected'),
                    demand_info.get('confidence'),
                    demand_info.get('region'),
                    demand_info.get('track')
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Demand metadata saved for {client_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving demand metadata: {str(e)}")
            return False
    
    def get_demand_metadata(self, client_name):
        """Get demand metadata for a specific client"""
        try:
            conn = psycopg2.connect(self.database_url)
            df = pd.read_sql_query("""
                SELECT * FROM demand_metadata 
                WHERE client_name = %s
            """, conn, params=[client_name])
            conn.close()
            return df.iloc[0] if not df.empty else None
            
        except Exception as e:
            logger.error(f"Error retrieving demand metadata: {str(e)}")
            return None