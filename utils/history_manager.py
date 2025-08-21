"""
History Manager for Assignment and Availability Tracking
Provides methods to query and analyze historical assignment and availability data
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class HistoryManager:
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
    
    def get_assignment_history(self, talent_name=None, client_name=None, days_back=30):
        """Get assignment history for specified talent/client within date range"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            where_conditions = []
            params = []
            
            if talent_name:
                where_conditions.append("talent_name = %s")
                params.append(talent_name)
            
            if client_name:
                where_conditions.append("client_name = %s")  
                params.append(client_name)
            
            if days_back:
                cutoff_date = datetime.now() - timedelta(days=days_back)
                where_conditions.append("changed_at >= %s")
                params.append(cutoff_date)
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f"""
                SELECT 
                    id, assignment_id, client_name, talent_name, role,
                    assigned_percentage, duration_months, start_date, end_date,
                    skills, status, action_type, changed_at, changed_by,
                    previous_values, new_values
                FROM assignment_history
                {where_clause}
                ORDER BY changed_at DESC
            """
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting assignment history: {str(e)}")
            return pd.DataFrame()
    
    def get_availability_history(self, talent_name=None, days_back=30):
        """Get availability history for specified talent within date range"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            where_conditions = []
            params = []
            
            if talent_name:
                where_conditions.append("talent_name = %s")
                params.append(talent_name)
            
            if days_back:
                cutoff_date = datetime.now() - timedelta(days=days_back)
                where_conditions.append("changed_at >= %s")
                params.append(cutoff_date)
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f"""
                SELECT 
                    id, talent_name, previous_availability, new_availability,
                    previous_assigned, new_assigned, change_reason,
                    changed_at, changed_by, related_assignment_id, related_client
                FROM availability_history
                {where_clause}
                ORDER BY changed_at DESC
            """
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting availability history: {str(e)}")
            return pd.DataFrame()
    
    def get_talent_assignment_timeline(self, talent_name):
        """Get complete assignment timeline for a specific talent"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get assignment history
            cursor.execute("""
                SELECT 
                    changed_at, action_type, client_name, assigned_percentage,
                    duration_months, start_date, end_date, status
                FROM assignment_history
                WHERE talent_name = %s
                ORDER BY changed_at DESC
            """, (talent_name,))
            
            assignment_history = cursor.fetchall()
            
            # Get availability history
            cursor.execute("""
                SELECT 
                    changed_at, previous_availability, new_availability,
                    change_reason, related_client
                FROM availability_history
                WHERE talent_name = %s
                ORDER BY changed_at DESC
            """, (talent_name,))
            
            availability_history = cursor.fetchall()
            conn.close()
            
            return {
                'assignments': assignment_history,
                'availability': availability_history
            }
            
        except Exception as e:
            logger.error(f"Error getting talent timeline: {str(e)}")
            return {'assignments': [], 'availability': []}
    
    def get_last_known_availability(self, talent_name):
        """Get the last known availability before current assignments"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT previous_availability, changed_at
                FROM availability_history
                WHERE talent_name = %s
                ORDER BY changed_at DESC
                LIMIT 1
            """, (talent_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return float(result[0]), result[1]
            else:
                return None, None
                
        except Exception as e:
            logger.error(f"Error getting last known availability: {str(e)}")
            return None, None
    
    def backup_current_assignments(self):
        """Manually backup current assignments before major changes"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Insert current assignments into history as baseline
            cursor.execute("""
                INSERT INTO assignment_history (
                    assignment_id, client_name, talent_name, role, assigned_percentage,
                    duration_months, start_date, end_date, skills, status,
                    action_type, new_values, changed_by
                )
                SELECT 
                    id, client_name, talent_name, role, assigned_percentage,
                    duration_months, start_date, end_date, skills, status,
                    'BACKUP', row_to_json(demand_supply_assignments.*), 'manual_backup'
                FROM demand_supply_assignments
                WHERE status = 'Active'
            """)
            
            # Backup current availability
            cursor.execute("""
                INSERT INTO availability_history (
                    talent_name, new_availability, change_reason, changed_by
                )
                SELECT 
                    name, availability_percentage, 'Manual backup', 'manual_backup'
                FROM talent_supply
            """)
            
            conn.commit()
            conn.close()
            
            logger.info("Manual backup of current assignments and availability completed")
            return True
            
        except Exception as e:
            logger.error(f"Error backing up assignments: {str(e)}")
            return False
    
    def log_availability_change(self, talent_name, previous_availability, new_availability, 
                               reason="Manual update", related_client=None, related_assignment_id=None):
        """Manually log an availability change"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO availability_history (
                    talent_name, previous_availability, new_availability,
                    change_reason, related_client, related_assignment_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (talent_name, previous_availability, new_availability, 
                  reason, related_client, related_assignment_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Logged availability change for {talent_name}: {previous_availability}% -> {new_availability}%")
            return True
            
        except Exception as e:
            logger.error(f"Error logging availability change: {str(e)}")
            return False
    
    def get_summary_stats(self):
        """Get summary statistics of historical data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Assignment history stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_changes,
                    COUNT(DISTINCT talent_name) as unique_talent,
                    COUNT(DISTINCT client_name) as unique_clients,
                    MIN(changed_at) as earliest_change,
                    MAX(changed_at) as latest_change
                FROM assignment_history
            """)
            
            assignment_stats = cursor.fetchone()
            
            # Availability history stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_changes,
                    COUNT(DISTINCT talent_name) as unique_talent,
                    MIN(changed_at) as earliest_change,
                    MAX(changed_at) as latest_change
                FROM availability_history
            """)
            
            availability_stats = cursor.fetchone()
            conn.close()
            
            return {
                'assignment_history': {
                    'total_changes': assignment_stats[0],
                    'unique_talent': assignment_stats[1],
                    'unique_clients': assignment_stats[2],
                    'earliest_change': assignment_stats[3],
                    'latest_change': assignment_stats[4]
                },
                'availability_history': {
                    'total_changes': availability_stats[0],
                    'unique_talent': availability_stats[1],
                    'earliest_change': availability_stats[2],
                    'latest_change': availability_stats[3]
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting summary stats: {str(e)}")
            return {}