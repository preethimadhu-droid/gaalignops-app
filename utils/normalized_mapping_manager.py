"""
Normalized Mapping Manager
Works with the properly normalized database structure where:
- demand_supply_assignments: Only assignment context (client_id, talent_id, %, duration, dates)
- clients: Client master data 
- unified_sales_data: Demand data with financial forecasts
- talent_supply: Supply data with calculated availability
"""

import psycopg2
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class NormalizedMappingManager:
    """Manage demand-supply assignments with normalized database structure"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
    
    def get_all_clients(self):
        """Get all unique clients for dropdown"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT master_client_id, client_name
                FROM master_clients 
                ORDER BY client_name
            """)
            
            columns = ['master_client_id', 'client_name']
            clients_df = pd.DataFrame(cursor.fetchall(), columns=columns)
            
            conn.close()
            return clients_df
            
        except Exception as e:
            logger.error(f"Error getting clients: {str(e)}")
            return pd.DataFrame()
    
    def get_client_demand_info(self, client_id):
        """Get demand information for a specific client"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get client basic info
            cursor.execute("""
                SELECT client_name, account_track, region, lob, offering
                FROM demand_planning 
                WHERE client_id = %s
            """, (client_id,))
            
            client_info = cursor.fetchone()
            if not client_info:
                return None
            
            # Get financial data from unified_sales_data
            cursor.execute("""
                SELECT 
                    month, 
                    metric_type, 
                    value, 
                    confidence,
                    financial_year
                FROM unified_sales_data usd
                JOIN demand_planning dp ON dp.client_name = usd.account_name
                WHERE dp.client_id = %s
                AND metric_type = 'Planned'
                AND value > 0
                ORDER BY month
            """, (client_id,))
            
            financial_data = cursor.fetchall()
            
            # Calculate duration from months with planned > 0
            active_months = []
            total_confidence = 0
            count_confidence = 0
            
            for row in financial_data:
                active_months.append(row[0])  # month
                if row[3]:  # confidence
                    total_confidence += float(row[3])
                    count_confidence += 1
            
            duration_months = len(set(active_months))
            avg_confidence = total_confidence / count_confidence if count_confidence > 0 else 0
            
            result = {
                'client_id': client_id,
                'client_name': client_info[0],
                'account_track': client_info[1],
                'region': client_info[2],
                'lob': client_info[3],
                'offering': client_info[4],
                'duration_months': duration_months,
                'active_months': sorted(list(set(active_months))),
                'avg_confidence': avg_confidence
            }
            
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"Error getting client demand info: {str(e)}")
            return None
    
    def save_demand_info(self, demand_data):
        """Save demand panel information (for future demand management features)"""
        try:
            # For now, this is a placeholder as the demand info is stored in unified_sales_data
            # Future enhancement could store additional demand metadata
            logger.info(f"Demand info saved for client: {demand_data.get('client_name', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving demand info: {str(e)}")
            return False
    
    def get_available_talent(self):
        """Get all available talent with current availability"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    id as talent_id,
                    name,
                    role,
                    skills,
                    region,
                    availability_percentage,
                    total_assignment_percentage,
                    employment_status,
                    type
                FROM talent_supply 
                WHERE employment_status IN ('Beach', 'Partially Allocated', 'Support')
                ORDER BY availability_percentage DESC, name
            """)
            
            columns = ['talent_id', 'name', 'role', 'skills', 'region', 
                      'availability_percentage', 'total_assignment_percentage', 
                      'employment_status', 'type']
            
            talent_df = pd.DataFrame(cursor.fetchall(), columns=columns)
            
            conn.close()
            return talent_df
            
        except Exception as e:
            logger.error(f"Error getting available talent: {str(e)}")
            return pd.DataFrame()
    
    def save_assignment(self, assignment_data):
        """Save talent assignment with proper ID references"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Insert assignment
            cursor.execute("""
                INSERT INTO demand_supply_assignments (
                    master_client_id, talent_id, assignment_percentage, duration_months,
                    start_month, end_month, status, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (master_client_id, talent_id) 
                DO UPDATE SET
                    assignment_percentage = EXCLUDED.assignment_percentage,
                    duration_months = EXCLUDED.duration_months,
                    start_month = EXCLUDED.start_month,
                    end_month = EXCLUDED.end_month,
                    status = EXCLUDED.status,
                    notes = EXCLUDED.notes,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                assignment_data['master_client_id'],
                assignment_data['talent_id'], 
                assignment_data['assignment_percentage'],
                assignment_data['duration_months'],
                assignment_data['start_date'],
                assignment_data['end_date'],
                assignment_data.get('status', 'Active'),
                assignment_data.get('notes', '')
            ))
            
            assignment_id = cursor.fetchone()[0]
            
            # Update talent total assignment and availability
            cursor.execute("""
                UPDATE talent_supply 
                SET 
                    total_assignment_percentage = (
                        SELECT COALESCE(SUM(assignment_percentage), 0)
                        FROM demand_supply_assignments 
                        WHERE talent_id = %s AND status = 'Active'
                    ),
                    availability_percentage = GREATEST(0, 100 - (
                        SELECT COALESCE(SUM(assignment_percentage), 0)
                        FROM demand_supply_assignments 
                        WHERE talent_id = %s AND status = 'Active'
                    ))
                WHERE id = %s
            """, (assignment_data['talent_id'], assignment_data['talent_id'], assignment_data['talent_id']))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Assignment saved successfully: ID {assignment_id}")
            return assignment_id
            
        except Exception as e:
            logger.error(f"Error saving assignment: {str(e)}")
            conn.rollback()
            conn.close()
            return None
    
    def get_all_assignments(self):
        """Get all assignments with full details"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    dsa.id as assignment_id,
                    mc.client_name,
                    '' as account_track,
                    '' as client_region,
                    '' as lob,
                    '' as offering,
                    ts.name as talent_name,
                    ts.role,
                    ts.skills,
                    ts.region as talent_region,
                    dsa.assignment_percentage,
                    dsa.duration_months,
                    dsa.start_month as assignment_start_date,
                    dsa.end_month as assignment_end_date,
                    dsa.status,
                    dsa.notes,
                    ts.availability_percentage,
                    ts.total_assignment_percentage,
                    1.0 as people_expected
                FROM demand_supply_assignments dsa
                JOIN master_clients mc ON dsa.master_client_id = mc.master_client_id
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                WHERE dsa.status = 'Active'
                ORDER BY mc.client_name, ts.name
            """)
            
            columns = [
                'assignment_id', 'client_name', 'account_track', 'client_region', 
                'lob', 'offering', 'talent_name', 'role', 'skills', 'talent_region',
                'assignment_percentage', 'duration_months', 'assignment_start_date', 'assignment_end_date',
                'status', 'notes', 'availability_percentage', 'total_assignment_percentage', 'people_expected'
            ]
            
            assignments_df = pd.DataFrame(cursor.fetchall(), columns=columns)
            
            conn.close()
            return assignments_df
            
        except Exception as e:
            logger.error(f"Error getting all assignments: {str(e)}")
            return pd.DataFrame()
    
    def delete_assignment(self, assignment_id):
        """Delete an assignment and update talent availability"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get talent_id before deletion
            cursor.execute("SELECT talent_id FROM demand_supply_assignments WHERE id = %s", (assignment_id,))
            result = cursor.fetchone()
            if not result:
                return False
                
            talent_id = result[0]
            
            # Delete assignment
            cursor.execute("DELETE FROM demand_supply_assignments WHERE id = %s", (assignment_id,))
            
            # Update talent availability
            cursor.execute("""
                UPDATE talent_supply 
                SET 
                    total_assignment_percentage = (
                        SELECT COALESCE(SUM(assignment_percentage), 0)
                        FROM demand_supply_assignments 
                        WHERE talent_id = %s AND status = 'Active'
                    ),
                    availability_percentage = GREATEST(0, 100 - (
                        SELECT COALESCE(SUM(assignment_percentage), 0)
                        FROM demand_supply_assignments 
                        WHERE talent_id = %s AND status = 'Active'
                    ))
                WHERE id = %s
            """, (talent_id, talent_id, talent_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Assignment {assignment_id} deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting assignment: {str(e)}")
            conn.rollback()
            conn.close()
            return False
    
    def get_assignment_summary(self):
        """Get summary statistics for assignments"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_assignments,
                    COUNT(DISTINCT client_name) as unique_clients,
                    COUNT(DISTINCT talent_name) as assigned_talent,
                    ROUND(AVG(assignment_percentage)::numeric, 2) as avg_assignment_percentage,
                    ROUND(AVG(duration_months)::numeric, 1) as avg_duration_months
                FROM assignment_details
                WHERE status = 'Active'
            """)
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'total_assignments': result[0],
                    'unique_clients': result[1], 
                    'assigned_talent': result[2],
                    'avg_assignment_percentage': result[3],
                    'avg_duration_months': result[4]
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting assignment summary: {str(e)}")
            return {}