"""
Corrected Demand-Supply Mapping Manager
Uses current database schema with master_clients and talent_supply tables
"""

import psycopg2
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CorrectedMappingManager:
    """Manage demand-supply assignment mappings with current schema"""
    
    def __init__(self, env_manager=None):
        self.database_url = os.environ.get('DATABASE_URL')
        
        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False
    
    def get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.use_dev_tables:
            return f"dev_{table_name}"
        return table_name
    
    def save_assignment(self, assignment_data):
        """Save assignment mapping using current database schema"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Validate required data
            if not assignment_data.get('client_name') or not assignment_data.get('talent_name'):
                raise Exception("Client name and talent name are required")
            
            # Check if this is an update (existing assignment) or new assignment
            assignment_id = assignment_data.get('assignment_id')
            is_update = assignment_id is not None
            
            if is_update:
                return self.update_assignment(assignment_data)
            else:
                return self.create_new_assignment(assignment_data)
            
        except Exception as e:
            logger.error(f"Error saving assignment: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def create_new_assignment(self, assignment_data):
        """Create a new assignment"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get master_client_id from client name
            master_clients_table = self.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", 
                          (assignment_data.get('client_name'),))
            client_result = cursor.fetchone()
            master_client_id = client_result[0] if client_result else None
            
            if not master_client_id:
                # Create new master client if not exists
                cursor.execute(f"INSERT INTO {master_clients_table} (client_name) VALUES (%s) RETURNING master_client_id", 
                              (assignment_data.get('client_name'),))
                master_client_id = cursor.fetchone()[0]
            
            # Get talent internal ID
            talent_supply_table = self.get_table_name('talent_supply')
            cursor.execute(f"SELECT id FROM {talent_supply_table} WHERE name = %s", 
                          (assignment_data.get('talent_name'),))
            internal_id_result = cursor.fetchone()
            internal_talent_id = internal_id_result[0] if internal_id_result else None
            
            if not internal_talent_id:
                raise Exception(f"Talent '{assignment_data.get('talent_name')}' not found in talent_supply table")
            
            # Insert new assignment
            demand_supply_assignments_table = self.get_table_name('demand_supply_assignments')
            cursor.execute(f"""
                INSERT INTO {demand_supply_assignments_table} 
                (client_id, master_client_id, talent_id, assignment_percentage, duration_months, 
                 start_date, end_date, status, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                master_client_id,
                master_client_id,
                internal_talent_id,
                assignment_data.get('assigned_percentage'),
                assignment_data.get('assignment_duration'),
                assignment_data.get('assignment_start_date'),
                assignment_data.get('assignment_end_date'),
                'Active',
                assignment_data.get('skills', '')
            ))
            assignment_id = cursor.fetchone()[0]
            logger.info(f"Created new assignment: {assignment_data.get('assigned_percentage')}% for talent {assignment_data.get('talent_name')} on client {assignment_data.get('client_name')}")
            
            # CRITICAL: Update demand_metadata status from "Ready for Staffing" to "Active" 
            # when first assignment is created for a client
            demand_metadata_table = self.get_table_name('demand_metadata')
            cursor.execute(f"""
                UPDATE {demand_metadata_table} 
                SET status = 'Active', updated_at = CURRENT_TIMESTAMP
                WHERE client_name = %s AND status = 'Ready for Staffing'
            """, (assignment_data.get('client_name'),))
            
            if cursor.rowcount > 0:
                logger.info(f"Updated demand_metadata status to 'Active' for {assignment_data.get('client_name')}")
            
            conn.commit()
            conn.close()
            
            # Recalculate availability after new assignment
            from utils.supply_data_manager import SupplyDataManager
            supply_manager = SupplyDataManager()
            availability_result = supply_manager.recalculate_availability(assignment_data.get('talent_name'))
            
            return {
                'success': True, 
                'assignment_id': assignment_id,
                'availability_update': availability_result,
                'percentage_changed': True,  # New assignment always represents a change
                'previous_percentage': 0.0,  # New assignment starts from 0%
                'new_percentage': float(assignment_data.get('assigned_percentage', 0))
            }
            
        except Exception as e:
            logger.error(f"Error creating assignment: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return {'success': False, 'error': str(e)}
    
    def update_assignment(self, assignment_data):
        """Update an existing assignment with proper availability adjustment"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            assignment_id = assignment_data.get('assignment_id')
            talent_name = assignment_data.get('talent_name')
            new_assignment_percent = float(assignment_data.get('assigned_percentage', 0))
            
            # Get current assignment percentage before update
            cursor.execute("""
                SELECT dsa.assignment_percentage, ts.name, mc.client_name
                FROM demand_supply_assignments dsa
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                JOIN master_clients mc ON dsa.master_client_id = mc.master_client_id
                WHERE dsa.id = %s
            """, (assignment_id,))
            
            current_result = cursor.fetchone()
            if not current_result:
                raise Exception(f"Assignment ID {assignment_id} not found")
            
            old_assignment_percent = float(current_result[0])
            current_talent_name = current_result[1]
            client_name = current_result[2]
            
            # Calculate the assignment change
            assignment_change = new_assignment_percent - old_assignment_percent
            
            logger.info(f"Updating assignment for {current_talent_name} on {client_name}: {old_assignment_percent}% → {new_assignment_percent}% (change: {assignment_change:+.1f}%)")
            
            # Backup current state before making changes
            from utils.enhanced_assignment_manager import EnhancedAssignmentManager
            enhanced_manager = EnhancedAssignmentManager()
            enhanced_manager.backup_current_data_before_change(
                current_talent_name, 
                f"Before assignment edit: {old_assignment_percent}% → {new_assignment_percent}%"
            )
            
            # Update the assignment record
            cursor.execute("""
                UPDATE demand_supply_assignments 
                SET assignment_percentage = %s,
                    duration_months = %s,
                    start_date = %s,
                    end_date = %s,
                    notes = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                new_assignment_percent,
                assignment_data.get('assignment_duration'),
                assignment_data.get('assignment_start_date'),
                assignment_data.get('assignment_end_date'),
                assignment_data.get('skills', ''),
                assignment_id
            ))
            
            # Option A: Recalculate availability using ONLY new assignments
            # Don't manually adjust - let the recalculation logic handle it properly
            
            conn.commit()
            conn.close()
            
            logger.info(f"Assignment updated successfully: {old_assignment_percent}% → {new_assignment_percent}% (change: {assignment_change:+.1f}%)")
            
            return {
                'success': True,
                'assignment_id': assignment_id,
                'assignment_change': assignment_change,
                'percentage_changed': abs(assignment_change) > 0,
                'previous_percentage': old_assignment_percent,
                'new_percentage': new_assignment_percent
            }
            
        except Exception as e:
            logger.error(f"Error updating assignment: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return {'success': False, 'error': str(e)}
    
    def get_assignment_by_id(self, assignment_id):
        """Get assignment details by ID for editing"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    dsa.id,
                    mc.client_name,
                    ts.name as talent_name,
                    dsa.assignment_percentage,
                    dsa.duration_months,
                    dsa.start_date,
                    dsa.end_date,
                    dsa.notes,
                    dsa.status
                FROM demand_supply_assignments dsa
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                JOIN master_clients mc ON dsa.master_client_id = mc.master_client_id
                WHERE dsa.id = %s
            """, (assignment_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'assignment_id': result[0],
                    'client_name': result[1],
                    'talent_name': result[2],
                    'assigned_percentage': result[3],
                    'assignment_duration': result[4],
                    'assignment_start_date': result[5],
                    'assignment_end_date': result[6],
                    'skills': result[7],
                    'status': result[8]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting assignment by ID {assignment_id}: {str(e)}")
            return None
    
    def get_all_assignments(self):
        """Get all assignments with client and talent details"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Use environment-aware table names
            demand_supply_assignments_table = self.get_table_name('demand_supply_assignments')
            master_clients_table = self.get_table_name('master_clients')
            talent_supply_table = self.get_table_name('talent_supply')
            
            cursor.execute(f"""
                SELECT 
                    dsa.id,
                    mc.client_name,
                    COALESCE(ts.name, 'Unknown') as talent_name,
                    ts.role,
                    dsa.assignment_percentage,
                    dsa.duration_months,
                    dsa.start_date,
                    dsa.end_date,
                    dsa.status,
                    dsa.notes,
                    ts.skills
                FROM {demand_supply_assignments_table} dsa
                LEFT JOIN {master_clients_table} mc ON dsa.master_client_id = mc.master_client_id
                LEFT JOIN {talent_supply_table} ts ON dsa.talent_id = ts.id
                ORDER BY mc.client_name, ts.name
            """)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting assignments: {str(e)}")
            return pd.DataFrame()
    
    def get_all_mappings(self):
        """Get all mappings with client and talent details - alias for get_all_assignments"""
        return self.get_all_assignments()
    
    def delete_assignment(self, client_name, talent_name):
        """Delete a specific assignment and recalculate availability"""
        try:
            logger.info(f"DEBUG: Starting delete_assignment for {talent_name} from {client_name}")
            
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get master_client_id and talent_id
            master_clients_table = self.get_table_name('master_clients')
            talent_supply_table = self.get_table_name('talent_supply')
            demand_supply_assignments_table = self.get_table_name('demand_supply_assignments')
            
            logger.info(f"DEBUG: Using tables - master_clients: {master_clients_table}, talent_supply: {talent_supply_table}, demand_supply_assignments: {demand_supply_assignments_table}")
            
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()
            logger.info(f"DEBUG: Client lookup result: {client_result}")
            
            cursor.execute(f"SELECT id FROM {talent_supply_table} WHERE name = %s", (talent_name,))
            talent_result = cursor.fetchone()
            logger.info(f"DEBUG: Talent lookup result: {talent_result}")
            
            if client_result and talent_result:
                master_client_id = client_result[0]
                talent_id = talent_result[0]
                
                logger.info(f"DEBUG: About to delete assignment with master_client_id={master_client_id}, talent_id={talent_id}")
                
                # First, check if record exists before deletion
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {demand_supply_assignments_table} 
                    WHERE master_client_id = %s AND talent_id = %s
                """, (master_client_id, talent_id))
                before_count = cursor.fetchone()[0]
                logger.info(f"DEBUG: Records found before deletion: {before_count}")
                
                # Delete the assignment
                cursor.execute(f"""
                    DELETE FROM {demand_supply_assignments_table} 
                    WHERE master_client_id = %s AND talent_id = %s
                """, (master_client_id, talent_id))
                
                deleted_count = cursor.rowcount
                logger.info(f"DEBUG: Rows deleted: {deleted_count}")
                
                # Verify deletion
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {demand_supply_assignments_table} 
                    WHERE master_client_id = %s AND talent_id = %s
                """, (master_client_id, talent_id))
                after_count = cursor.fetchone()[0]
                logger.info(f"DEBUG: Records found after deletion: {after_count}")
                
                # Check if this was the last assignment for the client
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {demand_supply_assignments_table} 
                    WHERE master_client_id = %s AND status = 'Active'
                """, (master_client_id,))
                
                remaining_assignments = cursor.fetchone()[0]
                
                # If no more assignments, update status back to "Ready for Staffing"
                if remaining_assignments == 0:
                    demand_metadata_table = self.get_table_name('demand_metadata')
                    cursor.execute(f"""
                        UPDATE {demand_metadata_table}
                        SET status = 'Ready for Staffing', updated_at = CURRENT_TIMESTAMP
                        WHERE client_name = %s AND status = 'Active'
                    """, (client_name,))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"Updated demand_metadata status to 'Ready for Staffing' for {client_name} after deleting last assignment")
                
                # Commit transaction
                logger.info(f"DEBUG: Committing transaction...")
                conn.commit()
                logger.info(f"DEBUG: Transaction committed successfully")
                conn.close()
                logger.info(f"DEBUG: Database connection closed")
                
                # After deleting assignment, recalculate availability for the talent
                from utils.supply_data_manager import SupplyDataManager
                supply_manager = SupplyDataManager()
                
                # Add small delay to ensure transaction is committed
                import time
                time.sleep(0.1)
                
                availability_result = supply_manager.recalculate_availability(talent_name)
                
                if availability_result['success']:
                    logger.info(f"Assignment deleted and availability updated for {talent_name}: {availability_result['remaining_availability']:.1f}% remaining, {availability_result['total_assigned']:.1f}% assigned, status: {availability_result['assignment_status']}")
                    return {
                        'success': True,
                        'availability_update': availability_result
                    }
                else:
                    logger.warning(f"Assignment deleted but availability update failed: {availability_result.get('error')}")
                    return {
                        'success': True,
                        'availability_error': availability_result.get('error')
                    }
            else:
                conn.close()
                return {'success': False, 'error': 'Client or talent not found'}
            
        except Exception as e:
            logger.error(f"Error deleting assignment: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_demand_metadata(self, client_name):
        """Get demand metadata with priority: user-edited dates > calculated dates from unified_sales_data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # First: Calculate start date from unified_sales_data (financial data)
            calculated_metadata = self._calculate_metadata_from_unified_sales_data(cursor, client_name)
            
            # Second: Check for user-edited overrides in demand_metadata table
            demand_metadata_table = self.get_table_name('demand_metadata')
            cursor.execute(f"""
                SELECT 
                    client_name,
                    track,
                    region,
                    duration_months,
                    people_expected,
                    start_date,
                    is_user_edited_date,
                    leads,
                    pending
                FROM {demand_metadata_table} 
                WHERE client_name = %s
                LIMIT 1
            """, (client_name,))
            
            override_result = cursor.fetchone()
            
            # Get confidence from master_clients table
            master_clients_table = self.get_table_name('master_clients')
            cursor.execute(f"SELECT confidence_level FROM {master_clients_table} WHERE client_name = %s", (client_name,))
            confidence_result = cursor.fetchone()
            confidence_value = confidence_result[0] if confidence_result and confidence_result[0] is not None else 50
            
            # Prioritize stored values over calculated values
            if override_result:
                # Use stored data as primary source
                metadata = {
                    'client_name': override_result[0],
                    'track': override_result[1] or '',
                    'region': override_result[2] or '',
                    'duration_months': override_result[3] or 1,
                    'people_expected': float(override_result[4]) if override_result[4] else 1.0,
                    'start_date': override_result[5],
                    'leads': float(override_result[7]) if len(override_result) > 7 and override_result[7] is not None else 0.0,
                    'pending': float(override_result[8]) if len(override_result) > 8 and override_result[8] is not None else 0.0,
                    'confidence_pct': confidence_value,
                    'is_user_edited': True,
                    'is_user_edited_date': bool(override_result[6]) if len(override_result) > 6 else False
                }
                
                # Calculate end_date from stored values
                if metadata['start_date'] and metadata['duration_months']:
                    start_date = metadata['start_date']
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    metadata['end_date'] = start_date + timedelta(days=int(metadata['duration_months']) * 30)
                
                # Add calculated start date for reference only
                if calculated_metadata:
                    metadata['calculated_start_date'] = calculated_metadata.get('start_date')
                    
            elif calculated_metadata:
                # Use calculated data as fallback only when no stored data exists
                metadata = calculated_metadata.copy()
                metadata['confidence_pct'] = confidence_value
                metadata['is_user_edited'] = False
                metadata['leads'] = 0.0
                metadata['pending'] = 0.0
                    
            elif override_result:
                # No calculated data available - use stored data as fallback
                is_user_edited_date = bool(override_result[6]) if len(override_result) > 6 else False
                
                metadata = {
                    'client_name': override_result[0],
                    'track': override_result[1] or '',
                    'region': override_result[2] or '',
                    'duration_months': override_result[3] or 1,
                    'people_expected': float(override_result[4]) if override_result[4] else 1.0,
                    'confidence_pct': confidence_value,
                    'start_date': override_result[5],
                    'leads': float(override_result[7]) if len(override_result) > 7 and override_result[7] is not None else 0.0,
                    'pending': float(override_result[8]) if len(override_result) > 8 and override_result[8] is not None else 0.0,
                    'calculated_start_date': None,
                    'is_user_edited': True,
                    'is_user_edited_date': is_user_edited_date
                }
            else:
                # No calculated data and no stored data
                metadata = None
                
            conn.close()
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting demand metadata: {str(e)}")
            return None
    
    def save_actual_dates(self, client_name, actual_start_date, actual_end_date):
        """Save actual start and end dates for a client to demand_metadata table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            demand_metadata_table = self.get_table_name('demand_metadata')
            
            # Check if record exists
            cursor.execute(f"SELECT client_name FROM {demand_metadata_table} WHERE client_name = %s", (client_name,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing record
                cursor.execute(f"""
                    UPDATE {demand_metadata_table}
                    SET actual_start_date = %s, actual_end_date = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE client_name = %s
                """, (actual_start_date, actual_end_date, client_name))
            else:
                # Insert new record with basic data
                cursor.execute(f"""
                    INSERT INTO {demand_metadata_table} 
                    (client_name, actual_start_date, actual_end_date, created_at, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (client_name, actual_start_date, actual_end_date))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Saved actual dates for {client_name}: {actual_start_date} to {actual_end_date}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving actual dates: {str(e)}")
            return False
    
    def get_duration_discrepancies(self):
        """Get clients where planned duration differs from actual duration"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            demand_metadata_table = self.get_table_name('demand_metadata')
            
            cursor.execute(f"""
                SELECT 
                    dm.client_name,
                    dm.duration_months as planned_duration,
                    dm.actual_start_date,
                    dm.actual_end_date,
                    CASE 
                        WHEN dm.actual_start_date IS NOT NULL AND dm.actual_end_date IS NOT NULL THEN
                            ROUND((dm.actual_end_date - dm.actual_start_date) / 30.44, 1)
                        ELSE NULL
                    END as actual_duration,
                    dm.people_expected,
                    dm.confidence_pct
                FROM {demand_metadata_table} dm
                WHERE dm.actual_start_date IS NOT NULL 
                AND dm.actual_end_date IS NOT NULL
                AND dm.duration_months IS NOT NULL
                AND ABS(dm.duration_months - ROUND((dm.actual_end_date - dm.actual_start_date) / 30.44, 1)) >= 0.5
                ORDER BY dm.client_name
            """)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting duration discrepancies: {str(e)}")
            return pd.DataFrame()
    
    def update_demand_for_actual_duration(self, client_name, forecasted_value, booked_value, billed_value):
        """Update demand values based on actual duration"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Update demand metadata
            demand_metadata_table = self.get_table_name('demand_metadata')
            cursor.execute(f"""
                UPDATE {demand_metadata_table}
                SET people_expected = %s, updated_at = CURRENT_TIMESTAMP
                WHERE client_name = %s
            """, (booked_value, client_name))
            
            # Update unified sales data for the actual duration period
            unified_sales_data_table = self.get_table_name('unified_sales_data')
            
            # Get the actual duration period
            cursor.execute(f"""
                SELECT actual_start_date, actual_end_date 
                FROM {demand_metadata_table}
                WHERE client_name = %s
            """, (client_name,))
            
            result = cursor.fetchone()
            if result:
                actual_start, actual_end = result
                
                # Calculate months in actual period
                from datetime import datetime
                import calendar
                
                current_date = actual_start
                months_to_update = []
                
                while current_date <= actual_end:
                    month_name = calendar.month_name[current_date.month]
                    year = current_date.year
                    months_to_update.append((month_name, year))
                    
                    # Move to next month
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1)
                
                # Update unified sales data for each month in actual period
                for month_name, year in months_to_update:
                    # Update Forecasted
                    cursor.execute(f"""
                        UPDATE {unified_sales_data_table}
                        SET value = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE account_name = %s AND month = %s AND year = %s AND metric_type = 'Forecasted'
                    """, (forecasted_value, client_name, month_name, year))
                    
                    # Update Booked
                    cursor.execute(f"""
                        UPDATE {unified_sales_data_table}
                        SET value = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE account_name = %s AND month = %s AND year = %s AND metric_type = 'Booked'
                    """, (booked_value, client_name, month_name, year))
                    
                    # Update Billed
                    cursor.execute(f"""
                        UPDATE {unified_sales_data_table}
                        SET value = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE account_name = %s AND month = %s AND year = %s AND metric_type = 'Billed'
                    """, (billed_value, client_name, month_name, year))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated demand values for {client_name}: Forecasted={forecasted_value}, Booked={booked_value}, Billed={billed_value}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating demand for actual duration: {str(e)}")
            return False
    
    def _calculate_metadata_from_unified_sales_data(self, cursor, client_name):
        """Calculate metadata from unified_sales_data financial records"""
        try:
            from datetime import datetime, timedelta
            
            # Get basic client information  
            unified_sales_data_table = self.env_manager.get_table_name('unified_sales_data')
            cursor.execute(f"""
                SELECT DISTINCT 
                    account_name,
                    account_track,
                    region,
                    owner
                FROM {unified_sales_data_table}
                WHERE account_name = %s
                LIMIT 1
            """, (client_name,))
            
            result = cursor.fetchone()
            if not result:
                return None
            
            # Calculate project timeline from monthly data with non-zero values
            # Use financial year logic: current year months first, then next year months
            from utils.financial_year_manager import FinancialYearManager
            fy_manager = FinancialYearManager()
            current_year = datetime.now().year
            
            # Get current financial year months in order (April-March)
            fy_months = fy_manager.get_financial_year_months_ordered()
            
            cursor.execute(f"""
                SELECT month, year, MIN(CASE 
                    WHEN month = 'April' THEN 4
                    WHEN month = 'May' THEN 5
                    WHEN month = 'June' THEN 6
                    WHEN month = 'July' THEN 7
                    WHEN month = 'August' THEN 8
                    WHEN month = 'September' THEN 9
                    WHEN month = 'October' THEN 10
                    WHEN month = 'November' THEN 11
                    WHEN month = 'December' THEN 12
                    WHEN month = 'January' THEN 1
                    WHEN month = 'February' THEN 2
                    WHEN month = 'March' THEN 3
                    ELSE 1
                END) as month_num
                FROM {unified_sales_data_table}
                WHERE account_name = %s AND value > 0
                AND metric_type IN ('Billed', 'Booked', 'Planned', 'Forecasted')
                GROUP BY month, year
                ORDER BY year, month_num
                LIMIT 1
            """, (client_name,))
            
            start_result = cursor.fetchone()
            
            cursor.execute(f"""
                SELECT month, year, CASE 
                    WHEN month = 'April' THEN 4
                    WHEN month = 'May' THEN 5
                    WHEN month = 'June' THEN 6
                    WHEN month = 'July' THEN 7
                    WHEN month = 'August' THEN 8
                    WHEN month = 'September' THEN 9
                    WHEN month = 'October' THEN 10
                    WHEN month = 'November' THEN 11
                    WHEN month = 'December' THEN 12
                    WHEN month = 'January' THEN 1
                    WHEN month = 'February' THEN 2
                    WHEN month = 'March' THEN 3
                    ELSE 1
                END as month_num
                FROM {unified_sales_data_table}
                WHERE account_name = %s AND value > 0
                AND metric_type IN ('Billed', 'Booked', 'Planned', 'Forecasted')
                GROUP BY month, year
                ORDER BY year DESC, month_num DESC
                LIMIT 1
            """, (client_name,))
            
            end_result = cursor.fetchone()
            
            cursor.execute(f"""
                SELECT COUNT(DISTINCT CONCAT(year, '-', month)) as total_months
                FROM {unified_sales_data_table}
                WHERE account_name = %s AND value > 0
                AND metric_type IN ('Billed', 'Booked', 'Planned', 'Forecasted')
            """, (client_name,))
            
            count_result = cursor.fetchone()
            
            if start_result and end_result and count_result:
                start_month_name, start_year, start_month_num = start_result
                end_month_name, end_year, end_month_num = end_result
                total_months = count_result[0]
                
                # Convert month names to numbers
                month_name_to_num = {
                    'January': 1, 'February': 2, 'March': 3, 'April': 4,
                    'May': 5, 'June': 6, 'July': 7, 'August': 8,
                    'September': 9, 'October': 10, 'November': 11, 'December': 12
                }
                
                start_month = month_name_to_num[start_month_name]
                end_month = month_name_to_num[end_month_name]
                
                start_date = datetime(int(start_year), int(start_month), 1).date()
                
                # Calculate end date as last day of end month
                if int(end_month) == 12:
                    end_date = datetime(int(end_year) + 1, 1, 1).date() - timedelta(days=1)
                else:
                    end_date = datetime(int(end_year), int(end_month) + 1, 1).date() - timedelta(days=1)
                
                duration_months = int(total_months)
            else:
                # No data with values > 0, return None instead of defaults
                return None
            
            metadata = {
                'client_name': result[0],
                'track': result[1],
                'region': result[2],
                'owner': result[3],
                'duration_months': duration_months,
                'people_expected': 1.0,  # Default value
                'start_date': start_date,
                'end_date': end_date,
                'calculated_from_financial_data': True
            }
            
            logger.info(f"Calculated metadata for {client_name}: Start={start_date}, Duration={duration_months} months")
            return metadata
            
        except Exception as e:
            logger.error(f"Error calculating metadata from unified_sales_data: {str(e)}")
            return None
    
    def save_demand_only_mapping(self, demand_data):
        """Save a demand mapping with no talent assigned - status: Ready for Staffing"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get or create master_client_id
            master_clients_table = self.env_manager.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", 
                          (demand_data.get('client_name'),))
            client_result = cursor.fetchone()
            master_client_id = client_result[0] if client_result else None
            
            if not master_client_id:
                # Create new master client if not exists
                cursor.execute(f"INSERT INTO {master_clients_table} (client_name) VALUES (%s) RETURNING master_client_id", 
                              (demand_data.get('client_name'),))
                master_client_id = cursor.fetchone()[0]
            
            # Get correct duration from unified_sales_data if not provided
            duration_months = demand_data.get('duration_months', 1)
            if duration_months == 1 or not duration_months:  # If default value, try to get from unified_sales_data
                unified_sales_data_table = self.env_manager.get_table_name('unified_sales_data')
                cursor.execute(f"SELECT DISTINCT duration FROM {unified_sales_data_table} WHERE account_name = %s AND duration IS NOT NULL LIMIT 1", 
                              (demand_data.get('client_name'),))
                duration_result = cursor.fetchone()
                if duration_result and duration_result[0]:
                    duration_months = duration_result[0]
            
            # Save demand metadata with "Ready for Staffing" status
            demand_metadata_table = self.env_manager.get_table_name('demand_metadata')
            cursor.execute(f"""
                INSERT INTO {demand_metadata_table}
                (client_name, people_expected, duration_months, start_date, end_date, 
                 confidence_pct, region, track, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (client_name) 
                DO UPDATE SET 
                    people_expected = EXCLUDED.people_expected,
                    duration_months = EXCLUDED.duration_months,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    confidence_pct = EXCLUDED.confidence_pct,
                    region = EXCLUDED.region,
                    track = EXCLUDED.track,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                demand_data.get('client_name'),
                demand_data.get('positions_expected', 1.0),
                duration_months,  # Use calculated duration
                demand_data.get('start_date'),
                demand_data.get('end_date'),
                demand_data.get('confidence_pct', 80.0),  # Will be overridden by master_clients logic in save_demand_assignment_mapping
                demand_data.get('region', ''),
                demand_data.get('track', ''),
                'Ready for Staffing'  # Status for demand with no talent assigned
            ))
            
            demand_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            
            logger.info(f"Demand-only mapping saved for {demand_data.get('client_name')} with status 'Ready for Staffing'")
            return {
                'success': True, 
                'demand_id': demand_id,
                'status': 'Ready for Staffing'
            }
            
        except Exception as e:
            logger.error(f"Error saving demand-only mapping: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_ready_for_staffing_demands(self):
        """Get all demands with 'Ready for Staffing' status - using environment-aware table names"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get environment-aware table names
            demand_metadata_table = self.get_table_name('demand_metadata')
            master_clients_table = self.get_table_name('master_clients')
            unified_sales_data_table = self.get_table_name('unified_sales_data')
            
            cursor.execute(f"""
                SELECT 
                    dm.id,
                    dm.client_name,
                    COALESCE(dm.leads, 0) as leads,
                    dm.people_expected,
                    CASE 
                        WHEN dm.duration_months <= 1 THEN COALESCE(usd.duration, dm.duration_months, 1)
                        ELSE dm.duration_months 
                    END as duration_months,
                    dm.start_date,
                    dm.end_date,
                    dm.status,
                    COALESCE(mc.confidence_level, 50) as confidence_pct,
                    COALESCE(usd.region, dm.region, 'N/A') as region,
                    dm.track,
                    dm.created_at,
                    dm.updated_at
                FROM {demand_metadata_table} dm
                LEFT JOIN {master_clients_table} mc ON dm.client_name = mc.client_name
                LEFT JOIN (
                    SELECT DISTINCT account_name, duration, region 
                    FROM {unified_sales_data_table} 
                    WHERE duration IS NOT NULL AND duration > 0
                ) usd ON dm.client_name = usd.account_name
                WHERE dm.status = 'Ready for Staffing'
                ORDER BY dm.created_at DESC
            """)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Error getting ready for staffing demands: {str(e)}")
            return pd.DataFrame()
    
    def save_demand_metadata(self, client_name, metadata):
        """Save demand metadata to demand_metadata table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Extract and validate metadata fields with proper type conversion
            people_expected = float(metadata.get('people_expected', 1.0))
            leads = float(metadata.get('leads', 0.0))
            pending = float(metadata.get('pending', 0.0))
            duration_months = int(metadata.get('duration_months', 12))
            
            # Handle date conversion properly with multiple format support
            start_date_raw = metadata.get('start_date', datetime.now().date())
            if isinstance(start_date_raw, str):
                # Try multiple date formats
                date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']
                start_date = None
                for fmt in date_formats:
                    try:
                        start_date = datetime.strptime(start_date_raw, fmt).date()
                        break
                    except ValueError:
                        continue
                if start_date is None:
                    raise ValueError(f"Unable to parse start_date: {start_date_raw}")
            elif isinstance(start_date_raw, datetime):
                start_date = start_date_raw.date()
            else:
                start_date = start_date_raw
            
            end_date_raw = metadata.get('end_date')
            if end_date_raw:
                if isinstance(end_date_raw, str):
                    # Try multiple date formats
                    date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']
                    end_date = None
                    for fmt in date_formats:
                        try:
                            end_date = datetime.strptime(end_date_raw, fmt).date()
                            break
                        except ValueError:
                            continue
                    if end_date is None:
                        raise ValueError(f"Unable to parse end_date: {end_date_raw}")
                elif isinstance(end_date_raw, datetime):
                    end_date = end_date_raw.date()
                else:
                    end_date = end_date_raw
            else:
                end_date = start_date + timedelta(days=duration_months * 30)
            
            # Use confidence from metadata (which comes from the form and should update master_clients)
            confidence_pct = float(metadata.get('confidence_pct', 50.0))
            
            # Update confidence in master_clients table first using ConfidenceConsolidator
            from utils.confidence_consolidator import ConfidenceConsolidator
            confidence_consolidator = ConfidenceConsolidator()
            confidence_consolidator.update_client_confidence(client_name, confidence_pct)
            
            region = str(metadata.get('region', '')).strip()
            track = str(metadata.get('track', '')).strip()
            is_user_edited_date = bool(metadata.get('is_user_edited_date', False))
            
            # Use environment-aware table name
            demand_metadata_table = self.get_table_name('demand_metadata')
            
            # Ensure required columns exist
            cursor.execute(f"""
                ALTER TABLE {demand_metadata_table} 
                ADD COLUMN IF NOT EXISTS is_user_edited_date BOOLEAN DEFAULT FALSE
            """)
            
            cursor.execute(f"""
                ALTER TABLE {demand_metadata_table} 
                ADD COLUMN IF NOT EXISTS leads DECIMAL(8,2) DEFAULT 0.0
            """)
            
            cursor.execute(f"""
                ALTER TABLE {demand_metadata_table} 
                ADD COLUMN IF NOT EXISTS pending DECIMAL(8,2) DEFAULT 0.0
            """)
            
            # Also add the column if we're in production mode
            try:
                cursor.execute(f"""
                    ALTER TABLE demand_metadata 
                    ADD COLUMN IF NOT EXISTS pending DECIMAL(8,2) DEFAULT 0.0
                """)
            except:
                pass  # Column already exists
            
            # Check if client has active assignments to determine correct status
            demand_supply_assignments_table = self.get_table_name('demand_supply_assignments')
            master_clients_table = self.get_table_name('master_clients')
            
            cursor.execute(f"""
                SELECT COUNT(dsa.id)
                FROM {demand_supply_assignments_table} dsa
                JOIN {master_clients_table} mc ON dsa.master_client_id = mc.master_client_id
                WHERE mc.client_name = %s AND dsa.status = 'Active'
            """, (client_name,))
            
            assignment_count = cursor.fetchone()[0]
            current_status = 'Active' if assignment_count > 0 else 'Ready for Staffing'
            
            # Insert or update demand metadata
            cursor.execute(f"""
                INSERT INTO {demand_metadata_table} 
                (client_name, people_expected, leads, pending, duration_months, start_date, end_date, 
                 confidence_pct, region, track, status, is_user_edited_date, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (client_name) 
                DO UPDATE SET 
                    people_expected = EXCLUDED.people_expected,
                    leads = EXCLUDED.leads,
                    pending = EXCLUDED.pending,
                    duration_months = EXCLUDED.duration_months,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    confidence_pct = EXCLUDED.confidence_pct,
                    region = EXCLUDED.region,
                    track = EXCLUDED.track,
                    status = EXCLUDED.status,
                    is_user_edited_date = EXCLUDED.is_user_edited_date,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                client_name,
                people_expected,
                leads,
                pending,
                duration_months,
                start_date,
                end_date,
                confidence_pct,
                region,
                track,
                current_status,  # Status based on actual assignments
                is_user_edited_date
            ))
            
            demand_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            
            logger.info(f"Demand metadata saved for {client_name}: people={people_expected}, duration={duration_months}, confidence={confidence_pct}")
            return {'success': True, 'demand_id': demand_id}
            
        except Exception as e:
            # Improved error logging with more detail
            error_msg = f"Error saving demand metadata for {client_name}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Metadata attempted to save: {metadata}")
            try:
                if 'conn' in locals():
                    conn.rollback()
                    conn.close()
            except:
                pass
            return {'success': False, 'error': error_msg}
    
    def delete_demand_mapping(self, client_name):
        """
        Delete entire demand mapping (all talent assigned to a client) and recalculate only affected talent availability
        This is a bulk operation that's more efficient than individual deletions
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get master_client_id
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()
            
            if not client_result:
                conn.close()
                return {'success': False, 'error': f'Client {client_name} not found'}
            
            master_client_id = client_result[0]
            
            # Get all talent assigned to this client before deletion
            cursor.execute("""
                SELECT DISTINCT ts.name
                FROM demand_supply_assignments dsa
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                WHERE dsa.master_client_id = %s AND dsa.status = 'Active'
            """, (master_client_id,))
            
            affected_talent = [row[0] for row in cursor.fetchall()]
            
            if not affected_talent:
                conn.close()
                return {'success': True, 'message': 'No active assignments found for this client', 'affected_talent': []}
            
            # Delete all assignments for this client in one operation
            demand_supply_assignments_table = self.get_table_name('demand_supply_assignments')
            cursor.execute(f"""
                DELETE FROM {demand_supply_assignments_table} 
                WHERE master_client_id = %s
            """, (master_client_id,))
            
            deleted_count = cursor.rowcount
            
            # CRITICAL: Update demand_metadata status back to "Ready for Staffing" 
            # when all assignments are deleted
            demand_metadata_table = self.get_table_name('demand_metadata')
            cursor.execute(f"""
                UPDATE {demand_metadata_table}
                SET status = 'Ready for Staffing', updated_at = CURRENT_TIMESTAMP
                WHERE client_name = %s AND status = 'Active'
            """, (client_name,))
            
            if cursor.rowcount > 0:
                logger.info(f"Updated demand_metadata status to 'Ready for Staffing' for {client_name} after deleting all assignments")
            
            conn.commit()
            conn.close()
            
            # Add small delay to ensure transaction is committed
            import time
            time.sleep(0.1)
            
            # Recalculate availability ONLY for affected talent (not entire supply table)
            from utils.supply_data_manager import SupplyDataManager
            supply_manager = SupplyDataManager()
            
            recalculation_results = []
            for talent_name in affected_talent:
                result = supply_manager.recalculate_availability(talent_name)
                recalculation_results.append(result)
            
            logger.info(f"Deleted demand mapping for {client_name}: {deleted_count} assignments deleted, {len(affected_talent)} talent availability recalculated")
            
            return {
                'success': True,
                'client_name': client_name,
                'deleted_count': deleted_count,
                'affected_talent': affected_talent,
                'recalculation_results': recalculation_results
            }
            
        except Exception as e:
            logger.error(f"Error deleting demand mapping for {client_name}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def unassign_all_talent(self, client_name):
        """
        Legacy method - redirects to delete_demand_mapping for consistency
        """
        return self.delete_demand_mapping(client_name)
    
    def delete_ready_for_staffing_demand(self, client_name):
        """
        Delete Ready for Staffing demand from demand_metadata table
        This removes the demand mapping but doesn't affect talent assignments since no talent is assigned
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Delete from demand_metadata table
            cursor.execute("""
                DELETE FROM demand_metadata 
                WHERE client_name = %s AND status = 'Ready for Staffing'
            """, (client_name,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"Deleted Ready for Staffing demand for {client_name}")
                
                # Reduce confidence to 50% using ConfidenceConsolidator
                from utils.confidence_consolidator import ConfidenceConsolidator
                confidence_consolidator = ConfidenceConsolidator()
                confidence_consolidator.update_client_confidence(client_name, 50)
                
                return {
                    'success': True,
                    'client_name': client_name,
                    'deleted_count': deleted_count,
                    'message': f'Ready for Staffing demand deleted for {client_name} and confidence reduced to 50%'
                }
            else:
                return {
                    'success': False, 
                    'error': f'No Ready for Staffing demand found for {client_name}'
                }
            
        except Exception as e:
            logger.error(f"Error deleting Ready for Staffing demand for {client_name}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def save_actual_dates(self, client_name, actual_start_date, actual_end_date):
        """Save actual start and end dates to metadata table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            demand_metadata_table = self.get_table_name('demand_metadata')
            
            # Update or insert actual dates
            cursor.execute(f"""
                INSERT INTO {demand_metadata_table}
                (client_name, actual_start_date, actual_end_date, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (client_name) 
                DO UPDATE SET 
                    actual_start_date = EXCLUDED.actual_start_date,
                    actual_end_date = EXCLUDED.actual_end_date,
                    updated_at = CURRENT_TIMESTAMP
            """, (client_name, actual_start_date, actual_end_date))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Actual dates saved for {client_name}: {actual_start_date} to {actual_end_date}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving actual dates for {client_name}: {str(e)}")
            return False
    
    def get_duration_discrepancies(self):
        """Get all clients with discrepancies between planned and actual durations"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            demand_metadata_table = self.get_table_name('demand_metadata')
            
            # Get clients with both planned and actual dates
            cursor.execute(f"""
                SELECT 
                    client_name,
                    duration_months as planned_duration,
                    CASE 
                        WHEN actual_start_date IS NOT NULL AND actual_end_date IS NOT NULL 
                        THEN ROUND(
                            EXTRACT(EPOCH FROM (actual_end_date::timestamp - actual_start_date::timestamp)) / (30 * 24 * 3600)
                        )
                        ELSE NULL
                    END as actual_duration
                FROM {demand_metadata_table}
                WHERE actual_start_date IS NOT NULL 
                    AND actual_end_date IS NOT NULL 
                    AND duration_months IS NOT NULL
            """)
            
            results = cursor.fetchall()
            conn.close()
            
            discrepancies = []
            for row in results:
                client_name, planned_duration, actual_duration = row
                
                if actual_duration is not None and planned_duration != actual_duration:
                    discrepancies.append({
                        'client_name': client_name,
                        'planned_duration': planned_duration,
                        'actual_duration': int(actual_duration)
                    })
            
            return discrepancies
            
        except Exception as e:
            logger.error(f"Error getting duration discrepancies: {str(e)}")
            return []
