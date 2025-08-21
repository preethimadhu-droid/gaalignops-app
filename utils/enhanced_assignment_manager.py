"""
Enhanced Assignment Manager with Comprehensive Logic
Implements backup, cumulative assignment calculation, and automatic availability updates
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class EnhancedAssignmentManager:
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
    
    def backup_current_data_before_change(self, talent_name, reason="Before assignment change"):
        """Backup current assignment and availability data before making changes"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get current talent data
            cursor.execute("""
                SELECT assignment_percentage, availability_percentage, total_assignment_percentage
                FROM talent_supply 
                WHERE name = %s
            """, (talent_name,))
            
            current_data = cursor.fetchone()
            if current_data:
                # Log to availability history
                cursor.execute("""
                    INSERT INTO availability_history (
                        talent_name, previous_availability, previous_assigned, 
                        change_reason, changed_by
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    talent_name,
                    current_data[1],  # availability_percentage
                    current_data[0],  # assignment_percentage
                    reason,
                    'enhanced_assignment_manager'
                ))
                
                conn.commit()
                logger.info(f"Backed up data for {talent_name}: Available={current_data[1]}%, Assigned={current_data[0]}%")
                return True
            
            conn.close()
            return False
            
        except Exception as e:
            logger.error(f"Error backing up data for {talent_name}: {str(e)}")
            return False
    
    def calculate_total_assignments(self, talent_name):
        """Calculate total assignments - ONLY from demand_supply_assignments table (Option A)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Option A: Get ONLY new assignments from demand_supply_assignments table
            # Legacy assignments in talent_supply.assignment_percentage are kept separate
            cursor.execute("""
                SELECT COALESCE(SUM(dsa.assignment_percentage), 0)
                FROM demand_supply_assignments dsa
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                WHERE ts.name = %s AND dsa.status = 'Active'
            """, (talent_name,))
            
            new_result = cursor.fetchone()
            new_assignments = float(new_result[0]) if new_result else 0.0
            
            conn.close()
            
            # Return ONLY new assignments - legacy assignments are tracked separately
            logger.info(f"New assignment calculation for {talent_name}: {new_assignments}% (legacy assignments kept separate)")
            return new_assignments
            
        except Exception as e:
            logger.error(f"Error calculating new assignments for {talent_name}: {str(e)}")
            return 0.0
    
    def get_legacy_assignments(self, talent_name):
        """Get legacy assignments from talent_supply table only"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COALESCE(assignment_percentage, 0) 
                FROM talent_supply 
                WHERE name = %s
            """, (talent_name,))
            
            legacy_result = cursor.fetchone()
            legacy_assignment = float(legacy_result[0]) if legacy_result else 0.0
            
            conn.close()
            return legacy_assignment
            
        except Exception as e:
            logger.error(f"Error getting legacy assignments for {talent_name}: {str(e)}")
            return 0.0
    
    def get_base_availability(self, talent_name):
        """Get base availability (100% capacity) for talent"""
        # In this system, we assume 100% base capacity for all talent
        # This could be extended to support different base capacities per talent
        return 100.0
    
    def update_supply_table_assignments(self, talent_name, new_assignment_percentage=None):
        """Update assignments using ONLY assignment_percentage column - add to existing and update availability"""
        try:
            # Get current assignment and availability from supply table BEFORE making changes
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COALESCE(assignment_percentage, 0), 
                       COALESCE(availability_percentage, 100)
                FROM talent_supply 
                WHERE name = %s
            """, (talent_name,))
            
            result = cursor.fetchone()
            if not result:
                conn.close()
                return {'success': False, 'error': f'Talent {talent_name} not found'}
            
            existing_assignment, existing_availability = result
            existing_assignment = float(existing_assignment)
            existing_availability = float(existing_availability)
            
            # Backup current data before making changes
            self.backup_current_data_before_change(talent_name, f"Before assignment update - Current: {existing_assignment}% assigned, {existing_availability}% available")
            
            if new_assignment_percentage is not None:
                # Add new assignment to existing assignment
                new_total_assignment = existing_assignment + new_assignment_percentage
                
                # Calculate new availability: Existing Availability - New Assignment = New Availability
                new_availability = max(0, existing_availability - new_assignment_percentage)
                
                logger.info(f"Assignment update for {talent_name}: {existing_assignment}% + {new_assignment_percentage}% = {new_total_assignment}%, Availability: {existing_availability}% - {new_assignment_percentage}% = {new_availability}%")
            else:
                # No change in assignment
                new_total_assignment = existing_assignment
                new_availability = existing_availability
            
            # Update supply table - ONLY assignment_percentage and availability_percentage
            cursor.execute("""
                UPDATE talent_supply 
                SET 
                    assignment_percentage = %s,
                    availability_percentage = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE name = %s
            """, (new_total_assignment, new_availability, talent_name))
            
            # Log the change to availability history
            cursor.execute("""
                INSERT INTO availability_history (
                    talent_name, previous_availability, new_availability, new_assigned, 
                    change_reason, changed_by
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                talent_name,
                existing_availability,
                new_availability,
                new_total_assignment,
                f"Assignment update: +{new_assignment_percentage}%" if new_assignment_percentage else "Recalculation",
                'enhanced_assignment_manager'
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated {talent_name}: Assignment {existing_assignment}% → {new_total_assignment}%, Availability {existing_availability}% → {new_availability}%")
            return {
                'success': True,
                'previous_assignment': existing_assignment,
                'new_total_assigned': new_total_assignment,
                'previous_availability': existing_availability,
                'new_availability': new_availability
            }
            
        except Exception as e:
            logger.error(f"Error updating supply table for {talent_name}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def recalculate_talent_availability(self, talent_name):
        """Recalculate availability for a talent based on current assignments"""
        try:
            # Calculate current total assignments
            total_assignments = self.calculate_total_assignments(talent_name)
            base_availability = self.get_base_availability(talent_name)
            new_availability = max(0, base_availability - total_assignments)
            
            # Update supply table
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE talent_supply 
                SET 
                    total_assignment_percentage = %s,
                    availability_percentage = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE name = %s
            """, (total_assignments, new_availability, talent_name))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Recalculated {talent_name}: Total={total_assignments}%, Available={new_availability}%")
            return {
                'success': True,
                'total_assigned': total_assignments,
                'availability': new_availability
            }
            
        except Exception as e:
            logger.error(f"Error recalculating availability for {talent_name}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def save_assignment_with_logic(self, assignment_data):
        """Save assignment with comprehensive backup and calculation logic"""
        try:
            talent_name = assignment_data['talent_name']
            client_name = assignment_data['client_name']
            assignment_percentage = float(assignment_data['assignment_percentage'])
            
            # Step 1: Backup current data
            backup_success = self.backup_current_data_before_change(
                talent_name, 
                f"New assignment for {client_name}: {assignment_percentage}%"
            )
            
            if not backup_success:
                logger.warning(f"Backup failed for {talent_name}, proceeding with assignment save")
            
            # Step 2: Check if assignment already exists and update instead of insert
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get talent_id and master_client_id
            cursor.execute("SELECT id FROM talent_supply WHERE name = %s", (talent_name,))
            talent_result = cursor.fetchone()
            if not talent_result:
                return {'success': False, 'error': f'Talent {talent_name} not found'}
            talent_id = talent_result[0]
            
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()
            if not client_result:
                return {'success': False, 'error': f'Client {client_name} not found'}
            master_client_id = client_result[0]
            
            # Check if assignment already exists
            cursor.execute("""
                SELECT id, assignment_percentage FROM demand_supply_assignments 
                WHERE master_client_id = %s AND talent_id = %s AND status = 'Active'
            """, (master_client_id, talent_id))
            
            existing_assignment = cursor.fetchone()
            
            if existing_assignment:
                # Update existing assignment (replace, not add)
                assignment_id = existing_assignment[0]
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
                    assignment_percentage,
                    assignment_data.get('duration_months', 1),
                    assignment_data.get('start_date'),
                    assignment_data.get('end_date'),
                    assignment_data.get('notes', ''),
                    assignment_id
                ))
                operation = "Updated"
            else:
                # Insert new assignment
                cursor.execute("""
                    INSERT INTO demand_supply_assignments (
                        client_id, talent_id, master_client_id, assignment_percentage,
                        duration_months, start_date, end_date, status, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    master_client_id,  # client_id
                    talent_id,
                    master_client_id,
                    assignment_percentage,
                    assignment_data.get('duration_months', 1),
                    assignment_data.get('start_date'),
                    assignment_data.get('end_date'),
                    'Active',
                    assignment_data.get('notes', '')
                ))
                assignment_id = cursor.fetchone()[0]
                operation = "Created"
            
            conn.commit()
            conn.close()
            
            # Step 3: Update supply table with new assignment percentage
            update_result = self.update_supply_table_assignments(talent_name, assignment_percentage)
            
            if update_result['success']:
                logger.info(f"Assignment {operation.lower()}: {talent_name} -> {client_name} ({assignment_percentage}%)")
                return {
                    'success': True,
                    'assignment_id': assignment_id,
                    'operation': operation,
                    'new_total_assigned': update_result['new_total_assigned'],
                    'new_availability': update_result['new_availability']
                }
            else:
                return {'success': False, 'error': 'Failed to update supply table'}
            
        except Exception as e:
            logger.error(f"Error saving assignment with logic: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def delete_assignment_with_logic(self, assignment_id):
        """Delete assignment and recalculate availability"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get assignment details before deletion
            cursor.execute("""
                SELECT ts.name, dsa.assignment_percentage, mc.client_name
                FROM demand_supply_assignments dsa
                JOIN talent_supply ts ON dsa.talent_id = ts.id
                JOIN master_clients mc ON dsa.master_client_id = mc.master_client_id
                WHERE dsa.id = %s
            """, (assignment_id,))
            
            assignment_details = cursor.fetchone()
            if not assignment_details:
                return {'success': False, 'error': 'Assignment not found'}
            
            talent_name, assignment_percentage, client_name = assignment_details
            
            # Backup current data
            self.backup_current_data_before_change(
                talent_name, 
                f"Deleting assignment for {client_name}: -{assignment_percentage}%"
            )
            
            # Delete the assignment (trigger will log to history)
            cursor.execute("DELETE FROM demand_supply_assignments WHERE id = %s", (assignment_id,))
            
            conn.commit()
            conn.close()
            
            # Recalculate availability
            recalc_result = self.recalculate_talent_availability(talent_name)
            
            if recalc_result['success']:
                logger.info(f"Assignment deleted: {talent_name} from {client_name} (-{assignment_percentage}%)")
                return {
                    'success': True,
                    'talent_name': talent_name,
                    'new_total_assigned': recalc_result['total_assigned'],
                    'new_availability': recalc_result['availability']
                }
            else:
                return {'success': False, 'error': 'Failed to recalculate availability'}
            
        except Exception as e:
            logger.error(f"Error deleting assignment: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_talent_display_data(self, talent_name):
        """Get talent data for display - always from supply table (single source)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    name,
                    role,
                    COALESCE(assignment_percentage, 0) as total_assigned,
                    COALESCE(availability_percentage, 100) as availability,
                    skills,
                    region,
                    type,
                    employment_status
                FROM talent_supply 
                WHERE name = %s
            """, (talent_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'name': result[0],
                    'role': result[1],
                    'total_assigned': float(result[2]),
                    'availability': float(result[3]),
                    'skills': result[4] or '',
                    'region': result[5] or '',
                    'type': result[6] or '',
                    'employment_status': result[7] or ''
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting talent display data: {str(e)}")
            return None
    
    def bulk_recalculate_all_availability(self):
        """Recalculate availability for all talent"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get all talent names
            cursor.execute("SELECT name FROM talent_supply")
            talent_names = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            success_count = 0
            for talent_name in talent_names:
                result = self.recalculate_talent_availability(talent_name)
                if result['success']:
                    success_count += 1
            
            logger.info(f"Bulk recalculation completed: {success_count}/{len(talent_names)} successful")
            return {
                'success': True,
                'total_talent': len(talent_names),
                'successful_updates': success_count
            }
            
        except Exception as e:
            logger.error(f"Error in bulk recalculation: {str(e)}")
            return {'success': False, 'error': str(e)}