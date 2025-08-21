"""
Candidate-to-Demand Mapping System
Automatically maps hired candidates to demand records and updates financial tracking.
"""

import psycopg2
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class CandidateDemandMapper:
    """
    Handles automatic mapping of hired candidates to demand records
    and updates financial tracking (booked/billed values).
    """
    
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_url)
    
    def find_matching_demand_records(self, client_id: int, role: str = None) -> List[Dict]:
        """
        Find demand records for a client that need staffing.
        
        Args:
            client_id (int): Client ID to match
            role (str): Optional role to match more precisely
            
        Returns:
            List[Dict]: Matching demand records with available positions
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Find demand records where booked > billed (unfulfilled demand)
            query = """
                SELECT 
                    id, client_id, account_name, offering, owner,
                    SUM(CASE WHEN metric_type = 'Booked' THEN value ELSE 0 END) as total_booked,
                    SUM(CASE WHEN metric_type = 'Billed' THEN value ELSE 0 END) as total_billed,
                    (SUM(CASE WHEN metric_type = 'Booked' THEN value ELSE 0 END) - 
                     SUM(CASE WHEN metric_type = 'Billed' THEN value ELSE 0 END)) as available_positions
                FROM unified_sales_data 
                WHERE client_id = %s
                GROUP BY id, client_id, account_name, offering, owner
                HAVING (SUM(CASE WHEN metric_type = 'Booked' THEN value ELSE 0 END) - 
                        SUM(CASE WHEN metric_type = 'Billed' THEN value ELSE 0 END)) > 0
                ORDER BY available_positions DESC
            """
            
            cursor.execute(query, (client_id,))
            results = cursor.fetchall()
            
            demand_records = []
            for row in results:
                demand_records.append({
                    'demand_id': row[0],
                    'client_id': row[1],
                    'account_name': row[2],
                    'offering': row[3],
                    'owner': row[4],
                    'total_booked': row[5] or 0,
                    'total_billed': row[6] or 0,
                    'available_positions': row[7] or 0
                })
            
            return demand_records
            
        finally:
            conn.close()
    
    def create_demand_supply_assignment(self, candidate_id: int, client_id: int, 
                                      talent_id: int = None, duration_months: int = 12) -> bool:
        """
        Create a demand-supply assignment record for hired candidate.
        
        Args:
            candidate_id (int): Candidate ID
            client_id (int): Client ID
            talent_id (int): Optional talent ID for supply mapping
            duration_months (int): Assignment duration
            
        Returns:
            bool: Success status
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Check if assignment already exists
            cursor.execute("""
                SELECT id FROM demand_supply_assignments 
                WHERE client_id = %s AND talent_id = %s
            """, (client_id, talent_id or candidate_id))
            
            if cursor.fetchone():
                logger.info(f"Assignment already exists for candidate {candidate_id} and client {client_id}")
                return True
            
            # Create new assignment
            start_date = datetime.now().date()
            end_date = datetime(start_date.year + 1, start_date.month, start_date.day).date()
            
            cursor.execute("""
                INSERT INTO demand_supply_assignments 
                (client_id, talent_id, assignment_percentage, duration_months, 
                 start_date, end_date, status, notes, master_client_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                client_id,
                talent_id or candidate_id,
                100,  # 100% allocated
                duration_months,
                start_date,
                end_date,
                'Allocated',  # Status = Allocated
                f'Auto-assigned from candidate onboarding - Candidate ID: {candidate_id}',
                client_id  # master_client_id same as client_id
            ))
            
            conn.commit()
            logger.info(f"Created demand-supply assignment for candidate {candidate_id}")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating assignment: {e}")
            return False
        finally:
            conn.close()
    
    def update_demand_financials(self, client_id: int, reduce_booked: int = 1, 
                               increase_billed: int = 1) -> bool:
        """
        Update demand financials by reducing booked and increasing billed values.
        
        Args:
            client_id (int): Client ID
            reduce_booked (int): Amount to reduce from booked
            increase_billed (int): Amount to increase in billed
            
        Returns:
            bool: Success status
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Find the most recent booked record to reduce
            cursor.execute("""
                SELECT id, value FROM unified_sales_data 
                WHERE client_id = %s AND metric_type = 'Booked' AND value > 0
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (client_id,))
            
            booked_record = cursor.fetchone()
            if not booked_record:
                logger.warning(f"No booked records found for client {client_id}")
                return False
            
            booked_id, current_booked = booked_record
            new_booked_value = max(0, current_booked - reduce_booked)
            
            # Update booked value
            cursor.execute("""
                UPDATE unified_sales_data 
                SET value = %s, updated_at = %s
                WHERE id = %s
            """, (new_booked_value, datetime.now(), booked_id))
            
            # Find or create billed record
            cursor.execute("""
                SELECT id, value FROM unified_sales_data 
                WHERE client_id = %s AND metric_type = 'Billed'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (client_id,))
            
            billed_record = cursor.fetchone()
            
            if billed_record:
                # Update existing billed record
                billed_id, current_billed = billed_record
                new_billed_value = current_billed + increase_billed
                
                cursor.execute("""
                    UPDATE unified_sales_data 
                    SET value = %s, updated_at = %s
                    WHERE id = %s
                """, (new_billed_value, datetime.now(), billed_id))
            else:
                # Create new billed record (copy structure from booked record)
                cursor.execute("""
                    SELECT account_name, account_track, connect_name, partner_connect, 
                           owner, source, industry, region, lob, offering, 
                           financial_year, year, month, month_number, partner_org, 
                           status, duration, owner_id, forecast_plan_id
                    FROM unified_sales_data WHERE id = %s
                """, (booked_id,))
                
                booked_data = cursor.fetchone()
                if booked_data:
                    cursor.execute("""
                        INSERT INTO unified_sales_data 
                        (account_name, account_track, connect_name, partner_connect, 
                         owner, source, industry, region, lob, offering, 
                         financial_year, year, month, month_number, metric_type, 
                         value, client_id, partner_org, status, duration, 
                         owner_id, forecast_plan_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        booked_data[0], booked_data[1], booked_data[2], booked_data[3],
                        booked_data[4], booked_data[5], booked_data[6], booked_data[7],
                        booked_data[8], booked_data[9], booked_data[10], booked_data[11],
                        booked_data[12], booked_data[13], 'Billed', increase_billed,
                        client_id, booked_data[14], booked_data[15], booked_data[16],
                        booked_data[17], booked_data[18], datetime.now(), datetime.now()
                    ))
            
            conn.commit()
            logger.info(f"Updated financials for client {client_id}: Booked reduced by {reduce_booked}, Billed increased by {increase_billed}")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating demand financials: {e}")
            return False
        finally:
            conn.close()
    
    def process_hired_candidate(self, candidate_id: int, client_id: int, 
                              candidate_name: str = "", role: str = "") -> Dict:
        """
        Complete process: Map hired candidate to demand and update financials.
        
        Args:
            candidate_id (int): Candidate ID
            client_id (int): Client ID  
            candidate_name (str): Candidate name for logging
            role (str): Role for better matching
            
        Returns:
            Dict: Processing results
        """
        results = {
            'success': False,
            'assignment_created': False,
            'financials_updated': False,
            'message': '',
            'demand_records_found': 0
        }
        
        try:
            # 1. Find matching demand records
            demand_records = self.find_matching_demand_records(client_id, role)
            results['demand_records_found'] = len(demand_records)
            
            if not demand_records:
                results['message'] = f"No unfulfilled demand found for client {client_id}"
                return results
            
            # 2. Create demand-supply assignment
            assignment_success = self.create_demand_supply_assignment(
                candidate_id, client_id, candidate_id, 12
            )
            results['assignment_created'] = assignment_success
            
            if not assignment_success:
                results['message'] = "Failed to create demand-supply assignment"
                return results
            
            # 3. Update financial tracking
            financial_success = self.update_demand_financials(client_id, 1, 1)
            results['financials_updated'] = financial_success
            
            if financial_success:
                results['success'] = True
                results['message'] = f"Successfully mapped {candidate_name} to demand and updated financials"
            else:
                results['message'] = "Assignment created but financial update failed"
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing hired candidate: {e}")
            results['message'] = f"Error: {str(e)}"
            return results
    
    def batch_process_hired_candidates(self, client_id: int = None) -> Dict:
        """
        Process all hired candidates for a client (or all clients if None).
        One-time setup function.
        
        Args:
            client_id (int): Optional specific client ID
            
        Returns:
            Dict: Batch processing results
        """
        conn = self.get_connection()
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        try:
            cursor = conn.cursor()
            
            # Get all hired candidates (On Boarded status)
            where_clause = "WHERE c.status = 'On Boarded'"
            params = []
            
            if client_id:
                where_clause += " AND c.hire_for_client_id = %s"
                params.append(client_id)
            
            query = f"""
                SELECT c.id, c.candidate_name, c.role, c.hire_for_client_id, mc.client_name
                FROM dev_candidate_data c
                JOIN master_clients mc ON c.hire_for_client_id = mc.master_client_id
                {where_clause}
                ORDER BY c.created_date DESC
            """
            
            cursor.execute(query, params)
            hired_candidates = cursor.fetchall()
            
            for candidate in hired_candidates:
                cand_id, cand_name, cand_role, cand_client_id, client_name = candidate
                
                # Process each candidate
                result = self.process_hired_candidate(
                    cand_id, cand_client_id, cand_name, cand_role or ""
                )
                
                results['processed'] += 1
                if result['success']:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                
                results['details'].append({
                    'candidate_name': cand_name,
                    'client_name': client_name,
                    'role': cand_role,
                    'success': result['success'],
                    'message': result['message']
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            results['details'].append({
                'error': str(e)
            })
            return results
        finally:
            conn.close()

def auto_map_on_status_change(candidate_id: int, new_status: str, client_id: int, 
                             candidate_name: str = "", role: str = "") -> bool:
    """
    Trigger automatic mapping when candidate status changes to 'On Boarded'.
    This function should be called from the candidate status update workflow.
    
    Args:
        candidate_id (int): Candidate ID
        new_status (str): New status value
        client_id (int): Client ID
        candidate_name (str): Candidate name
        role (str): Candidate role
        
    Returns:
        bool: Success status
    """
    if new_status != 'On Boarded':
        return True  # No action needed for other statuses
    
    import os
    mapper = CandidateDemandMapper(os.environ.get('DATABASE_URL'))
    result = mapper.process_hired_candidate(candidate_id, client_id, candidate_name, role)
    
    if result['success']:
        logger.info(f"Auto-mapped candidate {candidate_name} to demand on status change")
        return True
    else:
        logger.error(f"Failed to auto-map candidate {candidate_name}: {result['message']}")
        return False