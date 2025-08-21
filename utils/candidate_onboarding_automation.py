"""
Candidate Onboarding Automation Module

Handles automatic transition of candidates from "On Boarded" status 
to Unified Talent Management system with proper data mapping and 
demand-supply assignment creation.
"""

import logging
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CandidateOnboardingAutomation:
    """
    Automates the process of moving On-Boarded candidates to talent management
    """
    
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
        
    def get_db_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(self.db_url)
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def process_onboarded_candidate(self, candidate_id: int, status_change_date: datetime = None) -> bool:
        """
        Main function to process a candidate marked as On-Boarded
        
        Args:
            candidate_id: ID of the candidate in candidate_data table
            status_change_date: When the status was changed (defaults to now)
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not status_change_date:
            status_change_date = datetime.now()
            
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Step 1: Get candidate details
            candidate_data = self._get_candidate_details(cursor, candidate_id)
            if not candidate_data:
                logger.error(f"Candidate {candidate_id} not found")
                return False
            
            # Step 2: Check if already exists in talent_supply
            existing_talent = self._check_existing_talent(cursor, candidate_data)
            if existing_talent:
                logger.warning(f"Candidate {candidate_data['name']} already exists in talent_supply as ID {existing_talent}")
                return False
            
            # Step 3: Create talent record
            talent_id = self._create_talent_record(cursor, candidate_data, status_change_date)
            if not talent_id:
                logger.error(f"Failed to create talent record for candidate {candidate_id}")
                return False
            
            # Step 4: Create demand-supply assignment
            assignment_success = self._create_demand_supply_assignment(cursor, candidate_data, talent_id, status_change_date)
            
            # Step 5: Update candidate record with talent_id reference
            self._update_candidate_with_talent_id(cursor, candidate_id, talent_id)
            
            # Commit all changes
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully onboarded candidate {candidate_data['name']} (ID: {candidate_id}) to talent management (Talent ID: {talent_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error processing onboarded candidate {candidate_id}: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False
    
    def _get_candidate_details(self, cursor, candidate_id: int) -> Optional[Dict]:
        """Get candidate details from candidate_data table"""
        try:
            query = """
            SELECT 
                cd.id,
                cd.candidate_name,
                cd.role,
                cd.experience_level,
                cd.skills,
                cd.email_id,
                cd.location,
                cd.source,
                cd.vendor_partner,
                cd.hire_for_client_id,
                cd.contact_number,
                cd.expected_ctc,
                mc.client_name
            FROM candidate_data cd
            LEFT JOIN master_clients mc ON cd.hire_for_client_id = mc.master_client_id
            WHERE cd.id = %s AND cd.status = 'On Boarded'
            """
            
            cursor.execute(query, (candidate_id,))
            result = cursor.fetchone()
            
            if result:
                return {
                    'id': result[0],
                    'name': result[1],
                    'role': result[2],
                    'experience_level': result[3],
                    'skills': result[4],
                    'email_id': result[5],
                    'location': result[6],
                    'source': result[7],
                    'vendor_partner': result[8],
                    'hire_for_client_id': result[9],
                    'contact_number': result[10],
                    'expected_ctc': result[11],
                    'client_name': result[12]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting candidate details: {e}")
            return None
    
    def _check_existing_talent(self, cursor, candidate_data: Dict) -> Optional[int]:
        """Check if candidate already exists in talent_supply table"""
        try:
            # Check by email first (most reliable)
            if candidate_data.get('email_id'):
                cursor.execute(
                    "SELECT talent_id FROM talent_supply WHERE email_id = %s",
                    (candidate_data['email_id'],)
                )
                result = cursor.fetchone()
                if result:
                    return result[0]
            
            # Check by name as backup
            cursor.execute(
                "SELECT talent_id FROM talent_supply WHERE name ILIKE %s",
                (f"%{candidate_data['name']}%",)
            )
            result = cursor.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"Error checking existing talent: {e}")
            return None
    
    def _determine_talent_type(self, source: str, vendor_partner: str) -> str:
        """Determine if talent should be FTE or NFTE based on source"""
        if not source:
            return "FTE"
            
        source_lower = source.lower()
        vendor_indicators = ['vendor', 'partner', 'coffeebeans', 'gemberg', 'triad']
        
        # Check if source or vendor_partner indicates external vendor
        if any(indicator in source_lower for indicator in vendor_indicators):
            return "NFTE"
        
        if vendor_partner and any(indicator in vendor_partner.lower() for indicator in vendor_indicators):
            return "NFTE"
            
        return "FTE"
    
    def _extract_years_of_experience(self, experience_level: str) -> float:
        """Extract numeric years from experience level string"""
        if not experience_level:
            return 0.0
            
        try:
            # Handle formats like "4yrs", "3.2 years", "5+ years"
            import re
            numbers = re.findall(r'(\d+\.?\d*)', experience_level)
            if numbers:
                return float(numbers[0])
        except:
            pass
        
        return 0.0
    
    def _create_talent_record(self, cursor, candidate_data: Dict, doj: datetime) -> Optional[int]:
        """Create new talent record in talent_supply table"""
        try:
            talent_type = self._determine_talent_type(candidate_data.get('source', ''), candidate_data.get('vendor_partner', ''))
            years_exp = self._extract_years_of_experience(candidate_data.get('experience_level', ''))
            
            # Determine partner name
            partner = candidate_data.get('vendor_partner') or candidate_data.get('source', '')
            if talent_type == "FTE":
                partner = "Greyamp"
            
            # Generate unique talent_id - handle VARCHAR type
            # Get the highest numeric ID from existing talent_ids
            cursor.execute("""
                SELECT COALESCE(
                    MAX(CAST(REGEXP_REPLACE(talent_id, '[^0-9]', '', 'g') AS INTEGER)), 
                    0
                ) + 1 
                FROM talent_supply 
                WHERE talent_id ~ '^[A-Z]*[0-9]+$'
            """)
            next_id = cursor.fetchone()[0]
            
            # Create talent_id with appropriate prefix
            if talent_type == "NFTE":
                new_talent_id = f"NFTE{next_id:03d}"
                db_type = "Non-FTE"  # Database uses "Non-FTE"
            else:
                new_talent_id = f"FTE{next_id:03d}"
                db_type = "FTE"      # Database uses "FTE"
            
            insert_query = """
            INSERT INTO talent_supply (
                talent_id, name, role, grade, doj, assignment_status, type,
                assignment_percentage, availability_percentage, employment_status,
                email_id, years_of_exp, skills, region, partner,
                created_at, updated_at, source_candidate_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING talent_id
            """
            
            values = (
                new_talent_id,
                candidate_data['name'],
                candidate_data['role'],
                'Mid',  # Default grade, can be enhanced later
                doj.strftime('%Y-%m-%d'),  # Convert to string format
                'Allocated',
                db_type,
                100,  # 100% assignment
                0,    # 0% availability (fully allocated)
                'Active',
                candidate_data.get('email_id'),
                str(years_exp),  # Convert to string
                candidate_data.get('skills'),
                candidate_data.get('location'),
                partner,
                datetime.now(),
                datetime.now(),
                candidate_data['id']  # Reference to original candidate
            )
            
            cursor.execute(insert_query, values)
            result = cursor.fetchone()
            
            if result:
                logger.info(f"Created talent record with ID: {result[0]}")
                return result[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating talent record: {e}")
            return None
    
    def _create_demand_supply_assignment(self, cursor, candidate_data: Dict, talent_id: int, assignment_date: datetime) -> bool:
        """Create demand-supply assignment record"""
        try:
            # Find matching demand records for the client
            # First get client name from candidate data
            cursor.execute("SELECT client_name FROM master_clients WHERE master_client_id = %s", (candidate_data['hire_for_client_id'],))
            client_result = cursor.fetchone()
            if not client_result:
                logger.warning(f"Could not find client with ID {candidate_data['hire_for_client_id']}")
                return False
            
            client_name = client_result[0]
            
            demand_query = """
            SELECT dm.id, dm.track as role, dm.client_name
            FROM demand_metadata dm
            WHERE dm.client_name = %s 
            AND dm.status IN ('Pipeline', 'Ready for Staffing')
            ORDER BY dm.created_at DESC
            LIMIT 1
            """
            
            cursor.execute(demand_query, (client_name,))
            demand_result = cursor.fetchone()
            
            if demand_result:
                demand_id, demand_role, client_name = demand_result
                
                # Get the actual talent internal ID from the talent record
                cursor.execute("SELECT id FROM talent_supply WHERE talent_id = %s", (talent_id,))
                talent_record = cursor.fetchone()
                if not talent_record:
                    logger.error(f"Could not find talent record for {talent_id}")
                    return False
                
                talent_internal_id = talent_record[0]
                
                # Create assignment record using correct table structure
                assignment_query = """
                INSERT INTO demand_supply_assignments (
                    client_id, talent_id, assignment_percentage, 
                    duration_months, start_date, end_date, status, 
                    created_at, updated_at, master_client_id, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                assignment_values = (
                    candidate_data['hire_for_client_id'],  # client_id
                    talent_internal_id,  # talent_id (internal ID)
                    100,  # 100% assignment
                    12,  # Default 12 months
                    assignment_date.date(),
                    (assignment_date + timedelta(days=365)).date(),
                    'Active',
                    datetime.now(),
                    datetime.now(),
                    candidate_data['hire_for_client_id'],  # master_client_id
                    f"Auto-assigned from candidate onboarding - {candidate_data['name']}"
                )
                
                cursor.execute(assignment_query, assignment_values)
                
                # Update demand status to "Current Staffing"
                update_demand_query = """
                UPDATE demand_metadata 
                SET status = 'Current Staffing', updated_at = %s
                WHERE id = %s AND status IN ('Pipeline', 'Ready for Staffing')
                """
                cursor.execute(update_demand_query, (datetime.now(), demand_id))
                
                logger.info(f"Created demand-supply assignment for talent {talent_id} to client {candidate_data['hire_for_client_id']}")
                logger.info(f"Updated demand {demand_id} status to 'Current Staffing'")
                return True
            else:
                logger.warning(f"No matching demand found for client {candidate_data['hire_for_client_id']} and role {candidate_data['role']}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating demand-supply assignment: {e}")
            return False
    
    def _update_candidate_with_talent_id(self, cursor, candidate_id: int, talent_id: int):
        """Update candidate record with reference to talent_id"""
        try:
            # Add a column to track the talent_id if it doesn't exist
            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='candidate_data' AND column_name='linked_talent_id') THEN
                        ALTER TABLE candidate_data ADD COLUMN linked_talent_id INTEGER;
                    END IF;
                END $$;
            """)
            
            # Update the candidate record
            cursor.execute(
                "UPDATE candidate_data SET linked_talent_id = %s WHERE id = %s",
                (talent_id, candidate_id)
            )
            
        except Exception as e:
            logger.error(f"Error updating candidate with talent_id: {e}")
    
    def process_all_pending_onboarded_candidates(self) -> List[Dict]:
        """Process all candidates marked as On-Boarded but not yet in talent management"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Find candidates marked as On-Boarded but not yet in talent management
            query = """
            SELECT cd.id, cd.candidate_name, cd.status_last_changed
            FROM candidate_data cd
            WHERE cd.status = 'On Boarded'
            AND (cd.linked_talent_id IS NULL OR cd.linked_talent_id NOT IN (SELECT talent_id FROM talent_supply))
            ORDER BY cd.status_last_changed DESC
            """
            
            cursor.execute(query)
            pending_candidates = cursor.fetchall()
            
            results = []
            for candidate_id, name, status_change_date in pending_candidates:
                success = self.process_onboarded_candidate(candidate_id, status_change_date or datetime.now())
                results.append({
                    'candidate_id': candidate_id,
                    'name': name,
                    'success': success
                })
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing pending candidates: {e}")
            return []

# Utility function for external use
def auto_onboard_candidate(candidate_id: int) -> bool:
    """
    Convenience function to onboard a single candidate
    
    Args:
        candidate_id: ID of the candidate to onboard
        
    Returns:
        bool: True if successful
    """
    automation = CandidateOnboardingAutomation()
    return automation.process_onboarded_candidate(candidate_id)

def process_all_pending_onboarded() -> List[Dict]:
    """
    Convenience function to process all pending onboarded candidates
    
    Returns:
        List of results with candidate info and success status
    """
    automation = CandidateOnboardingAutomation()
    return automation.process_all_pending_onboarded_candidates()