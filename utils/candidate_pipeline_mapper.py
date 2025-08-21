"""
Candidate to Pipeline Stage Mapping System
Maps candidate statuses to pipeline stages for real-time Actual # calculations
"""
import psycopg2
import os
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class CandidatePipelineMapper:
    """Maps candidate statuses to pipeline stages for Supply Management calculations"""
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        
        # Define status-to-stage mapping based on actual database statuses
        self.status_stage_mapping = {
            'Initial Screening': ['Initial Screening', 'Sent to client'],
            'Technical Assessment': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment'],  
            'Interview Process': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment', 'Interview Scheduled', 'Interview Round'],
            'Final Selection': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment', 'Interview Scheduled', 'Interview Round', 'Selected', 'Final Round'],
            'Offer & Onboarding': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment', 'Interview Scheduled', 'Interview Round', 'Selected', 'Final Round', 'Offer Extended', 'Offer Accepted', 'Staffed'],
            'Offer Extended': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment', 'Interview Scheduled', 'Interview Round', 'Selected', 'Final Round', 'Offer Extended'],
            'Offer Accepted': ['Initial Screening', 'Sent to client', 'Code Pairing', 'Technical Round', 'Tech Assessment', 'Interview Scheduled', 'Interview Round', 'Selected', 'Final Round', 'Offer Extended', 'Offer Accepted'],
            'Staffed': ['Staffed']  # Only count actually staffed candidates
        }
        
        # Statuses to exclude from counting (negative outcomes) - but these will be counted separately as "Rejected"
        self.excluded_statuses = ['Screen Rejected', 'Rejected', 'Candidate RNR/Dropped', 'Requirement on hold', 'On Hold', 'Internal Dropped', 'Duplicate Profile']
        
        # Special stage statuses that represent rejections/negative outcomes
        self.rejection_statuses = ['Screen Rejected', 'Rejected', 'Candidate RNR/Dropped', 'On Hold', 'Internal Dropped', 'Duplicate Profile']
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_url)
    
    def get_candidate_count_for_stage(self, client: str, staffing_plan: str, role: str, 
                                    pipeline_stage: str, use_cumulative: bool = True) -> Dict:
        """
        Get candidate count for a specific pipeline stage
        
        Args:
            client: Client name from Supply Management
            staffing_plan: Supply Plan Name from Supply Management  
            role: Role from Supply Management
            pipeline_stage: Stage of Pipeline from Supply Management
            use_cumulative: If True, count current stage + all subsequent stages
            
        Returns:
            Dict with count and breakdown details
        """
        return self._get_candidate_counts(client, staffing_plan, role, pipeline_stage, use_cumulative, count_type='active')
    
    def get_rejected_count_for_plan(self, client: str, staffing_plan: str, role: str) -> Dict:
        """
        Get rejected candidate count for a specific supply plan
        
        Args:
            client: Client name from Supply Management
            staffing_plan: Supply Plan Name from Supply Management  
            role: Role from Supply Management
            
        Returns:
            Dict with rejected count and breakdown details
        """
        return self._get_candidate_counts(client, staffing_plan, role, None, False, count_type='rejected')
    
    def get_exited_process_count_for_plan(self, client: str, staffing_plan: str, role: str) -> Dict:
        """
        Get exited process candidate count (Dropped + Rejected + On-Hold) for a specific supply plan
        
        Args:
            client: Client name from Supply Management
            staffing_plan: Supply Plan Name from Supply Management  
            role: Role from Supply Management
            
        Returns:
            Dict with exited process count and breakdown details
        """
        return self._get_candidate_counts(client, staffing_plan, role, None, False, count_type='exited')
    
    def _get_candidate_counts(self, client: str, staffing_plan: str, role: str, 
                             pipeline_stage: Optional[str] = None, use_cumulative: bool = True, count_type: str = 'active') -> Dict:
        """
        Internal method to get candidate counts for both active and rejected candidates
        
        Args:
            client: Client name from Supply Management
            staffing_plan: Supply Plan Name from Supply Management  
            role: Role from Supply Management
            pipeline_stage: Stage of Pipeline from Supply Management (None for rejected counts)
            use_cumulative: If True, count current stage + all subsequent stages
            count_type: 'active' for normal pipeline counts, 'rejected' for rejection counts
            
        Returns:
            Dict with count and breakdown details
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Determine which statuses to count based on count_type
            if count_type == 'rejected':
                valid_statuses = self.rejection_statuses
            elif count_type == 'exited':
                # Exited process includes Dropped, Rejected, and On-Hold statuses
                valid_statuses = ['Dropped', 'Rejected', 'On Hold', 'Screen Rejected', 'Candidate RNR/Dropped', 'Internal Dropped']
            else:
                # Determine which statuses to count based on pipeline stage
                if pipeline_stage is None:
                    valid_statuses = []
                elif use_cumulative:
                    valid_statuses = self.status_stage_mapping.get(pipeline_stage, [])
                else:
                    # For current stage only, need to map individual statuses
                    stage_status_map = {
                        'Initial Screening': ['Initial Screening'],
                        'Technical Assessment': ['Technical Assessment'],
                        'Interview Process': ['Interview Scheduled', 'Interview Completed'],
                        'Final Selection': ['Selected'],
                        'Offer & Onboarding': ['Offer Extended', 'Offer Accepted', 'Staffed'],
                        'Offer Extended': ['Offer Extended'],
                        'Offer Accepted': ['Offer Accepted'],
                        'Staffed': ['Staffed']
                    }
                    valid_statuses = stage_status_map.get(pipeline_stage, [])
            
            if not valid_statuses and count_type not in ['rejected', 'exited']:
                return {'count': 0, 'breakdown': {}, 'error': f'No status mapping found for stage: {pipeline_stage}'}
            
            # Multi-level fallback matching with count_type support
            results = self._try_multi_level_matching(cursor, client, staffing_plan, role, valid_statuses, count_type)
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error calculating candidate count: {e}")
            return {'count': 0, 'breakdown': {}, 'error': str(e)}
    
    def _try_multi_level_matching(self, cursor, client: str, staffing_plan: str, 
                                role: str, valid_statuses: List[str], count_type: str = 'active') -> Dict:
        """
        Try multi-level matching strategy
        Level 1: Exact match (client + staffing_plan + role)
        Level 2: Plan owner match (client + staffing_plan_owner + role) 
        Level 3: Client + role match
        """
        
        # Level 1: Exact Match
        level1_result = self._execute_candidate_query(
            cursor, client, staffing_plan, role, valid_statuses, match_level="exact", count_type=count_type
        )
        
        if level1_result['count'] > 0:
            level1_result['match_level'] = 'exact'
            return level1_result
        
        # Level 2: Plan Owner Match (need to get owner from staffing_plans)
        try:
            cursor.execute("""
                SELECT created_by FROM staffing_plans 
                WHERE plan_name = %s 
                LIMIT 1
            """, (staffing_plan,))
            
            owner_result = cursor.fetchone()
            if owner_result:
                owner = owner_result[0]
                level2_result = self._execute_candidate_query(
                    cursor, client, None, role, valid_statuses, 
                    match_level="owner", owner=owner, count_type=count_type
                )
                
                if level2_result['count'] > 0:
                    level2_result['match_level'] = 'owner'
                    return level2_result
        except Exception as e:
            logger.warning(f"Level 2 matching failed: {e}")
        
        # Level 3: Client + Role Match
        level3_result = self._execute_candidate_query(
            cursor, client, None, role, valid_statuses, match_level="client_role", count_type=count_type
        )
        
        level3_result['match_level'] = 'client_role'
        return level3_result
    
    def _get_client_id_from_name(self, cursor, client_name: str) -> Optional[int]:
        """Convert client name to client ID for database queries"""
        try:
            from utils.environment_manager import EnvironmentManager
            env_mgr = EnvironmentManager()
            master_clients_table = env_mgr.get_table_name('master_clients')
            
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", [client_name])
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting client ID for {client_name}: {e}")
            return None

    def _execute_candidate_query(self, cursor, client: str, staffing_plan: Optional[str], 
                               role: str, valid_statuses: List[str], match_level: str,
                               owner: Optional[str] = None, count_type: str = 'active') -> Dict:
        """Execute the candidate counting query based on match level"""
        
        # First resolve client name to ID for proper matching
        client_id = self._get_client_id_from_name(cursor, client)
        if not client_id:
            return {'count': 0, 'breakdown': {}, 'error': f'Client ID not found for: {client}', 'query_used': match_level}
        
        from utils.environment_manager import EnvironmentManager
        env_mgr = EnvironmentManager()
        candidate_data_table = env_mgr.get_table_name('candidate_data')
        staffing_plans_table = env_mgr.get_table_name('staffing_plans')
        staffing_plan_generated_plans_table = env_mgr.get_table_name('staffing_plan_generated_plans')
        
        status_placeholders = ','.join(['%s'] * len(valid_statuses))
        
        # Build status condition based on count_type
        if count_type in ['rejected', 'exited']:
            # For rejected/exited counts, only include rejection/exit statuses
            status_condition = f"cd.status IN ({status_placeholders})"
            status_params = valid_statuses
        else:
            # For active counts, include valid statuses but exclude rejected ones
            excluded_placeholders = ','.join(['%s'] * len(self.excluded_statuses))
            status_condition = f"cd.status IN ({status_placeholders}) AND cd.status NOT IN ({excluded_placeholders})"
            status_params = valid_statuses + self.excluded_statuses
        
        if match_level == "exact":
            query = f"""
                SELECT 
                    cd.status,
                    COUNT(*) as status_count
                FROM {candidate_data_table} cd
                JOIN {staffing_plans_table} sp ON cd.staffing_plan_id = sp.id
                WHERE cd.hire_for_client_id = %s 
                    AND sp.plan_name = %s 
                    AND cd.staffing_role = %s
                    AND ({status_condition})
                GROUP BY cd.status
            """
            params = [client_id, staffing_plan, role] + status_params
            
        elif match_level == "owner":
            query = f"""
                SELECT 
                    cd.status,
                    COUNT(*) as status_count
                FROM {candidate_data_table} cd
                WHERE cd.hire_for_client_id = %s 
                    AND cd.staffing_owner = %s 
                    AND cd.role = %s
                    AND ({status_condition})
                GROUP BY cd.status
            """
            params = [client_id, owner, role] + status_params
            
        else:  # client_role
            query = f"""
                SELECT 
                    cd.status,
                    COUNT(*) as status_count
                FROM {candidate_data_table} cd
                WHERE cd.hire_for_client_id = %s 
                    AND cd.role = %s
                    AND ({status_condition})
                GROUP BY cd.status
            """
            params = [client_id, role] + status_params
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Process results
        total_count = sum(row[1] for row in results) if results else 0
        breakdown = {}
        
        for row in results:
            status = row[0]
            count = row[1]
            breakdown[status] = count
        
        return {
            'count': total_count,
            'breakdown': breakdown,
            'query_used': match_level
        }
    
    def get_all_supply_management_rejected_counts(self) -> Dict:
        """
        Calculate rejected counts for all Supply Management records
        
        Returns:
            Dict mapping supply record IDs to rejected counts
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all supply management records from staffing plans and generated plans
            cursor.execute("""
                SELECT 
                    spgp.id, mc.client_name, sp.plan_name as staffing_plan, 
                    spgp.role, sp.created_by as owner
                FROM staffing_plan_generated_plans spgp
                JOIN staffing_plans sp ON spgp.plan_id = sp.id
                JOIN master_clients mc ON sp.client_id = mc.master_client_id
                WHERE sp.status IN ('Active', 'Planning')
            """)
            
            supply_records = cursor.fetchall()
            results = {}
            
            for record in supply_records:
                record_id, client, staffing_plan, role, owner = record
                
                # Calculate rejected count for this record
                count_result = self.get_rejected_count_for_plan(client, staffing_plan, role)
                
                results[record_id] = {
                    'rejected_count': count_result['count'],
                    'breakdown': count_result['breakdown'],
                    'match_level': count_result.get('match_level', 'none'),
                    'error': count_result.get('error')
                }
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error calculating all rejected counts: {e}")
            return {}
    
    def get_all_supply_management_actual_counts(self, use_cumulative: bool = True) -> Dict:
        """
        Calculate actual counts for all Supply Management records
        
        Returns:
            Dict mapping supply record IDs to actual counts
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all supply management records from staffing plans and generated plans
            cursor.execute("""
                SELECT 
                    spgp.id, mc.client_name, sp.plan_name as staffing_plan, 
                    spgp.role, 'N/A' as stage_of_pipeline, sp.created_by as owner
                FROM staffing_plan_generated_plans spgp
                JOIN staffing_plans sp ON spgp.plan_id = sp.id
                JOIN master_clients mc ON sp.client_id = mc.master_client_id
                WHERE sp.status IN ('Active', 'Planning')
            """)
            
            supply_records = cursor.fetchall()
            results = {}
            
            for record in supply_records:
                record_id, client, staffing_plan, role, pipeline_stage, owner = record
                
                # Calculate actual count for this record
                count_result = self.get_candidate_count_for_stage(
                    client, staffing_plan, role, pipeline_stage, use_cumulative
                )
                
                results[record_id] = {
                    'actual_count': count_result['count'],
                    'breakdown': count_result['breakdown'],
                    'match_level': count_result.get('match_level', 'none'),
                    'error': count_result.get('error')
                }
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error calculating all actual counts: {e}")
            return {}
    
    def get_data_quality_report(self) -> Dict:
        """
        Generate a data quality report for candidate-pipeline mapping
        
        Returns:
            Dict with data quality insights
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Count candidates with unmatched staffing plans
            cursor.execute("""
                SELECT COUNT(*) 
                FROM candidate_data cd
                LEFT JOIN staffing_plans sp ON cd.staffing_plan_id = sp.id 
                WHERE sp.id IS NULL AND cd.staffing_plan_id IS NOT NULL
            """)
            result = cursor.fetchone()
            unmatched_plans = result[0] if result else 0
            
            # Count candidates with unrecognized statuses
            all_valid_statuses = set()
            for statuses in self.status_stage_mapping.values():
                all_valid_statuses.update(statuses)
            all_valid_statuses.update(self.excluded_statuses)
            
            status_placeholders = ','.join(['%s'] * len(all_valid_statuses))
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM candidate_data 
                WHERE status NOT IN ({status_placeholders})
            """, list(all_valid_statuses))
            result = cursor.fetchone()
            unrecognized_statuses = result[0] if result else 0
            
            # Count candidates missing client/role mapping
            cursor.execute("""
                SELECT COUNT(*) 
                FROM candidate_data 
                WHERE hire_for_client_id IS NULL OR role IS NULL
            """)
            result = cursor.fetchone()
            missing_mapping = result[0] if result else 0
            
            conn.close()
            
            return {
                'unmatched_staffing_plans': unmatched_plans,
                'unrecognized_statuses': unrecognized_statuses, 
                'missing_client_role_mapping': missing_mapping,
                'total_status_mappings': len(all_valid_statuses),
                'excluded_statuses': self.excluded_statuses
            }
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return {'error': str(e)}