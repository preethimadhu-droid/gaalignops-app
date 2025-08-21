"""
Workflow Manager for handling JIRA-style recruitment pipeline workflows
"""
import psycopg2
import pandas as pd
import os
from datetime import datetime

class WorkflowManager:
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_url)
    
    def get_workflow_states(self):
        """Get all available workflow states"""
        conn = self.get_connection()
        try:
            query = """
            SELECT id, state_name, state_color, state_order, 
                   is_initial_state, is_final_state, 
                   COALESCE(is_special, false) as is_special
            FROM workflow_states 
            ORDER BY state_order
            """
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def get_pipeline_workflow_states(self, pipeline_id):
        """Get workflow states associated with a specific pipeline"""
        conn = self.get_connection()
        try:
            # Check if there's a pipeline_workflow_states table for associations
            # If not, return all workflow states as they're global
            query = """
            SELECT DISTINCT ws.id, ws.state_name, ws.state_color, ws.state_order, 
                   ws.is_initial_state, ws.is_final_state,
                   COALESCE(ws.is_special, false) as is_special
            FROM workflow_states ws
            ORDER BY ws.state_order
            """
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def get_workflow_actions(self):
        """Get all available workflow actions"""
        conn = self.get_connection()
        try:
            query = """
            SELECT id, action_name, action_description, requires_comment
            FROM workflow_actions
            ORDER BY action_name
            """
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def get_workflow_transitions(self):
        """Get all workflow transitions with state names"""
        conn = self.get_connection()
        try:
            query = """
            SELECT wt.id, wt.from_state_id, wt.to_state_id, wt.transition_action,
                   wt.required_role, wt.conditions,
                   fs.state_name as from_state_name,
                   ts.state_name as to_state_name
            FROM workflow_transitions wt
            JOIN workflow_states fs ON wt.from_state_id = fs.id
            JOIN workflow_states ts ON wt.to_state_id = ts.id
            ORDER BY fs.state_order, ts.state_order
            """
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def create_workflow_transition(self, from_state_id, to_state_id, action_name, 
                                 required_role=None, conditions=None):
        """Create a new workflow transition"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO workflow_transitions 
                (from_state_id, to_state_id, transition_action, required_role, conditions)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (from_state_id, to_state_id, action_name, required_role, conditions))
            
            transition_id = cursor.fetchone()[0]
            conn.commit()
            return transition_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def create_workflow_state(self, state_name, state_color='#6c757d', 
                            state_order=None, is_initial=False, is_final=False, is_special=False):
        """Create a new workflow state"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Check if state already exists
            cursor.execute("SELECT id FROM workflow_states WHERE state_name = %s", (state_name,))
            existing_state = cursor.fetchone()
            if existing_state:
                return existing_state[0]
            
            # If no order specified, get next order
            if state_order is None:
                cursor.execute("SELECT COALESCE(MAX(state_order), 0) + 1 FROM workflow_states")
                state_order = cursor.fetchone()[0]
            
            # Check if is_special column exists, if not add it
            try:
                cursor.execute("""
                    INSERT INTO workflow_states 
                    (state_name, state_color, state_order, is_initial_state, is_final_state, is_special)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (state_name, state_color, state_order, is_initial, is_final, is_special))
            except Exception:
                # Column might not exist, add it first
                cursor.execute("ALTER TABLE workflow_states ADD COLUMN IF NOT EXISTS is_special BOOLEAN DEFAULT FALSE")
                conn.commit()
                cursor.execute("""
                    INSERT INTO workflow_states 
                    (state_name, state_color, state_order, is_initial_state, is_final_state, is_special)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (state_name, state_color, state_order, is_initial, is_final, is_special))
            
            state_id = cursor.fetchone()[0]
            conn.commit()
            return state_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def clear_pipeline_workflow_states(self, pipeline_id):
        """Clear all workflow states for a specific pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Delete pipeline-specific workflow state associations
            cursor.execute("DELETE FROM pipeline_workflow_states WHERE pipeline_id = %s", (pipeline_id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error clearing pipeline workflow states: {str(e)}")
            return False
    
    def add_workflow_state_to_pipeline(self, pipeline_id, state_name, state_color, is_initial=False, is_final=False):
        """Add a workflow state to a specific pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First, create or get the workflow state
            state_id = self.create_workflow_state(state_name, state_color, None, is_initial, is_final, False)
            
            # Check if pipeline_workflow_states table exists, create if not
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_workflow_states (
                    id SERIAL PRIMARY KEY,
                    pipeline_id INTEGER REFERENCES talent_pipelines(id) ON DELETE CASCADE,
                    workflow_state_id INTEGER REFERENCES workflow_states(id) ON DELETE CASCADE,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(pipeline_id, workflow_state_id)
                )
            """)
            
            # Associate the state with the pipeline
            cursor.execute("""
                INSERT INTO pipeline_workflow_states (pipeline_id, workflow_state_id)
                VALUES (%s, %s)
                ON CONFLICT (pipeline_id, workflow_state_id) DO NOTHING
            """, (pipeline_id, state_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error adding workflow state to pipeline: {str(e)}")
            return False
    
    def create_workflow_action(self, action_name, description=None):
        """Create a new workflow action"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO workflow_actions (action_name, description)
                VALUES (%s, %s)
                RETURNING id
            """, (action_name, description))
            
            action_id = cursor.fetchone()[0]
            conn.commit()
            return action_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def record_candidate_transition(self, pipeline_id, candidate_name, candidate_email,
                                  from_state_id, to_state_id, action_id, performed_by,
                                  comments=None, actual_tat_days=None):
        """Record a candidate's workflow state transition"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO candidate_workflow_history
                (pipeline_id, candidate_name, candidate_email, from_state_id, 
                 to_state_id, action_id, performed_by, comments, actual_tat_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (pipeline_id, candidate_name, candidate_email, from_state_id,
                  to_state_id, action_id, performed_by, comments, actual_tat_days))
            
            history_id = cursor.fetchone()[0]
            conn.commit()
            return history_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_candidate_history(self, pipeline_id, candidate_name=None):
        """Get candidate workflow history for a pipeline"""
        conn = self.get_connection()
        try:
            where_clause = "WHERE cwh.pipeline_id = %s"
            params = [pipeline_id]
            
            if candidate_name:
                where_clause += " AND cwh.candidate_name = %s"
                params.append(candidate_name)
            
            query = f"""
            SELECT cwh.*, 
                   fs.state_name as from_state_name,
                   ts.state_name as to_state_name,
                   wa.action_name
            FROM candidate_workflow_history cwh
            LEFT JOIN workflow_states fs ON cwh.from_state_id = fs.id
            LEFT JOIN workflow_states ts ON cwh.to_state_id = ts.id
            LEFT JOIN workflow_actions wa ON cwh.action_id = wa.id
            {where_clause}
            ORDER BY cwh.transition_date DESC
            """
            
            df = pd.read_sql_query(query, conn, params=params)
            return df
        finally:
            conn.close()
    
    def get_pipeline_tat_analysis(self, pipeline_id):
        """Get TAT analysis for a pipeline comparing planned vs actual"""
        conn = self.get_connection()
        try:
            # Get planned TAT from pipeline stages
            planned_query = """
            SELECT stage_name, tat_days as planned_tat, conversion_rate as planned_conversion
            FROM pipeline_stages 
            WHERE pipeline_id = %s
            ORDER BY stage_order
            """
            planned_df = pd.read_sql_query(planned_query, conn, params=[pipeline_id])
            
            # Get actual TAT from candidate history
            actual_query = """
            SELECT ts.state_name, 
                   AVG(cwh.actual_tat_days) as actual_tat,
                   COUNT(*) as total_transitions,
                   COUNT(CASE WHEN cwh.to_state_id IN 
                        (SELECT id FROM workflow_states WHERE is_final_state = TRUE) 
                        THEN 1 END) as successful_transitions
            FROM candidate_workflow_history cwh
            JOIN workflow_states ts ON cwh.to_state_id = ts.id
            WHERE cwh.pipeline_id = %s AND cwh.actual_tat_days IS NOT NULL
            GROUP BY ts.state_name, ts.state_order
            ORDER BY ts.state_order
            """
            actual_df = pd.read_sql_query(actual_query, conn, params=[pipeline_id])
            
            return planned_df, actual_df
        finally:
            conn.close()
    
    def delete_workflow_transition(self, transition_id):
        """Delete a workflow transition"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM workflow_transitions WHERE id = %s", (transition_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_available_transitions(self, from_state_id):
        """Get available transitions from a specific state"""
        conn = self.get_connection()
        try:
            query = """
            SELECT wt.*, ts.state_name as to_state_name, ts.state_color
            FROM workflow_transitions wt
            JOIN workflow_states ts ON wt.to_state_id = ts.id
            WHERE wt.from_state_id = %s
            ORDER BY ts.state_order
            """
            df = pd.read_sql_query(query, conn, params=[from_state_id])
            return df
        finally:
            conn.close()