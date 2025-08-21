"""
Robust Database Manager - Permanent solution for database connection issues
"""
import psycopg2
import streamlit as st
from typing import Optional, Tuple, Any, List
import logging

logger = logging.getLogger(__name__)

class RobustDatabaseManager:
    """Centralized, robust database operations manager"""
    
    def __init__(self, env_manager):
        self.env_manager = env_manager
        self.database_url = env_manager.get_database_url()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None, fetch: bool = True) -> Tuple[Optional[Any], Optional[str]]:
        """
        Execute database query with robust error handling
        Returns (result, error_message)
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch and query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
            elif query.strip().upper().startswith('SELECT'):
                result = cursor.fetchone()
            else:
                conn.commit()
                result = True
                
            conn.close()
            return result, None
            
        except Exception as e:
            error_msg = f"Database operation failed: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    def get_client_id(self, client_name: str) -> Tuple[Optional[int], Optional[str]]:
        """Get client ID from client name - robust version"""
        query = f"SELECT master_client_id FROM {self.env_manager.get_table_name('master_clients')} WHERE client_name = %s"
        result, error = self.execute_query(query, (client_name,), fetch=False)
        
        if error:
            return None, error
        if result and len(result) > 0:
            return result[0], None
        return None, "Client not found"
    
    def get_pipeline_id(self, pipeline_name: str, client_id: int) -> Tuple[Optional[int], Optional[str]]:
        """Get pipeline ID from name and client ID - robust version"""
        query = f"""
            SELECT id FROM {self.env_manager.get_table_name('talent_pipelines')} 
            WHERE name = %s AND client_id = %s
        """
        result, error = self.execute_query(query, (pipeline_name, client_id), fetch=False)
        
        if error:
            return None, error
        if result and len(result) > 0:
            return result[0], None
        return None, f"Pipeline '{pipeline_name}' not found for client_id {client_id}"

class SessionStateManager:
    """Centralized session state management"""
    
    @staticmethod
    def ensure_initialized():
        """Ensure all required session state keys are initialized"""
        defaults = {
            'authenticated': False,
            'user_info': {},
            'user_email': '',
            'user_permissions': {},
            'accessible_modules': [],
            'permission_manager': None
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    @staticmethod
    def clear_form_state(form_keys: List[str], preserve_keys: Optional[List[str]] = None):
        """Clear form state with optional key preservation"""
        preserve_keys = preserve_keys or []
        
        for key in form_keys:
            if key in st.session_state and key not in preserve_keys:
                del st.session_state[key]
    
    @staticmethod
    def save_and_clear_with_rerun(save_function, clear_keys: List[str], preserve_keys: Optional[List[str]] = None):
        """Execute save function, clear state, and rerun"""
        try:
            success = save_function()
            if success:
                SessionStateManager.clear_form_state(clear_keys, preserve_keys)
                st.success("✅ Saved successfully!")
                st.rerun()
                return True
            else:
                st.error("❌ Save failed")
                return False
        except Exception as e:
            st.error(f"❌ Save error: {str(e)}")
            return False

def get_robust_db_manager():
    """Get or create robust database manager instance"""
    if 'robust_db_manager' not in st.session_state:
        from utils.environment_manager import EnvironmentManager
        env_manager = EnvironmentManager()
        st.session_state.robust_db_manager = RobustDatabaseManager(env_manager)
    
    return st.session_state.robust_db_manager

def ensure_permission_manager():
    """Ensure permission manager is available and not None"""
    if 'permission_manager' not in st.session_state or st.session_state.permission_manager is None:
        try:
            from utils.permission_manager import PermissionManager
            st.session_state.permission_manager = PermissionManager()
        except Exception as e:
            logger.warning(f"Could not initialize permission manager: {e}")
            st.session_state.permission_manager = None
    
    return st.session_state.get('permission_manager')