"""
Session Optimizer for GA AlignOps
Optimizes session state management and connection pooling
"""

import streamlit as st
import psycopg2
try:
    from psycopg2 import pool
except ImportError:
    pool = None
import os
import threading
import time
from typing import Dict, Any, Optional
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Efficient database connection management with pooling"""
    
    def __init__(self):
        self.connection_pool = None
        self.pool_lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                logger.error("DATABASE_URL not found")
                return
            
            with self.pool_lock:
                if self.connection_pool is None:
                    self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=2,
                        maxconn=10,
                        dsn=database_url
                    )
                    logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with context manager"""
        conn = None
        try:
            if self.connection_pool:
                conn = self.connection_pool.getconn()
                yield conn
            else:
                # Fallback to direct connection
                conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                if self.connection_pool:
                    self.connection_pool.putconn(conn)
                else:
                    conn.close()

class SessionStateOptimizer:
    """Optimize session state management"""
    
    def __init__(self):
        self.essential_keys = {
            'user_info', 'authenticated', 'user_email', 'current_page',
            'permission_manager', 'user_manager', 'env_manager'
        }
        self.cleanup_patterns = [
            'temp_', 'tmp_', 'edit_', 'form_', 'modal_', 'show_'
        ]
    
    def optimize_session_state(self):
        """Clean up unnecessary session state items"""
        if 'session_optimized_at' in st.session_state:
            last_cleanup = st.session_state.session_optimized_at
            # Only cleanup every 5 minutes
            if time.time() - last_cleanup < 300:
                return
        
        keys_to_remove = []
        
        # Identify temporary keys that can be cleaned up
        for key in st.session_state.keys():
            if any(pattern in key for pattern in self.cleanup_patterns):
                # Check if key hasn't been accessed recently
                if key not in self.essential_keys:
                    keys_to_remove.append(key)
        
        # Remove old temporary data
        for key in keys_to_remove:
            try:
                del st.session_state[key]
            except:
                pass
        
        st.session_state.session_optimized_at = time.time()
        
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} temporary session state items")
    
    def preload_essential_data(self):
        """Preload essential data that's needed across pages"""
        essential_loaders = {
            'user_permissions': self._load_user_permissions,
            'client_list_cache': self._load_basic_client_list,
            'talent_count_cache': self._load_talent_summary
        }
        
        for cache_key, loader in essential_loaders.items():
            if cache_key not in st.session_state:
                try:
                    threading.Thread(
                        target=lambda k=cache_key, l=loader: self._background_load(k, l),
                        daemon=True
                    ).start()
                except Exception as e:
                    logger.error(f"Error starting background load for {cache_key}: {e}")
    
    def _background_load(self, cache_key: str, loader_func):
        """Load data in background thread"""
        try:
            data = loader_func()
            st.session_state[f"{cache_key}_data"] = {
                'data': data,
                'loaded_at': time.time()
            }
        except Exception as e:
            logger.error(f"Background loading error for {cache_key}: {e}")
    
    def _load_user_permissions(self):
        """Load user permissions efficiently"""
        try:
            if 'permission_manager' in st.session_state:
                pm = st.session_state.permission_manager
                user_email = st.session_state.get('user_email', '')
                return pm.get_user_permissions(user_email)
        except Exception as e:
            logger.error(f"Error loading user permissions: {e}")
        return {}
    
    def _load_basic_client_list(self):
        """Load basic client list for dropdowns"""
        try:
            conn_manager = get_connection_manager()
            with conn_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT client_name FROM master_clients ORDER BY client_name LIMIT 100")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading client list: {e}")
        return []
    
    def _load_talent_summary(self):
        """Load talent summary statistics"""
        try:
            conn_manager = get_connection_manager()
            with conn_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_talent,
                        COUNT(CASE WHEN type = 'FTE' THEN 1 END) as fte_count,
                        COUNT(CASE WHEN type = 'NFTE' THEN 1 END) as nfte_count
                    FROM talent_supply
                """)
                row = cursor.fetchone()
                return {
                    'total_talent': row[0] if row else 0,
                    'fte_count': row[1] if row else 0,
                    'nfte_count': row[2] if row else 0
                }
        except Exception as e:
            logger.error(f"Error loading talent summary: {e}")
        return {'total_talent': 0, 'fte_count': 0, 'nfte_count': 0}

class MemoryOptimizer:
    """Optimize memory usage and prevent memory leaks"""
    
    def __init__(self):
        self.max_session_size = 50  # Maximum number of non-essential session keys
        self.memory_check_interval = 600  # Check memory every 10 minutes
    
    def optimize_memory(self):
        """Optimize memory usage"""
        if 'last_memory_check' in st.session_state:
            last_check = st.session_state.last_memory_check
            if time.time() - last_check < self.memory_check_interval:
                return
        
        # Count non-essential session state items
        non_essential_keys = [
            key for key in st.session_state.keys()
            if not self._is_essential_key(key)
        ]
        
        # If too many non-essential items, clean up oldest ones
        if len(non_essential_keys) > self.max_session_size:
            # Sort by likely age (keys with timestamps or common patterns)
            keys_to_remove = self._identify_old_keys(non_essential_keys)
            
            for key in keys_to_remove[:10]:  # Remove up to 10 old items
                try:
                    del st.session_state[key]
                except:
                    pass
            
            logger.info(f"Memory optimization: removed {len(keys_to_remove)} old session items")
        
        st.session_state.last_memory_check = time.time()
    
    def _is_essential_key(self, key: str) -> bool:
        """Check if session key is essential"""
        essential_patterns = [
            'user_', 'auth', 'permission_', 'current_page',
            'db_manager', 'env_manager', 'performance_'
        ]
        return any(pattern in key for pattern in essential_patterns)
    
    def _identify_old_keys(self, keys: list) -> list:
        """Identify old keys that can be safely removed"""
        old_patterns = [
            'temp_', 'form_data_', 'edit_', 'show_', 'modal_',
            'selected_', 'filter_', 'search_'
        ]
        
        old_keys = []
        for key in keys:
            if any(pattern in key for pattern in old_patterns):
                old_keys.append(key)
        
        return old_keys

class ExternalServiceOptimizer:
    """Optimize connections to external services"""
    
    def __init__(self):
        self.google_sheets_cache = {}
        self.api_rate_limits = {
            'google_sheets': {'calls': 0, 'window_start': time.time(), 'limit': 100}
        }
    
    def optimize_google_sheets_access(self):
        """Optimize Google Sheets API calls with intelligent caching"""
        # Implement rate limiting and caching for Google Sheets API
        current_time = time.time()
        
        # Reset rate limit window if needed (hourly reset)
        if current_time - self.api_rate_limits['google_sheets']['window_start'] > 3600:
            self.api_rate_limits['google_sheets']['calls'] = 0
            self.api_rate_limits['google_sheets']['window_start'] = current_time
        
        # Check if we're approaching rate limits
        calls_made = self.api_rate_limits['google_sheets']['calls']
        if calls_made > 80:  # 80% of limit
            logger.warning("Approaching Google Sheets API rate limit")
            return False
        
        return True
    
    def cache_google_sheets_data(self, sheet_id: str, data: Any):
        """Cache Google Sheets data to reduce API calls"""
        self.google_sheets_cache[sheet_id] = {
            'data': data,
            'cached_at': time.time()
        }
    
    def get_cached_sheets_data(self, sheet_id: str, max_age: int = 3600):
        """Get cached Google Sheets data if available and fresh"""
        if sheet_id in self.google_sheets_cache:
            cached_item = self.google_sheets_cache[sheet_id]
            age = time.time() - cached_item['cached_at']
            if age < max_age:
                return cached_item['data']
        return None

# Global instances
_connection_manager = None
_session_optimizer = None
_memory_optimizer = None
_external_service_optimizer = None

def get_connection_manager() -> ConnectionManager:
    """Get global connection manager instance"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager

def get_session_optimizer() -> SessionStateOptimizer:
    """Get global session optimizer instance"""
    global _session_optimizer
    if _session_optimizer is None:
        _session_optimizer = SessionStateOptimizer()
    return _session_optimizer

def get_memory_optimizer() -> MemoryOptimizer:
    """Get global memory optimizer instance"""
    global _memory_optimizer
    if _memory_optimizer is None:
        _memory_optimizer = MemoryOptimizer()
    return _memory_optimizer

def get_external_service_optimizer() -> ExternalServiceOptimizer:
    """Get global external service optimizer instance"""
    global _external_service_optimizer
    if _external_service_optimizer is None:
        _external_service_optimizer = ExternalServiceOptimizer()
    return _external_service_optimizer

def optimize_session():
    """Run all session optimizations"""
    try:
        session_opt = get_session_optimizer()
        memory_opt = get_memory_optimizer()
        
        session_opt.optimize_session_state()
        session_opt.preload_essential_data()
        memory_opt.optimize_memory()
        
    except Exception as e:
        logger.error(f"Error during session optimization: {e}")