"""
Performance Manager for GA AlignOps
Implements asynchronous data fetching, lazy loading, and caching strategies
"""

import streamlit as st
import threading
import time
import asyncio
import concurrent.futures
from typing import Dict, Any, Callable, Optional, List
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    """Intelligent caching system for frequently accessed data"""
    
    def __init__(self):
        self.cache_duration = {
            'client_data': 300,      # 5 minutes
            'talent_data': 300,      # 5 minutes
            'dashboard_data': 180,   # 3 minutes
            'permissions': 600,      # 10 minutes
            'static_lists': 900,     # 15 minutes
            'user_data': 120         # 2 minutes
        }
        
    def get_cache_key(self, data_type: str, params: Dict = None) -> str:
        """Generate unique cache key"""
        key_parts = [data_type]
        if params:
            # Sort params for consistent key generation
            param_str = json.dumps(params, sort_keys=True, default=str)
            key_parts.append(hashlib.md5(param_str.encode()).hexdigest())
        return "_".join(key_parts)
    
    def is_cache_valid(self, cache_key: str, data_type: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in st.session_state:
            return False
            
        cached_item = st.session_state[cache_key]
        if not isinstance(cached_item, dict) or 'timestamp' not in cached_item:
            return False
            
        cache_age = time.time() - cached_item['timestamp']
        max_age = self.cache_duration.get(data_type, 300)
        
        return cache_age < max_age
    
    def get_cached_data(self, cache_key: str):
        """Retrieve cached data if valid"""
        if cache_key in st.session_state:
            cached_item = st.session_state[cache_key]
            if isinstance(cached_item, dict) and 'data' in cached_item:
                return cached_item['data']
        return None
    
    def cache_data(self, cache_key: str, data: Any):
        """Cache data with timestamp"""
        st.session_state[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }

class AsyncDataLoader:
    """Asynchronous data loading with background threads"""
    
    def __init__(self):
        self.cache_manager = CacheManager()
        self.loading_states = {}
        
    def load_data_async(self, data_type: str, fetch_function: Callable, 
                       params: Dict = None, force_reload: bool = False) -> Any:
        """Load data asynchronously with caching"""
        cache_key = self.cache_manager.get_cache_key(data_type, params)
        
        # Check cache first
        if not force_reload and self.cache_manager.is_cache_valid(cache_key, data_type):
            cached_data = self.cache_manager.get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data
        
        # Check if already loading
        if cache_key in self.loading_states and self.loading_states[cache_key]:
            # Show loading indicator while data is being fetched
            with st.spinner(f"Loading {data_type}..."):
                time.sleep(0.1)  # Small delay to prevent UI flashing
                # Check if loading completed
                if self.cache_manager.is_cache_valid(cache_key, data_type):
                    return self.cache_manager.get_cached_data(cache_key)
            return None
        
        # Start async loading
        self.loading_states[cache_key] = True
        
        def background_fetch():
            try:
                if params:
                    data = fetch_function(**params)
                else:
                    data = fetch_function()
                self.cache_manager.cache_data(cache_key, data)
                self.loading_states[cache_key] = False
                return data
            except Exception as e:
                logger.error(f"Error loading {data_type}: {e}")
                self.loading_states[cache_key] = False
                return None
        
        # Use thread pool for background loading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(background_fetch)
            try:
                # Wait briefly for quick operations
                data = future.result(timeout=2.0)
                return data
            except concurrent.futures.TimeoutError:
                # Continue loading in background
                st.info(f"Loading {data_type} in background...")
                return None

class LazyTabLoader:
    """Lazy loading system for tab content"""
    
    def __init__(self):
        self.loaded_tabs = set()
        self.tab_data = {}
        self.async_loader = AsyncDataLoader()
    
    def register_tab(self, tab_name: str, data_loaders: Dict[str, Callable]):
        """Register tab with its data loading functions"""
        self.tab_data[tab_name] = data_loaders
    
    def load_tab_data(self, tab_name: str) -> Dict[str, Any]:
        """Load data for specific tab only when needed"""
        if tab_name not in self.tab_data:
            return {}
        
        tab_key = f"tab_data_{tab_name}"
        
        # Check if already loaded and recent
        if (tab_key in st.session_state and 
            isinstance(st.session_state[tab_key], dict) and
            'timestamp' in st.session_state[tab_key]):
            
            cache_age = time.time() - st.session_state[tab_key]['timestamp']
            if cache_age < 300:  # 5 minutes
                return st.session_state[tab_key]['data']
        
        # Load data for this tab
        loaded_data = {}
        loaders = self.tab_data[tab_name]
        
        with st.spinner(f"Loading {tab_name} data..."):
            for data_type, loader_func in loaders.items():
                try:
                    data = self.async_loader.load_data_async(
                        f"{tab_name}_{data_type}", 
                        loader_func
                    )
                    if data is not None:
                        loaded_data[data_type] = data
                except Exception as e:
                    logger.error(f"Error loading {data_type} for {tab_name}: {e}")
                    loaded_data[data_type] = None
        
        # Cache the loaded data
        st.session_state[tab_key] = {
            'data': loaded_data,
            'timestamp': time.time()
        }
        
        self.loaded_tabs.add(tab_name)
        return loaded_data
    
    def prefetch_critical_data(self):
        """Prefetch critical data that's needed immediately after login"""
        critical_data = [
            ('user_permissions', self._load_user_permissions),
            ('navigation_data', self._load_navigation_data)
        ]
        
        for data_type, loader in critical_data:
            threading.Thread(
                target=lambda: self.async_loader.load_data_async(data_type, loader),
                daemon=True
            ).start()
    
    def _load_user_permissions(self):
        """Load user permissions in background"""
        try:
            from utils.permission_manager import PermissionManager
            if 'permission_manager' in st.session_state:
                pm = st.session_state.permission_manager
                user_email = st.session_state.get('user_email', '')
                return pm.get_user_permissions(user_email)
        except Exception as e:
            logger.error(f"Error loading user permissions: {e}")
            return {}
    
    def _load_navigation_data(self):
        """Load navigation-related data"""
        try:
            # Load basic navigation structure
            return {
                'pages': [
                    "Demand Planning",
                    "Supply Planning", 
                    "Demand - Supply Mapping",
                    "Insights & Reporting",
                    "Settings"
                ],
                'loaded_at': time.time()
            }
        except Exception as e:
            logger.error(f"Error loading navigation data: {e}")
            return {}

class PerformanceManager:
    """Main performance management system"""
    
    def __init__(self):
        self.cache_manager = CacheManager()
        self.async_loader = AsyncDataLoader()
        self.lazy_loader = LazyTabLoader()
        self.performance_metrics = {}
        
    def initialize(self):
        """Initialize performance management system"""
        # Set up session state for performance tracking
        if 'performance_manager_initialized' not in st.session_state:
            st.session_state.performance_manager_initialized = True
            st.session_state.performance_metrics = {}
            
            # Register tab data loaders
            self._register_tab_loaders()
            
            # Start prefetching critical data
            self.lazy_loader.prefetch_critical_data()
    
    def _register_tab_loaders(self):
        """Register all tab data loading functions"""
        try:
            # Demand Planning tab loaders
            self.lazy_loader.register_tab('demand_planning', {
                'unified_data': self._load_unified_sales_data,
                'targets': self._load_annual_targets,
                'client_list': self._load_client_list
            })
            
            # Supply Planning tab loaders
            self.lazy_loader.register_tab('supply_planning', {
                'talent_data': self._load_talent_supply,
                'assignments': self._load_talent_assignments,
                'availability': self._load_availability_data
            })
            
            # Demand-Supply Mapping tab loaders
            self.lazy_loader.register_tab('demand_supply_mapping', {
                'mappings': self._load_demand_supply_mappings,
                'client_data': self._load_client_list,
                'talent_data': self._load_talent_supply
            })
            
            # Insights & Reporting tab loaders
            self.lazy_loader.register_tab('insights_reporting', {
                'analytics_data': self._load_analytics_data,
                'dashboard_metrics': self._load_dashboard_metrics
            })
            
        except Exception as e:
            logger.error(f"Error registering tab loaders: {e}")
    
    def load_page_data(self, page_name: str) -> Dict[str, Any]:
        """Load data for specific page with performance tracking"""
        start_time = time.time()
        
        try:
            # Convert page name to tab key
            tab_key = page_name.lower().replace(' ', '_').replace('-', '_')
            data = self.lazy_loader.load_tab_data(tab_key)
            
            # Track performance
            load_time = time.time() - start_time
            self.performance_metrics[page_name] = {
                'load_time': load_time,
                'loaded_at': time.time(),
                'data_size': len(str(data)) if data else 0
            }
            
            return data
            
        except Exception as e:
            logger.error(f"Error loading page data for {page_name}: {e}")
            return {}
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            'metrics': self.performance_metrics,
            'cache_stats': self._get_cache_stats(),
            'memory_usage': self._get_memory_usage()
        }
    
    def _get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        cache_keys = [key for key in st.session_state.keys() 
                     if any(prefix in key for prefix in ['tab_data_', 'cache_', 'perf_'])]
        return {
            'cached_items': len(cache_keys),
            'session_keys': len(st.session_state.keys())
        }
    
    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        import sys
        return {
            'session_state_size': len(st.session_state),
            'python_memory': sys.getsizeof(st.session_state)
        }
    
    def clear_cache(self, data_type: str = None):
        """Clear cached data"""
        if data_type:
            # Clear specific data type
            keys_to_remove = [key for key in st.session_state.keys() 
                            if key.startswith(data_type)]
        else:
            # Clear all cached data
            keys_to_remove = [key for key in st.session_state.keys() 
                            if any(prefix in key for prefix in ['tab_data_', 'cache_', 'perf_'])]
        
        for key in keys_to_remove:
            del st.session_state[key]
    
    # Data loading functions
    def _load_unified_sales_data(self):
        """Load unified sales data efficiently"""
        try:
            from utils.unified_data_manager import UnifiedDataManager
            unified_db = UnifiedDataManager()
            return unified_db.get_all_data()
        except Exception as e:
            logger.error(f"Error loading unified sales data: {e}")
            return pd.DataFrame()
    
    def _load_annual_targets(self):
        """Load annual targets data"""
        try:
            import psycopg2
            import os
            from utils.environment_manager import EnvironmentManager
            env_manager = EnvironmentManager()
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            table_name = env_manager.get_table_name("annual_targets")
            return pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY year DESC", conn)
        except Exception as e:
            logger.error(f"Error loading annual targets: {e}")
            return pd.DataFrame()
    
    def _load_client_list(self):
        """Load client list efficiently"""
        try:
            import psycopg2
            import os
            from utils.environment_manager import EnvironmentManager
            env_manager = EnvironmentManager()
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            table_name = env_manager.get_table_name("master_clients")
            return pd.read_sql_query(f"SELECT DISTINCT client_name FROM {table_name} ORDER BY client_name", conn)
        except Exception as e:
            logger.error(f"Error loading client list: {e}")
            return pd.DataFrame()
    
    def _load_talent_supply(self):
        """Load talent supply data"""
        try:
            from utils.supply_data_manager import SupplyDataManager
            supply_manager = SupplyDataManager()
            return supply_manager.get_all_talent()
        except Exception as e:
            logger.error(f"Error loading talent supply: {e}")
            return pd.DataFrame()
    
    def _load_talent_assignments(self):
        """Load talent assignments"""
        try:
            import psycopg2
            import os
            from utils.environment_manager import EnvironmentManager
            env_manager = EnvironmentManager()
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            table_name = env_manager.get_table_name("demand_supply_assignments")
            return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        except Exception as e:
            logger.error(f"Error loading talent assignments: {e}")
            return pd.DataFrame()
    
    def _load_availability_data(self):
        """Load availability data"""
        try:
            from utils.supply_data_manager import SupplyDataManager
            supply_manager = SupplyDataManager()
            return supply_manager.get_availability_summary()
        except Exception as e:
            logger.error(f"Error loading availability data: {e}")
            return pd.DataFrame()
    
    def _load_demand_supply_mappings(self):
        """Load demand-supply mappings"""
        try:
            import psycopg2
            import os
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            return pd.read_sql_query("SELECT * FROM demand_supply_assignments", conn)
        except Exception as e:
            logger.error(f"Error loading demand supply mappings: {e}")
            return pd.DataFrame()
    
    def _load_analytics_data(self):
        """Load analytics data for insights"""
        try:
            from utils.unified_data_manager import UnifiedDataManager
            unified_db = UnifiedDataManager()
            data = unified_db.get_all_data()
            # Process data for analytics
            return data.groupby(['owner', 'month']).agg({
                'value': 'sum',
                'account_name': 'count'
            }).reset_index()
        except Exception as e:
            logger.error(f"Error loading analytics data: {e}")
            return pd.DataFrame()
    
    def _load_dashboard_metrics(self):
        """Load dashboard metrics"""
        try:
            from utils.unified_data_manager import UnifiedDataManager
            unified_db = UnifiedDataManager()
            data = unified_db.get_all_data()
            
            return {
                'total_revenue': data['value'].sum(),
                'total_accounts': data['account_name'].nunique(),
                'active_regions': data['region'].nunique(),
                'top_performers': data.groupby('owner')['value'].sum().nlargest(5).to_dict()
            }
        except Exception as e:
            logger.error(f"Error loading dashboard metrics: {e}")
            return {}

# Global performance manager instance
_performance_manager = None

def get_performance_manager() -> PerformanceManager:
    """Get global performance manager instance"""
    global _performance_manager
    if _performance_manager is None:
        _performance_manager = PerformanceManager()
        _performance_manager.initialize()
    return _performance_manager