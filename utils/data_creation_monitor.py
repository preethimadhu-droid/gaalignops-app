
"""
Data Creation Monitor
Prevents and tracks unauthorized test data creation by the AI agent
"""

import logging
import psycopg2
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import streamlit as st
import traceback

logger = logging.getLogger(__name__)

class DataCreationMonitor:
    """Monitor and prevent unauthorized data creation"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        self.monitoring_active = True
        self.authorized_operations = set()
        self.blocked_operations = []
        self.setup_logging()
        
    def setup_logging(self):
        """Setup dedicated logging for data creation monitoring"""
        self.logger = logging.getLogger('data_creation_monitor')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - DATA_MONITOR - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.WARNING)
    
    def authorize_data_operation(self, operation_id: str, description: str, user_initiated: bool = True):
        """Authorize a specific data operation"""
        if user_initiated:
            self.authorized_operations.add(operation_id)
            self.logger.info(f"AUTHORIZED: {operation_id} - {description}")
        else:
            self.logger.error(f"BLOCKED: Unauthorized data operation attempted - {operation_id} - {description}")
            return False
        return True
    
    def monitor_insert_operation(self, table_name: str, operation_details: Dict[str, Any], caller_info: str = ""):
        """Monitor INSERT operations for unauthorized test data creation"""
        try:
            # Get caller stack to identify who is creating data
            caller_stack = traceback.format_stack()
            
            # Check if this is a legitimate user-initiated operation
            user_email = getattr(st.session_state, 'user_email', 'unknown')
            
            # Analyze the operation for suspicious patterns
            is_suspicious = self._analyze_operation_legitimacy(table_name, operation_details, caller_stack)
            
            if is_suspicious:
                warning_msg = f"""
                🚨 SUSPICIOUS DATA CREATION DETECTED
                
                Table: {table_name}
                User: {user_email}
                Caller: {caller_info}
                Details: {operation_details}
                Time: {datetime.now().isoformat()}
                
                This operation appears to be creating test data without explicit user request.
                """
                
                self.logger.error(warning_msg)
                self.blocked_operations.append({
                    'timestamp': datetime.now().isoformat(),
                    'table': table_name,
                    'user': user_email,
                    'details': operation_details,
                    'caller': caller_info,
                    'stack': caller_stack[-5:]  # Last 5 stack frames
                })
                
                # Show warning in UI if Streamlit context is available
                if 'streamlit' in str(type(st)):
                    st.error(f"⚠️ Prevented unauthorized data creation in {table_name}")
                
                return False
            
            # Log legitimate operations
            self.logger.info(f"LEGITIMATE: Data operation in {table_name} by {user_email}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error monitoring insert operation: {str(e)}")
            return False
    
    def _analyze_operation_legitimacy(self, table_name: str, operation_details: Dict[str, Any], caller_stack: List[str]) -> bool:
        """Analyze if a data operation is legitimate or suspicious test data creation"""
        
        # Check for test data patterns in the data itself
        suspicious_patterns = [
            'test_', 'dummy_', 'fake_', 'sample_', 'mock_',
            'example_', 'demo_', 'placeholder_', 'temp_'
        ]
        
        # Convert operation details to string for analysis
        operation_str = str(operation_details).lower()
        
        # Flag if data contains suspicious test patterns
        for pattern in suspicious_patterns:
            if pattern in operation_str:
                self.logger.warning(f"Suspicious pattern '{pattern}' found in data: {operation_str[:100]}...")
                return True
        
        # Check caller stack for automated/AI-driven operations
        stack_str = '\n'.join(caller_stack[-3:])  # Last 3 frames
        
        # Flag operations from automated scripts or AI processes
        automated_indicators = [
            'automated_', 'auto_', 'generate_', 'populate_',
            'seed_', 'create_sample', 'bulk_create'
        ]
        
        for indicator in automated_indicators:
            if indicator in stack_str.lower():
                self.logger.warning(f"Automated operation detected: {indicator}")
                return True
        
        # Check if operation is creating large amounts of data (bulk operations)
        if isinstance(operation_details, dict):
            if 'count' in operation_details and operation_details.get('count', 0) > 10:
                self.logger.warning(f"Bulk operation detected: {operation_details.get('count')} records")
                return True
        
        return False
    
    def check_table_for_test_data(self, table_name: str) -> Dict[str, Any]:
        """Check existing table for suspicious test data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get sample data from table to analyze
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
            sample_data = cursor.fetchall()
            
            # Get column names
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND table_schema = 'public'
            """)
            columns = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            # Analyze data for test patterns
            test_data_found = []
            suspicious_patterns = ['test', 'dummy', 'fake', 'sample', 'mock', 'example', 'demo']
            
            for row in sample_data[:20]:  # Check first 20 rows
                row_str = str(row).lower()
                for pattern in suspicious_patterns:
                    if pattern in row_str:
                        test_data_found.append({
                            'pattern': pattern,
                            'data_sample': str(row)[:100] + '...' if len(str(row)) > 100 else str(row)
                        })
                        break
            
            return {
                'table': table_name,
                'total_records': len(sample_data),
                'suspicious_records': len(test_data_found),
                'test_patterns_found': test_data_found,
                'analysis_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'table': table_name,
                'error': str(e),
                'analysis_time': datetime.now().isoformat()
            }
    
    def scan_all_tables_for_test_data(self) -> Dict[str, Any]:
        """Comprehensive scan of all tables for test data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            scan_results = {
                'scan_timestamp': datetime.now().isoformat(),
                'tables_scanned': len(tables),
                'tables_with_test_data': 0,
                'detailed_results': {}
            }
            
            for table in tables:
                result = self.check_table_for_test_data(table)
                scan_results['detailed_results'][table] = result
                
                if result.get('suspicious_records', 0) > 0:
                    scan_results['tables_with_test_data'] += 1
            
            return scan_results
            
        except Exception as e:
            return {
                'error': str(e),
                'scan_timestamp': datetime.now().isoformat()
            }
    
    def get_monitoring_report(self) -> Dict[str, Any]:
        """Get comprehensive monitoring report"""
        return {
            'monitoring_status': 'ACTIVE' if self.monitoring_active else 'INACTIVE',
            'authorized_operations': len(self.authorized_operations),
            'blocked_operations': len(self.blocked_operations),
            'recent_blocked_operations': self.blocked_operations[-5:] if self.blocked_operations else [],
            'last_check': datetime.now().isoformat()
        }
    
    def display_monitoring_dashboard(self):
        """Display monitoring dashboard in Streamlit"""
        try:
            if st.sidebar.checkbox("🔍 Data Creation Monitor", value=False):
                with st.sidebar.expander("Data Creation Monitoring", expanded=True):
                    
                    # Status indicators
                    status = "🟢 ACTIVE" if self.monitoring_active else "🔴 INACTIVE"
                    st.metric("Monitor Status", status)
                    
                    # Recent activity
                    report = self.get_monitoring_report()
                    st.metric("Blocked Operations", report['blocked_operations'])
                    st.metric("Authorized Operations", report['authorized_operations'])
                    
                    # Quick scan button
                    if st.button("🔍 Scan for Test Data"):
                        with st.spinner("Scanning tables..."):
                            scan_results = self.scan_all_tables_for_test_data()
                            
                            if scan_results.get('tables_with_test_data', 0) > 0:
                                st.error(f"⚠️ Found test data in {scan_results['tables_with_test_data']} tables")
                            else:
                                st.success("✅ No suspicious test data found")
                    
                    # Show recent blocked operations
                    if report['recent_blocked_operations']:
                        st.warning("Recent Blocked Operations:")
                        for op in report['recent_blocked_operations'][-3:]:
                            st.caption(f"🚫 {op['table']} - {op['timestamp'][:16]}")
                    
        except Exception as e:
            logger.error(f"Error displaying monitoring dashboard: {str(e)}")

# Global monitor instance
data_creation_monitor = DataCreationMonitor()

def monitor_data_creation(table_name: str, operation_details: Dict[str, Any], caller_info: str = ""):
    """Monitor data creation operation"""
    return data_creation_monitor.monitor_insert_operation(table_name, operation_details, caller_info)

def authorize_data_operation(operation_id: str, description: str, user_initiated: bool = True):
    """Authorize a specific data operation"""
    return data_creation_monitor.authorize_data_operation(operation_id, description, user_initiated)

def scan_for_test_data():
    """Scan all tables for test data"""
    return data_creation_monitor.scan_all_tables_for_test_data()

def get_monitoring_status():
    """Get current monitoring status"""
    return data_creation_monitor.get_monitoring_report()
