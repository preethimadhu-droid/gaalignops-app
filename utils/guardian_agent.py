"""
Guardian Agent System
Monitors and validates changes to ensure existing functionality remains intact
"""

import logging
import psycopg2
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
import streamlit as st
import pandas as pd

logger = logging.getLogger(__name__)

class GuardianAgent:
    """Guardian Agent to protect existing functionality during development"""

    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        self.protection_level = os.environ.get('GUARDIAN_PROTECTION_LEVEL', 'STANDARD')
        self.critical_functions = [
            'authentication_system',
            'data_integrity',
            'permission_system',
            'core_database_operations',
            'user_management'
        ]

    def create_functionality_snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of current system functionality"""
        try:
            snapshot = {
                'timestamp': datetime.now().isoformat(),
                'database_health': self._check_database_health(),
                'critical_tables': self._verify_critical_tables(),
                'authentication_status': self._check_authentication_system(),
                'permission_integrity': self._check_permission_system(),
                'user_access_patterns': self._analyze_user_access(),
                'data_consistency': self._verify_data_consistency()
            }

            logger.info(f"Guardian: Functionality snapshot created - {len(snapshot)} checks completed")
            return snapshot

        except Exception as e:
            logger.error(f"Guardian: Error creating functionality snapshot: {str(e)}")
            return {}

    def validate_change_impact(self, change_description: str, affected_files: List[str]) -> Dict[str, Any]:
        """Validate the impact of proposed changes on existing functionality - AGGRESSIVE PROTECTION"""
        validation_result = {
            'safe_to_proceed': False,  # DEFAULT TO BLOCKING - MUST EXPLICITLY APPROVE
            'warnings': [],
            'critical_issues': [],
            'affected_systems': [],
            'recommendations': [],
            'protection_level': 'MAXIMUM'
        }

        # EXPANDED critical files list - protect EVERYTHING working
        critical_files = [
            'auth.py', 'app.py', 'utils/permission_manager.py', 
            'utils/user_manager.py', 'utils/unified_data_manager.py',
            'utils/supply_data_manager.py', 'utils/staffing_plans_manager.py',
            'utils/pipeline_manager.py', 'utils/corrected_mapping_manager.py',
            'utils/environment_manager.py', 'utils/sales_data_manager.py',
            'utils/role_manager.py', 'utils/mapping_manager.py'
        ]

        # BLOCK access to backup directories - PREVENTS RECURRING ISSUES
        backup_directories = [
            'backup_workingsystem8Aug', 'backup_workingsystem8aug',
            'testing_suite_backup_july14', 'migration_scripts_backup',
            'utils_backup_july12', 'utils_backup_july12_postgresql',
            'utils_backup_july14_before_pipeline_generation'
        ]

        # Check EVERY file being modified
        for file in affected_files:
            # BLOCK backup directory access - PREVENTS RECURRING ISSUES
            for backup_dir in backup_directories:
                if backup_dir in file:
                    validation_result['critical_issues'].append(f"BLOCKED: Agent attempting to access backup directory: {backup_dir}")
                    validation_result['recommendations'].append(f"Use current active file instead of backup: {file}")
                    logger.error(f"🚨 GUARDIAN: BLOCKED backup access - {file}")

            if any(critical in file for critical in critical_files):
                validation_result['affected_systems'].append(f"PROTECTED FILE: {file}")
                validation_result['critical_issues'].append(f"Attempting to modify protected working file: {file}")
                # BLOCK by default - must be explicitly approved

        # EXPANDED risky operations detection
        risky_keywords = [
            'delete', 'drop', 'truncate', 'modify schema', 'alter table',
            'change authentication', 'alter permissions', 'remove', 'clear',
            'reset', 'update', 'insert', 'replace', 'refactor', 'rewrite',
            'fix', 'improve', 'optimize', 'enhance', 'modify', 'edit'
        ]

        for keyword in risky_keywords:
            if keyword.lower() in change_description.lower():
                validation_result['critical_issues'].append(f"POTENTIALLY RISKY: {keyword} operation detected")

        # Only allow changes if explicitly requested AND non-critical
        safe_change_indicators = [
            'add new functionality', 'create new file', 'add new feature',
            'user explicitly requested', 'isolated change', 'new module'
        ]

        is_explicitly_safe = any(indicator.lower() in change_description.lower() 
                               for indicator in safe_change_indicators)

        # ONLY proceed if change is explicitly safe AND doesn't touch critical files
        validation_result['safe_to_proceed'] = is_explicitly_safe and is_non_critical

        if not validation_result['safe_to_proceed']:
            validation_result['recommendations'].append("BLOCKED: Change appears to modify working functionality")
            validation_result['recommendations'].append("To proceed: Explicitly state this is a new feature or isolated change")
            validation_result['recommendations'].append("Guardian Agent protecting existing functionality as requested")

        return validation_result

    def monitor_real_time_changes(self) -> bool:
        """Monitor system in real-time for functionality degradation"""
        try:
            # Check core functionality
            checks = {
                'database_connectivity': self._test_database_connection(),
                'authentication_working': self._test_authentication_flow(),
                'permission_system_active': self._test_permission_system(),
                'data_access_functional': self._test_data_access()
            }

            failed_checks = [check for check, status in checks.items() if not status]

            if failed_checks:
                logger.error(f"Guardian: System degradation detected - Failed checks: {failed_checks}")
                self._trigger_protection_protocol(failed_checks)
                return False

            logger.info("Guardian: Real-time monitoring passed all checks")
            return True

        except Exception as e:
            logger.error(f"Guardian: Error in real-time monitoring: {str(e)}")
            return False

    def _check_database_health(self) -> Dict[str, Any]:
        """Check database health and connectivity"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            # Check critical tables exist
            critical_tables = [
                'users', 'unified_sales_data', 'master_clients', 
                'talent_supply', 'role_groups', 'user_role_mappings'
            ]

            health_status = {}
            for table in critical_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                health_status[table] = {'exists': True, 'record_count': count}

            conn.close()
            return health_status

        except Exception as e:
            logger.error(f"Guardian: Database health check failed: {str(e)}")
            return {'error': str(e)}

    def _verify_critical_tables(self) -> Dict[str, bool]:
        """Verify all critical tables are intact"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            # Check table schemas haven't been corrupted
            cursor.execute("""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name IN ('users', 'unified_sales_data', 'master_clients')
                ORDER BY table_name, ordinal_position
            """)

            schema_info = cursor.fetchall()
            conn.close()

            return {
                'schema_intact': len(schema_info) > 0,
                'tables_verified': len(set(row[0] for row in schema_info))
            }

        except Exception as e:
            logger.error(f"Guardian: Table verification failed: {str(e)}")
            return {'schema_intact': False}

    def _check_authentication_system(self) -> Dict[str, Any]:
        """Check authentication system integrity"""
        try:
            # Verify auth.py functions are accessible
            from auth import check_auth, FallbackAuth

            # Test fallback auth instantiation
            fallback_auth = FallbackAuth()

            return {
                'auth_module_loaded': True,
                'fallback_auth_available': True,
                'allowed_emails_configured': len(fallback_auth.allowed_emails) > 0
            }

        except Exception as e:
            logger.error(f"Guardian: Authentication check failed: {str(e)}")
            return {'auth_module_loaded': False, 'error': str(e)}

    def _check_permission_system(self) -> Dict[str, Any]:
        """Check permission system integrity"""
        try:
            from utils.permission_manager import PermissionManager
            permission_manager = PermissionManager()

            # Test basic permission operations
            test_result = permission_manager.get_user_permissions('test@greyamp.com')

            return {
                'permission_manager_loaded': True,
                'permission_queries_working': isinstance(test_result, dict)
            }

        except Exception as e:
            logger.error(f"Guardian: Permission system check failed: {str(e)}")
            return {'permission_manager_loaded': False, 'error': str(e)}

    def _analyze_user_access(self) -> Dict[str, Any]:
        """Analyze current user access patterns"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            # Check active users and their roles
            cursor.execute("""
                SELECT COUNT(DISTINCT u.email) as total_users,
                       COUNT(DISTINCT urm.user_name) as users_with_roles,
                       COUNT(DISTINCT rg.group_name) as active_role_groups
                FROM users u
                LEFT JOIN user_role_mappings urm ON u.username = urm.user_name
                LEFT JOIN role_groups rg ON urm.role_group_id = rg.id
                WHERE u.status = 'Active'
            """)

            access_stats = cursor.fetchone()
            conn.close()

            return {
                'total_active_users': access_stats[0] if access_stats[0] else 0,
                'users_with_roles': access_stats[1] if access_stats[1] else 0,
                'active_role_groups': access_stats[2] if access_stats[2] else 0
            }

        except Exception as e:
            logger.error(f"Guardian: User access analysis failed: {str(e)}")
            return {'error': str(e)}

    def _verify_data_consistency(self) -> Dict[str, Any]:
        """Verify critical data consistency"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            # Check for data integrity issues
            checks = {}

            # Check Q2 billed data (your critical data point)
            cursor.execute("""
                SELECT COUNT(*) FROM unified_sales_data 
                WHERE metric_type = 'Billed' AND month = 'June' AND year = 2025
            """)
            checks['q2_billed_records'] = cursor.fetchone()[0]

            # Check for orphaned records
            cursor.execute("""
                SELECT COUNT(*) FROM user_role_mappings urm
                LEFT JOIN users u ON urm.user_name = u.username
                WHERE u.username IS NULL
            """)
            checks['orphaned_role_mappings'] = cursor.fetchone()[0]

            conn.close()
            return checks

        except Exception as e:
            logger.error(f"Guardian: Data consistency check failed: {str(e)}")
            return {'error': str(e)}

    def _test_database_connection(self) -> bool:
        """Test database connectivity"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            return True
        except:
            return False

    def _test_authentication_flow(self) -> bool:
        """Test authentication flow"""
        try:
            from auth import FallbackAuth
            fallback_auth = FallbackAuth()
            return len(fallback_auth.allowed_emails) > 0
        except:
            return False

    def _test_permission_system(self) -> bool:
        """Test permission system"""
        try:
            from utils.permission_manager import PermissionManager
            permission_manager = PermissionManager()
            return True
        except:
            return False

    def _test_data_access(self) -> bool:
        """Test data access functionality"""
        try:
            from utils.unified_data_manager import UnifiedDataManager
            unified_db = UnifiedDataManager()
            stats = unified_db.get_database_stats()
            return isinstance(stats, dict)
        except:
            return False

    def _trigger_protection_protocol(self, failed_checks: List[str]):
        """Trigger protection protocol when issues are detected"""
        logger.critical(f"Guardian: PROTECTION PROTOCOL ACTIVATED - Failed checks: {failed_checks}")

        # Store protection event
        protection_event = {
            'timestamp': datetime.now().isoformat(),
            'failed_checks': failed_checks,
            'protection_level': self.protection_level,
            'action_taken': 'SYSTEM_MONITORING_ALERT'
        }

        # In Streamlit context, show warning
        if 'streamlit' in str(type(st)):
            st.error(f"🚨 Guardian Protection: System issues detected - {', '.join(failed_checks)}")

    def display_guardian_status(self):
        """Display Guardian Agent status in Streamlit"""
        try:
            if st.sidebar.checkbox("🛡️ Guardian Status", value=False):
                with st.sidebar.expander("Guardian Agent Monitor", expanded=True):
                    snapshot = self.create_functionality_snapshot()

                    if snapshot:
                        st.success("✅ Guardian Active")
                        st.metric("Database Health", "✓" if snapshot.get('database_health') else "✗")
                        st.metric("Auth System", "✓" if snapshot.get('authentication_status', {}).get('auth_module_loaded') else "✗")
                        st.metric("Permissions", "✓" if snapshot.get('permission_integrity', {}).get('permission_manager_loaded') else "✗")

                        if st.button("🔍 Full System Check"):
                            monitoring_result = self.monitor_real_time_changes()
                            if monitoring_result:
                                st.success("All systems operational")
                            else:
                                st.error("System issues detected")
                    else:
                        st.error("❌ Guardian Offline")
        except Exception as e:
            logger.error(f"Guardian: Error displaying status: {str(e)}")

    def _is_protected_file(self, file_path: str) -> bool:
        """Check if a file is considered protected"""
        protected_files = [
            'utils/supply_data_manager.py', 
            'utils/staffing_plans_manager.py',
            'utils/pipeline_manager.py'
        ]
        return any(protected in file_path for protected in protected_files)

    def validate_change_request(self, change_description: str, affected_files: List[str]):
        """
        Validate if a change request is safe to proceed
        Returns: {'safe_to_proceed': bool, 'critical_issues': list, 'warnings': list}
        """
        validation_result = {
            'safe_to_proceed': False,
            'critical_issues': [],
            'warnings': []
        }

        # CRITICAL: Supply Plan Protection Check
        supply_plan_keywords = ['supply plan', 'staffing plan', 'pipeline generation', 'supply code']
        if any(keyword in change_description.lower() for keyword in supply_plan_keywords):
            validation_result['critical_issues'].append(
                "🚫 SUPPLY PLAN PROTECTION: Supply Plan code modifications are BLOCKED. "
                "User explicitly requested NO changes to working Supply Plan functionality. "
                "Any Supply Plan changes require EXPLICIT user authorization with specific scope."
            )

        # Check if any protected files are affected
        for file_path in affected_files:
            if self._is_protected_file(file_path):
                validation_result['critical_issues'].append(
                    f"PROTECTED FILE: {file_path} - Cannot modify without explicit user authorization"
                )

        # Only allow changes if explicitly requested AND non-critical
        safe_change_indicators = [
            'user explicitly requested this specific change',
            'user authorized this modification', 
            'create completely new file',
            'add isolated new functionality'
        ]

        is_explicitly_safe = any(indicator.lower() in change_description.lower() 
                               for indicator in safe_change_indicators)
        
        # Determine if the change is safe to proceed
        # Change is safe if it's explicitly marked as safe AND has no critical issues
        validation_result['safe_to_proceed'] = is_explicitly_safe and not validation_result['critical_issues']

        if not validation_result['safe_to_proceed']:
            validation_result['warnings'].append("Change is blocked by Guardian Agent to protect existing functionality.")
            validation_result['recommendations'].append("To proceed: Ensure change description explicitly states user authorization and scope.")
            validation_result['recommendations'].append("Avoid modifying protected files or Supply Plan code without explicit approval.")

        return validation_result

# Global Guardian instance
guardian_agent = GuardianAgent()

def enable_guardian_protection():
    """Enable Guardian Agent protection"""
    logger.info("🛡️ Guardian Agent protection enabled")
    return guardian_agent.create_functionality_snapshot()

def validate_change_safety(change_description: str, files: List[str] = None):
    """Validate if a change is safe to implement"""
    if files is None:
        files = []
    return guardian_agent.validate_change_impact(change_description, files)

def monitor_system_health():
    """Monitor system health in real-time"""
    return guardian_agent.monitor_real_time_changes()

def mandatory_change_validation(change_description: str, affected_files: List[str]):
    """MANDATORY validation - MUST be called before ANY code changes"""
    logger.critical(f"🛡️ GUARDIAN: Validating change request: {change_description}")

    # CHECK FOR BACKUP ACCESS ATTEMPTS - PREVENTS RECURRING ISSUES
    from .backup_isolation_system import validate_file_access

    for file_path in affected_files:
        backup_check = validate_file_access(file_path)
        if not backup_check['access_allowed']:
            logger.error(f"🚨 GUARDIAN: BLOCKING BACKUP ACCESS - {file_path}")
            logger.error(f"🚨 REASON: {backup_check['reason']}")
            if backup_check['redirect_to']:
                logger.info(f"✅ USE INSTEAD: {backup_check['redirect_to']}")
            raise Exception(f"BACKUP ACCESS BLOCKED: {backup_check['reason']}")

    validation = guardian_agent.validate_change_request(change_description, affected_files)

    if not validation['safe_to_proceed']:
        logger.error(f"🚨 GUARDIAN BLOCKING CHANGE: {validation['critical_issues']}")
        logger.error(f"🚨 AFFECTED SYSTEMS: {validation['affected_systems']}")
        logger.error(f"🚨 RECOMMENDATIONS: {validation['recommendations']}")

        # HARD STOP - do not proceed
        raise Exception(f"GUARDIAN AGENT BLOCKED CHANGE: {validation['critical_issues']}")

    logger.info(f"✅ GUARDIAN: Change approved - {change_description}")
    return validation

def protect_working_functionality():
    """Create a protection barrier around all working functionality"""
    try:
        # Take snapshot of current working state
        snapshot = guardian_agent.create_functionality_snapshot()

        # Monitor system health
        health_check = guardian_agent.monitor_real_time_changes()

        if not health_check:
            raise Exception("GUARDIAN: System health check failed - blocking all changes")

        logger.info("🛡️ GUARDIAN: Working functionality protection active")
        return True

    except Exception as e:
        logger.error(f"🚨 GUARDIAN: Protection setup failed: {str(e)}")
        return False