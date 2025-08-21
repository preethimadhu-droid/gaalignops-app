
"""
Change Validator - Validates code changes before implementation
Works with Guardian Agent to ensure safe development
"""

import ast
import logging
from typing import List, Dict, Any
from utils.guardian_agent import validate_change_safety

logger = logging.getLogger(__name__)

class ChangeValidator:
    """Validates changes before they're implemented"""
    
    def __init__(self):
        self.critical_functions = [
            'check_auth', 'login_page', 'user_header', 'require_auth',
            'get_all_data', 'update_record', 'add_new_record',
            'has_permission', 'get_user_permissions'
        ]
        
    def validate_code_change(self, file_path: str, old_code: str, new_code: str) -> Dict[str, Any]:
        """Validate a code change for safety"""
        validation_result = {
            'safe': True,
            'warnings': [],
            'errors': [],
            'recommendations': []
        }
        
        try:
            # Parse both versions
            old_ast = ast.parse(old_code)
            new_ast = ast.parse(new_code)
            
            # Extract function definitions
            old_functions = [node.name for node in ast.walk(old_ast) if isinstance(node, ast.FunctionDef)]
            new_functions = [node.name for node in ast.walk(new_ast) if isinstance(node, ast.FunctionDef)]
            
            # Check for removed critical functions
            removed_functions = set(old_functions) - set(new_functions)
            critical_removed = [f for f in removed_functions if f in self.critical_functions]
            
            if critical_removed:
                validation_result['errors'].append(f"Critical functions removed: {critical_removed}")
                validation_result['safe'] = False
            
            # Check for function signature changes
            for func_name in self.critical_functions:
                if func_name in old_functions and func_name in new_functions:
                    # Additional signature validation could be added here
                    pass
            
            # Use Guardian Agent for additional validation
            guardian_result = validate_change_safety(f"Code change in {file_path}", [file_path])
            
            if not guardian_result['safe_to_proceed']:
                validation_result['errors'].extend(guardian_result['critical_issues'])
                validation_result['safe'] = False
            
            validation_result['warnings'].extend(guardian_result['warnings'])
            validation_result['recommendations'].extend(guardian_result['recommendations'])
            
        except SyntaxError as e:
            validation_result['errors'].append(f"Syntax error in new code: {str(e)}")
            validation_result['safe'] = False
        except Exception as e:
            validation_result['errors'].append(f"Validation error: {str(e)}")
            validation_result['safe'] = False
            
        return validation_result
    
    def pre_deployment_check(self) -> Dict[str, Any]:
        """Run comprehensive pre-deployment checks"""
        from utils.guardian_agent import guardian_agent
        
        checks = {
            'guardian_snapshot': guardian_agent.create_functionality_snapshot(),
            'system_monitoring': guardian_agent.monitor_real_time_changes(),
            'deployment_ready': True
        }
        
        # Determine if deployment is safe
        if not checks['system_monitoring']:
            checks['deployment_ready'] = False
            checks['blocking_issues'] = ['System monitoring detected issues']
        
        return checks

# Global validator instance
change_validator = ChangeValidator()
