
"""
AI Assistant Integration Helper
Call out for coding assistance and validation
"""

import logging
from typing import Dict, List, Any, Optional
from .guardian_agent import guardian_agent

logger = logging.getLogger(__name__)

class AIAssistantHelper:
    """Helper class to call out for AI assistant support during development"""
    
    def __init__(self):
        self.assistant_name = "Replit Assistant"
        self.context_info = {}
    
    def request_code_review(self, files_changed: List[str], description: str) -> Dict[str, Any]:
        """Request AI assistant code review"""
        logger.info(f"🤖 Requesting {self.assistant_name} code review for: {description}")
        
        # Run Guardian validation first
        guardian_result = guardian_agent.validate_change_impact(description, files_changed)
        
        review_request = {
            'type': 'code_review',
            'description': description,
            'files_changed': files_changed,
            'guardian_validation': guardian_result,
            'request_message': f"Hey {self.assistant_name}, can you review this change?",
            'context': self.context_info
        }
        
        return review_request
    
    def request_debugging_help(self, error_message: str, file_path: str = None) -> Dict[str, Any]:
        """Request AI assistant debugging help"""
        logger.info(f"🐛 Requesting {self.assistant_name} debugging help")
        
        debug_request = {
            'type': 'debugging',
            'error_message': error_message,
            'file_path': file_path,
            'request_message': f"Hey {self.assistant_name}, help me debug this issue!",
            'system_health': guardian_agent.monitor_real_time_changes()
        }
        
        return debug_request
    
    def request_feature_implementation(self, feature_description: str, affected_areas: List[str]) -> Dict[str, Any]:
        """Request AI assistant help with feature implementation"""
        logger.info(f"✨ Requesting {self.assistant_name} feature implementation help")
        
        feature_request = {
            'type': 'feature_implementation',
            'description': feature_description,
            'affected_areas': affected_areas,
            'request_message': f"Hey {self.assistant_name}, help me implement this feature!",
            'safety_check': guardian_agent.validate_change_impact(feature_description, affected_areas)
        }
        
        return feature_request
    
    def emergency_assistance(self, issue_description: str) -> Dict[str, Any]:
        """Emergency call for AI assistant help"""
        logger.critical(f"🚨 EMERGENCY: Requesting immediate {self.assistant_name} assistance")
        
        emergency_request = {
            'type': 'emergency',
            'issue': issue_description,
            'request_message': f"🚨 URGENT: {self.assistant_name}, I need immediate help!",
            'system_snapshot': guardian_agent.create_functionality_snapshot(),
            'priority': 'CRITICAL'
        }
        
        return emergency_request
    
    def validate_coding_standards(self, code: str, file_path: str) -> Dict[str, Any]:
        """Validate code against GA AlignOps coding standards"""
        logger.info(f"🔍 Validating coding standards for {file_path}")
        
        validation_result = {
            'compliant': True,
            'violations': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Check for required imports in critical files
        if file_path in ['auth.py', 'app.py']:
            if 'from utils.guardian_agent import' not in code:
                validation_result['violations'].append("Missing Guardian Agent import")
                validation_result['compliant'] = False
        
        # Check for authentication checks
        if 'def ' in code and 'sensitive' in code.lower():
            if 'check_auth()' not in code:
                validation_result['warnings'].append("Sensitive function may need authentication check")
        
        # Check for error handling
        if 'def ' in code and 'try:' not in code:
            validation_result['warnings'].append("Function should include error handling")
        
        # Check for logging
        if 'def ' in code and 'logger.' not in code:
            validation_result['recommendations'].append("Consider adding logging statements")
        
        standards_check = {
            'type': 'standards_validation',
            'file_path': file_path,
            'validation_result': validation_result,
            'request_message': f"Hey {self.assistant_name}, review this code for standards compliance!"
        }
        
        return standards_check

# Global assistant helper
ai_assistant = AIAssistantHelper()

def call_assistant(request_type: str, **kwargs) -> Dict[str, Any]:
    """Quick function to call out for AI assistant help"""
    if request_type == "review":
        return ai_assistant.request_code_review(
            kwargs.get('files', []), 
            kwargs.get('description', 'Code review needed')
        )
    elif request_type == "debug":
        return ai_assistant.request_debugging_help(
            kwargs.get('error', 'Unknown error'), 
            kwargs.get('file', None)
        )
    elif request_type == "feature":
        return ai_assistant.request_feature_implementation(
            kwargs.get('description', 'New feature'), 
            kwargs.get('areas', [])
        )
    elif request_type == "emergency":
        return ai_assistant.emergency_assistance(
            kwargs.get('issue', 'Critical issue')
        )
    elif request_type == "standards":
        return ai_assistant.validate_coding_standards(
            kwargs.get('code', ''), 
            kwargs.get('file_path', 'unknown')
        )
    else:
        return {"error": "Unknown request type"}

def enforce_coding_standards(func):
    """Decorator to enforce coding standards on functions"""
    def wrapper(*args, **kwargs):
        # Log function call for monitoring
        logger.info(f"🔧 Calling function: {func.__name__}")
        
        # Check if Guardian validation needed for critical functions
        if func.__name__ in ['delete_data', 'modify_schema', 'change_permissions']:
            logger.warning(f"⚠️ Critical function called: {func.__name__} - Guardian validation recommended")
        
        try:
            result = func(*args, **kwargs)
            logger.info(f"✅ Function completed successfully: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"❌ Function failed: {func.__name__} - {str(e)}")
            # Call for debugging assistance
            call_assistant("debug", error=str(e), file=func.__module__)
            raise
    
    return wrapper
