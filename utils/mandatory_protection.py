"""
Mandatory Protection System
MUST be used before ANY code changes to prevent breaking working functionality
"""

import functools
import logging
from typing import List, Callable, Any, Optional
from .guardian_agent import mandatory_change_validation, protect_working_functionality

logger = logging.getLogger(__name__)

def protect_existing_functionality(change_description: str, affected_files: Optional[List[str]] = None):
    """
    MANDATORY DECORATOR - Must be used on ANY function that modifies code
    Prevents changes to working functionality unless explicitly approved
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # MANDATORY validation before ANY change
            try:
                if affected_files is None:
                    files = []
                else:
                    files = affected_files

                # Check if working functionality protection is active
                if not protect_working_functionality():
                    raise Exception("🚨 GUARDIAN: Cannot activate protection - BLOCKING ALL CHANGES")

                # Validate the specific change
                validation = mandatory_change_validation(change_description, files)

                # If we get here, change is approved
                logger.info(f"🛡️ GUARDIAN: Executing approved change - {change_description}")
                result = func(*args, **kwargs)

                # Post-change health check
                from .guardian_agent import monitor_system_health
                post_health = monitor_system_health()

                if not post_health:
                    logger.error("🚨 GUARDIAN: Post-change health check FAILED")
                    raise Exception("Change caused system degradation - rolling back recommended")

                logger.info(f"✅ GUARDIAN: Change completed successfully - {change_description}")
                return result

            except Exception as e:
                logger.error(f"🚨 GUARDIAN BLOCKED: {str(e)}")
                # Re-raise to prevent execution
                raise Exception(f"GUARDIAN PROTECTION: {str(e)}")

        return wrapper
    return decorator

def emergency_protection_override(reason: str):
    """
    EMERGENCY ONLY - Override protection for critical fixes
    Must provide detailed reason
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger.warning(f"⚠️ EMERGENCY OVERRIDE: {reason}")
            logger.warning(f"⚠️ BYPASSING GUARDIAN PROTECTION FOR: {func.__name__}")

            # Still monitor health
            from .guardian_agent import monitor_system_health
            pre_health = monitor_system_health()

            result = func(*args, **kwargs)

            post_health = monitor_system_health()

            if pre_health and not post_health:
                logger.error(f"🚨 EMERGENCY OVERRIDE CAUSED SYSTEM DEGRADATION: {reason}")

            return result
        return decorator
    return decorator

def enforce_supply_plan_protection():
    """Enforce protection for Supply Plan functionality"""
    protected_functions = [
        'staffing_plans_section',
        'create_staffing_plan', 
        'edit_staffing_plan',
        'pipeline_generation',
        'save_generated_plans'
    ]

    print("🛡️ SUPPLY PLAN PROTECTION: All Supply Plan functions are protected")
    print("🚫 Unauthorized modifications to Supply Plan code are BLOCKED")
    print("⚠️  USER DIRECTIVE: NO changes to working Supply Plan code without explicit request")
    print("🔒 Guardian Agent will REJECT any Supply Plan modifications")
    return True

# Usage examples:
"""
@protect_existing_functionality("Adding new user feature", ["utils/user_manager.py"])
def add_new_user_feature():
    # This will be validated before execution
    pass

@emergency_protection_override("Critical security fix required immediately")
def emergency_fix():
    # Only use in true emergencies
    pass
"""