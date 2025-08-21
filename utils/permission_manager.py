"""
Permission Manager for Role-Based Access Control
Handles dynamic permission checking and UI control based on user role groups
"""

import psycopg2
import os
from typing import Dict, List, Tuple, Optional
import streamlit as st

class PermissionManager:
    def __init__(self, env_manager=None):
        self.database_url = os.getenv('DATABASE_URL')
        self._user_permissions_cache = {}
        
        # Environment management for table routing
        self.env_manager = env_manager
        if self.env_manager:
            self.user_role_mappings_table = self.env_manager.get_table_name('user_role_mappings')
            self.role_groups_table = self.env_manager.get_table_name('role_groups')
            self.users_table = self.env_manager.get_table_name('users')
        else:
            # Fallback to production tables
            self.user_role_mappings_table = 'user_role_mappings'
            self.role_groups_table = 'role_groups'
            self.users_table = 'users'
        
        # Define module structure matching the application
        self.module_structure = {
            "Demand Planning": ["Target Setting", "Demand Tweaking", "Editable Plan View", "Demand Management"],
            "Supply Planning": ["Supply Planning", "Talent Management", "Pipeline Configuration", "Staffing Plans"],
            "Demand - Supply Mapping": ["Demand - Supply Mapping", "Add New Mapping", "View Mappings"],
            "Demand-Supply Mapping": ["Add New Mapping", "View Mappings"],
            "Insights & Reporting": ["Insights & Reporting", "Analytics Dashboard", "Export Functions"],
            "Settings": ["Settings", "User Management", "Roles & Role Groups", "Application Settings", "Database Status", "Environment", "Export Settings"]
        }
    
    def _get_username_variants(self, user_email: str) -> List[str]:
        """Generate multiple username variants to handle different naming formats"""
        variants = []
        
        # Extract base username from email (e.g., anna.pauly from anna.pauly@greyamp.com)
        base_username = user_email.split('@')[0]
        variants.append(base_username)
        
        # Add capitalized versions for display names (e.g., Anna Pauly)
        if '.' in base_username:
            parts = base_username.split('.')
            capitalized_name = ' '.join(part.capitalize() for part in parts)
            variants.append(capitalized_name)
        
        # Add first.last format variations
        variants.append(base_username.replace('.', ' ').title())
        
        # Add the original email as well in case it's stored as username
        variants.append(user_email)
        
        # Remove duplicates while preserving order
        unique_variants = []
        for variant in variants:
            if variant not in unique_variants:
                unique_variants.append(variant)
        
        return unique_variants
    
    def get_user_permissions(self, user_email: str) -> Dict:
        """Get all permissions for a user, with caching for performance"""
        # Always refresh permissions to ensure latest role group assignments
        # Remove from cache first to force fresh lookup
        if user_email in self._user_permissions_cache:
            del self._user_permissions_cache[user_email]
        
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Primary lookup: Join users table by email with user_role_mappings by username
            query = f'''
            SELECT DISTINCT rgp.module_name, rgp.sub_page, rgp.can_add, rgp.can_edit, rgp.can_delete, rgp.can_view
            FROM {self.users_table} u
            JOIN {self.user_role_mappings_table} urm ON u.username = urm.user_name
            JOIN role_group_permissions rgp ON urm.role_group_id = rgp.group_id
            JOIN {self.role_groups_table} rg ON urm.role_group_id = rg.id
            WHERE u.email = %s AND rg.status = 'Active' AND urm.status = 'active'
            '''
            
            cursor.execute(query, (user_email,))
            results = cursor.fetchall()
            
            # Fallback: Try username variants if email lookup fails
            if not results:
                username_variants = self._get_username_variants(user_email)
                for username in username_variants:
                    fallback_query = f'''
                    SELECT DISTINCT rgp.module_name, rgp.sub_page, rgp.can_add, rgp.can_edit, rgp.can_delete, rgp.can_view
                    FROM {self.user_role_mappings_table} urm
                    JOIN role_group_permissions rgp ON urm.role_group_id = rgp.group_id
                    JOIN {self.role_groups_table} rg ON urm.role_group_id = rg.id
                    WHERE urm.user_name = %s AND rg.status = 'Active' AND urm.status = 'active'
                    '''
                    
                    cursor.execute(fallback_query, (username,))
                    username_results = cursor.fetchall()
                    
                    if username_results:
                        results = username_results
                        break  # Found a match, use this one
            
            # Structure permissions by module and sub-page
            permissions = {}
            for row in results:
                module, sub_page, can_add, can_edit, can_delete, can_view = row
                
                if module not in permissions:
                    permissions[module] = {}
                
                permissions[module][sub_page] = {
                    'can_add': bool(can_add),
                    'can_edit': bool(can_edit),
                    'can_delete': bool(can_delete),
                    'can_view': bool(can_view)
                }
            
            conn.close()
            
            # Cache the permissions
            self._user_permissions_cache[user_email] = permissions
            return permissions
            
        except Exception as e:
            st.error(f"Error loading user permissions: {str(e)}")
            return {}
    
    def has_permission(self, user_email: str, module: str, sub_page: str, permission_type: str) -> bool:
        """Check if user has specific permission for module/sub-page"""
        permissions = self.get_user_permissions(user_email)
        
        # First check direct sub-page permission
        if module in permissions and sub_page in permissions[module]:
            return permissions[module][sub_page].get(f'can_{permission_type.lower()}', False)
        
        # Check for parent module permission that should inherit to all sub-pages
        # For Demand Planning, check if user has "Demand Management" permission
        if module == "Demand Planning" and module in permissions:
            if "Demand Management" in permissions[module]:
                return permissions[module]["Demand Management"].get(f'can_{permission_type.lower()}', False)
        
        # For Supply Planning, check if user has general "Supply Planning" permission  
        if module == "Supply Planning" and module in permissions:
            if "Supply Planning" in permissions[module]:
                return permissions[module]["Supply Planning"].get(f'can_{permission_type.lower()}', False)
        
        # For other modules, check if user has general module permission
        if module in permissions:
            # Check for a general module permission (same name as module)
            if module in permissions[module]:
                return permissions[module][module].get(f'can_{permission_type.lower()}', False)
            
            # For Settings, check if user has general "Settings" permission
            if module == "Settings" and "Settings" in permissions[module]:
                return permissions[module]["Settings"].get(f'can_{permission_type.lower()}', False)
            
            # For Insights & Reporting, check if user has general permission
            if module == "Insights & Reporting" and "Insights & Reporting" in permissions[module]:
                return permissions[module]["Insights & Reporting"].get(f'can_{permission_type.lower()}', False)
            
            # For Demand-Supply Mapping variations, check both naming conventions
            if "Demand" in module and "Supply" in module and "Mapping" in module:
                if "Demand - Supply Mapping" in permissions[module]:
                    return permissions[module]["Demand - Supply Mapping"].get(f'can_{permission_type.lower()}', False)
                if "Demand-Supply Mapping" in permissions[module]:
                    return permissions[module]["Demand-Supply Mapping"].get(f'can_{permission_type.lower()}', False)
        
        return False
    
    def can_access_module(self, user_email: str, module_name: str) -> bool:
        """Check if user can access a module (has View permission on any sub-page or parent permission)"""
        if module_name not in self.module_structure:
            return False
        
        # First check if user has parent module permission (e.g., "Demand Management" for Demand Planning)
        if module_name == "Demand Planning":
            if self.has_permission(user_email, module_name, "Demand Management", 'view'):
                return True
        elif module_name == "Supply Planning":
            if self.has_permission(user_email, module_name, "Supply Planning", 'view'):
                return True
        elif module_name == "Settings":
            if self.has_permission(user_email, module_name, "Settings", 'view'):
                return True
        elif module_name == "Insights & Reporting":
            if self.has_permission(user_email, module_name, "Insights & Reporting", 'view'):
                return True
        elif "Demand" in module_name and "Supply" in module_name and "Mapping" in module_name:
            if self.has_permission(user_email, module_name, "Demand - Supply Mapping", 'view') or \
               self.has_permission(user_email, module_name, "Demand-Supply Mapping", 'view'):
                return True
        
        # Then check individual sub-pages
        sub_pages = self.module_structure[module_name]
        for sub_page in sub_pages:
            if self.has_permission(user_email, module_name, sub_page, 'view'):
                return True
        
        return False
    
    def get_accessible_modules(self, user_email: str) -> List[str]:
        """Get list of modules user can access"""
        accessible_modules = []
        for module in self.module_structure.keys():
            if self.can_access_module(user_email, module):
                accessible_modules.append(module)
        
        return accessible_modules
    
    def get_allowed_actions(self, user_email: str, module: str, sub_page: str) -> Dict[str, bool]:
        """Get all allowed actions for user on specific module/sub-page"""
        return {
            'can_view': self.has_permission(user_email, module, sub_page, 'view'),
            'can_edit': self.has_permission(user_email, module, sub_page, 'edit'),
            'can_add': self.has_permission(user_email, module, sub_page, 'add'),
            'can_delete': self.has_permission(user_email, module, sub_page, 'delete')
        }
    
    def clear_user_cache(self, user_email: Optional[str] = None):
        """Clear permission cache for user or all users"""
        if user_email:
            self._user_permissions_cache.pop(user_email, None)
        else:
            self._user_permissions_cache.clear()
    
    def force_refresh_permissions(self, user_email: Optional[str] = None):
        """Force refresh of permissions by clearing cache"""
        self.clear_user_cache(user_email)
        if user_email:
            # Pre-load fresh permissions
            self.get_user_permissions(user_email)
    
    def is_admin_user(self, user_email: str) -> bool:
        """Check if user is an admin (for special admin-only features)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Try multiple username matching strategies
            username_variants = self._get_username_variants(user_email)
            
            for username in username_variants:
                # Check if user has admin role or is in admin-like role group
                query = f'''
                SELECT COUNT(*)
                FROM {self.user_role_mappings_table} urm
                JOIN {self.role_groups_table} rg ON urm.role_group_id = rg.id
                WHERE urm.user_name = %s AND (rg.group_name ILIKE '%admin%' OR rg.group_name = 'Administrator')
                AND urm.status = 'active' AND rg.status = 'Active'
                '''
                
                cursor.execute(query, (username,))
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    conn.close()
                    return True
            
            conn.close()
            return False
            
        except Exception:
            return False
    
    def get_permission_tooltip(self, user_email: str, module: str, sub_page: str) -> str:
        """Get tooltip text explaining user's access level"""
        actions = self.get_allowed_actions(user_email, module, sub_page)
        
        if not any(actions.values()):
            return "❌ No Access - Contact administrator for permissions"
        
        allowed_actions = [action.replace('can_', '').title() for action, allowed in actions.items() if allowed]
        
        if len(allowed_actions) == 4:
            return "✅ Full Access - Can View, Edit, Add, and Delete"
        elif len(allowed_actions) == 1 and 'View' in allowed_actions:
            return "👁️ View Only - Read-only access"
        else:
            return f"🔧 Limited Access - Can {', '.join(allowed_actions)}"
    
    def create_permission_button(self, label: str, action_type: str, user_email: str, module: str, sub_page: str, 
                                key: str = None, **button_kwargs) -> bool:
        """Create a button that respects permissions and shows appropriate tooltip"""
        has_perm = self.has_permission(user_email, module, sub_page, action_type)
        
        if has_perm:
            return st.button(label, key=key, **button_kwargs)
        else:
            # Show disabled button with tooltip
            tooltip = f"❌ No {action_type.title()} Permission - Contact administrator"
            st.button(label, key=key, disabled=True, help=tooltip, **button_kwargs)
            return False
    
    def permission_protected_button(self, label: str, user_email: str, module: str, sub_page: str, action_type: str = 'edit',
                                  key: str = None, **button_kwargs) -> bool:
        """Create a permission-protected button - alias for create_permission_button for backward compatibility"""
        return self.create_permission_button(label, action_type, user_email, module, sub_page, key, **button_kwargs)
    
    def show_access_denied_message(self, module: str, sub_page: str):
        """Show standardized access denied message"""
        st.error("🚫 **Access Denied**")
        st.warning(f"You don't have permission to access **{module} → {sub_page}**")
        st.info("Contact your administrator to request access to this feature.")
    
    def protect_page_access(self, user_email: str, module: str, sub_page: str) -> bool:
        """Protect page access - returns True if user can access, False if denied"""
        if self.has_permission(user_email, module, sub_page, 'view'):
            return True
        else:
            self.show_access_denied_message(module, sub_page)
            return False

# Global permission manager instance
permission_manager = PermissionManager()