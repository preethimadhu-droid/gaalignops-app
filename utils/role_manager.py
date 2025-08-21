#!/usr/bin/env python3
"""
Role Manager utility for handling roles and role groups database operations
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime

class RoleManager:
    def __init__(self, env_manager=None):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not found")
        
        # Environment management for table routing
        self.env_manager = env_manager
        if self.env_manager:
            self.roles_table = self.env_manager.get_table_name('roles')
            self.role_groups_table = self.env_manager.get_table_name('role_groups')
            self.user_role_mappings_table = self.env_manager.get_table_name('user_role_mappings')
            self.users_table = self.env_manager.get_table_name('users')
        else:
            # Fallback to production tables if no environment manager
            self.roles_table = 'roles'
            self.role_groups_table = 'role_groups'
            self.user_role_mappings_table = 'user_role_mappings'
            self.users_table = 'users'
    
    def _clear_user_permission_cache(self, *usernames):
        """Clear permission cache for specific users to force permission refresh"""
        try:
            import streamlit as st
            # Clear cache for permission manager if it exists
            if hasattr(st.session_state, 'permission_manager'):
                permission_manager = st.session_state.permission_manager
                if hasattr(permission_manager, '_user_permissions_cache'):
                    for username in usernames:
                        if username and username in permission_manager._user_permissions_cache:
                            del permission_manager._user_permissions_cache[username]
                        # Also try email format
                        if username and '@greyamp.com' not in username:
                            email_format = f"{username}@greyamp.com"
                            if email_format in permission_manager._user_permissions_cache:
                                del permission_manager._user_permissions_cache[email_format]
        except:
            # Silently ignore cache clearing errors - not critical
            pass
    
    def get_all_roles(self):
        """Get all roles from database"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = f'''
            SELECT id, role_name, description, status, created_date, created_by
            FROM {self.roles_table} 
            ORDER BY id ASC
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading roles: {str(e)}")
            return pd.DataFrame()
    
    def create_role(self, role_name, description, status='Active', created_by='admin'):
        """Create a new role"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if role already exists
            cursor.execute("SELECT id FROM roles WHERE role_name = %s", (role_name,))
            if cursor.fetchone():
                conn.close()
                return False, f"Role '{role_name}' already exists"
            
            cursor.execute('''
            INSERT INTO roles (role_name, description, status, created_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            ''', (role_name, description, status, created_by))
            
            role_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return True, f"Role '{role_name}' created successfully with ID {role_id}"
            
        except Exception as e:
            return False, f"Error creating role: {str(e)}"
    
    def update_role(self, role_id, role_name, description, status):
        """Update an existing role"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if role exists
            cursor.execute("SELECT id FROM roles WHERE id = %s", (role_id,))
            if not cursor.fetchone():
                conn.close()
                return False, f"Role with ID {role_id} not found"
            
            # Check if new role name conflicts with existing role (excluding current role)
            cursor.execute("SELECT id FROM roles WHERE role_name = %s AND id != %s", (role_name, role_id))
            if cursor.fetchone():
                conn.close()
                return False, f"Role name '{role_name}' already exists"
            
            cursor.execute('''
            UPDATE roles 
            SET role_name = %s, description = %s, status = %s, updated_date = %s
            WHERE id = %s
            ''', (role_name, description, status, datetime.now().isoformat(), role_id))
            
            conn.commit()
            conn.close()
            return True, f"Role '{role_name}' updated successfully"
            
        except Exception as e:
            return False, f"Error updating role: {str(e)}"
    
    def delete_role(self, role_id):
        """Delete a role (soft delete by setting status to Inactive)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if it's a system role
            cursor.execute("SELECT role_name FROM roles WHERE id = %s", (role_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return False, "Role not found"
            
            role_name = result[0]
            if role_name in ['admin', 'team_member']:
                conn.close()
                return False, "Cannot delete system roles"
            
            # Soft delete by setting status to Inactive
            cursor.execute('''
            UPDATE roles 
            SET status = 'Inactive', updated_date = %s
            WHERE id = %s
            ''', (datetime.now().isoformat(), role_id))
            
            conn.commit()
            conn.close()
            return True, f"Role '{role_name}' deactivated successfully"
            
        except Exception as e:
            return False, f"Error deleting role: {str(e)}"
    
    def get_all_role_groups(self, include_inactive=False):
        """Get active role groups from database (excludes inactive unless specified)"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            # Filter condition based on include_inactive parameter
            status_filter = "" if include_inactive else "WHERE rg.status = 'Active'"
            
            query = f'''
            SELECT rg.id, rg.group_name, rg.description, rg.status, 
                   STRING_AGG(r.role_name, ', ') as roles
            FROM role_groups rg
            LEFT JOIN role_group_mappings rgm ON rg.id = rgm.group_id
            LEFT JOIN roles r ON rgm.role_id = r.id
            {status_filter}
            GROUP BY rg.id, rg.group_name, rg.description, rg.status
            ORDER BY rg.id ASC
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading role groups: {str(e)}")
            return pd.DataFrame()
    
    def create_role_group(self, group_name, description, role_ids, status='Active', created_by='admin'):
        """Create a new role group with assigned roles"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if group already exists
            cursor.execute("SELECT id FROM role_groups WHERE group_name = %s", (group_name,))
            if cursor.fetchone():
                conn.close()
                return False, f"Role group '{group_name}' already exists"
            
            # Create role group
            cursor.execute('''
            INSERT INTO role_groups (group_name, description, status, created_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            ''', (group_name, description, status, created_by))
            
            group_id = cursor.fetchone()[0]
            
            # Add role mappings
            for role_id in role_ids:
                cursor.execute('''
                INSERT INTO role_group_mappings (role_id, group_id)
                VALUES (%s, %s)
                ''', (role_id, group_id))
            
            conn.commit()
            conn.close()
            return True, f"Role group '{group_name}' created successfully"
            
        except Exception as e:
            return False, f"Error creating role group: {str(e)}"
    
    def create_role_group_with_permissions(self, group_name, description, role_ids, status='Active', permissions_df=None, created_by='admin'):
        """Create a new role group with assigned roles and module permissions"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if group already exists
            cursor.execute("SELECT id FROM role_groups WHERE group_name = %s", (group_name,))
            if cursor.fetchone():
                conn.close()
                return False, f"Role group '{group_name}' already exists"
            
            # Create role group
            cursor.execute('''
            INSERT INTO role_groups (group_name, description, status, created_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            ''', (group_name, description, status, created_by))
            
            group_id = cursor.fetchone()[0]
            
            # Add role mappings
            for role_id in role_ids:
                cursor.execute('''
                INSERT INTO role_group_mappings (role_id, group_id)
                VALUES (%s, %s)
                ''', (role_id, group_id))
            
            # Add permissions if provided
            if permissions_df is not None and not permissions_df.empty:
                for _, row in permissions_df.iterrows():
                    cursor.execute('''
                    INSERT INTO role_group_permissions (group_id, module_name, sub_page, can_add, can_edit, can_delete, can_view)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (group_id, row['Module'], row['Sub-Page'], row['Add'], row['Edit'], row['Delete'], row['View']))
            
            conn.commit()
            conn.close()
            return True, f"Role group '{group_name}' created successfully with permissions"
            
        except Exception as e:
            return False, f"Error creating role group: {str(e)}"

    def get_available_roles_for_dropdown(self):
        """Get active roles for dropdown selections"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("SELECT id, role_name FROM roles WHERE status = 'Active' ORDER BY role_name")
            roles = cursor.fetchall()
            conn.close()
            
            return [(role[0], role[1]) for role in roles]
            
        except Exception as e:
            print(f"Error loading dropdown roles: {str(e)}")
            return []
    
    def get_role_group_details(self, group_id):
        """Get detailed information about a specific role group"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, group_name, description, status, created_date, updated_date
                FROM role_groups 
                WHERE id = %s
            """, (group_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'group_name': result[1],
                    'description': result[2],
                    'status': result[3],
                    'created_date': result[4],
                    'updated_date': result[5]
                }
            return None
            
        except Exception as e:
            print(f"Error getting role group details: {str(e)}")
            return None
    
    def get_user_role_mappings(self):
        """Get all user role mappings"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT urm.user_name, urm.role_group_id, rg.group_name
            FROM user_role_mappings urm
            JOIN role_groups rg ON urm.role_group_id = rg.id
            ORDER BY urm.user_name
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user role mappings: {str(e)}")
            return pd.DataFrame()
    
    def get_role_group_permissions(self, group_id):
        """Get permissions for a specific role group"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT module_name, sub_page, can_add, can_edit, can_delete, can_view
            FROM role_group_permissions 
            WHERE group_id = %s
            ORDER BY module_name, sub_page
            '''
            df = pd.read_sql_query(query, conn, params=(group_id,))
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading permissions: {str(e)}")
            return pd.DataFrame()
    
    def get_roles_for_group(self, group_id):
        """Get roles assigned to a specific role group"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT r.id, r.role_name 
            FROM roles r
            JOIN role_group_mappings rgm ON r.id = rgm.role_id
            WHERE rgm.group_id = %s AND r.status = 'Active'
            ORDER BY r.role_name
            ''', (group_id,))
            
            roles = cursor.fetchall()
            conn.close()
            
            return [{'id': role[0], 'role_name': role[1]} for role in roles]
            
        except Exception as e:
            print(f"Error loading roles for group: {str(e)}")
            return []
    
    def update_role_group(self, group_id, group_name, description, role_names, status='Active'):
        """Update an existing role group"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Update role group basic info
            cursor.execute('''
            UPDATE role_groups 
            SET group_name = %s, description = %s, status = %s, updated_date = %s
            WHERE id = %s
            ''', (group_name, description, status, datetime.now().isoformat(), group_id))
            
            # Delete existing role mappings
            cursor.execute('DELETE FROM role_group_mappings WHERE group_id = %s', (group_id,))
            
            # Add new role mappings
            if role_names:
                # Get role IDs for selected role names
                for role_name in role_names:
                    cursor.execute('SELECT id FROM roles WHERE role_name = %s AND status = %s', (role_name, 'Active'))
                    role_result = cursor.fetchone()
                    if role_result:
                        role_id = role_result[0]
                        cursor.execute('''
                        INSERT INTO role_group_mappings (role_id, group_id)
                        VALUES (%s, %s)
                        ''', (role_id, group_id))
            
            conn.commit()
            conn.close()
            return True, f"Role group '{group_name}' updated successfully"
            
        except Exception as e:
            return False, f"Error updating role group: {str(e)}"
    
    def delete_role_group(self, group_id, permanent=False):
        """Delete a role group (soft delete by default, permanent if specified)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get group name for confirmation message
            cursor.execute("SELECT group_name FROM role_groups WHERE id = %s", (group_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return False, "Role group not found"
            
            group_name = result[0]
            
            group_name = result[0]
            
            if permanent:
                # Permanent delete: remove all related records
                cursor.execute('DELETE FROM role_group_permissions WHERE group_id = %s', (group_id,))
                cursor.execute('DELETE FROM role_group_mappings WHERE group_id = %s', (group_id,))
                cursor.execute('DELETE FROM role_groups WHERE id = %s', (group_id,))
                message = f"Role group '{group_name}' permanently deleted"
            else:
                # Soft delete by setting status to Inactive
                cursor.execute('''
                UPDATE role_groups 
                SET status = 'Inactive', updated_date = %s
                WHERE id = %s
                ''', (datetime.now().isoformat(), group_id))
                message = f"Role group '{group_name}' deactivated successfully"
            
            conn.commit()
            conn.close()
            return True, message
            
        except Exception as e:
            return False, f"Error deleting role group: {str(e)}"
    
    def permanently_delete_inactive_groups(self):
        """Permanently delete all inactive role groups from the system"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get inactive groups first
            cursor.execute("SELECT id, group_name FROM role_groups WHERE status = 'Inactive'")
            inactive_groups = cursor.fetchall()
            
            if not inactive_groups:
                conn.close()
                return True, "No inactive role groups found to delete"
            
            deleted_count = 0
            deleted_names = []
            
            for group_id, group_name in inactive_groups:
                # Delete related permissions and mappings
                cursor.execute('DELETE FROM role_group_permissions WHERE group_id = %s', (group_id,))
                cursor.execute('DELETE FROM role_group_mappings WHERE group_id = %s', (group_id,))
                cursor.execute('DELETE FROM role_groups WHERE id = %s', (group_id,))
                deleted_count += 1
                deleted_names.append(group_name)
            
            conn.commit()
            conn.close()
            
            message = f"Permanently deleted {deleted_count} inactive role groups: {', '.join(deleted_names)}"
            return True, message
            
        except Exception as e:
            return False, f"Error permanently deleting inactive groups: {str(e)}"
    
    def get_users_for_mapping(self):
        """Get active users from users table for role mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT id, username, email, profile, status
            FROM users 
            WHERE status = 'Active'
            ORDER BY username
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading users for mapping: {str(e)}")
            return pd.DataFrame()
    
    def get_user_assigned_roles(self, user_email):
        """Get all roles assigned to a specific user"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT r.id, r.role_name, ur.assigned_date, ur.status
            FROM user_roles ur
            JOIN users u ON ur.user_id = u.id
            JOIN roles r ON ur.role_id = r.id
            WHERE u.email = %s AND ur.status = 'Active'
            ORDER BY r.role_name
            '''
            df = pd.read_sql_query(query, conn, params=[user_email])
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user roles: {str(e)}")
            return pd.DataFrame()

    def get_active_role_groups_for_dropdown(self):
        """Get active role groups for dropdown selection"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT id, group_name 
            FROM role_groups 
            WHERE status = 'Active'
            ORDER BY group_name
            '''
            cursor = conn.cursor()
            cursor.execute(query)
            role_groups = cursor.fetchall()
            conn.close()
            return role_groups
        except Exception as e:
            print(f"Error loading active role groups: {str(e)}")
            return []
    
    def create_user_role_mapping(self, user_email, role_id, assigned_by="system"):
        """Create user-role mapping using standardized user_roles table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Ensure user_roles table exists
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_roles (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                role_id INTEGER REFERENCES roles(id) ON DELETE CASCADE,
                assigned_by VARCHAR(255),
                assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'Active',
                UNIQUE(user_id, role_id)
            )
            ''')
            
            # Get user ID from email
            cursor.execute("SELECT id FROM users WHERE email = %s AND status = 'Active'", (user_email,))
            user_result = cursor.fetchone()
            if not user_result:
                conn.close()
                return False, f"User with email '{user_email}' not found or inactive"
            
            user_id = user_result[0]
            
            # Check if mapping already exists
            cursor.execute('''
            SELECT id FROM user_roles 
            WHERE user_id = %s AND role_id = %s AND status = 'Active'
            ''', (user_id, role_id))
            
            existing_mapping = cursor.fetchone()
            if existing_mapping:
                conn.close()
                return False, "User-role mapping already exists"
            
            # Create new mapping
            cursor.execute('''
            INSERT INTO user_roles (user_id, role_id, assigned_by, status)
            VALUES (%s, %s, %s, 'Active')
            ''', (user_id, role_id, assigned_by))
            
            conn.commit()
            conn.close()
            return True, f"User role mapping created successfully"
            
        except Exception as e:
            return False, f"Error creating user role mapping: {str(e)}"
    
    def get_all_user_role_mappings(self):
        """Get all active user role mappings from standardized user_roles table"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT 
                ur.id,
                u.username,
                u.email,
                r.role_name,
                ur.assigned_by,
                ur.assigned_date,
                ur.status
            FROM user_roles ur
            JOIN users u ON ur.user_id = u.id
            JOIN roles r ON ur.role_id = r.id
            WHERE ur.status = 'Active' AND u.status = 'Active'
            ORDER BY u.username
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user role mappings: {str(e)}")
            return pd.DataFrame()
    
    def delete_user_role_mapping(self, mapping_id):
        """Delete user role mapping from standardized user_roles table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get user and role info for confirmation
            cursor.execute('''
            SELECT u.username, r.role_name 
            FROM user_roles ur
            JOIN users u ON ur.user_id = u.id
            JOIN roles r ON ur.role_id = r.id
            WHERE ur.id = %s
            ''', (mapping_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return False, "Mapping not found"
            
            username, role_name = result
            
            # Soft delete by setting status to Inactive
            cursor.execute('''
            UPDATE user_roles 
            SET status = 'Inactive'
            WHERE id = %s
            ''', (mapping_id,))
            
            conn.commit()
            conn.close()
            return True, f"User role mapping for '{username}' ({role_name}) deleted successfully"
            
        except Exception as e:
            return False, f"Error deleting user role mapping: {str(e)}"
    
    def update_user_role_mapping(self, mapping_id, user_email, role_id, assigned_by="system"):
        """Update user role mapping in standardized user_roles table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if mapping exists
            cursor.execute("SELECT id FROM user_roles WHERE id = %s", (mapping_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return False, "Mapping not found"
            
            # Get user ID from email
            cursor.execute("SELECT id FROM users WHERE email = %s AND status = 'Active'", (user_email,))
            user_result = cursor.fetchone()
            if not user_result:
                conn.close()
                return False, f"User with email '{user_email}' not found or inactive"
            
            user_id = user_result[0]
            
            # Check for duplicate mapping (excluding current one)
            cursor.execute('''
            SELECT id FROM user_roles 
            WHERE user_id = %s AND role_id = %s AND status = 'Active' AND id != %s
            ''', (user_id, role_id, mapping_id))
            
            if cursor.fetchone():
                conn.close()
                return False, "Another mapping with this user-role combination already exists"
            
            # Update the mapping
            cursor.execute('''
            UPDATE user_roles 
            SET user_id = %s, role_id = %s, assigned_by = %s
            WHERE id = %s
            ''', (user_id, role_id, assigned_by, mapping_id))
            
            conn.commit()
            conn.close()
            return True, f"User role mapping updated successfully"
            
        except Exception as e:
            return False, f"Error updating user role mapping: {str(e)}"
    
    def update_role_group_with_permissions(self, group_id, group_name, group_description, role_ids, status, permissions_df):
        """Update role group with permissions"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Update role group basic info
            cursor.execute('''
            UPDATE role_groups 
            SET group_name = %s, group_description = %s, status = %s
            WHERE id = %s
            ''', (group_name, group_description, status, group_id))
            
            # Clear existing role mappings
            cursor.execute('DELETE FROM role_group_mappings WHERE group_id = %s', (group_id,))
            
            # Add new role mappings
            for role_id in role_ids:
                cursor.execute('''
                INSERT INTO role_group_mappings (group_id, role_id)
                VALUES (%s, %s)
                ''', (group_id, role_id))
            
            # Clear existing permissions
            cursor.execute('DELETE FROM role_group_permissions WHERE group_id = %s', (group_id,))
            
            # Add new permissions
            for _, row in permissions_df.iterrows():
                cursor.execute('''
                INSERT INTO role_group_permissions (group_id, module_name, sub_page, can_add, can_edit, can_delete, can_view)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (group_id, row['Module'], row['Sub-Page'], row['Add'], row['Edit'], row['Delete'], row['View']))
            
            conn.commit()
            conn.close()
            return True, f"Role group '{group_name}' updated successfully with permissions"
            
        except Exception as e:
            return False, f"Error updating role group with permissions: {str(e)}"
    
    def get_user_permissions(self, user_name):
        """Get user permissions based on their role group mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT 
                rgp.module_name,
                rgp.sub_page,
                rgp.can_add,
                rgp.can_edit,
                rgp.can_delete,
                rgp.can_view
            FROM user_role_mappings urm
            JOIN role_groups rg ON urm.role_group_id = rg.id
            JOIN role_group_permissions rgp ON rg.id = rgp.group_id
            WHERE urm.user_name = %s AND urm.status = 'Active' AND rg.status = 'Active'
            '''
            df = pd.read_sql_query(query, conn, params=(user_name,))
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user permissions: {str(e)}")
            return pd.DataFrame()
    
    def assign_user_to_role_group(self, user_identifier, group_name):
        """Assign user to a role group using email or username"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get the user's username - check if identifier is email or username
            if '@' in user_identifier:
                # It's an email, get username
                cursor.execute("SELECT username FROM users WHERE email = %s", (user_identifier,))
                user_result = cursor.fetchone()
                if not user_result:
                    conn.close()
                    return False, f"User with email '{user_identifier}' not found"
                username = user_result[0]
            else:
                # It's already a username
                username = user_identifier
            
            # Get role group ID from name
            cursor.execute("SELECT id FROM role_groups WHERE group_name = %s", (group_name,))
            group_result = cursor.fetchone()
            if not group_result:
                conn.close()
                return False, f"Role group '{group_name}' not found"
            role_group_id = group_result[0]
            
            # Check if mapping already exists
            cursor.execute("""
                SELECT id FROM user_role_mappings 
                WHERE user_name = %s AND role_group_id = %s
            """, (username, role_group_id))
            
            if cursor.fetchone():
                conn.close()
                return False, f"User '{username}' is already assigned to role group '{group_name}'"
            
            # Create the mapping
            cursor.execute("""
                INSERT INTO user_role_mappings (user_name, role_group_id, status, created_date)
                VALUES (%s, %s, 'Active', %s)
            """, (username, role_group_id, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            return True, f"User '{username}' assigned to role group '{group_name}' successfully"
            
        except Exception as e:
            return False, f"Error assigning user to role group: {str(e)}"
    
    def get_all_user_role_mappings(self):
        """Get all user-role mappings with user and group details"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT urm.id as mapping_id, urm.user_name, u.email, rg.group_name, 
                   urm.status as mapping_status, urm.created_date
            FROM user_role_mappings urm
            LEFT JOIN users u ON urm.user_name = u.username
            LEFT JOIN role_groups rg ON urm.role_group_id = rg.id
            ORDER BY urm.created_date DESC
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user-role mappings: {str(e)}")
            return pd.DataFrame()
    
    def update_user_role_mapping(self, mapping_id, new_user_id, new_role_group_id, status, team=None):
        """Update user-role mapping with proper transaction management"""
        conn = None
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Begin transaction
            cursor.execute("BEGIN")
            
            # Convert numpy types to native Python types
            mapping_id = int(mapping_id)
            new_user_id = int(new_user_id)
            new_role_group_id = int(new_role_group_id)
            status = str(status)
            team = str(team) if team else None
            
            # Get the original mapping info for rollback if needed
            cursor.execute("SELECT user_name FROM user_role_mappings WHERE id = %s", (mapping_id,))
            original_mapping = cursor.fetchone()
            if not original_mapping:
                cursor.execute("ROLLBACK")
                conn.close()
                return False, "Original mapping not found"
            
            original_username = original_mapping[0]
            
            # Get the new user's username and email
            cursor.execute("SELECT username, email FROM users WHERE id = %s", (new_user_id,))
            user_result = cursor.fetchone()
            if not user_result:
                cursor.execute("ROLLBACK")
                conn.close()
                return False, "User not found"
            username, user_email = user_result
            
            # Validate role group exists
            cursor.execute("SELECT group_name FROM role_groups WHERE id = %s AND status = 'Active'", (new_role_group_id,))
            role_result = cursor.fetchone()
            if not role_result:
                cursor.execute("ROLLBACK")
                conn.close()
                return False, "Role group not found or inactive"
            
            # Update the mapping
            cursor.execute("""
                UPDATE user_role_mappings 
                SET user_name = %s, role_group_id = %s, status = %s, team = %s
                WHERE id = %s
            """, (username, new_role_group_id, status, team, mapping_id))
            
            # Verify the update was successful
            if cursor.rowcount == 0:
                cursor.execute("ROLLBACK")
                conn.close()
                return False, "Failed to update mapping - no rows affected"
            
            # Commit transaction
            cursor.execute("COMMIT")
            conn.close()
            
            # Clear permission cache for affected users to force refresh
            self._clear_user_permission_cache(original_username, user_email)
            
            return True, f"User-role mapping updated successfully for {username}"
            
        except Exception as e:
            # Rollback on any error
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute("ROLLBACK")
                    conn.close()
                except:
                    pass
            return False, f"Error updating user-role mapping: {str(e)}"
    
    def delete_user_role_mapping(self, mapping_id):
        """Delete user-role mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Convert numpy types to native Python types
            mapping_id = int(mapping_id)
            
            cursor.execute("DELETE FROM user_role_mappings WHERE id = %s", (mapping_id,))
            
            conn.commit()
            conn.close()
            return True, "User-role mapping deleted successfully"
            
        except Exception as e:
            return False, f"Error deleting user-role mapping: {str(e)}"
    
    def get_user_permissions_summary(self, username):
        """Get user's permissions summary through role groups"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT rgp.module_name, rgp.sub_page, rgp.can_add, rgp.can_edit, 
                   rgp.can_delete, rgp.can_view, rg.group_name
            FROM user_role_mappings urm
            JOIN role_groups rg ON urm.role_group_id = rg.id
            JOIN role_group_permissions rgp ON rg.id = rgp.group_id
            WHERE urm.user_name = %s AND urm.status = 'Active' AND rg.status = 'Active'
            ORDER BY rgp.module_name, rgp.sub_page
            '''
            df = pd.read_sql_query(query, conn, params=(username,))
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading user permissions summary: {str(e)}")
            return pd.DataFrame()
    
    def create_user_role_mapping(self, user_id, role_group_id, status, team=None):
        """Create a new user-role mapping"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Convert numpy types to native Python types
            user_id = int(user_id)
            role_group_id = int(role_group_id)
            status = str(status)
            team = str(team) if team else None
            
            # Get the user's username
            cursor.execute("SELECT username FROM users WHERE id = %s", (user_id,))
            user_result = cursor.fetchone()
            if not user_result:
                conn.close()
                return False, "User not found"
            username = user_result[0]
            
            # Check if mapping already exists
            cursor.execute("""
                SELECT id FROM user_role_mappings 
                WHERE user_name = %s AND role_group_id = %s
            """, (username, role_group_id))
            
            if cursor.fetchone():
                conn.close()
                return False, f"User '{username}' is already assigned to this role group"
            
            # Create the mapping
            cursor.execute("""
                INSERT INTO user_role_mappings (user_name, role_group_id, status, team, created_date)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (username, role_group_id, status, team))
            
            conn.commit()
            conn.close()
            return True, f"User-role mapping created successfully"
            
        except Exception as e:
            return False, f"Error creating user-role mapping: {str(e)}"
    
    def get_all_users(self):
        """Get all users for dropdown selection"""
        try:
            conn = psycopg2.connect(self.database_url)
            query = '''
            SELECT id, username as name, email, status
            FROM users
            ORDER BY username
            '''
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading users: {str(e)}")
            return pd.DataFrame()