"""
User Management System for Role-Based Access Control
"""

import psycopg2
import hashlib
import os
from datetime import datetime
import pandas as pd

class UserManager:
    """Manage user accounts, authentication, and role-based access control"""
    
    def __init__(self, env_manager=None):
        self.database_url = os.environ.get('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not found")
        
        # Environment management for table routing
        self.env_manager = env_manager
        if self.env_manager:
            self.table_name = self.env_manager.get_table_name('users')
        else:
            # Fallback to production tables if no environment manager
            self.table_name = 'users'
            
        self.create_users_table()
        self.create_default_admin()
    
    def create_users_table(self):
        """Create users table if it doesn't exist"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            profile VARCHAR(50) NOT NULL CHECK(profile IN ('admin', 'team_member')),
            status VARCHAR(20) DEFAULT 'Active' CHECK(status IN ('Active', 'Inactive')),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            created_by VARCHAR(255),
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Add status column if it doesn't exist (for existing databases)
        cursor.execute(f'''
        DO $$ 
        BEGIN
            BEGIN
                ALTER TABLE {self.table_name} ADD COLUMN status VARCHAR(20) DEFAULT 'Active' CHECK(status IN ('Active', 'Inactive'));
            EXCEPTION
                WHEN duplicate_column THEN 
                    RAISE NOTICE 'column status already exists in {self.table_name}.';
            END;
        END;
        $$
        ''')
        
        conn.commit()
        conn.close()
    
    def create_default_admin(self):
        """Create default admin user if no users exist"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        # Check if any users exist
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        user_count = cursor.fetchone()[0]
        
        if user_count == 0:
            # Create default admin (OAuth-compatible, no password)
            default_admin = {
                'username': 'preethi.madhu',
                'email': 'preethi.madhu@greyamp.com',
                'password_hash': 'oauth_user',  # Special marker for OAuth users
                'profile': 'admin'
            }
            
            cursor.execute(f'''
            INSERT INTO {self.table_name} (username, email, password_hash, profile, created_by)
            VALUES (%s, %s, %s, %s, %s)
            ''', (
                default_admin['username'],
                default_admin['email'],
                default_admin['password_hash'],
                default_admin['profile'],
                'system'
            ))
            
            conn.commit()
        
        conn.close()
    
    def hash_password(self, password):
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password(self, password, password_hash):
        """Verify password against hash"""
        return self.hash_password(password) == password_hash
    
    def add_user(self, username, email, password, profile, created_by_email, team=None):
        """Add new user to the system"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if user already exists
            cursor.execute(f"SELECT id FROM {self.table_name} WHERE username = %s OR email = %s", (username, email))
            if cursor.fetchone():
                return False, "User with this username or email already exists"
            
            password_hash = self.hash_password(password)
            
            cursor.execute(f'''
            INSERT INTO {self.table_name} (username, email, password_hash, profile, created_by, team)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''', (username, email, password_hash, profile, created_by_email, team))
            
            conn.commit()
            conn.close()
            return True, "User created successfully"
            
        except Exception as e:
            return False, f"Error creating user: {str(e)}"
    
    def update_user(self, user_id, username=None, email=None, password=None, profile=None, team=None, updated_by_email=None):
        """Update existing user"""
        conn = None
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Build dynamic update query
            update_fields = []
            values = []
            
            # Check if username is provided and not empty
            if username is not None and username.strip():
                # Check for duplicate username (excluding current user)
                cursor.execute("SELECT id FROM users WHERE username = %s AND id != %s", (username, user_id))
                if cursor.fetchone():
                    conn.close()
                    return False, "Username already exists"
                update_fields.append("username = %s")
                values.append(username)
            
            # Check if email is provided and not empty
            if email is not None and email.strip():
                # Check for duplicate email (excluding current user)
                cursor.execute("SELECT id FROM users WHERE email = %s AND id != %s", (email, user_id))
                if cursor.fetchone():
                    conn.close()
                    return False, "Email already exists"
                update_fields.append("email = %s")
                values.append(email)
                
            # Check if password is provided and not empty
            if password is not None and password.strip():
                if len(password) < 6:
                    conn.close()
                    return False, "Password must be at least 6 characters long"
                update_fields.append("password_hash = %s")
                values.append(self.hash_password(password))
                
            # Profile can be updated even if it's the same value
            if profile is not None:
                update_fields.append("profile = %s")
                values.append(profile)
            
            # Team can be updated including empty values
            if team is not None:
                update_fields.append("team = %s")
                values.append(team if team.strip() else None)
            
            # Only proceed if there are fields to update
            if not update_fields:
                conn.close()
                return False, "No valid fields provided for update"
            
            # Always update the timestamp
            update_fields.append("updated_date = %s")
            values.append(datetime.now().isoformat())
            
            values.append(int(user_id))  # Ensure user_id is integer
            
            query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
            
            # Debug: Check if user exists before update
            cursor.execute("SELECT id FROM users WHERE id = %s", (int(user_id),))
            user_exists = cursor.fetchone()
            
            if not user_exists:
                conn.close()
                return False, f"User with ID {user_id} not found in database"
            
            cursor.execute(query, values)
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                return True, "User updated successfully"
            else:
                conn.close()
                return False, f"Update failed for user ID {user_id}"
                
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return False, f"Error updating user: {str(e)}"
    
    def delete_user(self, user_id):
        """Soft delete user (set inactive)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("UPDATE users SET is_active = FALSE WHERE id = %s", (user_id,))
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                return True, "User deactivated successfully"
            else:
                conn.close()
                return False, "User not found"
                
        except Exception as e:
            return False, f"Error deactivating user: {str(e)}"
    
    def get_all_users(self):
        """Get all users with status"""
        conn = psycopg2.connect(self.database_url)
        
        query = '''
        SELECT id, username, email, profile, status, created_date, last_login, created_by, team
        FROM users 
        ORDER BY created_date DESC
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_user_by_id(self, user_id):
        """Get user by ID"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, username, email, profile, created_date, last_login, is_active
        FROM users WHERE id = %s
        ''', (user_id,))
        
        user = cursor.fetchone()
        conn.close()
        
        if user:
            return {
                'id': user[0],
                'username': user[1],
                'email': user[2],
                'profile': user[3],
                'created_date': user[4],
                'last_login': user[5],
                'is_active': user[6]
            }
        return None
    
    def authenticate_user(self, username_or_email, password):
        """Authenticate user and update last login - only allow Active users"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, username, email, password_hash, profile, status, is_active
        FROM users 
        WHERE (username = %s OR email = %s) AND is_active = TRUE AND status = 'Active'
        ''', (username_or_email, username_or_email))
        
        user = cursor.fetchone()
        
        if user and self.verify_password(password, user[3]):
            # Update last login
            cursor.execute("UPDATE users SET last_login = %s WHERE id = %s", 
                         (datetime.now().isoformat(), user[0]))
            conn.commit()
            conn.close()
            
            return {
                'id': user[0],
                'username': user[1],
                'email': user[2],
                'profile': user[4],
                'status': user[5],
                'is_active': user[6]
            }
        
        conn.close()
        return None
    
    def get_user_stats(self):
        """Get user statistics"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        stats = {}
        
        # Total users
        cursor.execute("SELECT COUNT(*) FROM users")
        stats['total_users'] = cursor.fetchone()[0]
        
        # Active users
        cursor.execute("SELECT COUNT(*) FROM users WHERE status = 'Active'")
        stats['active_users'] = cursor.fetchone()[0]
        
        # Inactive users
        cursor.execute("SELECT COUNT(*) FROM users WHERE status = 'Inactive'")
        stats['inactive_users'] = cursor.fetchone()[0]
        
        # Users by status (for backward compatibility)
        cursor.execute("SELECT status, COUNT(*) FROM users GROUP BY status")
        status_counts = cursor.fetchall()
        stats['by_status'] = {status: count for status, count in status_counts}
        
        # Recent users (last 30 days)
        cursor.execute('''
        SELECT COUNT(*) FROM users 
        WHERE status = 'Active' AND created_date > NOW() - INTERVAL '30 days'
        ''')
        stats['recent_users'] = cursor.fetchone()[0]
        
        conn.close()
        return stats
    
    def get_or_create_oauth_user(self, email, name):
        """Get or create OAuth user and check if they're active"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Check if user exists
            cursor.execute('''
            SELECT id, username, email, profile, status, is_active 
            FROM users 
            WHERE email = %s
            ''', (email,))
            
            user = cursor.fetchone()
            
            if user:
                # Check if user is active
                if user[4] != 'Active' or not user[5]:  # status and is_active check
                    conn.close()
                    return None, "User account is inactive. Please contact administrator."
                
                conn.close()
                return {
                    'id': user[0],
                    'username': user[1],
                    'email': user[2],
                    'profile': user[3],
                    'status': user[4],
                    'is_active': user[5]
                }, "Existing user authenticated"
            else:
                # Create new OAuth user with team_member role
                username = email.split('@')[0]  # Use email prefix as username
                
                cursor.execute('''
                INSERT INTO users (username, email, password_hash, profile, status, created_by)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                ''', (username, email, 'oauth_user', 'team_member', 'Active', 'oauth_system'))
                
                user_id = cursor.fetchone()[0]
                conn.commit()
                conn.close()
                
                return {
                    'id': user_id,
                    'username': username,
                    'email': email,
                    'profile': 'team_member',
                    'status': 'Active',
                    'is_active': True
                }, "New user created via OAuth"
                
        except Exception as e:
            return None, f"Error in OAuth user management: {str(e)}"
    
    def update_oauth_user_login(self, email):
        """Update last login for OAuth user"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE users 
            SET last_login = %s 
            WHERE email = %s AND status = 'Active'
            ''', (datetime.now().isoformat(), email))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            return False
    
    def update_user_status(self, user_id, status, updated_by_email):
        """Update user status (Active/Inactive)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE users 
            SET status = %s, updated_date = %s 
            WHERE id = %s
            ''', (status, datetime.now().isoformat(), user_id))
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                return True, f"User status updated to {status} successfully"
            else:
                conn.close()
                return False, "User not found"
                
        except Exception as e:
            return False, f"Error updating user status: {str(e)}"
    
    def search_users(self, search_term="", role_filter="", status_filter="", page=1, per_page=20):
        """Search users with pagination"""
        try:
            conn = psycopg2.connect(self.database_url)
            
            # Build search query
            where_conditions = []
            params = []
            
            if search_term:
                where_conditions.append("(username ILIKE %s OR email ILIKE %s)")
                params.extend([f"%{search_term}%", f"%{search_term}%"])
            
            if role_filter and role_filter != "All":
                where_conditions.append("profile = %s")
                params.append(role_filter)
            
            if status_filter and status_filter != "All":
                where_conditions.append("status = %s")
                params.append(status_filter)
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM users {where_clause}"
            total_df = pd.read_sql_query(count_query, conn, params=params)
            total_count = int(total_df.iloc[0, 0])
            
            # Get paginated data
            offset = (page - 1) * per_page
            query = f'''
            SELECT id, username, email, profile, status, created_date, last_login, created_by
            FROM users 
            {where_clause}
            ORDER BY created_date DESC
            LIMIT %s OFFSET %s
            '''
            
            params.extend([per_page, offset])
            df = pd.read_sql_query(query, conn, params=params)
            
            conn.close()
            
            total_pages = max(1, (total_count + per_page - 1) // per_page)
            
            return df, total_count, total_pages
            
        except Exception as e:
            return pd.DataFrame(), 0, 1
    
    def is_admin(self, user_email):
        """Check if user is admin and active"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        cursor.execute("SELECT profile FROM users WHERE email = %s AND status = 'Active' AND is_active = TRUE", (user_email,))
        result = cursor.fetchone()
        conn.close()
        
        return result and result[0] == 'admin'
    
    def update_user_role(self, user_id, new_role):
        """Update user role/profile"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Validate role
            valid_roles = ['admin', 'team_member']
            if new_role not in valid_roles:
                return False, f"Invalid role. Must be one of: {valid_roles}"
            
            cursor.execute('''
            UPDATE users 
            SET profile = %s, updated_date = %s 
            WHERE id = %s
            ''', (new_role, datetime.now().isoformat(), user_id))
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                return True, f"User role updated to {new_role} successfully"
            else:
                conn.close()
                return False, "User not found"
                
        except Exception as e:
            return False, f"Error updating user role: {str(e)}"