"""
OAuth2 Google Authentication with Replit integration
Restricts access to greyamp.com domain users only
"""
import streamlit as st
import os
import logging
import psycopg2
import hashlib
import time
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import secrets
from urllib.parse import urlencode
from utils.permission_manager import PermissionManager

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def is_production_environment():
    """Check if running in production environment"""
    import os
    try:
        repl_domains = os.getenv("REPLIT_DOMAINS", "")
        # Check for actual production domain (not dev environments)
        return "replit.app" in repl_domains and "janeway.replit.dev" not in repl_domains
    except:
        return False

def has_ssl_issues():
    """Check if SSL certificate issues are present - environment based"""
    import os
    replit_domains = os.getenv("REPLIT_DOMAINS", "")
    
    # Use fallback authentication in development environments
    is_dev_env = "janeway.replit.dev" in replit_domains or "replit.dev" in replit_domains
    
    if is_dev_env:
        logger.info("Development environment detected - using fallback authentication")
        return True
    
    # Use Google OAuth in production
    return not is_production_environment()

class FallbackAuth:
    """Fallback authentication when OAuth has SSL issues"""
    
    def __init__(self):
        self.allowed_emails = [
            "preethi.madhu@greyamp.com",
            "anna.pauly@greyamp.com",
            "team@greyamp.com",
            "amaan.iqbal@greyamp.com",
            "likithashree.hm@greyamp.com"
        ]
    
    def show_fallback_login(self):
        """Show fallback login form for development environment"""
        
        # Center the login form
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            st.markdown("""
            <div style='text-align: center; padding: 3rem 2rem; margin: 2rem 0; 
                        border: 1px solid #e0e0e0; border-radius: 15px; 
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        color: white; box-shadow: 0 10px 25px rgba(0,0,0,0.1);'>
                <h1 style='margin-bottom: 0.5rem; font-size: 2.5rem;'>🏢</h1>
                <h1 style='margin: 0; font-size: 2rem;'>GA AlignOps</h1>
                <h3 style='margin: 1rem 0; font-weight: 300;'>Development Access Portal</h3>
                <p style='margin: 0; opacity: 0.9;'>Enterprise-grade demand forecasting and supply management</p>
            </div>
            """, unsafe_allow_html=True)

            # Clean environment notice
            st.info("🔐 **Secure Access Portal**: Enter your greyamp.com credentials to continue.")
            


            st.markdown("""
            <div style='text-align: center; margin: 2rem 0;'>
                <p style='font-size: 1.1rem; margin-bottom: 1.5rem; color: #333;'>
                    🔐 <strong>Secure Access Required</strong><br>
                    <span style='color: #666;'>Enter your greyamp.com email and access code</span>
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("login_form"):
                email = st.text_input(
                    "Email Address",
                    placeholder="your.email@greyamp.com",
                    key="fallback_email"
                )
                
                password = st.text_input(
                    "Access Code",
                    type="password",
                    placeholder="Enter access code",
                    key="fallback_password"
                )
                
                submitted = st.form_submit_button("🚀 Access Application", use_container_width=True)
            

                
            if submitted:
                if email and email in self.allowed_emails and password == "greyamp2025":
                    try:
                        # Set session state for authenticated user
                        st.session_state.authenticated = True
                        st.session_state.user_email = email
                        st.session_state.username = email.split('@')[0].replace('.', ' ').title()
                        st.session_state.user_info = {
                            'email': email,
                            'name': email.split('@')[0].replace('.', ' ').title(),
                            'picture': 'https://via.placeholder.com/40'
                        }
                        
                        # Update login time in database
                        try:
                            from utils.user_manager import UserManager
                            user_manager = UserManager()
                            user_manager.update_oauth_user_login(email)
                        except Exception as login_error:
                            logger.warning(f"Login time update failed for {email}: {str(login_error)}")
                        
                        # Load user permissions with error handling
                        try:
                            load_user_permissions(email)
                        except Exception as perm_error:
                            logger.warning(f"Permission loading failed for {email}: {str(perm_error)}")
                            # Set default permissions to allow access
                            st.session_state.user_permissions = {}
                            st.session_state.accessible_modules = ['Demand Planning', 'Supply Planning', 'Settings']
                            st.session_state.user_info['role'] = 'Team Member'
                        
                        st.success("✅ Authentication successful! Redirecting...")
                        # Add a flag to prevent infinite rerun loop
                        st.session_state.auth_just_completed = True
                        # Force page refresh to show main application
                        st.rerun()
                    except Exception as auth_error:
                        st.error("❌ Authentication failed. Please try again.")
                else:
                    st.error("❌ Invalid credentials. Please contact your administrator.")
            
            st.markdown("---")
            st.caption("🏢 Restricted to greyamp.com domain users only")
            st.caption("🔒 Contact IT support if you need access")

class ReplitGoogleAuth:
    def __init__(self):
        self.client_id = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")
        self.client_secret = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")
        self.allowed_domain = "greyamp.com"
        
        # Debug logging for OAuth configuration
        logger.info(f"OAuth Client ID configured: {'Yes' if self.client_id else 'No'}")
        logger.info(f"OAuth Client Secret configured: {'Yes' if self.client_secret else 'No'}")

        repl_slug = os.getenv("REPL_SLUG", "")
        repl_owner = os.getenv("REPL_OWNER", "")
        replit_domains = os.getenv("REPLIT_DOMAINS", "")

        # Enhanced environment detection and URI configuration
        logger.info(f"Environment detection - REPLIT_DOMAINS: {replit_domains}")
        logger.info(f"REPL_SLUG: {repl_slug}, REPL_OWNER: {repl_owner}")
        
        # Check for manual override first
        manual_override = os.getenv("OAUTH_REDIRECT_URI_OVERRIDE")
        if manual_override:
            self.redirect_uri = manual_override
            logger.info(f"Using manual redirect URI override: {self.redirect_uri}")
            return
        
        # Check if we're in development environment
        is_dev_env = "janeway.replit.dev" in replit_domains or "replit.dev" in replit_domains
        
        if is_dev_env:
            # Development environment - OAuth won't work, will use fallback
            self.redirect_uri = "https://development-environment-oauth-disabled/"
            logger.warning(f"Development environment detected - OAuth will use fallback authentication")
            # Disable OAuth functionality in development
            self.client_id = ""
            self.client_secret = ""
        else:
            # Production environment - always use the exact configured URL
            self.redirect_uri = "https://demand-forecast-master-preemadhu.replit.app/"
            logger.info(f"Production OAuth redirect URI: {self.redirect_uri}")


        self.scopes = [
            'openid',
            'email',
            'profile'
        ]

        self.auth_url = "https://accounts.google.com/o/oauth2/auth"
        self.token_url = "https://oauth2.googleapis.com/token"
        self.userinfo_url = "https://www.googleapis.com/oauth2/v2/userinfo"

    def get_auth_url(self):
        state = secrets.token_urlsafe(32)
        
        # Store state in multiple locations for reliability
        st.session_state.oauth_state = state
        st.session_state['oauth_state_backup'] = state

        if not self.client_id:
            return None

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": " ".join(self.scopes),
            "state": state,
            "access_type": "offline",
            "hd": self.allowed_domain,
            "prompt": "select_account"
        }

        auth_url = f"{self.auth_url}?{urlencode(params)}"
        return auth_url

    def exchange_code_for_token(self, code, state):
        stored_state = st.session_state.get('oauth_state')
        backup_state = st.session_state.get('oauth_state_backup')
        
        # Check primary state first, then backup
        valid_state = False
        if stored_state and state == stored_state:
            valid_state = True
        elif backup_state and state == backup_state:
            valid_state = True
        
        # Clear stored states after use
        if 'oauth_state' in st.session_state:
            del st.session_state['oauth_state']
        if 'oauth_state_backup' in st.session_state:
            del st.session_state['oauth_state_backup']

        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "redirect_uri": self.redirect_uri
        }

        # Enhanced debugging for OAuth token exchange
        logger.info(f"Token exchange attempt with redirect_uri: {self.redirect_uri}")
        logger.info(f"Client ID (first 10 chars): {self.client_id[:10]}...")
        logger.info(f"State validation successful: {valid_state}")

        # Add SSL verification settings for requests
        response = requests.post(self.token_url, data=data, verify=True, timeout=30)
        
        # Enhanced error logging
        if response.status_code == 200:
            logger.info("Token exchange successful")
            return response.json()
        else:
            logger.error(f"Token exchange failed - Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            logger.error(f"Request data (client_secret hidden): grant_type={data['grant_type']}, client_id={data['client_id'][:10]}..., redirect_uri={data['redirect_uri']}")
            raise Exception(f"Token exchange failed: {response.text}")

    def get_user_info(self, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(self.userinfo_url, headers=headers, verify=True, timeout=30)

        if response.status_code == 200:
            user_info = response.json()
            email = user_info.get('email', '')
            if not email.endswith('@greyamp.com'):
                raise Exception("Access denied. Only greyamp.com email addresses are allowed.")

            return user_info
        else:
            raise Exception(f"Failed to get user info: {response.text}")

    def is_authenticated(self):
        return st.session_state.get('authenticated', False)

    def get_current_user(self):
        return st.session_state.get('user_info', {})

def handle_oauth_callback():
    """Handle the OAuth callback from Google"""
    query_params = st.query_params

    if 'code' in query_params and 'state' in query_params:
        google_auth = ReplitGoogleAuth()
        try:
            code = query_params['code']
            state = query_params['state']

            # Exchange code for token
            token_response = google_auth.exchange_code_for_token(code, state)
            access_token = token_response.get('access_token')

            # Get user info
            user_info = google_auth.get_user_info(access_token)
            
            # Create user in database if not exists
            create_oauth_user(user_info)

            # Store in session state
            st.session_state.authenticated = True
            st.session_state.user_info = user_info
            st.session_state.user_email = user_info.get('email')
            st.session_state.username = user_info.get('email').split('@')[0]

            # Update login time in database
            try:
                from utils.user_manager import UserManager
                user_manager = UserManager()
                user_manager.update_oauth_user_login(user_info.get('email'))
            except Exception as login_error:
                logger.warning(f"Login time update failed for {user_info.get('email')}: {str(login_error)}")

            # Load user permissions
            load_user_permissions(user_info.get('email'))

            # Clear query params and force page refresh
            st.query_params.clear()
            
            # Show success message and trigger page refresh
            st.success("✅ Authentication successful! Redirecting to application...")
            st.rerun()
            
            # Return True to indicate successful authentication
            return True

        except Exception as e:
            logger.error(f"OAuth authentication failed: {str(e)}")
            st.session_state.authenticated = False
            return False
    elif 'error' in query_params:
        error = query_params.get('error', 'Unknown error')
        logger.error(f"OAuth error: {error}")
        st.session_state.authenticated = False
        return False
    
    return False

def get_user_role_from_database(user_email):
    """Get user's role group from database using email-based lookup"""
    try:
        import psycopg2
        import os
        from utils.environment_manager import EnvironmentManager
        
        # Get environment-appropriate table names
        env_manager = EnvironmentManager()
        users_table = env_manager.get_table_name('users')
        user_role_mappings_table = env_manager.get_table_name('user_role_mappings')
        role_groups_table = env_manager.get_table_name('role_groups')
        
        # Connect to database
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cursor = conn.cursor()
        
        # Primary lookup: Join users table by email with user_role_mappings by username
        query = f'''
        SELECT rg.group_name 
        FROM {users_table} u
        JOIN {user_role_mappings_table} urm ON u.username = urm.user_name
        JOIN {role_groups_table} rg ON urm.role_group_id = rg.id
        WHERE u.email = %s AND urm.status = 'active' AND rg.status = 'Active'
        ORDER BY urm.created_date DESC
        LIMIT 1
        '''
        
        cursor.execute(query, (user_email,))
        result = cursor.fetchone()
        
        if result:
            conn.close()
            return result[0]
        
        # Fallback: Try username variants if email lookup fails
        username_variants = [
            user_email.split('@')[0],  # e.g., "amaan.iqbal"
            user_email,               # full email
            user_email.split('@')[0].replace('.', ' ').title()  # e.g., "Amaan Iqbal"
        ]
        
        for username in username_variants:
            fallback_query = '''
            SELECT rg.group_name 
            FROM user_role_mappings urm
            JOIN role_groups rg ON urm.role_group_id = rg.id
            WHERE urm.user_name = %s AND urm.status = 'active' AND rg.status = 'Active'
            ORDER BY urm.created_date DESC
            LIMIT 1
            '''
            
            cursor.execute(fallback_query, (username,))
            result = cursor.fetchone()
            
            if result:
                conn.close()
                return result[0]
        
        conn.close()
        return "User"  # Default fallback if no role found
            
    except Exception as e:
        logger.error(f"Error getting user role from database: {str(e)}")
        return "User"

def load_user_permissions(user_email):
    """Load user permissions into session state"""
    try:
        from utils.permission_manager import PermissionManager
        from utils.environment_manager import EnvironmentManager
        
        # Check if in development environment
        env_manager = EnvironmentManager()
        if env_manager.is_development():
            # In development, grant all permissions to avoid access issues
            logger.info("Development environment - granting full permissions")
            all_permissions = {
                "Demand Planning": True,
                "Target Setting": True,
                "Demand Tweaking": True,
                "Editable Plan View": True,
                "Supply Planning": True,
                "Talent Management": True,
                "Pipeline Configuration": True,
                "Staffing Plans": True,
                "Demand - Supply Mapping": True,
                "Insights & Reporting": True,
                "Settings": True,
                "User Management": True,
                "Roles & Role Groups": True
            }
            accessible_modules = list(all_permissions.keys())
            
            # Set development permissions
            st.session_state.user_permissions = all_permissions
            st.session_state.accessible_modules = accessible_modules
            
            # Create a mock permission manager for development
            class MockPermissionManager:
                def has_permission(self, *args, **kwargs): return True
                def get_user_permissions(self, email): return all_permissions
                def get_accessible_modules(self, email): return accessible_modules
                def protect_page_access(self, *args, **kwargs): return True
                def check_module_permission(self, *args, **kwargs): return True
                def can_access_module(self, *args, **kwargs): return True
                def permission_protected_button(self, user_email, module, sub_page, action_type, label, **kwargs): 
                    return st.button(label, **kwargs)
            
            st.session_state.permission_manager = MockPermissionManager()
            
            # Set user role for development
            if 'user_info' in st.session_state:
                st.session_state.user_info['role'] = 'Super Admin'
            
            return all_permissions
        else:
            # Production environment - use real permission manager
            permission_manager = PermissionManager()
            permissions = permission_manager.get_user_permissions(user_email)
            accessible_modules = permission_manager.get_accessible_modules(user_email)
            
            # Get user's role from database and add to user_info
            user_role = get_user_role_from_database(user_email)
            if 'user_info' in st.session_state:
                st.session_state.user_info['role'] = user_role
            
            # Store in session state
            st.session_state.user_permissions = permissions
            st.session_state.accessible_modules = accessible_modules
            st.session_state.permission_manager = permission_manager
            
            return permissions
    except Exception as e:
        logger.error(f"Error loading user permissions: {str(e)}")
        # Fallback to minimal permissions
        st.session_state.user_permissions = {"Demand Planning": True}
        st.session_state.accessible_modules = ["Demand Planning"]
        return {"Demand Planning": True}



def enable_development_mode():
    """Enable development mode with temporary user access"""
    email = 'preethi.madhu@greyamp.com'
    temp_user_info = {
        'email': email,
        'name': 'Development User',
        'given_name': 'Development',
        'family_name': 'User',
        'picture': None
    }
    
    # Create user in database if not exists
    create_oauth_user(temp_user_info)
    
    # Set session state
    st.session_state.authenticated = True
    st.session_state.user_info = temp_user_info
    st.session_state.user_email = email
    st.session_state.username = email.split('@')[0]
    
    # Update login time in database
    try:
        from utils.user_manager import UserManager
        user_manager = UserManager()
        user_manager.update_oauth_user_login(email)
    except Exception as login_error:
        logger.warning(f"Login time update failed for {email}: {str(login_error)}")

    # Load user permissions (this will also add the role to user_info)
    load_user_permissions(email)
    
    return True

def login_page():
    """Display the login page with fallback authentication due to SSL issues."""
    
    # Check if SSL issues are present - use fallback authentication
    if has_ssl_issues():
        try:
            fallback_auth = FallbackAuth()
            fallback_auth.show_fallback_login()
        except Exception as e:
            st.error(f"Authentication error: {e}")
        return
    
    # If we reach here, we should show Google OAuth
    
    # Original OAuth flow (kept for when SSL is fixed)
    google_auth = ReplitGoogleAuth()
    
    # Check for OAuth callback first
    query_params = st.query_params
    if 'code' in query_params:
        handle_oauth_callback()
        return
    
    auth_url = google_auth.get_auth_url()

    if not auth_url:
        st.error("Authentication service not configured. Please contact administrator.")
        return

    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("""
        <div style='text-align: center; padding: 3rem 2rem; margin: 2rem 0; 
                    border: 1px solid #e0e0e0; border-radius: 15px; 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    color: white; box-shadow: 0 10px 25px rgba(0,0,0,0.1);'>
            <h1 style='margin-bottom: 0.5rem; font-size: 2.5rem;'>🏢</h1>
            <h1 style='margin: 0; font-size: 2rem;'>GA AlignOps</h1>
            <h3 style='margin: 1rem 0; font-weight: 300;'>Production Access Portal</h3>
            <p style='margin: 0; opacity: 0.9;'>Enterprise-grade demand forecasting and supply management</p>
        </div>
        """, unsafe_allow_html=True)

        # Production Environment Notice
        st.success("🚀 **Production Environment**: Using secure Google OAuth authentication.")

        st.markdown("""
        <div style='text-align: center; margin: 2rem 0;'>
            <p style='font-size: 1.1rem; margin-bottom: 1.5rem; color: #333;'>
                🔐 <strong>Google Sign-In Required</strong><br>
                <span style='color: #666;'>Click below to authenticate with your greyamp.com account</span>
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Google Sign-In button with target="_self" to avoid popup issues
        st.markdown(f"""
        <div style='text-align: center;'>
            <a href='{auth_url}' target='_self' style='display: inline-block; padding: 0.75rem 1.5rem; 
                font-size: 1.2rem; color: white; background-color: #4285F4; 
                border-radius: 5px; text-decoration: none; box-shadow: 0 4px 6px rgba(0,0,0,0.1);'>
                🔑 Sign in with Google
            </a>
        </div>
        """, unsafe_allow_html=True)

        # Features section
        st.markdown("---")
        with st.expander("📋 Application Features"):
            st.markdown("""
            **🎯 Target Management**
            - Annual and quarterly target setting
            - Owner-specific target allocation
            - Dynamic balance tracking

            **📊 Demand Planning & Forecasting**
            - Advanced statistical models
            - Scenario planning and what-if analysis
            - Historical data analysis

            **📈 Sales Dashboard Analytics** 
            - Real-time performance metrics
            - Interactive visualizations
            - Regional and LoB insights

            **🔧 Data Management Tools**
            - CSV import/export functionality
            - Database persistence
            - Bulk data operations
            """)

        # Security notice
        st.markdown("---")
        st.info("🔒 **Security Notice**: This application requires valid Google credentials for greyamp.com domain.")

def user_header():
    """Display user information in the header after authentication."""
    user_info = st.session_state.get('user_info', {})
    if user_info:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**Welcome to GA AlignOps** - {user_info.get('name', 'User')}")
        with col2:
            if st.button("🚪 Logout", key="logout_button"):
                logout()

def logout():
    """Clear session state and logout user"""
    logger.info("User logging out")
    # Clear all session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

def require_auth():
    """Require authentication to access certain pages."""
    # First check if this is an OAuth callback
    query_params = st.query_params
    if 'code' in query_params:
        handle_oauth_callback()
        return

    # Check if user is authenticated
    if not check_auth():
        login_page()
        st.stop()

def get_user_profile(user_email):
    """Get user profile with role information from database using flexible matching"""
    try:
        database_url = os.getenv('DATABASE_URL', '')
        if not database_url:
            logger.error("DATABASE_URL not configured")
            return None

        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        # Get basic user info first
        cursor.execute("""
            SELECT username, email, profile, status
            FROM users
            WHERE email = %s AND status = 'Active'
        """, (user_email,))

        user_result = cursor.fetchone()
        
        if not user_result:
            conn.close()
            return None
        
        # Get role using flexible matching (same logic as get_user_role_from_database)
        username_variants = [
            user_email.split('@')[0],  # e.g., "amaan.iqbal"
            user_email,               # full email
            user_email.split('@')[0].replace('.', ' ').title()  # e.g., "Amaan Iqbal"
        ]
        
        role_name = None
        for username in username_variants:
            cursor.execute("""
                SELECT rg.group_name
                FROM user_role_mappings urm
                JOIN role_groups rg ON urm.role_group_id = rg.id
                WHERE urm.user_name = %s AND urm.status = 'active' AND rg.status = 'Active'
                ORDER BY urm.created_date DESC
                LIMIT 1
            """, (username,))
            
            role_result = cursor.fetchone()
            if role_result:
                role_name = role_result[0]
                break
        
        conn.close()

        return {
            'username': user_result[0],
            'email': user_result[1], 
            'profile': user_result[2],
            'status': user_result[3],
            'role': role_name if role_name else 'No Role Assigned'
        }

    except Exception as e:
        logger.error(f"Error getting user profile: {str(e)}")
        return None

def create_oauth_user(user_info):
    """Create user in database from OAuth info if not exists"""
    try:
        database_url = os.getenv('DATABASE_URL', '')
        if not database_url:
            logger.error("DATABASE_URL not configured")
            return
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        email = user_info.get('email', '')
        name = user_info.get('name', '')
        username = email.split('@')[0] if email else ''
        
        # Check if user exists
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cursor.fetchone():
            conn.close()
            return  # User already exists
        
        # Create new user
        cursor.execute("""
            INSERT INTO users (username, email, profile, status, created_at)
            VALUES (%s, %s, %s, 'Active', %s)
            ON CONFLICT (username) DO NOTHING
        """, (username, email, name, datetime.now()))
        
        conn.commit()
        conn.close()
        logger.info(f"Created new OAuth user: {email}")
        
    except Exception as e:
        logger.error(f"Error creating OAuth user: {str(e)}")

# Legacy authentication functions for backward compatibility
class SimpleAuth:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL', '')

    def check_credentials(self, username, password):
        """Check user credentials against database"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT password_hash, status, email 
                FROM users 
                WHERE username = %s
            """, (username,))

            result = cursor.fetchone()
            conn.close()

            if result and result[1] == 'Active':
                stored_hash = result[0]
                if self.verify_password(password, stored_hash):
                    return True, result[2]  # Return email for OAuth compatibility

            return False, None

        except Exception as e:
            logger.error(f"Error checking credentials: {str(e)}")
            return False, None

    def verify_password(self, password, stored_hash):
        """Verify password against stored hash"""
        return hashlib.sha256(password.encode()).hexdigest() == stored_hash

def check_auth():
    """Main authentication check function used by app.py"""
    logger.info("Starting authentication check")
    
    # Check if user is already authenticated (OAuth or fallback)
    if st.session_state.get('authenticated', False):
        logger.info("User authenticated successfully")
        return True
    
    logger.info("User not authenticated")
    return False

def check_admin_auth():
    """Legacy admin authentication check function"""
    logger.info("Starting admin authentication check")

    # Check if this is an OAuth callback
    query_params = st.experimental_get_query_params()
    if 'code' in query_params:
        handle_oauth_callback()
        return True

    # Check if user is already authenticated
    if st.session_state.get('authenticated', False):
        logger.info("User authenticated via admin system")
        return True

    logger.info("User not authenticated - admin login required")
    return False

if __name__ == "__main__":
    # Test the authentication system
    google_auth = ReplitGoogleAuth()
    if not google_auth.is_authenticated():
        login_page()
    else:
        user_info = google_auth.get_current_user()
        st.write(f"Welcome back, {user_info.get('name')}!")