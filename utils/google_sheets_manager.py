"""
Google Sheets Manager for Integrated Authentication
Handles Google Sheets API using existing authentication and data synchronization
"""

import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pytz
from typing import Optional, Tuple, Dict, Any
import re
import streamlit as st
import warnings
from .database_connection import get_database_config, get_database_connection
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_APIS_AVAILABLE = True
except ImportError:
    GOOGLE_APIS_AVAILABLE = False

logger = logging.getLogger(__name__)

class GoogleSheetsManager:
    """Manages Google Sheets integration with existing authentication"""
    
    def __init__(self):
        self.credentials = None
        self.service = None
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/userinfo.profile'
        ]
        
        # Default configuration - using provided spreadsheet
        self.default_config = {
            'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/1HKbqpMo5oGBNy-N-wmjWfruEVawadplTF2h4wrFCKg0/edit?gid=1206139251#gid=1206139251',
            'spreadsheet_id': '1HKbqpMo5oGBNy-N-wmjWfruEVawadplTF2h4wrFCKg0',
            'range_name': 'DataAggregator!A:Z',
            'table_name': 'DataAggregator'
        }
        
        if not GOOGLE_APIS_AVAILABLE:
            logger.error("Google API dependencies not available")
        
        # Load OAuth credentials from environment
        self._load_oauth_config()
        
        # Initialize with existing authentication if available
        self._initialize_service()
    
    def _load_oauth_config(self):
        """Load OAuth configuration from environment variables"""
        try:
            # Use individual OAuth credentials from secrets
            client_id = os.getenv('GOOGLE_OAUTH_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_OAUTH_CLIENT_SECRET')
            
            if client_id and client_secret:
                self.oauth_config = {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'redirect_uris': ['https://demand-forecast-master-preemadhu.replit.app/']
                }
                logger.info("OAuth configuration loaded from individual secrets")
            else:
                # Fallback to GOOGLE_OAUTH_SECRETS if available
                oauth_secrets = os.getenv('GOOGLE_OAUTH_SECRETS')
                if oauth_secrets:
                    raw_config = json.loads(oauth_secrets)
                    
                    # Handle different OAuth config formats
                    if 'web' in raw_config:
                        # Already in correct format with 'web' wrapper
                        self.oauth_config = raw_config['web']
                        logger.info("OAuth configuration loaded (web format)")
                    elif 'client_id' in raw_config:
                        # Raw format - use as is
                        self.oauth_config = raw_config
                        logger.info("OAuth configuration loaded (raw format)")
                    else:
                        # Unknown format
                        logger.error(f"Unexpected OAuth config format: {list(raw_config.keys())}")
                        self.oauth_config = None
                        
                    if self.oauth_config:
                        logger.info(f"OAuth config keys: {list(self.oauth_config.keys())}")
                else:
                    self.oauth_config = None
                    logger.warning("No OAuth configuration found in environment")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in GOOGLE_OAUTH_SECRETS: {e}")
            self.oauth_config = None
        except Exception as e:
            logger.error(f"Error loading OAuth configuration: {e}")
            self.oauth_config = None
    
    def _initialize_service(self):
        """Initialize Google Sheets service with automatic authentication setup"""
        try:
            if not GOOGLE_APIS_AVAILABLE or not self.oauth_config:
                logger.warning("Google APIs not available or OAuth config missing")
                return False
            
            # Prioritize OAuth2 web application flow over service account
            # First, try service account authentication only if type is explicitly service_account
            if 'type' in self.oauth_config and self.oauth_config['type'] == 'service_account':
                from google.oauth2.service_account import Credentials as ServiceCredentials
                self.credentials = ServiceCredentials.from_service_account_info(
                    self.oauth_config, scopes=self.scopes
                )
                self.service = build('sheets', 'v4', credentials=self.credentials)
                logger.info("Service account authentication successful")
                return True
            
            # Check if we already have stored credentials in session state
            if hasattr(st, 'session_state') and 'google_sheets_credentials' in st.session_state:
                cred_data = st.session_state.google_sheets_credentials
                
                # If it's just a flag that service account worked, recreate service
                if cred_data.get('type') == 'service_account':
                    from google.oauth2.service_account import Credentials as ServiceCredentials
                    self.credentials = ServiceCredentials.from_service_account_info(
                        self.oauth_config, scopes=self.scopes
                    )
                    self.service = build('sheets', 'v4', credentials=self.credentials)
                    return True
                
                # Check if OAuth credentials are valid
                if all(key in cred_data for key in ['token', 'client_id', 'client_secret']):
                    # Regular OAuth credentials
                    self.credentials = Credentials(
                        token=cred_data.get('token'),
                        refresh_token=cred_data.get('refresh_token'),
                        client_id=cred_data.get('client_id'),
                        client_secret=cred_data.get('client_secret'),
                        token_uri=cred_data.get('token_uri', 'https://oauth2.googleapis.com/token')
                    )
                    
                    # Refresh if needed
                    try:
                        if self.credentials.expired and self.credentials.refresh_token:
                            self.credentials.refresh(Request())
                            # Update session with refreshed token
                            st.session_state.google_sheets_credentials['token'] = self.credentials.token
                    except Exception as refresh_error:
                        logger.warning(f"Token refresh failed: {refresh_error}")
                        # Clear invalid credentials
                        if hasattr(st, 'session_state'):
                            if 'google_sheets_credentials' in st.session_state:
                                del st.session_state.google_sheets_credentials
                        return False
                    
                    self.service = build('sheets', 'v4', credentials=self.credentials)
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            return False
    
    def get_current_app_url(self) -> str:
        """Get the current application URL for OAuth redirect - clean URL without parameters"""
        # Use clean redirect URI for OAuth (no query parameters for token exchange)
        app_url = 'https://demand-forecast-master-preemadhu.replit.app/'
        logger.info(f"Using clean OAuth redirect URL: {app_url}")
        return app_url

    def authenticate_with_existing_flow(self):
        """Setup authentication using existing app authentication"""
        try:
            if not GOOGLE_APIS_AVAILABLE or not self.oauth_config:
                return False, "Google API dependencies not available or OAuth config missing"
            
            # Try service account authentication first
            if 'type' in self.oauth_config and self.oauth_config['type'] == 'service_account':
                from google.oauth2.service_account import Credentials as ServiceCredentials
                
                self.credentials = ServiceCredentials.from_service_account_info(
                    self.oauth_config, scopes=self.scopes
                )
                self.service = build('sheets', 'v4', credentials=self.credentials)
                
                # Store in session state for persistence
                if hasattr(st, 'session_state'):
                    st.session_state.google_sheets_credentials = {
                        'type': 'service_account',
                        'configured': True
                    }
                
                logger.info("Service account authentication successful")
                return True, "Service account authentication successful"
            
            # For OAuth2, generate direct Google Sheets OAuth URL
            else:
                # Generate direct OAuth URL for Google Sheets
                logger.info("Generating direct OAuth URL for Google Sheets access")
                
                # Build OAuth URL with Google Sheets scope
                from urllib.parse import urlencode
                
                params = {
                    'client_id': self.oauth_config['client_id'],
                    'redirect_uri': self.get_current_app_url(),
                    'scope': 'openid email profile https://www.googleapis.com/auth/spreadsheets.readonly',
                    'response_type': 'code',
                    'access_type': 'offline',
                    'prompt': 'consent'
                }
                
                auth_url = f"{self.oauth_config.get('auth_uri', 'https://accounts.google.com/o/oauth2/auth')}?{urlencode(params)}"
                logger.info(f"Generated OAuth URL for Google Sheets authentication")
                
                return True, auth_url
            
        except Exception as e:
            logger.error(f"Authentication setup failed: {e}")
            return False, str(e)
    
    def complete_oauth_flow(self, authorization_code: str = None, state: str = None):
        """Complete OAuth flow with authorization code using OAuth2 web application flow"""
        try:
            if not authorization_code:
                return False, "Authorization code is required for OAuth2 flow"
                
            # Clean and validate authorization code
            authorization_code = authorization_code.strip()
            if not authorization_code:
                return False, "Authorization code cannot be empty"
                
            # Remove any URL encoding artifacts
            if '%2F' in authorization_code:
                authorization_code = authorization_code.replace('%2F', '/')
            if '%3D' in authorization_code:
                authorization_code = authorization_code.replace('%3D', '=')
                
            logger.info(f"Processing authorization code (first 20 chars): {authorization_code[:20]}...")
                
            if not self.oauth_config:
                return False, "OAuth configuration not available"
            
            # Skip service account method - use OAuth2 web application flow with individual secrets
            if 'type' in self.oauth_config and self.oauth_config['type'] == 'service_account':
                return False, "OAuth2 flow requires web application configuration, not service account"
            
            # Exchange authorization code for credentials using OAuth2 web application flow
            import requests
            
            # Use clean redirect URI (no query parameters)
            redirect_uri = self.get_current_app_url()
            
            data = {
                'code': authorization_code,
                'client_id': self.oauth_config['client_id'],
                'client_secret': self.oauth_config['client_secret'],
                'redirect_uri': redirect_uri,
                'grant_type': 'authorization_code'
            }
            
            logger.info(f"Exchanging authorization code with redirect_uri: {redirect_uri}")
            logger.info(f"Using client_id: {self.oauth_config['client_id'][:10]}...")
            
            response = requests.post(self.oauth_config.get('token_uri', 'https://oauth2.googleapis.com/token'), data=data, timeout=30)
            
            if response.status_code == 200:
                token_data = response.json()
                
                # Create credentials object
                self.credentials = Credentials(
                    token=token_data['access_token'],
                    refresh_token=token_data.get('refresh_token'),
                    client_id=self.oauth_config['client_id'],
                    client_secret=self.oauth_config['client_secret'],
                    token_uri=self.oauth_config.get('token_uri', 'https://oauth2.googleapis.com/token')
                )
                
                # Store credentials in session state
                st.session_state.google_sheets_credentials = {
                    'token': token_data['access_token'],
                    'refresh_token': token_data.get('refresh_token'),
                    'client_id': self.oauth_config['client_id'],
                    'client_secret': self.oauth_config['client_secret'],
                    'token_uri': self.oauth_config.get('token_uri', 'https://oauth2.googleapis.com/token'),
                    'type': 'oauth2_web'
                }
                
                # Initialize service
                self.service = build('sheets', 'v4', credentials=self.credentials)
                
                logger.info("OAuth2 web application authentication completed successfully")
                return True, "OAuth2 web application authentication completed successfully!"
            else:
                error_data = response.json() if response.content else {}
                error_msg = error_data.get('error_description', 'Token exchange failed')
                logger.error(f"Token exchange failed: {response.status_code} - {error_msg}")
                return False, f"Authentication failed: {error_msg}"
                
        except Exception as e:
            logger.error(f"OAuth flow completion failed: {e}")
            return False, f"Authentication error: {str(e)}"
    
    def extract_spreadsheet_id(self, url: str) -> Optional[str]:
        """Extract spreadsheet ID from Google Sheets URL"""
        try:
            # Pattern to match Google Sheets URL and extract ID
            pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            match = re.search(pattern, url)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.error(f"Failed to extract spreadsheet ID: {e}")
            return None
    
    def reset_authentication(self) -> bool:
        """Reset authentication state and clear stored credentials"""
        try:
            # Clear service and credentials
            self.service = None
            self.credentials = None
            
            # Clear session state
            if hasattr(st, 'session_state') and 'google_sheets_credentials' in st.session_state:
                del st.session_state.google_sheets_credentials
                logger.info("Cleared stored credentials from session state")
            
            logger.info("Authentication state reset successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset authentication: {e}")
            return False
    
    def is_authenticated(self) -> bool:
        """Check if Google Sheets service is authenticated and ready"""
        return self.service is not None
    
    def fetch_sheet_data(self, spreadsheet_id: str = None, range_name: str = None) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Fetch data from Google Sheets using default or provided parameters
        
        Args:
            spreadsheet_id: Google Sheets ID (optional, uses default)
            range_name: Sheet range (optional, uses default)
            
        Returns:
            Tuple of (DataFrame, message)
        """
        if not GOOGLE_APIS_AVAILABLE:
            return None, "Google API dependencies not installed"
        
        # Use defaults if not provided
        if not spreadsheet_id:
            spreadsheet_id = self.default_config['spreadsheet_id']
        if not range_name:
            range_name = self.default_config['range_name']
        
        # Ensure we have service
        if not self.service and not self._initialize_service():
            return None, "Google Sheets authentication required"
        
        try:
            # Call the Sheets API
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                return None, "No data found in the specified range"
            
            # Convert to DataFrame
            if len(values) > 1:
                df = pd.DataFrame(values[1:], columns=values[0])
            else:
                df = pd.DataFrame(values, columns=[f'Column_{i}' for i in range(len(values[0]))])
            
            logger.info(f"Successfully fetched {len(df)} rows from Google Sheets")
            return df, f"Successfully fetched {len(df)} rows from Google Sheets"
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {e}")
            return None, f"API error: {e}"
        except Exception as e:
            logger.error(f"Failed to fetch sheet data: {e}")
            return None, str(e)
    
    def store_data_in_database(self, df: pd.DataFrame, table_name: str = None) -> Tuple[bool, str]:
        """
        Store DataFrame in PostgreSQL database with incremental updates
        Only appends new or updated rows instead of replacing all data
        
        Args:
            df: DataFrame to store
            table_name: Database table name (optional, uses default)
            
        Returns:
            Tuple of (success, message)
        """
        if table_name is None:
            table_name = self.default_config['table_name']
            
        try:
            # Use centralized database connection utility
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Ensure DataAggregator table exists with enhanced structure for tracking changes
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                row_hash VARCHAR(64) UNIQUE,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            cursor.execute(create_table_sql)
            
            # Add index on row_hash for faster lookups if it doesn't exist
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_row_hash 
                ON {table_name} (row_hash)
            """)
            
            new_rows = 0
            updated_rows = 0
            
            # Process each row for incremental updates
            if not df.empty:
                for _, row in df.iterrows():
                    # Convert row to JSON and create hash for change detection
                    row_data = row.to_dict()
                    
                    # Handle NaN values
                    for key, value in row_data.items():
                        if pd.isna(value):
                            row_data[key] = None
                    
                    # Create a hash of the row data for change detection
                    import hashlib
                    row_json = json.dumps(row_data, sort_keys=True)
                    row_hash = hashlib.sha256(row_json.encode()).hexdigest()
                    
                    # Check if this exact row already exists (exact duplicate prevention)
                    cursor.execute(f"""
                        SELECT id FROM {table_name} WHERE row_hash = %s
                    """, (row_hash,))
                    
                    exact_duplicate = cursor.fetchone()
                    
                    if exact_duplicate is None:
                        # Check for logical candidate duplicate (same candidate + role + client + status)
                        candidate_name = row_data.get('Candidate name', '')
                        role = row_data.get('Role', '')
                        client = row_data.get('Potential Client', '')
                        status = row_data.get('Status', '')
                        source = row_data.get('Source', '')
                        
                        if candidate_name and candidate_name.strip():  # Only check if we have a candidate name
                            # Check for logical duplicates using Python string parsing instead of complex SQL
                            cursor.execute(f"SELECT id, data FROM {table_name} WHERE data LIKE %s", (f"%{candidate_name}%",))
                            existing_records = cursor.fetchall()
                            
                            logical_duplicate = None
                            for existing_record in existing_records:
                                existing_data = existing_record[1]
                                # Parse existing data to check for matches
                                if (f"'Candidate name': '{candidate_name}'" in existing_data and
                                    f"'Role': '{role}'" in existing_data and
                                    f"'Potential Client': '{client}'" in existing_data and
                                    f"'Status': '{status}'" in existing_data and
                                    f"'Source': '{source}'" in existing_data):
                                    logical_duplicate = existing_record
                                    break
                            
                            logical_duplicate = cursor.fetchone()
                            
                            if logical_duplicate is None:
                                # This is a genuinely new candidate record - insert it
                                try:
                                    cursor.execute(f"""
                                        INSERT INTO {table_name} (data, row_hash, synced_at, created_at, updated_at) 
                                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                    """, (json.dumps(row_data), row_hash))
                                    new_rows += 1
                                except psycopg2.IntegrityError:
                                    # Handle rare case of hash collision - update existing
                                    conn.rollback()
                                    cursor.execute(f"""
                                        UPDATE {table_name} 
                                        SET data = %s, updated_at = CURRENT_TIMESTAMP, synced_at = CURRENT_TIMESTAMP
                                        WHERE row_hash = %s
                                    """, (json.dumps(row_data), row_hash))
                                    updated_rows += 1
                                    conn.commit()
                            # else: logical duplicate exists, skip this row
                        else:
                            # No candidate name, insert anyway (might be header or other data)
                            try:
                                cursor.execute(f"""
                                    INSERT INTO {table_name} (data, row_hash, synced_at, created_at, updated_at) 
                                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                """, (json.dumps(row_data), row_hash))
                                new_rows += 1
                            except psycopg2.IntegrityError:
                                conn.rollback()
                                cursor.execute(f"""
                                    UPDATE {table_name} 
                                    SET data = %s, updated_at = CURRENT_TIMESTAMP, synced_at = CURRENT_TIMESTAMP
                                    WHERE row_hash = %s
                                """, (json.dumps(row_data), row_hash))
                                updated_rows += 1
                                conn.commit()
                    # If exact row exists with same hash, skip it (no changes)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            message = f"Incremental sync completed: {new_rows} new rows added"
            if updated_rows > 0:
                message += f", {updated_rows} rows updated"
            
            total_processed = new_rows + updated_rows
            skipped = len(df) - total_processed
            if skipped > 0:
                message += f", {skipped} rows unchanged (skipped)"
            
            logger.info(f"Incremental sync: {message}")
            return True, message
            
        except Exception as e:
            logger.error(f"Database storage failed: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            return False, str(e)
    
    def get_last_sync_time(self, table_name: str = None) -> Optional[datetime]:
        """Get last sync timestamp from database"""
        if table_name is None:
            table_name = self.default_config['table_name']
            
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            if not cursor.fetchone()[0]:
                cursor.close()
                conn.close()
                return None
            
            cursor.execute(f"SELECT MAX(synced_at) FROM {table_name}")
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] if result and result[0] else None
            
        except Exception as e:
            logger.error(f"Failed to get last sync time: {e}")
            return None
    
    def sync_data_now(self) -> Tuple[bool, str]:
        """
        Manually sync data from Google Sheets to database with incremental updates and automatic transformations
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Fetch data from sheets
            df, fetch_message = self.fetch_sheet_data()
            
            if df is None:
                return False, f"Failed to fetch data: {fetch_message}"
            
            # Process only new/updated rows for incremental sync
            success, store_message = self.store_data_in_database(df)
            
            if success:
                # Trigger automatic consolidation with transformations
                consolidation_success, consolidation_message = self._trigger_automatic_consolidation()
                
                if consolidation_success:
                    return True, f"Incremental sync and transformation completed: {store_message}. {consolidation_message}"
                else:
                    return True, f"Incremental sync completed but consolidation had issues: {store_message}. Consolidation: {consolidation_message}"
            else:
                return False, f"Failed to store data: {store_message}"
                
        except Exception as e:
            logger.error(f"Data sync failed: {e}")
            return False, str(e)
    
    def _trigger_automatic_consolidation(self) -> Tuple[bool, str]:
        """
        Trigger automatic consolidation with transformations after Google Sheets sync
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Import and run the enhanced consolidation function
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(__file__)))
            from consolidate_candidate_data import consolidate_candidate_data
            
            logger.info("Triggering automatic consolidation with status transformations...")
            consolidate_candidate_data()
            
            return True, "Automatic consolidation with transformations completed successfully"
            
        except Exception as e:
            logger.error(f"Automatic consolidation failed: {e}")
            return False, f"Consolidation error: {str(e)}"
    
    def check_scheduled_sync(self) -> Tuple[bool, str]:
        """
        Check if scheduled sync should run (8pm IST daily)
        
        Returns:
            Tuple of (should_run, message)
        """
        try:
            # Get current IST time
            ist_tz = pytz.timezone('Asia/Kolkata')
            current_ist = datetime.now(ist_tz)
            
            # Check if it's around 8pm IST (allow 1 hour window)
            if not (19 <= current_ist.hour <= 21):
                return False, "Not scheduled sync time"
            
            # Check last sync
            last_sync = self.get_last_sync_time()
            if last_sync:
                # Convert to IST
                if last_sync.tzinfo is None:
                    last_sync = pytz.utc.localize(last_sync)
                last_sync_ist = last_sync.astimezone(ist_tz)
                
                # If already synced today, skip
                if last_sync_ist.date() == current_ist.date():
                    return False, "Already synced today"
            
            return True, "Ready for scheduled sync"
            
        except Exception as e:
            logger.error(f"Scheduled sync check failed: {e}")
            return False, str(e)
    
    def cleanup_old_records(self, table_name: str = None, days_to_keep: int = 30) -> Tuple[bool, str]:
        """
        Clean up old duplicate records while preserving recent data
        
        Args:
            table_name: Database table name (optional, uses default)
            days_to_keep: Number of days of data to keep (default: 30)
            
        Returns:
            Tuple of (success, message)
        """
        if table_name is None:
            table_name = self.default_config['table_name']
            
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Keep only the most recent record for each unique row_hash
            # and remove old records beyond the retention period
            cleanup_sql = f"""
            WITH ranked_records AS (
                SELECT id, row_hash, created_at,
                       ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY created_at DESC) as rn
                FROM {table_name}
            ),
            records_to_keep AS (
                SELECT id FROM ranked_records 
                WHERE rn = 1 AND created_at > CURRENT_TIMESTAMP - INTERVAL '{days_to_keep} days'
            )
            DELETE FROM {table_name} 
            WHERE id NOT IN (SELECT id FROM records_to_keep)
            """
            
            cursor.execute(cleanup_sql)
            deleted_count = cursor.rowcount
            
            conn.commit()
            cursor.close()
            conn.close()
            
            message = f"Cleaned up {deleted_count} old/duplicate records, kept recent unique data from last {days_to_keep} days"
            logger.info(message)
            return True, message
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            return False, str(e)
    
    def get_data_preview(self, table_name: str = None, limit: int = 10) -> Tuple[Optional[pd.DataFrame], str]:
        """Get preview of data from database table"""
        if table_name is None:
            table_name = self.default_config['table_name']
            
        try:
            conn = get_database_connection()
            
            # Check if table exists
            query = f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT {limit}"
            df = pd.read_sql_query(query, conn)
            
            conn.close()
            
            if df.empty:
                return None, "No data found in database"
            
            return df, f"Found {len(df)} records"
            
        except Exception as e:
            logger.error(f"Failed to get data preview: {e}")
            return None, str(e)
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get comprehensive sync status information"""
        try:
            ist_tz = pytz.timezone('Asia/Kolkata')
            current_ist = datetime.now(ist_tz)
            
            # Get last sync time
            last_sync = self.get_last_sync_time()
            last_sync_str = "Never"
            if last_sync:
                if last_sync.tzinfo is None:
                    last_sync = pytz.utc.localize(last_sync)
                last_sync_ist = last_sync.astimezone(ist_tz)
                last_sync_str = last_sync_ist.strftime('%Y-%m-%d %H:%M:%S IST')
            
            # Calculate next sync time
            next_sync = current_ist.replace(hour=20, minute=0, second=0, microsecond=0)
            if current_ist.hour >= 20:
                next_sync += timedelta(days=1)
            
            # Get data count
            try:
                preview_df, _ = self.get_data_preview(limit=1000)
                data_count = len(preview_df) if preview_df is not None else 0
            except:
                data_count = 0
            
            return {
                'authenticated': self.is_authenticated(),
                'current_time': current_ist.strftime('%Y-%m-%d %H:%M:%S IST'),
                'last_sync': last_sync_str,
                'next_sync': next_sync.strftime('%Y-%m-%d %H:%M:%S IST'),
                'data_count': data_count,
                'spreadsheet_id': self.default_config['spreadsheet_id'],
                'table_name': self.default_config['table_name']
            }
            
        except Exception as e:
            logger.error(f"Failed to get sync status: {e}")
            return {
                'authenticated': False,
                'error': str(e)
            }


# Global instance
sheets_manager = GoogleSheetsManager()