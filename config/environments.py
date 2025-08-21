"""
Environment Configuration for GA AlignOps
Handles Development and Production environment setup with data segregation
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class EnvironmentConfig:
    """Environment configuration data class"""
    name: str
    database_url: str
    table_prefix: str
    google_sheets_config: Dict[str, Any]
    oauth_config: Dict[str, Any]
    scheduler_config: Dict[str, Any]
    features: Dict[str, bool]
    data_sync_enabled: bool
    debug_mode: bool

class EnvironmentManager:
    """
    Enhanced Environment Manager for GA AlignOps
    Provides complete environment separation for development and production
    """
    
    def __init__(self):
        self.environment = self._detect_environment()
        self.config = self._load_environment_config()
        self._setup_environment()
    
    def _detect_environment(self) -> str:
        """Detect current environment"""
        # Check explicit environment variable first
        env_var = os.getenv('GAALIGNOPS_ENV')
        if env_var in ['development', 'production']:
            return env_var
        
        # Auto-detect based on deployment context
        replit_slug = os.getenv('REPL_SLUG', '')
        replit_owner = os.getenv('REPL_OWNER', '')
        
        # Production: replit.app domain AND owned by greyampmanagement
        if '.replit.app' in replit_slug and replit_owner == 'greyampmanagement':
            return 'production'
        
        # Development: everything else
        return 'development'
    
    def _load_environment_config(self) -> EnvironmentConfig:
        """Load environment-specific configuration"""
        if self.environment == 'production':
            return self._get_production_config()
        else:
            return self._get_development_config()
    
    def _get_production_config(self) -> EnvironmentConfig:
        """Production environment configuration"""
        return EnvironmentConfig(
            name='production',
            database_url=os.getenv('DATABASE_URL'),
            table_prefix='',  # No prefix for production
            google_sheets_config={
                'enabled': True,
                'sync_schedule': '0 20 * * *',  # 8 PM IST daily
                'production_sheets': True,
                'data_validation': True
            },
            oauth_config={
                'enabled': True,
                'domain_restriction': 'greyamp.com',
                'ssl_required': True,
                'fallback_auth': False
            },
            scheduler_config={
                'enabled': True,
                'auto_start': True,
                'background_mode': True
            },
            features={
                'advanced_analytics': True,
                'ml_forecasting': True,
                'real_time_sync': True,
                'production_data_protection': True
            },
            data_sync_enabled=True,
            debug_mode=False
        )
    
    def _get_development_config(self) -> EnvironmentConfig:
        """Development environment configuration"""
        return EnvironmentConfig(
            name='development',
            database_url=os.getenv('DATABASE_URL'),
            table_prefix='dev_',  # dev_ prefix for development
            google_sheets_config={
                'enabled': True,
                'sync_schedule': '0 20 * * *',  # 8 PM IST daily
                'production_sheets': False,
                'data_validation': False
            },
            oauth_config={
                'enabled': False,  # Use fallback auth in development
                'domain_restriction': 'greyamp.com',
                'ssl_required': False,
                'fallback_auth': True
            },
            scheduler_config={
                'enabled': True,
                'auto_start': True,
                'background_mode': False
            },
            features={
                'advanced_analytics': True,
                'ml_forecasting': True,
                'real_time_sync': False,
                'production_data_protection': False
            },
            data_sync_enabled=True,
            debug_mode=True
        )
    
    def _setup_environment(self):
        """Setup environment-specific configurations"""
        # Set environment variables
        os.environ['GAALIGNOPS_ENV'] = self.environment
        os.environ['GAALIGNOPS_DEBUG'] = str(self.config.debug_mode).lower()
        
        # Create development tables if needed
        if self.is_development():
            self._ensure_development_tables()
    
    def _ensure_development_tables(self):
        """Ensure development tables exist with proper structure"""
        try:
            import psycopg2
            
            # Check if database URL is configured
            if not self.config.database_url or self.config.database_url == "Not configured":
                print("⚠️ Database URL not configured - skipping table creation")
                return
            
            conn = psycopg2.connect(self.config.database_url)
            cursor = conn.cursor()
            
            # Core tables for development
            core_tables = [
                'unified_sales_data',
                'master_clients',
                'talent_supply',
                'demand_supply_assignments',
                'annual_targets',
                'owner_targets',
                'users',
                'roles',
                'role_groups',
                'role_group_mappings',
                'staffing_plans',
                'billing_data',
                'analytics_metrics'
            ]
            
            for table in core_tables:
                dev_table = self.get_table_name(table)
                
                # Create development table if it doesn't exist
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {dev_table} 
                    (LIKE {table} INCLUDING ALL)
                """)
                
                # Copy data if production table has data
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # Clear existing development data
                    cursor.execute(f"TRUNCATE TABLE {dev_table} CASCADE")
                    
                    # Copy fresh production data
                    cursor.execute(f"""
                        INSERT INTO {dev_table} 
                        SELECT * FROM {table}
                        ON CONFLICT DO NOTHING
                    """)
                    print(f"✅ Synced {count} records to {dev_table}")
            
            conn.commit()
            conn.close()
            print("✅ Development environment setup completed")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not setup development tables: {str(e)}")
    
    def get_table_name(self, base_table_name: str) -> str:
        """Get environment-specific table name"""
        return f"{self.config.table_prefix}{base_table_name}"
    
    def get_database_url(self) -> str:
        """Get database URL for current environment"""
        return self.config.database_url
    
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == 'production'
    
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.environment == 'development'
    
    def get_config(self) -> EnvironmentConfig:
        """Get current environment configuration"""
        return self.config
    
    def get_environment_info(self) -> Dict[str, Any]:
        """Get detailed environment information"""
        database_url = self.config.database_url or "Not configured"
        database_display = database_url[:50] + '...' if len(database_url) > 50 else database_url
        
        return {
            'environment': self.environment,
            'table_prefix': self.config.table_prefix,
            'database_url': database_display,
            'features': self.config.features,
            'oauth_enabled': self.config.oauth_config['enabled'],
            'data_sync_enabled': self.config.data_sync_enabled,
            'debug_mode': self.config.debug_mode
        }
    
    def sync_production_to_development(self) -> bool:
        """Sync production data to development (one-way only)"""
        if self.is_production():
            print("❌ Cannot sync from production environment")
            return False
        
        print("🔄 Syncing production data to development...")
        self._ensure_development_tables()
        return True
    
    def get_google_sheets_config(self) -> Dict[str, Any]:
        """Get Google Sheets configuration for current environment"""
        return self.config.google_sheets_config
    
    def get_oauth_config(self) -> Dict[str, Any]:
        """Get OAuth configuration for current environment"""
        return self.config.oauth_config
    
    def get_scheduler_config(self) -> Dict[str, Any]:
        """Get scheduler configuration for current environment"""
        return self.config.scheduler_config
    
    def is_feature_enabled(self, feature_name: str) -> bool:
        """Check if a specific feature is enabled"""
        return self.config.features.get(feature_name, False)
