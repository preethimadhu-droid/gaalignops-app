"""
Database Connection Utility
Provides consistent database connection handling across the application
"""
import os
import psycopg2
from urllib.parse import urlparse

def get_database_config():
    """
    Get database configuration from DATABASE_URL or individual environment variables
    Returns a dictionary suitable for psycopg2.connect()
    """
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        # Use psycopg2's built-in URL parsing which handles special characters correctly
        try:
            # psycopg2 can parse DATABASE_URL directly, but we need to extract components
            # for the dictionary format
            from urllib.parse import urlparse, unquote
            parsed = urlparse(database_url)
            
            # Manually decode the password to handle special characters
            password = unquote(parsed.password) if parsed.password else None
            
            return {
                'host': parsed.hostname,
                'database': parsed.path[1:],  # Remove leading '/'
                'user': parsed.username,
                'password': password,
                'port': parsed.port or 5432
            }
        except Exception as e:
            print(f"Error parsing DATABASE_URL: {e}")
            # Fall through to individual variables
    
    # Fallback to individual environment variables
    return {
        'host': os.getenv('PGHOST', 'localhost'),
        'database': os.getenv('PGDATABASE', 'gaalignops_dev'),
        'user': os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD'),
        'port': int(os.getenv('PGPORT', 5432))
    }

def get_database_connection():
    """
    Create and return a database connection using the current configuration
    """
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        # Use DATABASE_URL directly with psycopg2 which handles special characters correctly
        try:
            return psycopg2.connect(database_url)
        except Exception as e:
            print(f"Database connection error with DATABASE_URL: {str(e)}")
            # Fall through to individual config
    
    # Fallback to individual environment variables
    config = get_database_config()
    try:
        return psycopg2.connect(**config)
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        print(f"Connection config: {config}")
        raise
