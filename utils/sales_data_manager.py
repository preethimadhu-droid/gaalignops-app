import pandas as pd
import psycopg2
import os
from datetime import datetime
import re
from .production_data_protection import check_production_safety, safe_table_create
from .database_connection import get_database_connection

class SalesDataManager:
    """Manage sales data with proper database structure and date extraction"""
    
    def __init__(self, env_manager=None):
        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False
        
        self.create_tables()
    
    def get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.use_dev_tables:
            return f"dev_{table_name}"
        return table_name
    
    def create_tables(self):
        """Create database tables for sales data ONLY if they don't exist - PRODUCTION SAFE"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Check if key tables exist
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name IN ('unified_sales_data', 'master_clients')
                );
            """)
            tables_exist = cursor.fetchone()[0]
            
            if tables_exist:
                print("Production sales tables detected - running safe schema updates only")
                self._run_safe_sales_migrations(cursor)
                conn.commit()
                conn.close()
                return
                
            print("Creating sales data tables (no existing tables detected)")
        except Exception as e:
            print(f"Error checking table existence: {e}")
            
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create accounts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(255) NOT NULL,
                account_track VARCHAR(255),
                connect_name VARCHAR(255),
                owner VARCHAR(255),
                source VARCHAR(255),
                industry VARCHAR(255),
                region VARCHAR(255),
                lob VARCHAR(255),
                offering VARCHAR(255),
                confidence INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create sales_data table with extracted date components
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                id SERIAL PRIMARY KEY,
                account_id INTEGER,
                financial_year VARCHAR(50) NOT NULL,
                year INTEGER NOT NULL,
                month VARCHAR(50) NOT NULL,
                month_number INTEGER NOT NULL,
                metric_type VARCHAR(100) NOT NULL,
                value DECIMAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )
        """)
        
        # Add connect_name column if it doesn't exist (for existing databases)
        try:
            cursor.execute("ALTER TABLE accounts ADD COLUMN connect_name VARCHAR(255)")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            # Column already exists, ignore
            conn.rollback()
        except:
            conn.rollback()
        
        # Add partner_connect column if it doesn't exist (for existing databases)
        try:
            cursor.execute("ALTER TABLE accounts ADD COLUMN partner_connect VARCHAR(255)")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            # Column already exists, ignore
            conn.rollback()
        except:
            conn.rollback()
        
        conn.commit()
        conn.close()
    
    def extract_date_components(self, column_name):
        """Extract year, month, financial year, and metric type from column names like '2025-April_Planned'"""
        pattern = r'(\d{4})-(\w+)_(\w+)'
        match = re.match(pattern, column_name)
        
        if match:
            year = int(match.group(1))
            month = match.group(2)
            metric_type = match.group(3)
            
            # Convert month name to number
            month_mapping = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4,
                'May': 5, 'June': 6, 'July': 7, 'August': 8,
                'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            month_number = month_mapping.get(month, 0)
            
            # Calculate financial year (April to March)
            # If month is April-December, FY is the same year
            # If month is January-March, FY is the previous year
            if month_number >= 4:  # April to December
                financial_year = f"FY{year}"
            else:  # January to March
                financial_year = f"FY{year-1}"
            
            return financial_year, year, month, month_number, metric_type
        
        return None, None, None, None, None
    
    def clean_monetary_value(self, value):
        """Clean monetary values and convert to float"""
        if pd.isna(value) or value == '' or value == '$0.00' or value == '0':
            return 0.0
        try:
            return float(str(value).replace('$', '').replace(',', ''))
        except:
            return 0.0
    
    def load_csv_to_database(self, csv_path, force_overwrite=False):
        """Load CSV data into database with proper structure
        
        Args:
            csv_path: Path to CSV file
            force_overwrite: If True, allows data overwrite (DANGEROUS - only for initial setup)
        """
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Use production data protection system  
        cursor.execute("SELECT COUNT(*) FROM unified_sales_data")
        existing_records = cursor.fetchone()[0]
        
        allowed, message = check_production_safety("unified_sales_data", "CSV_OVERWRITE", force_overwrite, existing_records)
        
        if not allowed:
            print(f"\n🔒 CSV LOAD BLOCKED - PRODUCTION DATA PROTECTION")
            print(f"📊 Existing records: {existing_records}")
            print(f"🚫 {message}")
            print(f"⚠️  To override: use force_overwrite=True parameter")
            conn.close()
            return 0
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Clear existing data ONLY if force_overwrite is True
        if force_overwrite:
            cursor.execute("DELETE FROM sales_data")
            cursor.execute("DELETE FROM accounts")
        
        account_id_map = {}
        
        for index, row in df.iterrows():
            # Insert account data
            cursor.execute("""
                INSERT INTO accounts (account_name, account_track, owner, source, industry, region, lob, offering, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['Account'],
                row.get('Account-Track', ''),
                row.get('Owner', ''),
                row.get('Source', ''),
                row.get('Industry', ''),
                row.get('Region', ''),
                row.get('LoB', ''),
                row.get('Offering', ''),
                row.get('Confidence', 0)
            ))
            
            account_id = cursor.fetchone()[0]
            account_id_map[index] = account_id
            
            # Process all date columns
            for column in df.columns:
                financial_year, year, month, month_number, metric_type = self.extract_date_components(column)
                
                if financial_year and year and month and metric_type:
                    value = self.clean_monetary_value(row[column])
                    
                    cursor.execute("""
                        INSERT INTO sales_data (account_id, financial_year, year, month, month_number, metric_type, value)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (account_id, financial_year, year, month, month_number, metric_type, value))
        
        conn.commit()
        conn.close()
        
        return len(df)
    
    def get_sales_data_summary(self):
        """Get summary of sales data from database"""
        conn = get_database_connection()
        
        query = """
            SELECT 
                a.account_name,
                a.account_track,
                a.owner,
                a.source,
                a.industry,
                a.region,
                a.lob,
                a.offering,
                a.confidence,
                sd.financial_year,
                sd.year,
                sd.month,
                sd.month_number,
                sd.metric_type,
                sd.value
            FROM accounts a
            JOIN sales_data sd ON a.id = sd.account_id
            ORDER BY a.account_name, sd.financial_year, sd.month_number, sd.metric_type
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_editable_view(self):
        """Get data in editable format for the UI with separate Year, Month, Metric Type columns"""
        conn = get_database_connection()
        
        # Get all data with separate fields for easy reading including all account details
        query = """
            SELECT 
                a.account_name,
                a.account_track,
                a.connect_name,
                a.partner_connect,
                a.owner,
                a.source,
                a.industry,
                a.region,
                a.lob,
                a.offering,
                a.confidence,
                sd.financial_year,
                sd.year,
                sd.month,
                sd.metric_type,
                sd.value
            FROM accounts a
            JOIN sales_data sd ON a.id = sd.account_id
            ORDER BY a.account_name, sd.financial_year, sd.month_number, sd.metric_type
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def update_sales_value(self, account_name, financial_year, year, month, metric_type, new_value):
        """Update a specific sales value in the database"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE sales_data 
                SET value = %s
                WHERE account_id = (SELECT id FROM accounts WHERE account_name = %s)
                AND financial_year = %s AND year = %s AND month = %s AND metric_type = %s
            """, (new_value, account_name, financial_year, year, month, metric_type))
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            return rows_affected > 0
            
        except Exception as e:
            print(f"Error updating sales value: {str(e)}")
            return False
    
    def update_account_info(self, original_account_name, new_data):
        """Update account information including name and other details"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Get original track to make update more specific
            original_track = new_data.get('original_track', '')
            
            # First check if the specific account+track combination exists
            cursor.execute("""
                SELECT COUNT(*) FROM accounts 
                WHERE account_name = %s AND account_track = %s
            """, (original_account_name, original_track))
            account_exists = cursor.fetchone()[0] > 0
            
            if not account_exists:
                # Fallback to just account name if track-specific lookup fails
                cursor.execute("SELECT COUNT(*) FROM accounts WHERE account_name = %s", (original_account_name,))
                account_exists = cursor.fetchone()[0] > 0
                
                if not account_exists:
                    print(f"Debug: Account '{original_account_name}' not found in database")
                    conn.close()
                    return False
            
            print(f"Debug: Updating account '{original_account_name}' (track: '{original_track}') with data: {new_data}")
            
            # Update using more specific criteria if we have the original track
            if original_track:
                cursor.execute("""
                    UPDATE accounts 
                    SET account_name = %s, account_track = %s, connect_name = %s, partner_connect = %s, owner = %s, 
                        source = %s, industry = %s, region = %s, lob = %s, offering = %s, confidence = %s
                    WHERE account_name = %s AND account_track = %s
                """, (
                    new_data.get('account_name', original_account_name),
                    new_data.get('account_track', ''),
                    new_data.get('connect_name', ''),
                    new_data.get('partner_connect', ''),
                    new_data.get('owner', ''),
                    new_data.get('source', ''),
                    new_data.get('industry', ''),
                    new_data.get('region', ''),
                    new_data.get('lob', ''),
                    new_data.get('offering', ''),
                    new_data.get('confidence', 0),
                    original_account_name,
                    original_track
                ))
            else:
                # Fallback to original logic
                cursor.execute("""
                    UPDATE accounts 
                    SET account_name = %s, account_track = %s, connect_name = %s, partner_connect = %s, owner = %s, 
                        source = %s, industry = %s, region = %s, lob = %s, offering = %s, confidence = %s
                    WHERE account_name = %s
                """, (
                    new_data.get('account_name', original_account_name),
                    new_data.get('account_track', ''),
                    new_data.get('connect_name', ''),
                    new_data.get('partner_connect', ''),
                    new_data.get('owner', ''),
                    new_data.get('source', ''),
                    new_data.get('industry', ''),
                    new_data.get('region', ''),
                    new_data.get('lob', ''),
                    new_data.get('offering', ''),
                    new_data.get('confidence', 0),
                    original_account_name
                ))
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            print(f"Debug: Account update affected {rows_affected} rows")
            return rows_affected > 0
            
        except Exception as e:
            print(f"Error updating account info: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return False
    
    def update_sales_record(self, original_data, new_data):
        """Update a complete sales record including account and sales data"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Update sales_data record
            cursor.execute("""
                UPDATE sales_data 
                SET financial_year = %s, year = %s, month = %s, metric_type = %s, value = %s
                WHERE account_id = (SELECT id FROM accounts WHERE account_name = %s)
                AND financial_year = %s AND year = %s AND month = %s AND metric_type = %s
            """, (
                new_data.get('financial_year'),
                new_data.get('year'), 
                new_data.get('month'),
                new_data.get('metric_type'),
                new_data.get('value'),
                original_data.get('account_name'),  # Find by original account name
                original_data.get('financial_year'),
                original_data.get('year'),
                original_data.get('month'),
                original_data.get('metric_type')
            ))
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            return rows_affected > 0
            
        except Exception as e:
            print(f"Error updating sales record: {str(e)}")
            return False
    
    def add_new_record(self, account_name, account_track, connect_name, partner_connect, owner, source, industry, region, lob, offering, financial_year, year, month, month_number, metric_type, value):
        """Add a new record to the database"""
        conn = get_database_connection()
        cursor = conn.cursor()
        
        try:
            # First, check if account already exists
            cursor.execute("SELECT id FROM accounts WHERE account_name = %s", (account_name,))
            account_result = cursor.fetchone()
            
            if account_result:
                account_id = account_result[0]
                # Update account information if it exists
                cursor.execute("""
                    UPDATE accounts 
                    SET account_track = %s, connect_name = %s, partner_connect = %s, owner = %s, source = %s, industry = %s, region = %s, lob = %s, offering = %s
                    WHERE id = %s
                """, (account_track, connect_name, partner_connect, owner, source, industry, region, lob, offering, account_id))
            else:
                # Insert new account
                cursor.execute("""
                    INSERT INTO accounts (account_name, account_track, connect_name, partner_connect, owner, source, industry, region, lob, offering, confidence)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (account_name, account_track, connect_name, partner_connect, owner, source, industry, region, lob, offering, 0))
                account_id = cursor.fetchone()[0]
            
            # Insert sales data record
            cursor.execute("""
                INSERT INTO sales_data (account_id, financial_year, year, month, month_number, metric_type, value)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (account_id, financial_year, year, month, month_number, metric_type, value))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error saving record: {str(e)}")  # Debug output
            return False
        finally:
            conn.close()
    
    def get_database_stats(self):
        """Get database statistics"""
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM accounts")
        account_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sales_data")
        sales_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(value) FROM sales_data WHERE metric_type = 'Planned'")
        total_planned = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(value) FROM sales_data WHERE metric_type = 'Forecasted'")
        total_forecasted = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'accounts': account_count,
            'sales_records': sales_records,
            'total_planned': total_planned,
            'total_forecasted': total_forecasted
        }
    
    def bulk_update_records(self, updates_list):
        """Perform bulk updates for better performance and transaction safety"""
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
        successful_updates = 0
        failed_updates = 0
        
        try:
            # Start transaction
            cursor.execute("BEGIN TRANSACTION")
            
            for update in updates_list:
                try:
                    # Update account if needed
                    if 'account_update' in update:
                        account_data = update['account_update']
                        cursor.execute("""
                            UPDATE accounts 
                            SET account_name = %s, account_track = %s, connect_name = %s, partner_connect = %s, 
                                owner = %s, source = %s, industry = %s, region = %s, lob = %s, offering = %s, confidence = %s
                            WHERE account_name = %s
                        """, (
                            account_data['account_name'],
                            account_data['account_track'],
                            account_data['connect_name'],
                            account_data['partner_connect'],
                            account_data['owner'],
                            account_data['source'],
                            account_data['industry'],
                            account_data['region'],
                            account_data['lob'],
                            account_data['offering'],
                            account_data['confidence'],
                            account_data['original_account_name']
                        ))
                    
                    # Update sales data if needed
                    if 'sales_update' in update:
                        sales_data = update['sales_update']
                        cursor.execute("""
                            UPDATE sales_data 
                            SET financial_year = %s, year = %s, month = %s, metric_type = %s, value = %s
                            WHERE account_id = (SELECT id FROM accounts WHERE account_name = %s)
                            AND financial_year = %s AND year = %s AND month = %s AND metric_type = %s
                        """, (
                            sales_data['new_financial_year'],
                            sales_data['new_year'],
                            sales_data['new_month'],
                            sales_data['new_metric_type'],
                            sales_data['new_value'],
                            sales_data['account_name'],
                            sales_data['original_financial_year'],
                            sales_data['original_year'],
                            sales_data['original_month'],
                            sales_data['original_metric_type']
                        ))
                    
                    successful_updates += 1
                    
                except Exception as e:
                    failed_updates += 1
                    print(f"Error in bulk update: {str(e)}")
            
            # Commit transaction
            conn.commit()
            
        except Exception as e:
            # Rollback on error
            conn.rollback()
            print(f"Bulk update transaction failed: {str(e)}")
            
        finally:
            conn.close()
        
        return successful_updates, failed_updates
    
    def verify_data_integrity(self):
        """Verify database integrity and return any issues"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            issues = []
            
            # Check for orphaned sales_data records
            cursor.execute("""
                SELECT COUNT(*) FROM sales_data sd 
                LEFT JOIN accounts a ON sd.account_id = a.id 
                WHERE a.id IS NULL
            """)
            orphaned_records = cursor.fetchone()[0]
            if orphaned_records > 0:
                issues.append(f"{orphaned_records} orphaned sales records found")
            
            # Check for duplicate account names
            cursor.execute("""
                SELECT account_name, COUNT(*) as count 
                FROM accounts 
                GROUP BY account_name 
                HAVING count > 1
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                issues.append(f"{len(duplicates)} duplicate account names found")
            
            # Check for missing required fields
            cursor.execute("SELECT COUNT(*) FROM accounts WHERE account_name IS NULL OR account_name = ''")
            missing_names = cursor.fetchone()[0]
            if missing_names > 0:
                issues.append(f"{missing_names} accounts with missing names")
            
            conn.close()
            
            return {
                'is_healthy': len(issues) == 0,
                'issues': issues
            }
            
        except Exception as e:
            return {
                'is_healthy': False,
                'issues': [f"Database check failed: {str(e)}"]
            }
    
    def _run_safe_sales_migrations(self, cursor):
        """Run safe schema migrations for sales tables that don't affect existing data"""
        try:
            # Safe migrations that add columns or indexes without data loss
            migrations = [
                # Add partner_connect column if it doesn't exist
                """
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'unified_sales_data' AND column_name = 'partner_connect'
                    ) THEN
                        ALTER TABLE unified_sales_data ADD COLUMN partner_connect VARCHAR(255);
                    END IF;
                END $$;
                """,
                # Add partner_org column if it doesn't exist
                """
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'unified_sales_data' AND column_name = 'partner_org'
                    ) THEN
                        ALTER TABLE unified_sales_data ADD COLUMN partner_org VARCHAR(255);
                    END IF;
                END $$;
                """,
                # Add status column to master_clients if it doesn't exist
                """
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'master_clients' AND column_name = 'status'
                    ) THEN
                        ALTER TABLE master_clients ADD COLUMN status VARCHAR(100) DEFAULT 'Active Lead';
                    END IF;
                END $$;
                """,
                # Add more safe migrations here as needed
            ]
            
            for migration in migrations:
                if migration.strip():  # Skip empty migrations
                    cursor.execute(migration)
                    
            print("Safe sales schema migrations completed")
            
        except Exception as e:
            print(f"Error running safe sales migrations: {str(e)}")
            raise