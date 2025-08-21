"""
Simplified data manager that works with the unified sales data table
for better data persistence and easier editing.
"""

import logging
import psycopg2
import pandas as pd
import os
from datetime import datetime
from .environment_manager import EnvironmentManager
from .data_creation_monitor import monitor_data_creation

class UnifiedDataManager:
    """Simplified data manager for the unified sales data table"""

    def __init__(self):
        self.env_manager = EnvironmentManager()
        self.database_url = self.env_manager.get_database_url()
        self.table_name = self.env_manager.get_table_name('unified_sales_data')

    def get_all_data(self):
        """Get all data from the unified table"""
        try:
            # Use a timeout to prevent database locking
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))

            query = f"""
                SELECT id, account_name, account_track, connect_name, partner_connect, partner_org,
                       owner, source, industry, region, lob, offering, status, duration,
                       financial_year, year, month, metric_type, value
                FROM {self.table_name}
                ORDER BY account_name, account_track, financial_year, year, 
                         CASE month 
                             WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                             WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                             WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                             WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                         END, metric_type
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            # Handle None values
            df['connect_name'] = df['connect_name'].fillna('')
            df['partner_connect'] = df['partner_connect'].fillna('')
            df['partner_org'] = df['partner_org'].fillna('')
            df['status'] = df['status'].fillna('Active Lead')

            return df

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return pd.DataFrame()

    def get_master_client_id(self, client_name):
        """Get master_client_id for a given client name"""
        try:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()

            master_clients_table = self.env_manager.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", (client_name,))
            result = cursor.fetchone()
            conn.close()

            return result[0] if result else None

        except Exception as e:
            print(f"Error getting master client ID: {str(e)}")
            return None

    def get_clients(self):
        """Get all clients from master_clients table"""
        try:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))

            master_clients_table = self.env_manager.get_table_name('master_clients')
            query = f"SELECT master_client_id, client_name FROM {master_clients_table} ORDER BY client_name"
            df = pd.read_sql_query(query, conn)
            conn.close()

            return df

        except Exception as e:
            print(f"Error getting clients: {str(e)}")
            return pd.DataFrame(columns=['master_client_id', 'client_name'])

    def update_record(self, record_id, updated_data):
        """Update a specific record by ID (legacy method for backward compatibility)"""
        try:
            # Use a timeout to prevent database locking
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()

            # Build the update query dynamically based on provided fields
            set_clauses = []
            values = []

            updatable_fields = [
                'account_name', 'account_track', 'connect_name', 'partner_connect', 'partner_org',
                'owner', 'source', 'industry', 'region', 'lob', 'offering', 'status',
                'financial_year', 'year', 'month', 'metric_type', 'value'
            ]

            for field in updatable_fields:
                if field in updated_data:
                    set_clauses.append(f"{field} = %s")
                    # Convert numpy types to native Python types
                    field_value = updated_data[field]
                    if hasattr(field_value, 'dtype'):  # numpy type
                        if 'int' in str(field_value.dtype):
                            field_value = int(field_value)
                        elif 'float' in str(field_value.dtype):
                            field_value = float(field_value)
                        else:
                            field_value = str(field_value)
                    values.append(field_value)

            if not set_clauses:
                conn.close()
                return False

            # Add updated timestamp
            set_clauses.append("updated_at = %s")
            values.append(datetime.now())
            # Convert record_id from numpy if needed
            if hasattr(record_id, 'dtype'):
                record_id = int(record_id)
            values.append(record_id)

            query = f"""
                UPDATE {self.table_name} 
                SET {', '.join(set_clauses)}
                WHERE id = %s
            """

            print(f"Debug: Updating record {record_id} with: {updated_data}")
            cursor.execute(query, values)

            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()

            print(f"Debug: Update affected {rows_affected} rows")
            return rows_affected > 0

        except Exception as e:
            print(f"Error updating record: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return False

    def update_or_insert_record(self, account_name, year, month, metric_type, value, additional_fields=None):
        """Update existing record or insert if not found using business keys"""
        try:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()

            # Convert numpy types to native Python types
            if hasattr(value, 'dtype'):
                if 'int' in str(value.dtype):
                    value = int(value)
                elif 'float' in str(value.dtype):
                    value = float(value)
                else:
                    value = str(value)

            if hasattr(year, 'dtype'):
                year = int(year)

            # Try to update existing record first
            cursor.execute(f"""
                UPDATE {self.table_name} 
                SET value = %s, updated_at = %s
                WHERE account_name = %s AND year = %s AND month = %s AND metric_type = %s
            """, [value, datetime.now(), account_name, year, month, metric_type])

            rows_affected = cursor.rowcount

            # If no rows affected, insert new record
            if rows_affected == 0:
                # Get additional fields for insert if provided
                insert_fields = {
                    'account_name': account_name,
                    'year': year,
                    'month': month,
                    'metric_type': metric_type,
                    'value': value,
                    'updated_at': datetime.now()
                }

                if additional_fields:
                    insert_fields.update(additional_fields)

                # Convert any numpy types in additional fields
                for key, val in insert_fields.items():
                    if hasattr(val, 'dtype'):
                        if 'int' in str(val.dtype):
                            insert_fields[key] = int(val)
                        elif 'float' in str(val.dtype):
                            insert_fields[key] = float(val)
                        else:
                            insert_fields[key] = str(val)

                columns = list(insert_fields.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)

                cursor.execute(f"""
                    INSERT INTO {self.table_name} ({column_names})
                    VALUES ({placeholders})
                """, list(insert_fields.values()))

                print(f"Debug: Inserted new record for {account_name} {year} {month} {metric_type} with value {value}")
            else:
                print(f"Debug: Updated existing record for {account_name} {year} {month} {metric_type} with value {value}")

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            print(f"Error updating/inserting record: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return False

    def bulk_update_records(self, updates_list):
        """Perform bulk updates with transaction safety"""
        try:
            # Check production protection for bulk updates
            if len(updates_list) > 10:  # Consider > 10 updates as bulk operation
                allowed, message = check_production_safety("unified_sales_data", "BULK_UPDATE", False)
                if not allowed:
                    print(f"🔒 Bulk update blocked: {message}")
                    return 0

            # Use a timeout to prevent database locking
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()

            success_count = 0

            for i, update in enumerate(updates_list):
                record_id = int(update['id'])  # Convert to regular Python int
                updated_data = update['data']

                # Build the update query
                set_clauses = []
                values = []

                updatable_fields = [
                    'account_name', 'account_track', 'connect_name', 'partner_connect', 'partner_org',
                    'owner', 'source', 'industry', 'region', 'lob', 'offering', 'status', 'duration',
                    'financial_year', 'year', 'month', 'metric_type', 'value'
                ]

                for field in updatable_fields:
                    if field in updated_data:
                        set_clauses.append(f"{field} = %s")
                        values.append(updated_data[field])

                if set_clauses:
                    # Add updated timestamp
                    set_clauses.append("updated_at = %s")
                    values.append(datetime.now())
                    values.append(record_id)

                    query = f"""
                        UPDATE unified_sales_data 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                    """

                    cursor.execute(query, values)
                    rows_affected = cursor.rowcount

                    if rows_affected > 0:
                        success_count += 1

            conn.commit()
            conn.close()

            return success_count

        except Exception as e:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
            return 0

    def add_new_record(self, record_data):
        """Add a new record to the unified table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            # Monitor data creation
            operation_details = {
                'operation': 'add_new_record',
                'table': self.table_name,
                'record_data': str(record_data)[:200],
            }
            if not monitor_data_creation(self.table_name, operation_details, "unified_data_manager.add_new_record"):
                logging.error(f"Data creation blocked for table {self.table_name} by add_new_record")
                return None

            # Calculate month number
            month_number = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
            }.get(record_data.get('month', 'January'), 1)



            cursor.execute("""
                INSERT INTO unified_sales_data 
                (account_name, account_track, connect_name, partner_connect, partner_org, owner, source,
                 industry, region, lob, offering, status, duration, financial_year, year, month, month_number,
                 metric_type, value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_data.get('account_name', ''),
                record_data.get('account_track', ''),
                record_data.get('connect_name', ''),
                record_data.get('partner_connect', ''),
                record_data.get('partner_org', ''),
                record_data.get('owner', ''),
                record_data.get('source', ''),
                record_data.get('industry', ''),
                record_data.get('region', ''),
                record_data.get('lob', ''),
                record_data.get('offering', ''),
                record_data.get('status', 'Active Lead'),
                record_data.get('duration', 0),
                record_data.get('financial_year', ''),
                record_data.get('year', 2025),
                record_data.get('month', 'January'),
                month_number,
                record_data.get('metric_type', 'Planned'),
                record_data.get('value', 0.0)
            ))

            conn.commit()
            new_id = cursor.lastrowid
            conn.close()

            logging.info(f"✅ Legitimate data add completed for {self.table_name}")
            return new_id

        except Exception as e:
            logging.error(f"Error adding new record: {str(e)}")
            try:
                conn.close()
            except:
                pass
            return None

    def save_data(self, data, table_name, if_exists='replace'):
        """Save data to the database with monitoring"""
        try:
            # Monitor data creation
            operation_details = {
                'operation': 'save_data',
                'table': table_name,
                'record_count': len(data) if hasattr(data, '__len__') else 'unknown',
                'if_exists': if_exists,
                'sample_data': str(data.head(2).to_dict()) if hasattr(data, 'head') else str(data)[:200]
            }

            # Check if this operation is authorized
            if not monitor_data_creation(table_name, operation_details, "unified_data_manager.save_data"):
                logging.error(f"Data creation blocked for table {table_name}")
                return False

            table_name = self.env_manager.get_table_name(table_name)
            conn = psycopg2.connect(self.database_url)

            if if_exists == 'replace':
                # Drop and recreate table
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()

            data.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.close()
            logging.info(f"✅ Legitimate data save completed for {table_name}")
            return True
        except Exception as e:
            logging.error(f"Error saving data to {table_name}: {str(e)}")
            return False

    def get_database_stats(self):
        """Get database statistics"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM unified_sales_data")
            total_records = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT account_name) FROM unified_sales_data")
            unique_accounts = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT account_name || '-' || account_track) FROM unified_sales_data")
            unique_tracks = cursor.fetchone()[0]

            conn.close()

            return {
                'total_records': total_records,
                'unique_accounts': unique_accounts,
                'unique_tracks': unique_tracks
            }

        except Exception as e:
            print(f"Error getting database stats: {str(e)}")
            return {}