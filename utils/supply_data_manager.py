"""
Supply Data Manager for FTE and NFTE talent management
Unified database structure with conditional validation for NFTE records
"""

import psycopg2
import pandas as pd
import logging
import os
from datetime import datetime
from .production_data_protection import check_production_safety, safe_table_create

logger = logging.getLogger(__name__)

class SupplyDataManager:
    """Manage unified supply data for both FTE and NFTE talent"""

    def __init__(self, env_manager=None):
        self.database_url = os.environ.get('DATABASE_URL')

        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False

        self.create_tables()

    def get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.use_dev_tables:
            return f"dev_{table_name}"
        return table_name

    def get_connection(self, retries=3):
        """Get database connection with retry logic and connection pooling"""
        import time
        for attempt in range(retries):
            try:
                # Use centralized database connection utility
                from .database_connection import get_database_connection
                conn = get_database_connection()
                conn.autocommit = False
                return conn
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                    time.sleep(0.5)  # Brief delay before retry
                    continue
                else:
                    logger.error(f"All database connection attempts failed: {e}")
                    raise

    def create_tables(self):
        """Create unified supply data table ONLY if it doesn't exist - PRODUCTION SAFE"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'talent_supply'
                );
            """)
            table_exists = cursor.fetchone()[0]

            if table_exists:
                # Table exists - only run safe schema migrations (add columns if needed)
                print("talent_supply table exists - running safe schema updates only")
                self._run_safe_migrations(cursor)
                conn.commit()
                conn.close()
                return

            print("Creating talent_supply table (no existing table detected)")
            # Create unified talent table combining FTE and NFTE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS talent_supply (
                    id SERIAL PRIMARY KEY,
                    talent_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    grade TEXT,
                    doj TEXT,
                    assignment_status TEXT NOT NULL,
                    type TEXT NOT NULL CHECK (type IN ('FTE', 'Non-FTE')),
                    assigned_to TEXT,
                    assignment_percentage REAL DEFAULT 0,
                    availability_percentage REAL DEFAULT 100,
                    employment_status TEXT,
                    email_id TEXT,
                    years_of_exp TEXT,
                    skills TEXT,
                    region TEXT,
                    partner TEXT,
                    billable TEXT,
                    client TEXT,
                    track TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            conn.close()
            print("Supply database tables created successfully")

        except Exception as e:
            print(f"Error creating supply tables: {str(e)}")
            raise

    def _run_safe_migrations(self, cursor):
        """Run safe schema migrations that don't affect existing data"""
        try:
            # Example: Add new columns if they don't exist
            migrations = [
                # Add any new columns here with IF NOT EXISTS pattern
                """
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'talent_supply' AND column_name = 'new_column_example'
                    ) THEN
                        ALTER TABLE talent_supply ADD COLUMN new_column_example TEXT;
                    END IF;
                END $$;
                """,
                # Add more safe migrations here as needed
            ]

            for migration in migrations:
                if migration.strip():  # Skip empty migrations
                    cursor.execute(migration)

            print("Safe schema migrations completed")

        except Exception as e:
            print(f"Error running safe migrations: {str(e)}")
            raise

    def load_csv_data(self, fte_csv_path, nfte_csv_path, force_overwrite=False):
        """Load and merge FTE and NFTE CSV data into unified table

        Args:
            fte_csv_path: Path to FTE CSV file
            nfte_csv_path: Path to NFTE CSV file
            force_overwrite: If True, allows data overwrite (DANGEROUS - only for initial setup)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use production data protection system
            cursor.execute("SELECT COUNT(*) FROM talent_supply")
            existing_records = cursor.fetchone()[0]

            allowed, message = check_production_safety("talent_supply", "CSV_OVERWRITE", force_overwrite, existing_records)

            if not allowed:
                print(f"\n🔒 CSV LOAD BLOCKED - PRODUCTION DATA PROTECTION")
                print(f"📊 Existing talent records: {existing_records}")
                print(f"🚫 {message}")
                print(f"⚠️  To override: use force_overwrite=True parameter")
                conn.close()
                return 0

            # Load FTE data
            fte_df = pd.read_csv(fte_csv_path)
            print(f"Loaded {len(fte_df)} FTE records")

            # Load NFTE data
            nfte_df = pd.read_csv(nfte_csv_path)
            print(f"Loaded {len(nfte_df)} NFTE records")

            # Standardize column names and structure
            fte_standardized = self._standardize_fte_data(fte_df)
            nfte_standardized = self._standardize_nfte_data(nfte_df)

            # Combine datasets
            combined_df = pd.concat([fte_standardized, nfte_standardized], ignore_index=True)

            # Clear existing data ONLY if force_overwrite is True
            if force_overwrite:
                cursor.execute("DELETE FROM talent_supply")

            # Insert combined data
            for _, row in combined_df.iterrows():
                cursor.execute("""
                    INSERT INTO talent_supply (
                        talent_id, name, role, grade, doj, assignment_status, type,
                        assigned_to, assignment_percentage, availability_percentage,
                        employment_status, email_id, years_of_exp, skills, region,
                        partner, billable, client, track
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))

            conn.commit()
            conn.close()

            print(f"Successfully loaded {len(combined_df)} total talent records")
            return len(combined_df)

        except Exception as e:
            print(f"Error loading CSV data: {str(e)}")
            raise

    def _standardize_fte_data(self, df):
        """Standardize FTE data to unified format"""
        standardized = pd.DataFrame()

        standardized['talent_id'] = df['FTE_ID']
        standardized['name'] = df['Name']
        standardized['role'] = df['Role']
        standardized['grade'] = df['Grade']
        standardized['doj'] = df['DoJ']
        standardized['assignment_status'] = df['Assignment Status']
        standardized['type'] = df['Type']
        standardized['assigned_to'] = df['Assigned to']
        standardized['assignment_percentage'] = df['Assignment %']
        standardized['availability_percentage'] = df['Availability %']
        standardized['employment_status'] = df['Employement Status']
        standardized['email_id'] = df['Email IDs']
        standardized['years_of_exp'] = df['Years of exp']
        standardized['skills'] = df['Skills']
        standardized['region'] = df['Region']
        standardized['partner'] = df['Partner']
        standardized['billable'] = df['Billable (Y/N)']
        standardized['client'] = df['Cliient']  # Note: typo in original CSV
        standardized['track'] = df['Track']

        return standardized

    def _standardize_nfte_data(self, df):
        """Standardize NFTE data to unified format"""
        standardized = pd.DataFrame()

        standardized['talent_id'] = df['NFTE_ID']
        standardized['name'] = df['Name']
        standardized['role'] = df['Role']
        standardized['grade'] = df['Grade']
        standardized['doj'] = df['DoJ']
        standardized['assignment_status'] = df['Assignment Status']
        standardized['type'] = df['Type']
        standardized['assigned_to'] = df['Assigned to']
        standardized['assignment_percentage'] = df['Assignment']  # NFTE uses 'Assignment' field
        standardized['availability_percentage'] = df['Availability %']
        standardized['employment_status'] = df['Employement Status']
        standardized['email_id'] = df['Email IDs']
        standardized['years_of_exp'] = df['Years of exp']
        standardized['skills'] = df['Skills']
        standardized['region'] = df['Region']
        standardized['partner'] = df['Partner']
        standardized['billable'] = df['Billable (Y/N)']
        standardized['client'] = df['Client']
        standardized['track'] = df['Track']

        return standardized

    def get_all_talent_data(self):
        """Get all talent data from unified table"""
        try:
            conn = self.get_connection()
            env_table_talent_supply = self.env_manager.get_table_name('talent_supply') if self.env_manager else 'talent_supply'
            query = f"""
                SELECT * FROM {env_table_talent_supply} 
                ORDER BY type, name
            """
            print(f"DEBUG: Executing query: {query}")  # Debug log
            df = pd.read_sql_query(query, conn)
            print(f"DEBUG: Retrieved {len(df)} records, columns: {list(df.columns)}")  # Debug log
            if not df.empty:
                print(f"DEBUG: First few talent_ids: {df['talent_id'].head().tolist()}")  # Debug log
            conn.close()
            return df

        except Exception as e:
            print(f"Error retrieving talent data: {str(e)}")
            return pd.DataFrame()

    def get_talent_by_type(self, talent_type):
        """Get talent data filtered by type (FTE or Non-FTE)"""
        try:
            conn = self.get_connection()
            env_table_talent_supply = self.env_manager.get_table_name('talent_supply') if self.env_manager else 'talent_supply'
            query = """
                SELECT * FROM talent_supply 
                WHERE type = %s
                ORDER BY name
            """
            df = pd.read_sql_query(query, conn, params=[talent_type])
            conn.close()
            return df

        except Exception as e:
            print(f"Error retrieving {talent_type} data: {str(e)}")
            return pd.DataFrame()

    def update_talent_record(self, record_id, updated_data):
        """Update a specific talent record"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Build update query dynamically
            set_clauses = []
            values = []

            for field, value in updated_data.items():
                if field != 'id':  # Don't update ID
                    set_clauses.append(f"{field} = %s")
                    # Convert numpy types to native Python types
                    if hasattr(value, 'item'):  # Check if it's a numpy type
                        values.append(value.item())
                    else:
                        values.append(value)

            if set_clauses:
                values.append(record_id)
                query = """
                    UPDATE talent_supply 
                    SET {}, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """.format(', '.join(set_clauses))
                cursor.execute(query, values)

                rows_affected = cursor.rowcount
                conn.commit()
                conn.close()

                print(f"Updated talent record {record_id}: {rows_affected} rows affected")
                return rows_affected > 0

        except Exception as e:
            print(f"Error updating talent record {record_id}: {str(e)}")
            return False

    def add_talent_record(self, talent_data):
        """Add a new talent record"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO talent_supply (
                    talent_id, name, role, grade, doj, assignment_status, type,
                    assigned_to, assignment_percentage, availability_percentage,
                    employment_status, email_id, years_of_exp, skills, region,
                    partner, billable, client, track
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                talent_data.get('talent_id'),
                talent_data.get('name'),
                talent_data.get('role'),
                talent_data.get('grade', ''),
                talent_data.get('doj', ''),
                talent_data.get('assignment_status'),
                talent_data.get('type'),
                talent_data.get('assigned_to', ''),
                talent_data.get('assignment_percentage', 0),
                talent_data.get('availability_percentage', 100),
                talent_data.get('employment_status', ''),
                talent_data.get('email_id', ''),
                talent_data.get('years_of_exp', ''),
                talent_data.get('skills', ''),
                talent_data.get('region', ''),
                talent_data.get('partner', ''),
                talent_data.get('billable', ''),
                talent_data.get('client', ''),
                talent_data.get('track', '')
            ))

            conn.commit()
            conn.close()

            print(f"Added new talent record: {talent_data.get('talent_id')}")
            return True

        except Exception as e:
            print(f"Error adding talent record: {str(e)}")
            return False

    def validate_nfte_mandatory_fields(self, record):
        """Validate mandatory fields for NFTE records"""
        mandatory_fields = ['partner', 'client', 'assigned_to']
        missing_fields = []

        if record.get('type') == 'Non-FTE':
            for field in mandatory_fields:
                if not record.get(field) or str(record.get(field)).strip() == '':
                    missing_fields.append(field)

        return missing_fields

    def get_all_talent(self):
        """Get all talent data - required by performance manager"""
        return self.get_all_talent_data()

    def get_availability_summary(self):
        """Get availability summary - required by performance manager"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT 
                    assignment_status,
                    COUNT(*) as count,
                    AVG(availability_percentage) as avg_availability
                FROM talent_supply 
                WHERE availability_percentage IS NOT NULL
                GROUP BY assignment_status
                ORDER BY count DESC
            """)

            results = cursor.fetchall()
            conn.close()

            availability_data = []
            for row in results:
                availability_data.append({
                    'status': row[0] or 'Unassigned',
                    'count': row[1],
                    'avg_availability': round(row[2] or 0, 1)
                })

            return availability_data

        except Exception as e:
            logger.error(f"Error getting availability summary: {str(e)}")
            return []

    def get_supply_statistics(self):
        """Get supply statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Total counts
            cursor.execute("SELECT COUNT(*) FROM talent_supply")
            total_talent = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM talent_supply WHERE type = 'FTE'")
            total_fte = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM talent_supply WHERE type = 'Non-FTE'")
            total_nfte = cursor.fetchone()[0]

            # Availability statistics
            cursor.execute("""
                SELECT AVG(availability_percentage) 
                FROM talent_supply 
                WHERE availability_percentage IS NOT NULL
            """)
            avg_availability = cursor.fetchone()[0] or 0

            conn.close()

            return {
                'total_talent': total_talent,
                'total_fte': total_fte,
                'total_nfte': total_nfte,
                'avg_availability': round(avg_availability, 1)
            }

        except Exception as e:
            print(f"Error getting supply statistics: {str(e)}")
            return {}

    def bulk_update_records(self, updates_list):
        """Perform bulk updates with transaction safety"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            updates_applied = 0

            for update in updates_list:
                record_id = update.get('id')
                updated_data = {k: v for k, v in update.items() if k != 'id'}

                # Build update query
                set_clauses = []
                values = []

                for field, value in updated_data.items():
                    # Skip None values and convert empty strings properly
                    if value is None:
                        set_clauses.append(f"{field} = NULL")
                    else:
                        set_clauses.append(f"{field} = %s")
                        # Convert numpy types to native Python types
                        if hasattr(value, 'item'):  # Check if it's a numpy type
                            values.append(value.item())
                        else:
                            values.append(value)

                if set_clauses:
                    if record_id is not None:
                        values.append(record_id)
                        query = """
                            UPDATE talent_supply 
                            SET {}, updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """.format(', '.join(set_clauses))
                        cursor.execute(query, values)
                    else:
                        print(f"Skipping update - no record_id provided: {update}")
                else:
                    print(f"No valid fields to update for record: {update}")

                if set_clauses and record_id is not None:
                    updates_applied += cursor.rowcount

            conn.commit()
            conn.close()

            print(f"Bulk update completed: {updates_applied} records updated")
            return updates_applied

        except Exception as e:
            print(f"Error in bulk update: {str(e)}")
            return 0

    def get_talent_availability(self, name):
        """Get current availability percentage for a talent"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT availability_percentage FROM talent_supply 
                WHERE name = %s
            """, (name,))

            result = cursor.fetchone()
            conn.close()

            if result:
                return float(result[0])
            else:
                print(f"Talent not found: {name}")
                return 0.0

        except Exception as e:
            print(f"Error getting talent availability: {str(e)}")
            return 0.0

    def recalculate_availability(self, talent_name):
        """
        Recalculate availability and assignment status for a talent based on all their assignments
        Returns remaining availability = 100% - total assigned percentage
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get talent ID from name
            cursor.execute("SELECT id FROM talent_supply WHERE name = %s", (talent_name,))
            talent_result = cursor.fetchone()
            if not talent_result:
                conn.close()
                return {'success': False, 'error': f'Talent {talent_name} not found'}

            talent_id = talent_result[0]

            # Get existing assignment percentage from talent_supply table (legacy data)
            cursor.execute("""
                SELECT COALESCE(assignment_percentage, 0) as existing_assigned
                FROM talent_supply 
                WHERE id = %s
            """, (talent_id,))

            existing_assigned_result = cursor.fetchone()
            existing_assigned = float(existing_assigned_result[0]) if existing_assigned_result else 0.0

            # Calculate total assigned percentage from new demand_supply_assignments table
            cursor.execute("""
                SELECT COALESCE(SUM(assignment_percentage), 0) as new_assigned
                FROM demand_supply_assignments 
                WHERE talent_id = %s AND status = 'Active'
            """, (talent_id,))

            new_assigned_result = cursor.fetchone()
            new_assigned = float(new_assigned_result[0]) if new_assigned_result else 0.0

            # Option A: Use ONLY new assignments for availability calculation
            # Legacy assignments are kept separate in assignment_percentage column
            # New assignments determine current availability

            # Calculate remaining availability: Base 100% - ONLY New Assigned
            remaining_availability = max(0, 100.0 - new_assigned)

            # Determine assignment status based on ONLY new assignments
            if new_assigned == 0:
                assignment_status = "Beach"
            elif new_assigned >= 100:
                assignment_status = "Allocated"
            else:
                assignment_status = "Partially Allocated"

            # Update talent_supply table - keep legacy assignments unchanged, update availability based on new assignments only
            cursor.execute("""
                UPDATE talent_supply 
                SET availability_percentage = %s,
                    assignment_status = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (remaining_availability, assignment_status, talent_id))

            conn.commit()
            conn.close()

            print(f"Recalculated availability for {talent_name} (Option A): Legacy={existing_assigned:.1f}% (kept separate), New={new_assigned:.1f}% assigned, {remaining_availability:.1f}% remaining, status: {assignment_status}")

            return {
                'success': True,
                'talent_name': talent_name,
                'legacy_assigned': existing_assigned,
                'new_assigned': new_assigned,
                'remaining_availability': remaining_availability,
                'assignment_status': assignment_status
            }

        except Exception as e:
            print(f"Error recalculating availability for {talent_name}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def recalculate_all_availability(self):
        """Recalculate availability for all talent in the database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get all talent names
            cursor.execute("SELECT name FROM talent_supply")
            all_talent = cursor.fetchall()
            conn.close()

            updated_count = 0
            errors = []

            for talent_row in all_talent:
                talent_name = talent_row[0]
                result = self.recalculate_availability(talent_name)
                if result['success']:
                    updated_count += 1
                else:
                    errors.append(f"{talent_name}: {result.get('error', 'Unknown error')}")

            return {
                'success': True,
                'updated_count': updated_count,
                'total_talent': len(all_talent),
                'errors': errors
            }

        except Exception as e:
            print(f"Error recalculating all availability: {str(e)}")
            return {'success': False, 'error': str(e)}

    def update_availability_direct(self, name, new_availability):
        """Update availability directly to a specific value"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Update availability directly
            cursor.execute("""
                UPDATE talent_supply 
                SET availability_percentage = %s 
                WHERE name = %s
            """, (new_availability, name))

            conn.commit()
            conn.close()
            print(f"Updated availability for {name} to {new_availability}%")
            return True

        except Exception as e:
            print(f"Error updating availability directly: {str(e)}")
            return False

    def calculate_talent_assignments(self, talent_name):
        """Calculate total assignment percentage for a talent from all active assignments"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get all active assignments for this talent
            cursor.execute("""
                SELECT SUM(assignment_percentage) as total_assigned
                FROM demand_supply_assignments 
                WHERE talent_id IN (SELECT id FROM talent_supply WHERE name = %s)
                AND (end_date IS NULL OR end_date >= CURRENT_DATE)
            """, (talent_name,))

            result = cursor.fetchone()
            total_assigned = float(result[0]) if result and result[0] else 0.0

            cursor.close()
            conn.close()

            return total_assigned

        except Exception as e:
            print(f"Error calculating assignments for {talent_name}: {e}")
            return 0.0

    def get_talent_base_availability(self, talent_name):
        """Get the original/base availability percentage for a talent (assuming 100% base capacity)"""
        # For now, we'll assume base capacity is 100% for all talent
        # This could be enhanced to store original capacity in a separate field
        return 100.0



    def initialize_all_talent_availability(self):
        """Initialize availability for all talent based on current assignments"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get all talent names
            cursor.execute("SELECT name FROM talent_supply")
            talent_names = [row[0] for row in cursor.fetchall()]

            conn.close()

            updated_count = 0
            for talent_name in talent_names:
                result = self.recalculate_availability(talent_name)
                if result['success']:
                    updated_count += 1

            print(f"Initialized availability for {updated_count} out of {len(talent_names)} talent records")
            return {
                'success': True,
                'total_talent': len(talent_names),
                'updated_count': updated_count
            }

        except Exception as e:
            print(f"Error initializing talent availability: {e}")
            return {'success': False, 'error': str(e)}