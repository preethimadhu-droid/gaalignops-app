"""
Staffing Plans Management System
Comprehensive staffing forecasting with pipeline integration
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import os
import json
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.express as px
import logging

logger = logging.getLogger(__name__)

class StaffingPlansManager:
    """Manage staffing plans with pipeline forecasting integration"""

    def __init__(self, env_manager=None):
        self.db_url = os.getenv("DATABASE_URL")

        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False

        self._ensure_staffing_tables()

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
                # Add connection timeout and keepalive settings for better stability
                conn = psycopg2.connect(
                    self.db_url,
                    connect_timeout=10,
                    keepalives_idle=600,
                    keepalives_interval=30,
                    keepalives_count=3
                )
                conn.autocommit = False
                return conn
            except psycopg2.OperationalError as e:
                if attempt < retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                    time.sleep(0.5)  # Brief delay before retry
                    continue
                else:
                    logger.error(f"All database connection attempts failed: {e}")
                    raise

    def _ensure_staffing_tables(self):
        """Ensure staffing plans tables exist"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Staffing plans table should already exist from create_pipeline_tables.py
            # Just ensure it has the correct structure
            pass

            # Create staffing_requirements table for detailed forecasting
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staffing_requirements (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    stage_name VARCHAR(255) NOT NULL,
                    required_candidates INTEGER NOT NULL,
                    milestone_date DATE NOT NULL,
                    current_candidates INTEGER DEFAULT 0,
                    calculated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES staffing_plans(id) ON DELETE CASCADE
                )
            """)

            # Create pipeline_planning_details table for storing detailed planning data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_planning_details (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    role VARCHAR(255) NOT NULL,
                    skills TEXT,
                    positions INTEGER NOT NULL DEFAULT 1,
                    onboard_by DATE,
                    pipeline_id INTEGER,
                    pipeline_owner VARCHAR(255),
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES staffing_plans(id) ON DELETE CASCADE,
                    FOREIGN KEY (pipeline_id) REFERENCES talent_pipelines(id) ON DELETE SET NULL
                )
            """)

            # Add pipeline_owner column if it doesn't exist (for existing installations)
            try:
                cursor.execute("""
                    ALTER TABLE pipeline_planning_details 
                    ADD COLUMN IF NOT EXISTS pipeline_owner VARCHAR(255)
                """)
            except Exception:
                pass  # Column might already exist

            # Create pipeline_requirements_actual table for storing actual stage data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_requirements_actual (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    role VARCHAR(255) NOT NULL,
                    stage_name VARCHAR(255) NOT NULL,
                    actual_at_stage INTEGER DEFAULT 0,
                    profiles_in_pipeline INTEGER NOT NULL,
                    needed_by_date DATE NOT NULL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES staffing_plans(id) ON DELETE CASCADE,
                    UNIQUE (plan_id, role, stage_name)
                )
            """)

            # Create pipeline_plan_actuals table for storing pipeline plan actual numbers
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_plan_actuals (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    stage VARCHAR(255) NOT NULL,
                    actual_num INTEGER DEFAULT 0,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES staffing_plans(id) ON DELETE CASCADE,
                    UNIQUE(plan_id, stage)
                )
            """)

            # Create staffing_plan_generated_plans table for storing generated pipeline plans
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staffing_plan_generated_plans (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    role VARCHAR(255) NOT NULL,
                    pipeline_id INTEGER,
                    pipeline_name VARCHAR(255),
                    generated_data JSONB,
                    created_by VARCHAR(255),
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES staffing_plans(id) ON DELETE CASCADE
                )
            """)

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error creating staffing tables: {str(e)}")

    def save_pipeline_requirements_actual(self, plan_id, role, stage_data):
        """Save actual pipeline requirements data with new columns"""
        try:
            logger.info(f"SAVE DEBUG: Starting save for plan_id={plan_id}, role={role}")
            logger.info(f"SAVE DEBUG: Number of stages to save: {len(stage_data)}")

            conn = self.get_connection()
            if conn is None:
                logger.error("SAVE DEBUG: Failed to get database connection")
                return False

            cursor = conn.cursor()

            # Add new columns if they don't exist
            try:
                cursor.execute("""
                    ALTER TABLE pipeline_requirements_actual 
                    ADD COLUMN IF NOT EXISTS actual_converted INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS actual_conversion_pct DECIMAL(5,2) DEFAULT 0.00
                """)
            except Exception as e:
                logger.warning(f"Column addition warning (may already exist): {str(e)}")

            saved_count = 0
            for i, stage_info in enumerate(stage_data):
                logger.info(f"SAVE DEBUG: Processing stage {i+1}/{len(stage_data)}: {stage_info['stage_name']}")
                logger.info(f"SAVE DEBUG: actual_at_stage={stage_info['actual_at_stage']}, actual_converted={stage_info.get('actual_converted', 0)}")

                try:
                    cursor.execute("""
                        INSERT INTO pipeline_requirements_actual 
                        (plan_id, role, stage_name, actual_at_stage, actual_converted, profiles_in_pipeline, needed_by_date, updated_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (plan_id, role, stage_name) 
                        DO UPDATE SET 
                            actual_at_stage = EXCLUDED.actual_at_stage,
                            actual_converted = EXCLUDED.actual_converted,
                            profiles_in_pipeline = EXCLUDED.profiles_in_pipeline,
                            needed_by_date = EXCLUDED.needed_by_date,
                            updated_date = CURRENT_TIMESTAMP
                    """, (
                        plan_id, 
                        role, 
                        stage_info['stage_name'],
                        stage_info['actual_at_stage'],
                        stage_info.get('actual_converted', 0),
                        stage_info['profiles_in_pipeline'],
                        stage_info['needed_by_date']
                    ))
                    saved_count += 1
                    logger.info(f"SAVE DEBUG: Successfully processed stage {stage_info['stage_name']}")

                except Exception as stage_error:
                    logger.error(f"SAVE DEBUG: Error saving stage {stage_info['stage_name']}: {str(stage_error)}")
                    logger.error(f"SAVE DEBUG: Stage data: {stage_info}")
                    raise stage_error

            conn.commit()
            conn.close()
            logger.info(f"SAVE DEBUG: Successfully saved {saved_count} stages for role {role}")
            return True

        except Exception as e:
            logger.error(f"Error saving pipeline requirements actual data: {str(e)}")
            logger.error(f"SAVE DEBUG: Full error details - plan_id={plan_id}, role={role}")
            import traceback
            logger.error(f"SAVE DEBUG: Traceback: {traceback.format_exc()}")
            return False

    def get_pipeline_requirements_actual(self, plan_id, role):
        """Get actual pipeline requirements data with new columns"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT stage_name, actual_at_stage, COALESCE(actual_converted, 0) as actual_converted, 
                       profiles_in_pipeline, needed_by_date
                FROM pipeline_requirements_actual
                WHERE plan_id = %s AND role = %s
                ORDER BY needed_by_date
            """, (plan_id, role))

            results = cursor.fetchall()
            conn.close()

            actual_data = {}
            for row in results:
                stage_name, actual_at_stage, actual_converted, profiles_in_pipeline, needed_by_date = row
                actual_data[stage_name] = {
                    'actual_at_stage': actual_at_stage,
                    'actual_converted': actual_converted,
                    'profiles_in_pipeline': profiles_in_pipeline,
                    'needed_by_date': needed_by_date
                }

            return actual_data
        except Exception as e:
            logger.error(f"Error getting pipeline requirements actual data: {str(e)}")
            return {}

    def calculate_pipeline_health(self, actual_at_stage, profiles_in_pipeline, needed_by_date):
        """Calculate pipeline health based on actual vs required and dates"""
        from datetime import date

        if profiles_in_pipeline == 0:
            return "Red", "No profiles required"

        # Calculate percentage
        percentage = (actual_at_stage / profiles_in_pipeline) * 100

        # Check if past due date
        current_date = date.today()
        is_past_due = current_date > needed_by_date

        # Updated logic: Past due only shows red if actual != required
        if is_past_due and actual_at_stage != profiles_in_pipeline:
            return "Red", f"Past due date ({needed_by_date})"
        elif is_past_due and actual_at_stage == profiles_in_pipeline:
            # Past due but target met - use percentage logic
            if percentage < 50:
                return "Red", f"{percentage:.1f}% (<50%)"
            elif percentage <= 80:
                return "Amber", f"{percentage:.1f}% (50-80%)"
            else:
                return "Green", f"{percentage:.1f}% (Target met)"
        elif percentage < 50:
            return "Red", f"{percentage:.1f}% (<50%)"
        elif percentage <= 80:
            return "Amber", f"{percentage:.1f}% (50-80%)"
        else:
            return "Green", f"{percentage:.1f}% (>80%)"

    def get_all_staffing_plans(self):
        """Get all staffing plans with client and pipeline info and candidate status counts"""
        try:
            conn = self.get_connection()
            query = f"""
                SELECT 
                    sp.*,
                    CASE 
                        WHEN sp.client_id IS NULL THEN sp.plan_name || ' Client'
                        ELSE COALESCE(mc.client_name, 'Unknown Client')
                    END as client_name,
                    tp.name as pipeline_name,
                    ROUND((sp.staffed_positions::DECIMAL / sp.planned_positions) * 100, 1) as completion_percentage
                FROM {self.get_table_name('staffing_plans')} sp
                LEFT JOIN {self.get_table_name('master_clients')} mc ON sp.client_id = mc.master_client_id
                LEFT JOIN {self.get_table_name('talent_pipelines')} tp ON sp.pipeline_id = tp.id
                ORDER BY sp.created_date DESC
            """
            logger.info(f"RETRIEVE DEBUG: Executing query: {query}")
            df = pd.read_sql_query(query, conn)
            logger.info(f"RETRIEVE DEBUG: Retrieved {len(df)} rows from database")
            conn.close()

            # Fill NaN client_names with "Unknown Client" for display
            if not df.empty and 'client_name' in df.columns:
                df['client_name'] = df['client_name'].fillna('Unknown Client')
                logger.info(f"RETRIEVE DEBUG: DataFrame columns: {list(df.columns)}")
                logger.info(f"RETRIEVE DEBUG: First row data: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")

            return df
        except Exception as e:
            logger.error(f"RETRIEVE ERROR: {str(e)}")
            import traceback
            logger.error(f"RETRIEVE TRACEBACK: {traceback.format_exc()}")
            return pd.DataFrame()

    def get_roles_count(self, plan_id):
        """Get the count of roles linked to a staffing plan"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT COUNT(DISTINCT role) as roles_count
                FROM {self.get_table_name('pipeline_planning_details')}
                WHERE plan_id = %s AND role IS NOT NULL AND role != ''
            """, (plan_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting roles count for plan {plan_id}: {str(e)}")
            return 0

    def get_staffing_plan(self, plan_id):
        """Get a specific staffing plan by ID with data integrity check"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                SELECT 
                    sp.*,
                    mc.client_name,
                    tp.name as pipeline_name
                FROM {self.get_table_name('staffing_plans')} sp
                LEFT JOIN {self.get_table_name('master_clients')} mc ON sp.client_id = mc.master_client_id
                LEFT JOIN {self.get_table_name('talent_pipelines')} tp ON sp.pipeline_id = tp.id
                WHERE sp.id = %s
            """, (plan_id,))

            result = cursor.fetchone()

            if result:
                columns = [desc[0] for desc in cursor.description]
                plan_data = dict(zip(columns, result))

                # Data integrity check: if client_id exists but client_name is None, clear the client_id
                if plan_data.get('client_id') and not plan_data.get('client_name'):
                    logger.warning(f"Plan {plan_id} has orphaned client_id {plan_data['client_id']} - clearing reference")
                    cursor.execute(f"""
                        UPDATE {self.get_table_name('staffing_plans')} 
                        SET client_id = NULL 
                        WHERE id = %s
                    """, (plan_id,))
                    conn.commit()
                    plan_data['client_id'] = None
                    plan_data['client_name'] = None

                conn.close()
                return plan_data

            conn.close()
            return None

        except Exception as e:
            logger.error(f"Error getting staffing plan {plan_id}: {str(e)}")
            return None

    def create_staffing_plan(self, plan_data, staffing_plan_rows=None):
        """Create a new staffing plan with role details"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get client_id from master_clients table
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", 
                         (plan_data['client_name'],))
            client_result = cursor.fetchone()
            client_id = client_result[0] if client_result else None

            cursor.execute(f"""
                INSERT INTO {self.get_table_name('staffing_plans')} 
                (plan_name, client_id, planned_positions, target_start_date, target_end_date, target_hires, pipeline_id, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                plan_data['plan_name'],
                client_id,
                plan_data['planned_positions'],
                plan_data['from_date'],
                plan_data['to_date'],
                plan_data.get('target_hires', 0),
                plan_data.get('pipeline_id'),
                datetime.now()
            ))

            plan_id = cursor.fetchone()[0]

            # Activate the main pipeline if one is specified in the staffing plan
            if plan_data.get('pipeline_id'):
                try:
                    from utils.pipeline_manager import PipelineManager
                    pipeline_manager = PipelineManager()
                    pipeline_manager.activate_pipeline(plan_data['pipeline_id'])
                    logger.info(f"Main pipeline {plan_data['pipeline_id']} activated due to Supply Plan creation")
                except Exception as activation_error:
                    logger.warning(f"Failed to activate main pipeline {plan_data['pipeline_id']}: {activation_error}")

            # Save staffing plan row details if provided
            if staffing_plan_rows:
                self._save_staffing_plan_rows(cursor, plan_id, staffing_plan_rows, plan_data['client_name'])

            conn.commit()
            conn.close()
            return plan_id

        except Exception as e:
            logger.error(f"Error creating staffing plan: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def _save_staffing_plan_rows(self, cursor, plan_id, staffing_plan_rows, client_name):
        """Save staffing plan row details to database"""
        try:
            current_time = datetime.now()

            for row in staffing_plan_rows:
                # Get pipeline_id from pipeline name
                pipeline_id = None
                if row.get('pipeline') and row['pipeline'] != "-- Select a pipeline --":
                    cursor.execute(f"""
                        SELECT tp.id FROM {self.get_table_name('talent_pipelines')} tp 
                        JOIN {self.get_table_name('master_clients')} mc ON tp.client_id = mc.master_client_id 
                        WHERE tp.name = %s AND mc.client_name = %s
                    """, (row['pipeline'], client_name))
                    pipeline_result = cursor.fetchone()
                    if pipeline_result:
                        pipeline_id = pipeline_result[0]

                cursor.execute(f"""
                    INSERT INTO {self.get_table_name('pipeline_planning_details')} 
                    (plan_id, role, skills, positions, onboard_by, pipeline_id, pipeline_owner, created_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    plan_id, row['role'], row['skills'], row['positions'],
                    row['staffed_by_date'], pipeline_id, row['owner'], current_time
                ))

                # Activate the pipeline when it gets linked to a Supply Plan
                if pipeline_id:
                    try:
                        from utils.pipeline_manager import PipelineManager
                        pipeline_manager = PipelineManager(self.env_manager)
                        pipeline_manager.activate_pipeline(pipeline_id)
                        logger.info(f"Pipeline {pipeline_id} activated due to Supply Plan linkage")
                    except Exception as activation_error:
                        logger.warning(f"Failed to activate pipeline {pipeline_id}: {activation_error}")

        except Exception as e:
            logger.error(f"Error saving staffing plan rows: {str(e)}")
            raise e

    def delete_staffing_plan(self, plan_id):
        """Delete a staffing plan and handle pipeline deactivation"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get all pipeline IDs linked to this plan before deletion
            cursor.execute(f"""
                SELECT DISTINCT pipeline_id FROM {self.get_table_name('pipeline_planning_details')} 
                WHERE plan_id = %s AND pipeline_id IS NOT NULL
            """, (plan_id,))
            linked_pipeline_ids = [row[0] for row in cursor.fetchall()]

            # Delete plan details first
            cursor.execute(f"""
                DELETE FROM {self.get_table_name('pipeline_planning_details')} 
                WHERE plan_id = %s
            """, (plan_id,))

            # Delete generated plans
            cursor.execute(f"""
                DELETE FROM {self.get_table_name('staffing_plan_generated_plans')} 
                WHERE plan_id = %s
            """, (plan_id,))

            # Delete the main plan
            cursor.execute(f"""
                DELETE FROM {self.get_table_name('staffing_plans')} 
                WHERE id = %s
            """, (plan_id,))

            conn.commit()

            # Check and deactivate pipelines that no longer have any linked plans
            if linked_pipeline_ids:
                try:
                    from utils.pipeline_manager import PipelineManager
                    pipeline_manager = PipelineManager(self.env_manager)

                    for pipeline_id in linked_pipeline_ids:
                        pipeline_manager.check_and_deactivate_pipeline(pipeline_id)
                        logger.info(f"Checked pipeline {pipeline_id} for deactivation after plan deletion")

                except Exception as deactivation_error:
                    logger.warning(f"Failed to check pipeline deactivation: {deactivation_error}")

            conn.close()
            logger.info(f"Successfully deleted staffing plan {plan_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting staffing plan {plan_id}: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def save_generated_pipeline_plan(self, plan_id, role_detail_id, pipeline_stages):
        """Save generated pipeline plan to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Clear existing generated pipeline data for this role
            cursor.execute(f"""
                DELETE FROM {self.get_table_name('staffing_plan_generated_pipelines')} 
                WHERE plan_id = %s AND role_detail_id = %s
            """, (plan_id, role_detail_id))

            # Insert new pipeline stages
            for stage in pipeline_stages:
                cursor.execute(f"""
                    INSERT INTO {self.get_table_name('staffing_plan_generated_pipelines')}
                    (plan_id, role_detail_id, stage_name, candidates_needed, timeline_date, conversion_rate, tat_days)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    plan_id, role_detail_id, stage['stage_name'], stage['candidates_needed'],
                    stage['timeline_date'], stage.get('conversion_rate'), stage.get('tat_days')
                ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving generated pipeline plan: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def get_staffing_plan_rows(self, plan_id):
        """Get staffing plan row details from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                SELECT ppd.*, tp.name as pipeline_name
                FROM {self.get_table_name('pipeline_planning_details')} ppd
                LEFT JOIN {self.get_table_name('talent_pipelines')} tp ON ppd.pipeline_id = tp.id
                WHERE ppd.plan_id = %s
                ORDER BY ppd.id
            """, (plan_id,))

            results = cursor.fetchall()
            conn.close()

            rows = []
            for row in results:
                rows.append({
                    'id': row[0],
                    'role': row[2],
                    'skills': row[3] or "",
                    'positions': row[4],
                    'staffed_by_date': row[5],
                    'pipeline': row[9] if row[9] else "-- Select a pipeline --",
                    'owner': row[8] or ""
                })

            return rows

        except Exception as e:
            logger.error(f"Error getting staffing plan rows: {str(e)}")
            return []

    def get_generated_pipeline_plan(self, plan_id, role_detail_id):
        """Get generated pipeline plan from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                SELECT stage_name, candidates_needed, timeline_date, conversion_rate, tat_days
                FROM {self.get_table_name('staffing_plan_generated_pipelines')}
                WHERE plan_id = %s AND role_detail_id = %s
                ORDER BY timeline_date
            """, (plan_id, role_detail_id))

            results = cursor.fetchall()
            conn.close()

            pipeline_stages = []
            for row in results:
                pipeline_stages.append({
                    'stage_name': row[0],
                    'candidates_needed': row[1],
                    'timeline_date': row[2],
                    'conversion_rate': row[3],
                    'tat_days': row[4]
                })

            return pipeline_stages

        except Exception as e:
            logger.error(f"Error getting generated pipeline plan: {str(e)}")
            return []

    def get_generated_plans_for_plan(self, plan_id):
        """Get all generated pipeline plans for a staffing plan from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                SELECT role, pipeline_id, pipeline_name, generated_data, pipeline_owner
                FROM {self.get_table_name('staffing_plan_generated_plans')}
                WHERE plan_id = %s
                ORDER BY created_date
            """, (plan_id,))

            results = cursor.fetchall()
            conn.close()

            generated_plans = []
            for row in results:
                # Parse the stored stages data (it's stored as JSON)
                try:
                    import json
                    stages_data = row[3]
                    if isinstance(stages_data, str):
                        stages = json.loads(stages_data)
                    elif isinstance(stages_data, list):
                        stages = stages_data
                    else:
                        stages = []
                except Exception as parse_error:
                    logger.error(f"Error parsing stages data: {parse_error}")
                    stages = []

                generated_plans.append({
                    'role': row[0],
                    'pipeline_id': row[1], 
                    'pipeline_name': row[2],
                    'stages': stages,
                    'pipeline_results': stages,  # Add this key that display logic expects
                    'pipeline_owner': row[4]
                })

            logger.info(f"Retrieved {len(generated_plans)} generated plans from database for plan {plan_id}")
            return generated_plans

        except Exception as e:
            logger.error(f"Error getting generated plans for plan {plan_id}: {str(e)}")
            return []

    def save_generated_plans_to_database(self, plan_id, generated_pipeline_data):
        """Save generated pipeline plans to the database"""
        conn = None
        try:
            # CRITICAL DEBUG: Add print statements to ensure visibility
            print("="*80)
            print(f"PIPELINE SAVE START: plan_id={plan_id}, data_count={len(generated_pipeline_data)}")
            print("="*80)

            with open("debug_save_flow.txt", "a") as f:
                f.write(f"\nPIPELINE SAVE START: plan_id={plan_id}, data_count={len(generated_pipeline_data)}\n")

            logger.info(f"SAVE DEBUG: Starting to save {len(generated_pipeline_data)} plans for plan_id {plan_id}")
            logger.info(f"SAVE DEBUG: Generated data structure: {generated_pipeline_data}")
            logger.info(f"SAVE DEBUG: Environment check - env_manager: {self.env_manager}")
            logger.info(f"SAVE DEBUG: Environment check - use_dev_tables: {self.use_dev_tables}")

            # Additional validation logging
            for i, data in enumerate(generated_pipeline_data):
                logger.info(f"SAVE DEBUG: Item {i} - role: {data.get('role')}, pipeline_id: {data.get('pipeline_id')}, pipeline_name: {data.get('pipeline_name')}")
                logger.info(f"SAVE DEBUG: Item {i} - stages type: {type(data.get('stages'))}, stages length: {len(data.get('stages', []))}")
                if data.get('stages'):
                    logger.info(f"SAVE DEBUG: Item {i} - first stage sample: {data['stages'][0] if data['stages'] else 'None'}")

            # Get database connection
            conn = self.get_connection()
            cursor = conn.cursor()
            logger.info("SAVE DEBUG: Database connection established successfully")

            # Clear existing generated plans for this staffing plan
            table_name = self.get_table_name('staffing_plan_generated_plans')
            logger.info(f"SAVE DEBUG: Using table name: {table_name}")
            logger.info(f"SAVE DEBUG: get_table_name logic - use_dev_tables: {self.use_dev_tables}")

            # Double-check foreign key target table exists
            if self.use_dev_tables:
                cursor.execute("SELECT COUNT(*) FROM dev_staffing_plans WHERE id = %s", (plan_id,))
            else:
                cursor.execute("SELECT COUNT(*) FROM staffing_plans WHERE id = %s", (plan_id,))
            plan_exists = cursor.fetchone()[0]
            logger.info(f"SAVE DEBUG: Plan {plan_id} exists in {'dev_' if self.use_dev_tables else ''}staffing_plans: {plan_exists > 0}")

            cursor.execute(f"""
                DELETE FROM {table_name} 
                WHERE plan_id = %s
            """, (plan_id,))
            logger.info(f"SAVE DEBUG: Cleared existing plans for plan_id {plan_id}")

            # Save each generated plan
            saved_count = 0
            for i, plan_data in enumerate(generated_pipeline_data):
                logger.info(f"SAVE DEBUG: Processing plan {i+1}: {plan_data}")

                # Prepare the data for insertion - be more flexible about data structure
                stages_data = plan_data.get('stages', [])
                if not stages_data:
                    stages_data = plan_data.get('pipeline_results', [])

                # If still no stages_data, try to use the whole plan_data as stages
                if not stages_data and isinstance(plan_data, dict):
                    # Check if plan_data itself contains stage information
                    if 'stage_name' in plan_data or any('stage' in str(k).lower() for k in plan_data.keys()):
                        stages_data = [plan_data]  # Wrap single stage in list

                logger.info(f"SAVE DEBUG: Plan {i+1} final stages_data: {stages_data}")

                # Only skip if absolutely no stage data
                if not stages_data:
                    logger.warning(f"SAVE DEBUG: No stage data found for plan {i+1}, skipping")
                    continue

                # Custom JSON encoder to handle date objects
                def json_serial(obj):
                    """JSON serializer for objects not serializable by default json code"""
                    if isinstance(obj, (datetime, date)):
                        return obj.isoformat()
                    raise TypeError(f"Type {type(obj)} not serializable")

                # Prepare insert values with detailed logging
                insert_values = (
                    plan_id,
                    plan_data.get('role', ''),
                    plan_data.get('pipeline_id'),
                    plan_data.get('pipeline_name', ''),
                    json.dumps(stages_data, default=json_serial),  # Store pipeline stages as JSON with date handling
                    plan_data.get('created_by', 'admin'),
                    datetime.now(),
                    plan_data.get('pipeline_owner', '')
                )

                logger.info(f"SAVE DEBUG: Step 5.{i+1} - Preparing INSERT for plan {i+1}")
                logger.info(f"SAVE DEBUG: Insert values: plan_id={insert_values[0]}, role='{insert_values[1]}', pipeline_id={insert_values[2]}")
                logger.info(f"SAVE DEBUG: Insert values: pipeline_name='{insert_values[3]}', pipeline_owner='{insert_values[7]}'")
                logger.info(f"SAVE DEBUG: Insert values: JSON data length={len(insert_values[4])}")
                logger.info(f"SAVE DEBUG: JSON data preview: {insert_values[4][:200]}...")

                try:
                    cursor.execute(f"""
                        INSERT INTO {table_name}
                        (plan_id, role, pipeline_id, pipeline_name, generated_data, created_by, created_date, pipeline_owner)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, insert_values)
                    saved_count += 1
                    logger.info(f"SAVE DEBUG: Step 5.{i+1} ✓ - Successfully inserted plan {i+1}")
                except Exception as insert_error:
                    logger.error(f"SAVE DEBUG: Step 5.{i+1} ❌ - INSERT failed: {str(insert_error)}")
                    logger.error(f"SAVE DEBUG: Insert error type: {type(insert_error).__name__}")
                    raise insert_error

            logger.info(f"SAVE DEBUG: Step 6 - Committing transaction with {saved_count} saved plans")
            conn.commit()
            logger.info(f"SAVE DEBUG: Step 6 ✓ - Transaction committed successfully")

            logger.info(f"SAVE DEBUG: Step 7 - Closing database connection")
            conn.close()
            logger.info(f"SAVE DEBUG: Step 7 ✓ - Database connection closed")

            logger.info(f"SAVE DEBUG: ===== SAVE PROCESS COMPLETED SUCCESSFULLY =====")
            logger.info(f"SAVE DEBUG: Final result: {saved_count} generated plans saved to database")
            return True

        except Exception as e:
            import traceback

            # CRITICAL DEBUG: Ensure we see the failure
            print("="*80)
            print(f"CRITICAL SAVE FAILURE: {str(e)}")
            print(f"Exception type: {type(e).__name__}")
            print("="*80)

            with open("debug_save_flow.txt", "a") as f:
                f.write(f"\nCRITICAL SAVE FAILURE: {str(e)}\n")
                f.write(f"Exception type: {type(e).__name__}\n")
                f.write(f"Full traceback: {traceback.format_exc()}\n")

            logger.error(f"SAVE DEBUG: Error saving generated pipeline plans: {str(e)}")
            logger.error(f"SAVE DEBUG: Exception type: {type(e).__name__}")
            logger.error(f"SAVE DEBUG: Full traceback: {traceback.format_exc()}")
            logger.error(f"SAVE DEBUG: Plan ID: {plan_id}, Data count: {len(generated_pipeline_data) if generated_pipeline_data else 0}")
            # Log the problematic data structure
            if generated_pipeline_data:
                logger.error(f"SAVE DEBUG: Generated data sample: {generated_pipeline_data[0]}")
                logger.error(f"SAVE DEBUG: Generated data keys: {list(generated_pipeline_data[0].keys()) if generated_pipeline_data[0] else 'No keys'}")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            return False

    def update_staffing_plan(self, plan_id, plan_data, staffing_plan_rows=None):
        """Update an existing staffing plan with role details"""
        logger.info(f"UPDATE STAFFING PLAN CALLED - Plan ID: {plan_id}, Data keys: {list(plan_data.keys())}")
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Debug logging to see what we receive
            logger.info(f"Updating plan {plan_id} with data: {plan_data}")

            # Check if we have client_name in plan_data
            if 'client_name' not in plan_data:
                logger.error(f"Missing client_name in plan_data. Keys available: {list(plan_data.keys())}")
                raise Exception(f"Missing client_name in plan_data. Keys available: {list(plan_data.keys())}")

            # Get client_id from master_clients table (using correct environment table)
            logger.info(f"Looking up client: '{plan_data['client_name']}' in table {self.get_table_name('master_clients')}")
            cursor.execute(f"SELECT master_client_id FROM {self.get_table_name('master_clients')} WHERE client_name = %s", 
                         (plan_data['client_name'],))
            client_result = cursor.fetchone()
            client_id = client_result[0] if client_result else None

            if client_id is None:
                logger.error(f"Client not found: '{plan_data['client_name']}' - available clients:")
                cursor.execute(f"SELECT client_name FROM {self.get_table_name('master_clients')} LIMIT 10")
                available_clients = cursor.fetchall()
                logger.error(f"Available clients: {[c[0] for c in available_clients]}")
                return False
            else:
                logger.info(f"Found client_id {client_id} for client '{plan_data['client_name']}'")

            cursor.execute(f"""
                UPDATE {self.get_table_name('staffing_plans')} 
                SET plan_name = %s, client_id = %s, planned_positions = %s, 
                    target_start_date = %s, target_end_date = %s, target_hires = %s, pipeline_id = %s
                WHERE id = %s
            """, (
                plan_data.get('plan_name', ''),
                client_id,
                plan_data.get('planned_positions', 0),
                plan_data.get('target_start_date') or plan_data.get('from_date'),
                plan_data.get('target_end_date') or plan_data.get('to_date'),
                plan_data.get('target_hires', 0),
                plan_data.get('pipeline_id'),
                plan_id
            ))

            # Clear existing role details and save new ones
            if staffing_plan_rows is not None:
                cursor.execute(f"DELETE FROM {self.get_table_name('pipeline_planning_details')} WHERE plan_id = %s", (plan_id,))
                cursor.execute(f"DELETE FROM {self.get_table_name('staffing_plan_generated_pipelines')} WHERE plan_id = %s", (plan_id,))

                if staffing_plan_rows:
                    self._save_staffing_plan_rows(cursor, plan_id, staffing_plan_rows, plan_data['client_name'])

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error updating staffing plan {plan_id}: {str(e)}")
            logger.error(f"Plan data received: {plan_data}")
            if conn:
                conn.rollback()
                conn.close()
            return False



    def update_staffing_plan_generated_stages(self, plan_id, updated_stages):
        """Update the generated stages with actual profile numbers"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            staffing_plan_generated_plans_table = self.get_table_name('staffing_plan_generated_plans')

            # Update the stages in the staffing_plan_generated_plans table
            cursor.execute(f"""
                UPDATE {staffing_plan_generated_plans_table} 
                SET stages = %s 
                WHERE id = %s
            """, (json.dumps(updated_stages), plan_id))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error updating generated stages for plan {plan_id}: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def save_temp_generated_plans(self, plan_id, generated_data):
        """Save generated plans to temp storage to survive form resets"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create temp table if not exists
            temp_table = self.get_table_name('temp_generated_plans')
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {temp_table} (
                    plan_id VARCHAR(100),
                    generated_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (plan_id)
                )
            """)

            # Save or update temp data
            cursor.execute(f"""
                INSERT INTO {temp_table} (plan_id, generated_data)
                VALUES (%s, %s)
                ON CONFLICT (plan_id) DO UPDATE SET 
                generated_data = EXCLUDED.generated_data,
                created_at = CURRENT_TIMESTAMP
            """, (str(plan_id), json.dumps(generated_data)))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving temp generated plans: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def load_temp_generated_plans(self, plan_id):
        """Load generated plans from temp storage"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            temp_table = self.get_table_name('temp_generated_plans')
            cursor.execute(f"""
                SELECT generated_data FROM {temp_table} 
                WHERE plan_id = %s
            """, (str(plan_id),))

            result = cursor.fetchone()
            conn.close()

            if result:
                return json.loads(result[0])
            return []

        except Exception as e:
            logger.error(f"Error loading temp generated plans: {str(e)}")
            return []

    def clear_temp_generated_plans(self, plan_id):
        """Clear temp storage after successful save"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            temp_table = self.get_table_name('temp_generated_plans')
            cursor.execute(f"DELETE FROM {temp_table} WHERE plan_id = %s", (str(plan_id),))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error clearing temp generated plans: {str(e)}")
            return False

    def get_demand_staffing_info(self, client_name):
        """Get demand staffing information for a client"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get demand metadata for planned positions and dates
            cursor.execute("""
                SELECT people_expected, start_date, end_date, duration_months
                FROM demand_metadata 
                WHERE client_name = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (client_name,))

            demand_result = cursor.fetchone()
            if demand_result:
                people_expected, start_date, end_date, duration_months = demand_result
                return {
                    'planned_positions': int(people_expected) if people_expected else 0,
                    'start_date': start_date,
                    'end_date': end_date,
                    'duration_months': duration_months
                }

            conn.close()
            return None

        except Exception as e:
            logger.error(f"Error getting demand staffing info: {str(e)}")
            return None

    def get_total_open_positions(self, client_name):
        """Get total open positions for a client based on demand-supply mapping"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get expected positions from demand metadata
            cursor.execute("""
                SELECT people_expected 
                FROM demand_metadata 
                WHERE client_name = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (client_name,))

            demand_result = cursor.fetchone()
            expected_positions = demand_result[0] if demand_result and demand_result[0] else 0

            # Get staffed positions from demand_supply_assignments via master_clients
            cursor.execute("""
                SELECT COUNT(*) 
                FROM demand_supply_assignments dsa
                JOIN master_clients mc ON mc.master_client_id = dsa.master_client_id
                WHERE mc.client_name = %s
            """, (client_name,))

            staffed_result = cursor.fetchone()
            staffed_positions = staffed_result[0] if staffed_result else 0

            # Calculate total open positions
            total_open = max(0, expected_positions - staffed_positions)

            conn.close()
            return {
                'expected_positions': expected_positions,
                'staffed_positions': staffed_positions,
                'total_open_positions': total_open
            }

        except Exception as e:
            logger.error(f"Error getting total open positions: {str(e)}")
            return {
                'expected_positions': 0,
                'staffed_positions': 0,
                'total_open_positions': 0
            }

    def calculate_duration_months(self, start_date, end_date):
        """Calculate duration in months between two dates - shows total months from start to end"""
        if not start_date or not end_date:
            return 0

        # Convert to datetime if they're date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        # Calculate total months difference
        months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

        # Always add 1 to include both start and end months
        # This handles the case where start=July 31 and end=August 1 should be 2 months
        months_diff += 1

        # For staffing plans, minimum duration is 1 month
        return max(1, months_diff)

    def get_staffing_plan_details(self, plan_id):
        """Get detailed staffing plan with requirements"""
        try:
            conn = self.get_connection()

            # Get plan details
            plan_query = """
                SELECT 
                    sp.*,
                    mc.client_name,
                    tp.name as pipeline_name
                FROM staffing_plans sp
                LEFT JOIN master_clients mc ON sp.client_id = mc.master_client_id
                LEFT JOIN talent_pipelines tp ON sp.pipeline_id = tp.id
                WHERE sp.id = %s
            """
            plan_df = pd.read_sql_query(plan_query, conn, params=[plan_id])

            # Get requirements
            req_query = """
                SELECT * FROM staffing_requirements 
                WHERE plan_id = %s 
                ORDER BY milestone_date
            """
            req_df = pd.read_sql_query(req_query, conn, params=[int(plan_id)])

            conn.close()
            return plan_df.iloc[0] if not plan_df.empty else None, req_df
        except Exception as e:
            logger.error(f"Error getting staffing plan details: {str(e)}")
            return None, pd.DataFrame()

    def create_staffing_plan_legacy(self, plan_name, client_name, pipeline_id, target_hires, planned_positions, safety_buffer_pct=0.0):
        """Create new staffing plan with individual parameters (legacy method)"""
        return self.create_staffing_plan_with_dates(
            plan_name, client_name, pipeline_id, target_hires, planned_positions,
            datetime.now().date(), (datetime.now() + timedelta(days=90)).date(), safety_buffer_pct
        )

    def create_staffing_plan_with_dates(self, plan_name, client_name, pipeline_id, target_hires, planned_positions, 
                                      start_date, end_date, safety_buffer_pct=0.0):
        """Create new staffing plan with specified dates"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get or create client_id from client_name
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()
            if client_result:
                client_id = client_result[0]
            else:
                # Create new client if doesn't exist
                cursor.execute("INSERT INTO master_clients (client_name) VALUES (%s) RETURNING master_client_id", (client_name,))
                client_id = cursor.fetchone()[0]

            # Use current user as created_by
            created_by = "preethi.madhu@greyamp.com"  # Default user

            cursor.execute("""
                INSERT INTO staffing_plans 
                (plan_name, client_id, pipeline_id, target_hires, planned_positions, 
                 target_start_date, target_end_date, safety_buffer_pct, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                plan_name,
                int(client_id) if client_id is not None else None,
                int(pipeline_id) if pipeline_id is not None else None,
                int(target_hires) if target_hires is not None else 0,
                int(planned_positions) if planned_positions is not None else 0,
                start_date,
                end_date,
                float(safety_buffer_pct) if safety_buffer_pct is not None else 0.0,
                created_by
            ))

            plan_id = cursor.fetchone()[0]

            # Activate pipeline if one is specified
            if pipeline_id:
                try:
                    from utils.pipeline_manager import PipelineManager
                    pipeline_manager = PipelineManager()
                    pipeline_manager.activate_pipeline(int(pipeline_id))
                    logger.info(f"Pipeline {pipeline_id} activated due to Supply Plan creation")
                except Exception as activation_error:
                    logger.warning(f"Failed to activate pipeline {pipeline_id}: {activation_error}")

            conn.commit()
            conn.close()
            return plan_id
        except Exception as e:
            logger.error(f"Error creating staffing plan: {str(e)}")
            return None

    def create_staffing_plan_dict(self, plan_data):
        """Create new staffing plan with dictionary data (backward compatibility)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO staffing_plans 
                (plan_name, client_id, pipeline_id, target_hires, planned_positions, 
                 target_start_date, target_end_date, safety_buffer_pct, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                plan_data['plan_name'],
                plan_data['client_id'],
                plan_data['pipeline_id'],
                plan_data['target_hires'],
                plan_data['planned_positions'],
                plan_data['target_start_date'],
                plan_data['target_end_date'],
                plan_data.get('safety_buffer_pct', 0.0),  # Default to 0 if not provided
                plan_data['created_by']
            ))

            plan_id = cursor.fetchone()[0]

            # Activate pipeline if one is specified
            if plan_data.get('pipeline_id'):
                try:
                    from utils.pipeline_manager import PipelineManager
                    pipeline_manager = PipelineManager()
                    pipeline_manager.activate_pipeline(plan_data['pipeline_id'])
                    logger.info(f"Pipeline {plan_data['pipeline_id']} activated due to Supply Plan creation")
                except Exception as activation_error:
                    logger.warning(f"Failed to activate pipeline {plan_data['pipeline_id']}: {activation_error}")

            conn.commit()
            conn.close()
            return plan_id
        except Exception as e:
            logger.error(f"Error creating staffing plan: {str(e)}")
            return None

    def update_staffing_plan(self, plan_id, plan_data):
        """Update existing staffing plan"""
        import traceback
        try:
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Called with plan_id={plan_id}")
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: plan_data type: {type(plan_data)}")
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: plan_data keys: {list(plan_data.keys()) if hasattr(plan_data, 'keys') else 'Not a dict'}")
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: plan_data contents: {plan_data}")

            # Check for required fields
            required_fields = ['plan_name', 'client_id', 'pipeline_id', 'target_hires', 'planned_positions', 'target_start_date', 'target_end_date']
            missing_fields = []
            for field in required_fields:
                if field not in plan_data:
                    missing_fields.append(field)
                    logger.error(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Missing required field: {field}")

            if missing_fields:
                logger.error(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Missing fields: {missing_fields}")
                raise KeyError(f"Missing required fields: {missing_fields}")

            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: All required fields present, proceeding with update")

            conn = self.get_connection()
            cursor = conn.cursor()

            # Use environment-aware table names
            staffing_plans_table = self.get_table_name('staffing_plans')
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Using table: {staffing_plans_table}")

            cursor.execute(f"""
                UPDATE {staffing_plans_table}
                SET plan_name = %s, client_id = %s, pipeline_id = %s, 
                    target_hires = %s, planned_positions = %s,
                    target_start_date = %s, target_end_date = %s, 
                    safety_buffer_pct = %s, staffed_positions = %s
                WHERE id = %s
            """, (
                plan_data['plan_name'],
                plan_data['client_id'],
                plan_data['pipeline_id'],
                plan_data['target_hires'],
                plan_data['planned_positions'],
                plan_data['target_start_date'],
                plan_data['target_end_date'],
                plan_data.get('safety_buffer_pct', 0.0),  # Default to 0 if not provided
                plan_data.get('staffed_positions', 0),
                plan_id
            ))

            conn.commit()
            conn.close()
            logger.info(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Successfully updated plan {plan_id}")
            return True
        except Exception as e:
            logger.error(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Error updating staffing plan: {str(e)}")
            logger.error(f"🔧 DEBUG UPDATE_STAFFING_PLAN: Full traceback: {traceback.format_exc()}")
            return False

    def get_staffing_plan_by_id(self, plan_id):
        """Get staffing plan details by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use environment-aware table names
            staffing_plans_table = self.get_table_name('staffing_plans')
            master_clients_table = self.get_table_name('master_clients')
            
            cursor.execute(f"""
                SELECT sp.id, sp.plan_name, mc.client_name, sp.client_id, sp.pipeline_id,
                       sp.target_hires, sp.planned_positions, sp.staffed_positions,
                       sp.target_start_date, sp.target_end_date, sp.safety_buffer_pct,
                       sp.created_by
                FROM {staffing_plans_table} sp
                JOIN {master_clients_table} mc ON sp.client_id = mc.master_client_id
                WHERE sp.id = %s
            """, (plan_id,))

            result = cursor.fetchone()
            conn.close()

            if result:
                return {
                    'id': result[0],
                    'plan_name': result[1],
                    'client_name': result[2],
                    'client_id': result[3],
                    'pipeline_id': result[4],
                    'target_hires': result[5],
                    'planned_positions': result[6],
                    'staffed_positions': result[7],
                    'target_start_date': result[8],
                    'target_end_date': result[9],
                    'safety_buffer_pct': result[10],
                    'created_by': result[11]
                }
            return None

        except Exception as e:
            logger.error(f"Error getting staffing plan by ID: {str(e)}")
            return None

    def update_staffing_plan_with_dates(self, plan_id, plan_name, client_name, pipeline_id, 
                                      target_hires, planned_positions, start_date, end_date, safety_buffer_pct=0.0):
        """Update existing staffing plan with specified dates"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get or create client_id from client_name
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()
            if client_result:
                client_id = client_result[0]
            else:
                # Create new client if doesn't exist
                cursor.execute("INSERT INTO master_clients (client_name) VALUES (%s) RETURNING master_client_id", (client_name,))
                client_id = cursor.fetchone()[0]

            cursor.execute("""
                UPDATE staffing_plans 
                SET plan_name = %s, client_id = %s, pipeline_id = %s, 
                    target_hires = %s, planned_positions = %s,
                    target_start_date = %s, target_end_date = %s, 
                    safety_buffer_pct = %s
                WHERE id = %s
            """, (
                plan_name,
                int(client_id) if client_id is not None else None,
                int(pipeline_id) if pipeline_id is not None else None,
                int(target_hires) if target_hires is not None else 0,
                int(planned_positions) if planned_positions is not None else 0,
                start_date,
                end_date,
                float(safety_buffer_pct) if safety_buffer_pct is not None else 0.0,
                int(plan_id)
            ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error updating staffing plan with dates: {str(e)}")
            return False



    def get_pipeline_plan_actuals(self, plan_id, pipeline_id):
        """Load existing pipeline plan actuals from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT stage_name, profiles_in_pipeline, profiles_planned, planned_conversion_rate,
                       actual_profiles, actual_conversion_rate, needed_by_date
                FROM pipeline_plan_actuals 
                WHERE plan_id = %s AND pipeline_id = %s
                ORDER BY id
            """, (plan_id, pipeline_id))

            results = cursor.fetchall()
            conn.close()

            if results:
                pipeline_data = []
                for row in results:
                    stage_name, profiles_in_pipeline, profiles_planned, planned_conversion_rate, \
                    actual_profiles, actual_conversion_rate, needed_by_date = row

                    pipeline_data.append({
                        'Stage': stage_name,
                        '# in Pipeline': profiles_in_pipeline,
                        '# Planned': profiles_planned,
                        'Planned Conversion Rate': f"{planned_conversion_rate:.1f}%",
                        'Actual #': actual_profiles,
                        'Actual Conversion Rate': f"{actual_conversion_rate:.1f}%",
                        'To be Filled By Date': needed_by_date.strftime('%m/%d/%Y') if needed_by_date else ''
                    })
                return pipeline_data

            return None

        except Exception as e:
            logger.error(f"Error loading pipeline plan actuals: {str(e)}")
            return None

    def calculate_staffing_requirements(self, plan_id, pipeline_manager):
        """Calculate detailed staffing requirements using pipeline"""
        try:
            plan, _ = self.get_staffing_plan_details(plan_id)
            if not plan:
                return None

            # Calculate requirements using pipeline manager
            requirements = pipeline_manager.calculate_pipeline_requirements(
                plan['pipeline_id'],
                plan['target_hires'],
                plan['target_end_date'],
                float(plan['safety_buffer_pct'])
            )

            if not requirements:
                return None

            # Save requirements to database
            conn = self.get_connection()
            cursor = conn.cursor()

            # Clear existing requirements
            cursor.execute("DELETE FROM staffing_requirements WHERE plan_id = %s", (plan_id,))

            # Insert new requirements
            for req in requirements:
                cursor.execute("""
                    INSERT INTO staffing_requirements 
                    (plan_id, stage_name, required_candidates, milestone_date)
                    VALUES (%s, %s, %s, %s)
                """, (plan_id, req['stage_name'], req['required_candidates'], req['milestone_date']))

            conn.commit()
            conn.close()

            return requirements
        except Exception as e:
            logger.error(f"Error calculating staffing requirements: {str(e)}")
            return None

    def generate_timeline_visualization(self, plan_id):
        """Generate timeline visualization for staffing plan"""
        try:
            plan, requirements = self.get_staffing_plan_details(plan_id)
            if not plan or requirements.empty:
                return None

            # Create timeline chart
            fig = go.Figure()

            # Add milestones
            for _, req in requirements.iterrows():
                fig.add_trace(go.Scatter(
                    x=[req['milestone_date']],
                    y=[req['stage_name']],
                    mode='markers',
                    marker=dict(
                        size=req['required_candidates'] / 10,  # Scale marker size
                        color='blue',
                        symbol='diamond'
                    ),
                    text=f"{req['stage_name']}<br>Required: {req['required_candidates']} candidates",
                    hovertemplate="%{text}<br>Date: %{x}<extra></extra>",
                    name=req['stage_name']
                ))

            # Add target dates
            fig.add_vline(
                x=plan['target_start_date'],
                line_dash="dash",
                line_color="green",
                annotation_text="Target Start"
            )

            fig.add_vline(
                x=plan['target_end_date'],
                line_dash="dash",
                line_color="red",
                annotation_text="Target End"
            )

            fig.update_layout(
                title=f"Staffing Plan Timeline: {plan['plan_name']}",
                xaxis_title="Date",
                yaxis_title="Pipeline Stage",
                hovermode='closest',
                height=400
            )

            return fig
        except Exception as e:
            logger.error(f"Error generating timeline visualization: {str(e)}")
            return None

    def generate_monthly_forecast_chart(self, plan_id):
        """Generate monthly forecast progression chart"""
        try:
            plan, requirements = self.get_staffing_plan_details(plan_id)
            if not plan or requirements.empty:
                return None

            # Create monthly progression
            start_date = min(requirements['milestone_date'])
            end_date = plan['target_end_date']

            # Generate monthly data points
            current_date = start_date.replace(day=1)
            monthly_data = []

            while current_date <= end_date:
                # Calculate cumulative progress for this month
                completed_requirements = requirements[requirements['milestone_date'] <= current_date]
                if not completed_requirements.empty:
                    total_progress = completed_requirements['required_candidates'].sum()
                else:
                    total_progress = 0

                monthly_data.append({
                    'month': current_date,
                    'cumulative_candidates': total_progress,
                    'target_hires': plan['target_hires']
                })

                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)

            df = pd.DataFrame(monthly_data)

            # Create chart
            fig = go.Figure()

            # Add cumulative candidates line
            fig.add_trace(go.Scatter(
                x=df['month'],
                y=df['cumulative_candidates'],
                mode='lines+markers',
                name='Cumulative Pipeline',
                line=dict(color='blue')
            ))

            # Add target line
            fig.add_trace(go.Scatter(
                x=df['month'],
                y=df['target_hires'],
                mode='lines',
                name='Target Hires',
                line=dict(color='green', dash='dash')
            ))

            fig.update_layout(
                title=f"Monthly Forecast Progression: {plan['plan_name']}",
                xaxis_title="Month",
                yaxis_title="Candidates",
                hovermode='x unified',
                height=400
            )

            return fig
        except Exception as e:
            logger.error(f"Error generating monthly forecast chart: {str(e)}")
            return None

    def get_staffing_summary_stats(self):
        """Get summary statistics for all staffing plans"""
        try:
            plans_df = self.get_all_staffing_plans()

            if plans_df.empty:
                return {
                    'total_plans': 0,
                    'total_positions': 0,
                    'total_staffed': 0,
                    'avg_completion': 0
                }

            return {
                'total_plans': len(plans_df),
                'total_positions': plans_df['planned_positions'].sum(),
                'total_staffed': plans_df['staffed_positions'].sum(),
                'avg_completion': plans_df['completion_percentage'].mean()
            }
        except Exception as e:
            logger.error(f"Error getting staffing summary stats: {str(e)}")
            return {
                'total_plans': 0,
                'total_positions': 0,
                'total_staffed': 0,
                'avg_completion': 0
            }

    def export_staffing_plan_csv(self, plan_id):
        """Export staffing plan details to CSV format"""
        try:
            plan, requirements = self.get_staffing_plan_details(plan_id)
            if not plan:
                return None

            export_data = []

            # Add plan overview
            export_data.append({
                'Type': 'Plan Overview',
                'Item': 'Plan Name',
                'Value': plan['plan_name'],
                'Date': '',
                'Notes': ''
            })

            export_data.append({
                'Type': 'Plan Overview',
                'Item': 'Client',
                'Value': plan.get('client_name', 'N/A'),
                'Date': '',
                'Notes': ''
            })

            export_data.append({
                'Type': 'Plan Overview',
                'Item': 'Target Hires',
                'Value': plan['target_hires'],
                'Date': '',
                'Notes': ''
            })

            # Add requirements
            for _, req in requirements.iterrows():
                export_data.append({
                    'Type': 'Requirement',
                    'Item': req['stage_name'],
                    'Value': req['required_candidates'],
                    'Date': req['milestone_date'].strftime('%Y-%m-%d'),
                    'Notes': f"Pipeline stage milestone"
                })

            return pd.DataFrame(export_data)
        except Exception as e:
            logger.error(f"Error exporting staffing plan: {str(e)}")
            return None

    def save_pipeline_planning_details(self, plan_id, planning_data):
        """Save pipeline planning details for a staffing plan"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # First, delete existing planning details for this plan
            cursor.execute("DELETE FROM pipeline_planning_details WHERE plan_id = %s", (plan_id,))

            # Insert new planning details
            for row in planning_data:
                cursor.execute("""
                    INSERT INTO pipeline_planning_details 
                    (plan_id, role, skills, positions, onboard_by, pipeline_id, pipeline_owner)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(plan_id),
                    str(row.get('role', '')),
                    str(row.get('skills', '')),
                    int(row.get('positions', 1)),
                    row.get('onboard_by'),
                    int(row.get('pipeline_id')) if row.get('pipeline_id') is not None else None,
                    str(row.get('pipeline_owner', ''))
                ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error saving pipeline planning details: {str(e)}")
            return False

    def get_pipeline_planning_details(self, plan_id):
        """Get pipeline planning details for a staffing plan"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT role, skills, positions, onboard_by, pipeline_id, pipeline_owner
                FROM pipeline_planning_details
                WHERE plan_id = %s
                ORDER BY id
            """, (plan_id,))

            results = cursor.fetchall()
            conn.close()

            planning_data = []
            for row in results:
                planning_data.append({
                    'role': row[0] or '',
                    'skills': row[1] or '',
                    'positions': row[2] or 1,
                    'onboard_by': row[3],
                    'pipeline_id': row[4],
                    'pipeline_owner': row[5] or ''
                })

            return planning_data if planning_data else [{
                'role': '',
                'skills': '',
                'positions': 1,
                'onboard_by': None,
                'pipeline_id': None,
                'pipeline_owner': ''
            }]

        except Exception as e:
            logger.error(f"Error getting pipeline planning details: {str(e)}")
            return [{
                'role': '',
                'skills': '',
                'positions': 1,
                'onboard_by': None,
                'pipeline_id': None,
                'pipeline_owner': ''
            }]

    def get_staffing_plan_by_id(self, plan_id):
        """Get staffing plan details by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT sp.*, mc.client_name 
                FROM staffing_plans sp
                LEFT JOIN master_clients mc ON sp.client_id = mc.master_client_id
                WHERE sp.id = %s
            """, (plan_id,))

            result = cursor.fetchone()
            conn.close()

            if result:
                return {
                    'id': result[0],
                    'plan_name': result[1],
                    'client_id': result[2],
                    'pipeline_id': result[3],
                    'target_hires': result[4],
                    'planned_positions': result[5],
                    'staffed_positions': result[6],
                    'target_start_date': result[7],
                    'target_end_date': result[8],
                    'safety_buffer_pct': result[9],
                    'status': result[10],
                    'created_date': result[11],
                    'created_by': result[12],
                    'client_name': result[13] if len(result) > 13 else 'Unknown Client'
                }
            return None

        except Exception as e:
            logger.error(f"Error getting staffing plan by ID: {str(e)}")
            return None

    def save_pipeline_plan(self, plan_data):
        """Save complete pipeline plan with owner details"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Check if this is a new plan or update to existing plan
            plan_name = plan_data.get('plan_name', '')

            # Calculate total planned positions from roles data
            roles_data = plan_data.get('roles', [])
            total_planned_positions = sum(role.get('positions', 1) for role in roles_data)
            if total_planned_positions == 0:
                total_planned_positions = 1  # Default minimum

            logger.info(f"Saving pipeline plan: {plan_name}")
            logger.info(f"Roles data: {len(roles_data)} roles")
            logger.info(f"Total planned positions: {total_planned_positions}")
            logger.info(f"Date range: {plan_data.get('from_date')} to {plan_data.get('to_date')}")

            # Create or get plan_id from staffing_plans table
            cursor.execute("""
                SELECT id FROM staffing_plans WHERE plan_name = %s LIMIT 1
            """, (plan_name,))

            result = cursor.fetchone()

            if result:
                plan_id = result[0]
                # Update existing plan with calculated positions
                cursor.execute("""
                    UPDATE staffing_plans 
                    SET target_start_date = %s, target_end_date = %s, planned_positions = %s, created_date = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (plan_data.get('from_date'), plan_data.get('to_date'), total_planned_positions, plan_id))
            else:
                # Get a valid client_id from the master_clients table (using correct column name)
                cursor.execute("SELECT master_client_id FROM master_clients LIMIT 1")
                client_result = cursor.fetchone()
                client_id = client_result[0] if client_result else 1
                logger.info(f"Using client_id: {client_id} for new staffing plan")

                # Create new plan using calculated planned_positions
                cursor.execute("""
                    INSERT INTO staffing_plans (
                        plan_name, client_id, target_start_date, target_end_date, 
                        target_hires, planned_positions, safety_buffer_pct, created_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING id
                """, (
                    plan_name,
                    client_id,
                    plan_data.get('from_date'),
                    plan_data.get('to_date'),
                    0,  # Default target hires
                    total_planned_positions,  # Use calculated positions
                    0.0  # Default safety buffer
                ))
                plan_id = cursor.fetchone()[0]

            # Save pipeline planning details with owner information
            if roles_data:
                self.save_pipeline_planning_details(plan_id, roles_data)

            conn.commit()
            conn.close()

            logger.info(f"Pipeline plan saved successfully: {plan_name} (ID: {plan_id}) with {total_planned_positions} planned positions")
            return True

        except Exception as e:
            logger.error(f"Error saving pipeline plan: {str(e)}")
            return False

    def get_demand_data_for_client(self, client_name):
        """Get demand data including leads and people_expected for a client"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT leads, people_expected, confidence_pct, region, start_date, duration_months
                FROM demand_metadata 
                WHERE client_name = %s
                ORDER BY created_at DESC 
                LIMIT 1
            """, (client_name,))

            result = cursor.fetchone()
            conn.close()

            if result:
                return {
                    'leads': float(result[0] or 0),
                    'people_expected': float(result[1] or 0),
                    'confidence_pct': float(result[2] or 0),
                    'region': result[3],
                    'start_date': result[4],
                    'duration_months': int(result[5] or 0)
                }
            return None

        except Exception as e:
            logger.error(f"Error getting demand data for client {client_name}: {str(e)}")
            return None

    def get_total_assignments_for_client(self, client_name):
        """Get total assigned people for a client (count of people, not percentage-based)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get master_client_id first
            cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name = %s", (client_name,))
            client_result = cursor.fetchone()

            if not client_result:
                conn.close()
                return 0.0

            master_client_id = client_result[0]

            # Count assignments for this client (each person assigned = 1, regardless of percentage)
            cursor.execute("""
                SELECT COUNT(DISTINCT talent_id)
                FROM demand_supply_assignments 
                WHERE master_client_id = %s 
                AND assignment_percentage > 0
                AND status = 'Active'
            """, (master_client_id,))

            result = cursor.fetchone()
            conn.close()

            return float(result[0] if result and result[0] else 0)

        except Exception as e:
            logger.error(f"Error getting total assignments for client {client_name}: {str(e)}")
            return 0.0

    def save_pipeline_plan_actuals(self, plan_id, pipeline_id, pipeline_data):
        """Save pipeline plan actual numbers to database"""
        try:
            logger.info(f"DEBUG SAVE: Starting save_pipeline_plan_actuals with plan_id={plan_id}, pipeline_id={pipeline_id}")
            logger.info(f"DEBUG SAVE: Pipeline data length: {len(pipeline_data)}")
            logger.info(f"DEBUG SAVE: Sample data: {pipeline_data[:2] if len(pipeline_data) >= 2 else pipeline_data}")

            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table names
            pipeline_plan_actuals_table = self.get_table_name('pipeline_plan_actuals')
            staffing_plans_table = self.get_table_name('staffing_plans')
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            logger.info(f"DEBUG SAVE: Using table {pipeline_plan_actuals_table}")

            # Create table if it doesn't exist
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {pipeline_plan_actuals_table} (
                    id SERIAL PRIMARY KEY,
                    plan_id INTEGER NOT NULL,
                    pipeline_id INTEGER NOT NULL,
                    stage_name VARCHAR(255) NOT NULL,
                    profiles_in_pipeline INTEGER NOT NULL,
                    profiles_planned INTEGER NOT NULL,
                    planned_conversion_rate DECIMAL(5,2) NOT NULL,
                    actual_profiles INTEGER NOT NULL DEFAULT 0,
                    actual_conversion_rate DECIMAL(5,2) NOT NULL DEFAULT 0.0,
                    needed_by_date DATE NOT NULL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES {staffing_plans_table}(id) ON DELETE CASCADE,
                    FOREIGN KEY (pipeline_id) REFERENCES {talent_pipelines_table}(id) ON DELETE CASCADE,
                    UNIQUE(plan_id, pipeline_id, stage_name)
                )
            """)
            logger.info("DEBUG SAVE: Table created/exists")

            # Clear existing data for this plan and pipeline
            cursor.execute(f"""
                DELETE FROM {pipeline_plan_actuals_table} 
                WHERE plan_id = %s AND pipeline_id = %s
            """, (plan_id, pipeline_id))
            deleted_rows = cursor.rowcount
            logger.info(f"DEBUG SAVE: Deleted {deleted_rows} existing records")

            # Insert new pipeline plan data
            inserted_count = 0
            for i, stage_data in enumerate(pipeline_data):
                logger.info(f"DEBUG SAVE: Processing stage {i}: {stage_data}")

                try:
                    # Parse the date
                    needed_by_date = stage_data['needed_by_date']
                    if isinstance(needed_by_date, str):
                        # Try different date formats
                        try:
                            needed_by_date = datetime.strptime(needed_by_date, '%m/%d/%Y').date()
                        except ValueError:
                            try:
                                needed_by_date = datetime.strptime(needed_by_date, '%Y-%m-%d').date()
                            except ValueError:
                                logger.warning(f"DEBUG SAVE: Could not parse date {needed_by_date}, using today")
                                needed_by_date = datetime.now().date()

                    cursor.execute(f"""
                        INSERT INTO {pipeline_plan_actuals_table} 
                        (plan_id, pipeline_id, stage_name, profiles_in_pipeline, profiles_planned, 
                         planned_conversion_rate, actual_profiles, actual_conversion_rate, needed_by_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        int(plan_id),
                        int(pipeline_id),
                        str(stage_data['stage_name']),
                        int(stage_data['profiles_in_pipeline']),
                        int(stage_data['profiles_planned']),
                        float(stage_data['planned_conversion_rate']),
                        int(stage_data['actual_profiles']),
                        float(stage_data['actual_conversion_rate']),
                        needed_by_date
                    ))
                    inserted_count += 1
                    logger.info(f"DEBUG SAVE: Successfully inserted record {i} for stage {stage_data['stage_name']}")

                except Exception as stage_error:
                    logger.error(f"DEBUG SAVE: Error inserting stage {i}: {stage_error}")
                    logger.error(f"DEBUG SAVE: Stage data: {stage_data}")
                    raise stage_error

            conn.commit()
            logger.info(f"DEBUG SAVE: Committed {inserted_count} records successfully")
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving pipeline plan actuals: {str(e)}")
            logger.error(f"DEBUG SAVE: Full error details - plan_id: {plan_id}, pipeline_id: {pipeline_id}")
            logger.error(f"DEBUG SAVE: Pipeline data: {pipeline_data}")
            import traceback
            logger.error(f"DEBUG SAVE: Traceback: {traceback.format_exc()}")
            return False

    def load_pipeline_plan_actuals(self, plan_id, pipeline_id):
        """Load saved pipeline plan actual numbers from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            pipeline_plan_actuals_table = self.get_table_name('pipeline_plan_actuals')

            cursor.execute(f"""
                SELECT stage_name, profiles_in_pipeline, profiles_planned, 
                       planned_conversion_rate, actual_profiles, actual_conversion_rate, 
                       needed_by_date
                FROM {pipeline_plan_actuals_table} 
                WHERE plan_id = %s AND pipeline_id = %s
                ORDER BY stage_name
            """, (plan_id, pipeline_id))

            results = cursor.fetchall()
            conn.close()

            if results:
                pipeline_data = []
                for row in results:
                    stage_name, profiles_in_pipeline, profiles_planned, planned_conv_rate, actual_profiles, actual_conv_rate, needed_by_date = row

                    # Recalculate actual conversion rate to ensure accuracy
                    if profiles_in_pipeline > 0:
                        recalculated_actual_rate = (actual_profiles / profiles_in_pipeline) * 100
                    else:
                        recalculated_actual_rate = 0.0

                    pipeline_data.append({
                        'Stage': stage_name,
                        '# in Pipeline': profiles_in_pipeline,
                        '# Planned': profiles_planned,
                        'Planned Conversion Rate': f"{planned_conv_rate:.1f}%",
                        'Actual #': actual_profiles,
                        'Actual Conversion Rate': f"{recalculated_actual_rate:.1f}%",
                        'To be Filled By Date': needed_by_date.strftime('%m/%d/%Y')
                    })
                return pipeline_data
            return None

        except Exception as e:
            logger.error(f"Error loading pipeline plan actuals: {str(e)}")
            return None
    def save_pipeline_plan_actual(self, plan_id, stage, actual_num):
        """Save or update actual values for pipeline plan stage"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            pipeline_plan_actuals_table = self.get_table_name('pipeline_plan_actuals')

            # Check if record exists
            cursor.execute(f"""
                SELECT id FROM {pipeline_plan_actuals_table} 
                WHERE plan_id = %s AND stage_name = %s
            """, (plan_id, stage))

            existing = cursor.fetchone()

            if existing:
                # Update existing record
                cursor.execute(f"""
                    UPDATE {pipeline_plan_actuals_table} 
                    SET actual_profiles = %s, updated_date = CURRENT_TIMESTAMP
                    WHERE plan_id = %s AND stage_name = %s
                """, (actual_num, plan_id, stage))
            else:
                # Insert new record
                cursor.execute(f"""
                    INSERT INTO {pipeline_plan_actuals_table} (plan_id, stage_name, actual_profiles, created_date)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                """, (plan_id, stage, actual_num))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving pipeline plan actual: {str(e)}")
            return False

    def get_pipeline_plan_actuals(self, plan_id):
        """Get actual values for pipeline plan stages"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            pipeline_plan_actuals_table = self.get_table_name('pipeline_plan_actuals')

            cursor.execute(f"""
                SELECT stage_name, actual_profiles 
                FROM {pipeline_plan_actuals_table} 
                WHERE plan_id = %s
            """, (plan_id,))

            results = cursor.fetchall()
            conn.close()

            # Convert to dictionary for easy lookup
            actuals_dict = {}
            for stage_name, actual_profiles in results:
                actuals_dict[stage_name] = actual_profiles

            return actuals_dict

        except Exception as e:
            logger.error(f"Error getting pipeline plan actuals: {str(e)}")
            return {}

    def save_pipeline_plan_actuals(self, plan_id, actuals_data):
        """Save multiple pipeline plan actual values"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get pipeline_id for this plan
            cursor.execute("SELECT pipeline_id FROM staffing_plans WHERE id = %s", (plan_id,))
            pipeline_result = cursor.fetchone()
            pipeline_id = pipeline_result[0] if pipeline_result else None

            if not pipeline_id:
                logger.error(f"No pipeline_id found for plan_id={plan_id}")
                return False

            # Save each stage's actual value
            saved_count = 0

            for stage_name, actual_profiles in actuals_data.items():
                try:
                    # Ensure actual_profiles is an integer
                    actual_profiles_int = int(actual_profiles)

                    # Execute the UPDATE
                    cursor.execute("""
                        UPDATE pipeline_plan_actuals 
                        SET actual_profiles = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE plan_id = %s AND pipeline_id = %s AND stage_name = %s
                    """, (actual_profiles_int, plan_id, pipeline_id, stage_name))

                    # Check if the operation affected any rows
                    if cursor.rowcount > 0:
                        saved_count += 1

                except Exception as stage_error:
                    logger.error(f"Error processing stage '{stage_name}': {str(stage_error)}")
                    continue

            # Commit the transaction
            conn.commit()
            conn.close()

            # Return success if all stages were saved
            return saved_count == len(actuals_data)

        except Exception as e:
            logger.error(f"Error in save_pipeline_plan_actuals: {str(e)}")
            return False

    def save_staffing_plan_rows(self, plan_id, staffing_rows):
        """Save staffing plan row details to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table names
            pipeline_planning_details_table = self.get_table_name('pipeline_planning_details')

            # First, delete existing rows for this plan
            cursor.execute(f"""
                DELETE FROM {pipeline_planning_details_table} WHERE plan_id = %s
            """, (plan_id,))

            # Insert new rows
            for row in staffing_rows:
                cursor.execute(f"""
                    INSERT INTO {pipeline_planning_details_table} 
                    (plan_id, role, skills, positions, onboard_by, pipeline_id, pipeline_owner)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    plan_id,
                    row.get('role', ''),
                    row.get('skills', ''),
                    row.get('positions', 1),
                    row.get('staffed_by_date'),
                    self._get_pipeline_id_by_name(row.get('pipeline', '')),
                    row.get('owner', '')
                ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving staffing plan rows: {str(e)}")
            return False

    def load_staffing_plan_rows(self, plan_id):
        """Load staffing plan row details from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Fix: Query the correct table where roles are actually stored
            staffing_plan_generated_plans_table = self.get_table_name('staffing_plan_generated_plans')
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            cursor.execute(f"""
                SELECT spgp.role, 'Default Skills' as skills, 1 as positions, 
                       CURRENT_DATE as onboard_by, 
                       tp.name as pipeline_name, spgp.pipeline_owner
                FROM {staffing_plan_generated_plans_table} spgp
                LEFT JOIN {talent_pipelines_table} tp ON spgp.pipeline_id = tp.id
                WHERE spgp.plan_id = %s
                ORDER BY spgp.id
            """, (plan_id,))

            rows = cursor.fetchall()
            conn.close()

            # Convert to the format expected by the UI
            staffing_rows = []
            for row in rows:
                staffing_rows.append({
                    'role': row[0],
                    'skills': row[1],
                    'positions': row[2],
                    'staffed_by_date': row[3],
                    'pipeline': row[4] or '-- Select a pipeline --',
                    'owner': row[5] or '-- Select Owner --'
                })

            return staffing_rows

        except Exception as e:
            logger.error(f"Error loading staffing plan rows: {str(e)}")
            return []

    def _serialize_pipeline_data(self, data):
        """Convert date objects to strings for JSON serialization"""
        if isinstance(data, list):
            return [self._serialize_pipeline_data(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._serialize_pipeline_data(value) for key, value in data.items()}
        elif hasattr(data, 'isoformat'):  # date/datetime objects
            return data.isoformat()
        else:
            return data

    def save_generated_pipeline_plan(self, plan_id, pipeline_data):
        """Save generated pipeline plan data to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            staffing_plan_generated_plans_table = self.get_table_name('staffing_plan_generated_plans')

            # Delete existing generated plans for this staffing plan
            cursor.execute(f"""
                DELETE FROM {staffing_plan_generated_plans_table} WHERE plan_id = %s
            """, (plan_id,))

            # Insert new generated plan data
            generated_plans = pipeline_data.get('generated_plans', [])
            created_by = pipeline_data.get('created_by', 'admin')

            for plan in generated_plans:
                # Serialize the pipeline results to handle date objects
                # Handle both 'pipeline_results' and 'stages' keys for compatibility
                pipeline_results = plan.get('pipeline_results', [])
                if not pipeline_results:
                    pipeline_results = plan.get('stages', [])
                
                serialized_results = self._serialize_pipeline_data(pipeline_results)

                cursor.execute(f"""
                    INSERT INTO {staffing_plan_generated_plans_table} 
                    (plan_id, role, pipeline_id, pipeline_name, generated_data, created_by, created_date, pipeline_owner)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    plan_id,
                    plan.get('role', ''),
                    plan.get('pipeline_id'),
                    plan.get('pipeline_name', ''),
                    json.dumps(serialized_results),
                    created_by,
                    datetime.now(),
                    plan.get('pipeline_owner', '')
                ))

            conn.commit()
            conn.close()
            logger.info(f"Saved {len(generated_plans)} generated pipeline plans for staffing plan {plan_id}")
            return True

        except Exception as e:
            logger.error(f"Error saving generated pipeline plan: {str(e)}")
            return False

    def load_generated_pipeline_plan(self, plan_id):
        """Load generated pipeline plan data from database"""
        try:
            logger.info(f"LOAD DEBUG: Loading generated pipeline plan for plan_id={plan_id}")
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            staffing_plan_generated_plans_table = self.get_table_name('staffing_plan_generated_plans')

            cursor.execute(f"""
                SELECT role, pipeline_id, pipeline_name, generated_data, pipeline_owner
                FROM {staffing_plan_generated_plans_table} 
                WHERE plan_id = %s
                ORDER BY id
            """, (plan_id,))

            rows = cursor.fetchall()
            conn.close()

            logger.info(f"LOAD DEBUG: Found {len(rows)} pipeline plan rows")

            # Convert to the format expected by the UI
            generated_plans = []
            for row in rows:
                logger.info(f"LOAD DEBUG: Processing row - role: {row[0]}, pipeline: {row[2]}, data: {row[3]}")

                # Parse the JSON data from the generated_data field
                try:
                    if isinstance(row[3], str):
                        pipeline_results = json.loads(row[3])
                    else:
                        pipeline_results = row[3]  # Already parsed
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"LOAD DEBUG: Error parsing JSON data: {e}")
                    pipeline_results = []

                generated_plans.append({
                    'role': row[0],
                    'pipeline_id': row[1],
                    'pipeline_name': row[2],
                    'pipeline_results': pipeline_results,
                    'pipeline_owner': row[4]
                })

            logger.info(f"LOAD DEBUG: Returning {len(generated_plans)} generated plans")
            logger.info(f"LOAD DEBUG: First plan structure: {generated_plans[0] if generated_plans else 'None'}")
            return generated_plans

        except Exception as e:
            logger.error(f"Error loading generated pipeline plan: {str(e)}")
            return []

    def _get_pipeline_id_by_name(self, pipeline_name):
        """Helper method to get pipeline ID by name"""
        if not pipeline_name or pipeline_name == '-- Select a pipeline --':
            return None

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-specific table name
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            cursor.execute(f"""
                SELECT id FROM {talent_pipelines_table} WHERE name = %s AND is_active = true
            """, (pipeline_name,))

            result = cursor.fetchone()
            conn.close()

            return result[0] if result else None

        except Exception as e:
            logger.error(f"Error getting pipeline ID for name '{pipeline_name}': {str(e)}")
            return None