"""
Pipeline Management Utility
Comprehensive pipeline configuration and management system
"""
import psycopg2
import pandas as pd
import os
from datetime import datetime, timedelta
import logging

class PipelineManager:
    """Manage talent pipeline configurations, templates, and forecasting"""

    def __init__(self, env_manager=None):
        # Configure logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.db_url = os.getenv("DATABASE_URL")

        # Environment management for table routing
        self.env_manager = env_manager
        self.use_dev_tables = env_manager and env_manager.is_development() if env_manager else False

    def get_table_name(self, table_name):
        """Get environment-specific table name"""
        if self.env_manager:
            return self.env_manager.get_table_name(table_name)
        return table_name

    def get_connection(self, retries=3):
        """Get database connection with retry logic and connection pooling"""
        import time
        for attempt in range(retries):
            try:
                # Use env_manager's database connection if available
                if self.env_manager and hasattr(self.env_manager, 'get_database_connection'):
                    try:
                        conn = self.env_manager.get_database_connection()
                        if conn:
                            print(f"DEBUG: Using env_manager database connection")
                            return conn
                    except Exception as e:
                        print(f"DEBUG: env_manager connection failed: {e}, falling back to direct connection")
                
                # Fallback to direct connection
                if not self.db_url:
                    raise Exception("DATABASE_URL environment variable not set")

                # Add connection timeout and keepalive settings for better stability
                conn = psycopg2.connect(
                    self.db_url,
                    connect_timeout=10,
                    keepalives_idle=600,
                    keepalives_interval=30,
                    keepalives_count=3
                )
                conn.autocommit = False
                print(f"DEBUG: Using direct database connection")
                return conn
            except psycopg2.OperationalError as e:
                if attempt < retries - 1:
                    print(f"Database connection attempt {attempt + 1} failed: {e}")
                    time.sleep(0.5)  # Brief delay before retry
                    continue
                else:
                    print(f"All database connection attempts failed: {e}")
                    raise
            except Exception as e:
                print(f"Database connection error: {e}")
                raise

    # Pipeline Management
    def get_all_pipelines(self):
        """Get all pipeline configurations"""
        try:
            conn = self.get_connection()

            # Use environment-specific table names
            talent_pipelines_table = self.get_table_name('talent_pipelines')
            pipeline_stages_table = self.get_table_name('pipeline_stages')
            master_clients_table = self.get_table_name('master_clients')

            query = f"""
                SELECT 
                    tp.id,
                    tp.name,
                    mc.client_name,
                    tp.description,
                    tp.is_active,
                    tp.is_internal,
                    tp.created_date,
                    (SELECT COUNT(*) FROM {pipeline_stages_table} ps WHERE ps.pipeline_id = tp.id) as stage_count
                FROM {talent_pipelines_table} tp
                LEFT JOIN {master_clients_table} mc ON tp.client_id = mc.master_client_id
                ORDER BY tp.created_date DESC
            """
            df = pd.read_sql_query(query, conn)
            if conn:
                conn.close()
            return df
        except Exception as e:
            print(f"Error getting pipelines: {str(e)}")
            return pd.DataFrame()

    def get_pipeline_details(self, pipeline_id):
        """Get detailed pipeline information including stages"""
        try:
            print(f"DEBUG: get_pipeline_details called with pipeline_id={pipeline_id}")
            conn = self.get_connection()

            # Use environment-specific table names
            talent_pipelines_table = self.get_table_name('talent_pipelines')
            pipeline_stages_table = self.get_table_name('pipeline_stages')
            master_clients_table = self.get_table_name('master_clients')
            
            print(f"DEBUG: Using tables - talent_pipelines: {talent_pipelines_table}, pipeline_stages: {pipeline_stages_table}, master_clients: {master_clients_table}")

            # Get pipeline info
            pipeline_query = f"""
                SELECT 
                    tp.*,
                    mc.client_name
                FROM {talent_pipelines_table} tp
                LEFT JOIN {master_clients_table} mc ON tp.client_id = mc.master_client_id
                WHERE tp.id = %s
            """
            pipeline_df = pd.read_sql_query(pipeline_query, conn, params=[int(pipeline_id)])

            # Get stages
            stages_query = f"""
                SELECT * FROM {pipeline_stages_table}
                WHERE pipeline_id = %s
                ORDER BY stage_order
            """
            stages_df = pd.read_sql_query(stages_query, conn, params=[int(pipeline_id)])

            if conn:
                conn.close()
            # Convert pandas Series to dict, handling numpy types
            if not pipeline_df.empty:
                pipeline_series = pipeline_df.iloc[0]
                pipeline_dict = {}
                for key, value in pipeline_series.items():
                    if hasattr(value, 'item'):  # numpy types have .item() method
                        pipeline_dict[key] = value.item()
                    else:
                        pipeline_dict[key] = value
                return pipeline_dict, stages_df
            else:
                return None, stages_df
        except Exception as e:
            print(f"Error getting pipeline details: {str(e)}")
            return None, pd.DataFrame()

    def get_pipeline_stages(self, pipeline_id):
        """Get all stages for a specific pipeline"""
        try:
            conn = self.get_connection()

            # Use environment-specific table name
            pipeline_stages_table = self.get_table_name('pipeline_stages')

            query = f"""
                SELECT id as stage_id, stage_name, conversion_rate as conversion_percentage, tat_days, stage_description
                FROM {pipeline_stages_table}
                WHERE pipeline_id = %s
                ORDER BY stage_order
            """
            stages_df = pd.read_sql_query(query, conn, params=[int(pipeline_id)])
            if conn:
                conn.close()

            if not stages_df.empty:
                return stages_df.to_dict('records')
            else:
                return []
        except Exception as e:
            print(f"Error getting pipeline stages: {str(e)}")
            return []

    def create_pipeline(self, name, client_id, description, created_by):
        """Create new pipeline configuration - defaults to Inactive status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use environment-specific table name
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            # Create pipeline with is_active = false by default
            cursor.execute(f"""
                INSERT INTO {talent_pipelines_table} (name, client_id, description, created_by, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (name, client_id, description, created_by, False))

            pipeline_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return pipeline_id
        except Exception as e:
            print(f"Error creating pipeline: {str(e)}")
            return None

    def activate_pipeline(self, pipeline_id):
        """Activate a pipeline when it gets linked to a Supply Plan"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use environment-specific table name
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            cursor.execute(f"""
                UPDATE {talent_pipelines_table} 
                SET is_active = true 
                WHERE id = %s
            """, (pipeline_id,))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error activating pipeline: {str(e)}")
            return False

    def check_and_update_pipeline_status(self, pipeline_id):
        """Check if pipeline has Supply Plans and update status accordingly"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use environment-specific table names
            staffing_plans_table = self.get_table_name('staffing_plans')
            talent_pipelines_table = self.get_table_name('talent_pipelines')

            # Check if pipeline has any active Supply Plans
            cursor.execute(f"""
                SELECT COUNT(*) FROM {staffing_plans_table} 
                WHERE pipeline_id = %s
            """, (pipeline_id,))

            supply_plan_count = cursor.fetchone()[0]

            # Update pipeline status based on Supply Plan linkage
            new_status = supply_plan_count > 0
            cursor.execute(f"""
                UPDATE {talent_pipelines_table} 
                SET is_active = %s 
                WHERE id = %s
            """, (new_status, pipeline_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating pipeline status: {str(e)}")
            return False

    def add_pipeline_stage(self, pipeline_id, stage_name, stage_order, conversion_rate, tat_days, description=""):
        """Add stage to pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            stages_table = self.get_table_name("pipeline_stages")

            cursor.execute(f"""
                INSERT INTO {stages_table} 
                (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (pipeline_id, stage_name, stage_order, float(conversion_rate), int(tat_days), description))

            stage_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return stage_id
        except Exception as e:
            print(f"Error adding pipeline stage: {str(e)}")
            return None

    def update_pipeline_stage(self, stage_id, stage_name, conversion_rate, tat_days, description=""):
        """Update pipeline stage"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            stages_table = self.get_table_name("pipeline_stages")

            cursor.execute(f"""
                UPDATE {stages_table} 
                SET stage_name = %s, conversion_rate = %s, tat_days = %s, stage_description = %s
                WHERE id = %s
            """, (stage_name, float(conversion_rate), int(tat_days), description, stage_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating pipeline stage: {str(e)}")
            return False

    def add_pipeline_stage(self, pipeline_id, stage_name, conversion_rate, tat_days, description=""):
        """Add new pipeline stage"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get next stage order
            cursor.execute("SELECT COALESCE(MAX(stage_order), 0) + 1 FROM pipeline_stages WHERE pipeline_id = %s", (pipeline_id,))
            stage_order = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO pipeline_stages 
                (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (pipeline_id, stage_name, stage_order, float(conversion_rate), int(tat_days), description))

            stage_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return stage_id
        except Exception as e:
            print(f"Error adding pipeline stage: {str(e)}")
            return None

    def delete_pipeline_stage(self, stage_id):
        """Delete pipeline stage"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            stages_table = self.get_table_name("pipeline_stages")

            cursor.execute(f"DELETE FROM {stages_table} WHERE id = %s", (stage_id,))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error deleting pipeline stage: {str(e)}")
            return False

    def clear_pipeline_stages(self, pipeline_id):
        """Clear all stages for a pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            stages_table = self.get_table_name("pipeline_stages")

            cursor.execute(f"DELETE FROM {stages_table} WHERE pipeline_id = %s", (pipeline_id,))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error clearing pipeline stages: {str(e)}")
            return False

    def update_pipeline_client(self, pipeline_id, client_id):
        """Update pipeline client association"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("UPDATE talent_pipelines SET client_id = %s WHERE id = %s", (client_id, pipeline_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating pipeline client: {str(e)}")
            return False

    def delete_pipeline(self, pipeline_id):
        """Delete pipeline configuration"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get environment-aware table names
            pipelines_table = self.get_table_name("talent_pipelines")
            stages_table = self.get_table_name("pipeline_stages")
            staffing_table = self.get_table_name("staffing_plans")

            print(f"DEBUG: Environment: {self.env_manager.environment}")
            print(f"DEBUG: Deleting pipeline {pipeline_id} from DEVELOPMENT table {pipelines_table}")
            print(f"DEBUG: Stages will be deleted from DEVELOPMENT table {stages_table}")
            print(f"DEBUG: Production tables (talent_pipelines, pipeline_stages) will NOT be affected")

            # Check if pipeline exists
            cursor.execute(f"SELECT id, name, is_active FROM {pipelines_table} WHERE id = %s", (pipeline_id,))
            pipeline_result = cursor.fetchone()

            if not pipeline_result:
                conn.close()
                print(f"DEBUG: Pipeline {pipeline_id} not found in {pipelines_table}")
                return False, f"Pipeline {pipeline_id} not found"

            print(f"DEBUG: Found pipeline: {pipeline_result}")

            if pipeline_result[2]:  # Pipeline is active
                conn.close()
                print(f"DEBUG: Pipeline {pipeline_id} is active, cannot delete")
                return False, "Cannot delete active pipeline. Please deactivate first."

            # Check if pipeline is referenced by staffing plans
            cursor.execute(f"SELECT COUNT(*) FROM {staffing_table} WHERE pipeline_id = %s", (pipeline_id,))
            staffing_count = cursor.fetchone()[0]
            print(f"DEBUG: Found {staffing_count} staffing plans referencing pipeline {pipeline_id}")

            if staffing_count > 0:
                conn.close()
                return False, f"Cannot delete pipeline. It is referenced by {staffing_count} staffing plan(s). Please remove those plans first."

            # Delete pipeline stages first (cascading delete)
            cursor.execute(f"DELETE FROM {stages_table} WHERE pipeline_id = %s", (pipeline_id,))
            stages_deleted = cursor.rowcount
            print(f"DEBUG: Deleted {stages_deleted} stages from {stages_table}")

            # Then delete the pipeline
            cursor.execute(f"DELETE FROM {pipelines_table} WHERE id = %s", (pipeline_id,))
            pipelines_deleted = cursor.rowcount
            print(f"DEBUG: Deleted {pipelines_deleted} pipelines from {pipelines_table}")

            conn.commit()
            conn.close()

            if pipelines_deleted > 0:
                return True, "Pipeline deleted successfully"
            else:
                return False, "Pipeline was not deleted - no rows affected"
        except Exception as e:
            print(f"Error deleting pipeline: {str(e)}")
            return False, f"Error deleting pipeline: {str(e)}"

    def toggle_pipeline_status(self, pipeline_id):
        """Toggle pipeline active status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE talent_pipelines 
                SET is_active = NOT is_active 
                WHERE id = %s
                RETURNING is_active
            """, (pipeline_id,))

            new_status = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return new_status
        except Exception as e:
            print(f"Error toggling pipeline status: {str(e)}")
            return None

    def update_pipeline_status(self, pipeline_id, new_status):
        """Update pipeline active status to specific value"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                UPDATE {self.get_table_name('talent_pipelines')} 
                SET is_active = %s 
                WHERE id = %s
            """, (new_status, pipeline_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating pipeline status: {str(e)}")
            return False

    def activate_pipeline(self, pipeline_id):
        """Activate pipeline when linked to a plan"""
        return self.update_pipeline_status(pipeline_id, True)

    def check_and_deactivate_pipeline(self, pipeline_id):
        """Check if pipeline has any linked plans, and deactivate if not"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Count plans linked to this pipeline
            cursor.execute(f"""
                SELECT COUNT(*) FROM {self.get_table_name('pipeline_planning_details')} 
                WHERE pipeline_id = %s
            """, (pipeline_id,))
            linked_plans_count = cursor.fetchone()[0]

            # If no plans are linked, deactivate the pipeline
            if linked_plans_count == 0:
                cursor.execute(f"""
                    UPDATE {self.get_table_name('talent_pipelines')} 
                    SET is_active = false 
                    WHERE id = %s
                """, (pipeline_id,))
                conn.commit()
                self.logger.info(f"Pipeline {pipeline_id} deactivated - no linked plans")
                conn.close()
                return True
            else:
                self.logger.info(f"Pipeline {pipeline_id} remains active - {linked_plans_count} linked plans")
                conn.close()
                return False

        except Exception as e:
            self.logger.error(f"Error checking pipeline linkage: {str(e)}")
            if conn:
                conn.close()
            return False

    def reorder_pipeline_stages(self, pipeline_id, stage_order_mapping):
        """Reorder pipeline stages safely by using temporary orders first"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            stages_table = self.get_table_name('pipeline_stages')

            # First, set all stages to temporary negative orders to avoid constraint violations
            for stage_id in stage_order_mapping.keys():
                cursor.execute(f"""
                    UPDATE {stages_table} 
                    SET stage_order = -ABS(id) 
                    WHERE id = %s
                """, (stage_id,))

            # Then set the actual desired orders
            for stage_id, new_order in stage_order_mapping.items():
                cursor.execute(f"""
                    UPDATE {stages_table} 
                    SET stage_order = %s 
                    WHERE id = %s
                """, (new_order, stage_id))

            conn.commit()
            conn.close()
            self.logger.info(f"Successfully reordered stages for pipeline {pipeline_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error reordering pipeline stages: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def update_pipeline(self, pipeline_id, name, description, is_active):
        """Update pipeline basic information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE talent_pipelines 
                SET name = %s, description = %s, is_active = %s
                WHERE id = %s
            """, (name, description, is_active, pipeline_id))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating pipeline: {str(e)}")
            return False

    def update_pipeline_with_stages(self, pipeline_id, name, description, stages, is_active=True):
        """Update pipeline with complete stages replacement"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Start transaction
            cursor.execute("BEGIN")

            # Update pipeline basic info
            cursor.execute("""
                UPDATE talent_pipelines 
                SET name = %s, description = %s, is_active = %s
                WHERE id = %s
            """, (name, description, is_active, pipeline_id))

            # Delete all existing stages for this pipeline
            cursor.execute("DELETE FROM pipeline_stages WHERE pipeline_id = %s", (pipeline_id,))

            # Insert new stages
            for i, stage in enumerate(stages):
                cursor.execute("""
                    INSERT INTO pipeline_stages 
                    (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    pipeline_id, 
                    stage.get('name', ''), 
                    i + 1,  # stage_order starts from 1
                    float(stage.get('conversion_rate', 0)), 
                    int(stage.get('tat_days', 0)), 
                    stage.get('description', '')
                ))

            # Commit transaction
            cursor.execute("COMMIT")
            conn.close()
            return True
        except Exception as e:
            try:
                cursor.execute("ROLLBACK")
            except:
                pass
            print(f"Error updating pipeline with stages: {str(e)}")
            return False



    # Template Management
    def get_pipeline_templates(self):
        """Get all pipeline templates"""
        try:
            conn = self.get_connection()
            query = """
                SELECT 
                    pt.*,
                    COUNT(ts.id) as stage_count
                FROM pipeline_templates pt
                LEFT JOIN template_stages ts ON pt.id = ts.template_id
                GROUP BY pt.id, pt.template_name, pt.description, pt.industry, pt.role_category, pt.is_default, pt.created_date
                ORDER BY pt.is_default DESC, pt.template_name
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error getting pipeline templates: {str(e)}")
            return pd.DataFrame()

    def get_template_stages(self, template_id):
        """Get stages for a template"""
        try:
            conn = self.get_connection()
            query = """
                SELECT * FROM template_stages
                WHERE template_id = %s
                ORDER BY stage_order
            """
            df = pd.read_sql_query(query, conn, params=[int(template_id)])
            conn.close()
            return df
        except Exception as e:
            print(f"Error getting template stages: {str(e)}")
            return pd.DataFrame()

    def create_pipeline_from_template(self, template_id, pipeline_name, client_id, created_by):
        """Create pipeline from template"""
        try:
            # Create pipeline
            pipeline_id = self.create_pipeline(pipeline_name, client_id, f"Created from template", created_by)

            if not pipeline_id:
                return None

            # Get template stages
            template_stages = self.get_template_stages(template_id)

            # Add stages to pipeline
            for _, stage in template_stages.iterrows():
                self.add_pipeline_stage(
                    pipeline_id,
                    stage['stage_name'],
                    stage['stage_order'],
                    stage['default_conversion_rate'],
                    stage['default_tat_days'],
                    stage['stage_description']
                )

            return pipeline_id
        except Exception as e:
            print(f"Error creating pipeline from template: {str(e)}")
            return None

    # Forecasting
    def calculate_pipeline_requirements(self, pipeline_id, target_hires, target_date, safety_buffer=20):
        """Calculate pipeline requirements for target hires"""
        import math
        try:
            # Get pipeline stages
            _, stages_df = self.get_pipeline_details(pipeline_id)

            if stages_df.empty:
                return None

            stages = stages_df.sort_values('stage_order', ascending=False)  # Reverse order for calculation

            requirements = []
            current_required = target_hires

            for _, stage in stages.iterrows():
                # Apply safety buffer to final target
                if stage['stage_order'] == stages['stage_order'].max():
                    current_required = target_hires * (1 + safety_buffer / 100)

                # Calculate required candidates for this stage using correct formula
                conversion_rate = float(stage['conversion_rate']) / 100
                if conversion_rate > 0:
                    required_candidates = math.ceil(current_required / conversion_rate)
                else:
                    required_candidates = current_required * 2  # Fallback

                # Calculate milestone date
                total_tat = stages[stages['stage_order'] >= stage['stage_order']]['tat_days'].sum()
                milestone_date = target_date - timedelta(days=int(total_tat))

                requirements.append({
                    'stage_id': stage['id'],
                    'stage_name': stage['stage_name'],
                    'stage_order': stage['stage_order'],
                    'required_candidates': required_candidates,
                    'conversion_rate': float(stage['conversion_rate']),
                    'tat_days': int(stage['tat_days']),
                    'milestone_date': milestone_date
                })

                current_required = required_candidates

            # Sort by stage order
            requirements.sort(key=lambda x: x['stage_order'])

            return requirements
        except Exception as e:
            print(f"Error calculating pipeline requirements: {str(e)}")
            return None

    def calculate_reverse_pipeline(self, pipeline_id, target_hires, onboard_date):
        """Calculate pipeline requirements working backwards from target hires using correct mathematical logic"""
        import math
        from datetime import datetime, timedelta

        try:
            print(f"DEBUG: Starting calculate_reverse_pipeline for pipeline_id={pipeline_id}")
            _, stages_df = self.get_pipeline_details(pipeline_id)
            
            print(f"DEBUG: Retrieved stages_df - empty: {stages_df.empty}, shape: {stages_df.shape if hasattr(stages_df, 'shape') else 'No shape'}")
            print(f"DEBUG: Stages columns: {list(stages_df.columns) if not stages_df.empty else 'No columns'}")

            if stages_df.empty:
                print(f"DEBUG: stages_df is empty - returning None")
                return None

            # Filter out special terminal stages that don't have TAT or conversion rates
            # These stages are for tracking actuals in Supply Management, not for pipeline generation
            special_stages = ['Dropped', 'Rejected', 'On Hold', 'Reject']
            stages = stages_df[~stages_df['stage_name'].isin(special_stages)]
            
            # Additional filter: only include stages with positive stage_order AND positive conversion rate
            # This ensures we only include active pipeline stages that contribute to the hiring flow
            # Special stages like 'On Hold' (stage_order = -1), 'Dropped' (stage_order = -1) are excluded
            stages = stages[(stages['stage_order'] > 0) & (stages['conversion_rate'] > 0)]
            
            print(f"DEBUG: Filtered stages - excluded special stages: {special_stages}")
            print(f"DEBUG: Only keeping stages with stage_order > 0 AND conversion_rate > 0")
            print(f"DEBUG: This will exclude 'On Hold' (stage_order = -1), 'Dropped' (stage_order = -1), 'Rejected' (conversion_rate = 0)")
            
            # Sort by stage order (highest to lowest for reverse calculation)
            stages = stages.sort_values('stage_order', ascending=False)
            
            print(f"DEBUG: After filtering - {len(stages)} active pipeline stages (excluded {len(stages_df) - len(stages)} special/terminal stages)")

            if stages.empty:
                return None

            # Convert onboard_date to datetime.date if it's a string
            if isinstance(onboard_date, str):
                current_date = datetime.strptime(onboard_date, '%Y-%m-%d').date()
            else:
                current_date = onboard_date

            results = []
            current_target = target_hires

            # Work backwards through stages
            print(f"DEBUG: Processing {len(stages)} active pipeline stages for reverse calculation")
            for _, stage in stages.iterrows():
                stage_name = stage['stage_name']
                conversion_rate = float(stage['conversion_rate'])
                tat_days = int(stage['tat_days'])
                
                print(f"DEBUG: Processing stage '{stage_name}' - conversion: {conversion_rate}%, TAT: {tat_days} days")

                # Skip stages with zero conversion rate (these are terminal stages)
                # Special stages like 'Dropped', 'Rejected', 'On Hold' have 0% conversion
                # and are used for tracking actuals in Supply Management, not for pipeline planning
                if conversion_rate <= 0:
                    print(f"DEBUG: Skipping stage '{stage['stage_name']}' with 0% conversion rate")
                    continue

                # CORRECTED FORMULA: To get 'current_target' outputs from a stage with 'conversion_rate'% success,
                # you need: current_target ÷ (conversion_rate ÷ 100) inputs
                # Example: To get 4 hires from 80% conversion stage: 4 ÷ 0.80 = 5 people needed
                conversion_decimal = conversion_rate / 100.0
                profiles_in_pipeline = math.ceil(current_target / conversion_decimal)

                # Calculate needed by date: subtract TAT from current date
                needed_by_date = current_date - timedelta(days=tat_days)

                results.append({
                    'stage_name': stage['stage_name'],
                    'stage_order': stage['stage_order'],
                    'profiles_in_pipeline': profiles_in_pipeline,
                    'profiles_converted': current_target,
                    'conversion_rate': conversion_rate,
                    'tat_days': tat_days,
                    'needed_by_date': needed_by_date
                })

                # Next stage needs the profiles_in_pipeline as their target
                current_target = profiles_in_pipeline
                current_date = needed_by_date

            # Reverse the results to show stages in correct order (first to last)
            results.reverse()
            
            print(f"DEBUG: Pipeline generation complete - {len(results)} stages included:")
            for result in results:
                print(f"  - {result['stage_name']}: {result['profiles_in_pipeline']} profiles needed, {result['conversion_rate']}% conversion, {result['tat_days']} days TAT")
            
            return results
        except Exception as e:
            print(f"Error calculating reverse pipeline: {str(e)}")
            return None

    # Performance Analytics
    def calculate_pipeline_performance(self, pipeline_id):
        """Calculate pipeline performance metrics"""
        try:
            _, stages_df = self.get_pipeline_details(pipeline_id)

            if stages_df.empty:
                return None

            total_tat = stages_df['tat_days'].sum()
            avg_conversion = stages_df['conversion_rate'].mean()
            stage_count = len(stages_df)

            # Calculate efficiency score (higher conversion, lower TAT = better)
            efficiency_score = (avg_conversion / total_tat) * 100

            # Calculate overall pipeline conversion
            overall_conversion = 1.0
            for _, stage in stages_df.iterrows():
                overall_conversion *= (float(stage['conversion_rate']) / 100)
            overall_conversion *= 100

            return {
                'total_tat_days': int(total_tat),
                'average_conversion_rate': float(avg_conversion),
                'overall_conversion_rate': float(overall_conversion),
                'stage_count': stage_count,
                'efficiency_score': float(efficiency_score)
            }
        except Exception as e:
            print(f"Error calculating pipeline performance: {str(e)}")
            return None

    def get_industry_benchmarks(self):
        """Get industry benchmark data"""
        return {
            'Software Engineering': {
                'avg_tat_days': 32,
                'avg_conversion_rate': 65,
                'typical_stages': 4
            },
            'Data Science': {
                'avg_tat_days': 28,
                'avg_conversion_rate': 58,
                'typical_stages': 5
            },
            'Sales': {
                'avg_tat_days': 25,
                'avg_conversion_rate': 72,
                'typical_stages': 3
            },
            'Marketing': {
                'avg_tat_days': 30,
                'avg_conversion_rate': 68,
                'typical_stages': 4
            },
            'Product Management': {
                'avg_tat_days': 35,
                'avg_conversion_rate': 62,
                'typical_stages': 4
            }
        }

    def generate_performance_recommendations(self, pipeline_performance, industry):
        """Generate performance improvement recommendations"""
        benchmarks = self.get_industry_benchmarks()
        benchmark = benchmarks.get(industry, benchmarks['Software Engineering'])

        recommendations = []

        # TAT recommendations
        if pipeline_performance['total_tat_days'] > benchmark['avg_tat_days']:
            recommendations.append({
                'type': 'warning',
                'category': 'Time to Fill',
                'message': f"TAT is {pipeline_performance['total_tat_days'] - benchmark['avg_tat_days']} days above industry average",
                'suggestion': "Consider streamlining interview processes or parallel scheduling"
            })

        # Conversion rate recommendations
        if pipeline_performance['average_conversion_rate'] < benchmark['avg_conversion_rate']:
            recommendations.append({
                'type': 'info',
                'category': 'Conversion Rate',
                'message': f"Average conversion rate is {benchmark['avg_conversion_rate'] - pipeline_performance['average_conversion_rate']:.1f}% below industry average",
                'suggestion': "Review screening criteria and interview quality"
            })

        # Stage count recommendations
        if pipeline_performance['stage_count'] > benchmark['typical_stages']:
            recommendations.append({
                'type': 'warning',
                'category': 'Process Complexity',
                'message': f"Pipeline has {pipeline_performance['stage_count'] - benchmark['typical_stages']} more stages than typical",
                'suggestion': "Consider consolidating stages to reduce candidate drop-off"
            })

        return recommendations

    # Client Integration
    def get_clients_for_dropdown(self):
        """Get clients for dropdown selection"""
        try:
            conn = self.get_connection()
            query = "SELECT master_client_id, client_name FROM master_clients ORDER BY client_name"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"Error getting clients: {str(e)}")
            return pd.DataFrame()