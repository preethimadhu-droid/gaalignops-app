"""
Demand Pipeline Manager
Handles demand pipeline configuration and workflow states management
Created: July 24, 2025
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import logging
from datetime import datetime
import os
from .database_connection import get_database_config, get_database_connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DemandPipelineManager:
    def __init__(self):
        # Use the centralized database connection utility
        self.db_config = get_database_config()
        self.init_tables()

    def get_connection(self):
        """Create database connection"""
        try:
            return get_database_connection()
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def init_tables(self):
        """Initialize demand pipeline tables"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create demand_pipelines table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demand_pipelines (
                    id SERIAL PRIMARY KEY,
                    pipeline_name VARCHAR(255) NOT NULL,
                    region VARCHAR(100) NOT NULL,
                    prospect_type VARCHAR(100) NOT NULL,
                    description TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'Active'
                )
            """)

            # Create demand_pipeline_stages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS demand_pipeline_stages (
                    id SERIAL PRIMARY KEY,
                    pipeline_id INTEGER REFERENCES demand_pipelines(id) ON DELETE CASCADE,
                    stage_name VARCHAR(255) NOT NULL,
                    conversion_rate DECIMAL(5,2) NOT NULL,
                    tat_days INTEGER NOT NULL,
                    stage_description TEXT,
                    stage_order INTEGER DEFAULT 1,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Demand pipeline tables initialized successfully")

        except Exception as e:
            logger.error(f"Error initializing demand pipeline tables: {str(e)}")
            raise

    def create_pipeline(self, pipeline_name, region, prospect_type, description=""):
        """Create a new demand pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO demand_pipelines (name, region, prospect_type, description)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (pipeline_name, region, prospect_type, description))

            pipeline_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Created demand pipeline: {pipeline_name} with ID: {pipeline_id}")
            return pipeline_id

        except Exception as e:
            logger.error(f"Error creating demand pipeline: {str(e)}")
            raise

    def add_pipeline_stage(self, pipeline_id, stage_name, conversion_rate, tat_days, stage_description="", stage_order=1):
        """Add a stage to a demand pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO demand_pipeline_stages 
                (demand_pipeline_id, stage_name, conversion_rate, tat_value, tat_unit, stage_description, stage_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (pipeline_id, stage_name, conversion_rate, tat_days, 'days', stage_description, stage_order))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Added stage '{stage_name}' to pipeline ID: {pipeline_id}")
            return True

        except Exception as e:
            logger.error(f"Error adding pipeline stage: {str(e)}")
            raise

    def get_all_pipelines(self):
        """Get all demand pipelines"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cursor.execute("""
                SELECT dp.*, 
                       COUNT(dps.id) as stage_count
                FROM demand_pipelines dp
                LEFT JOIN demand_pipeline_stages dps ON dp.id = dps.demand_pipeline_id
                WHERE dp.status = 'Active'
                GROUP BY dp.id, dp.name, dp.region, dp.prospect_type, dp.description, dp.created_date, dp.status
                ORDER BY dp.created_date DESC
            """)

            pipelines = cursor.fetchall()
            cursor.close()
            conn.close()

            return [dict(pipeline) for pipeline in pipelines]

        except Exception as e:
            logger.error(f"Error retrieving demand pipelines: {str(e)}")
            return []

    def get_pipeline_by_id(self, pipeline_id):
        """Get a specific demand pipeline with its stages"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get pipeline details
            cursor.execute("""
                SELECT * FROM demand_pipelines 
                WHERE id = %s AND status = 'Active'
            """, (pipeline_id,))
            
            pipeline = cursor.fetchone()
            
            if pipeline:
                # Get pipeline stages
                cursor.execute("""
                    SELECT * FROM demand_pipeline_stages 
                    WHERE demand_pipeline_id = %s 
                    ORDER BY stage_order, id
                """, (pipeline_id,))
                
                stages = cursor.fetchall()
                pipeline = dict(pipeline)
                pipeline['stages'] = [dict(stage) for stage in stages]

            cursor.close()
            conn.close()

            return pipeline

        except Exception as e:
            logger.error(f"Error retrieving demand pipeline: {str(e)}")
            return None

    def update_pipeline(self, pipeline_id, pipeline_name, region, prospect_type, description=""):
        """Update demand pipeline details"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE demand_pipelines 
                SET name = %s, region = %s, prospect_type = %s, description = %s
                WHERE id = %s
            """, (pipeline_name, region, prospect_type, description, pipeline_id))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Updated demand pipeline ID: {pipeline_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating demand pipeline: {str(e)}")
            return False

    def delete_pipeline_stages(self, pipeline_id):
        """Delete all stages for a pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                DELETE FROM demand_pipeline_stages WHERE demand_pipeline_id = %s
            """, (pipeline_id,))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Deleted all stages for pipeline ID: {pipeline_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting pipeline stages: {str(e)}")
            return False

    def delete_pipeline(self, pipeline_id):
        """Delete a demand pipeline (soft delete)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE demand_pipelines 
                SET status = 'Deleted' 
                WHERE id = %s
            """, (pipeline_id,))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Deleted demand pipeline ID: {pipeline_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting demand pipeline: {str(e)}")
            return False