import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, Float, MetaData, inspect
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import json
import logging
import psycopg2
from .production_data_protection import ProductionDataProtection

class DatabaseManager:
    """Handle database operations for demand planning application"""
    
    def __init__(self):
        self.engine = None
        self.metadata = MetaData()
        self.protection = ProductionDataProtection()
        self.connect()
    
    def connect(self):
        """Establish database connection - Use PostgreSQL"""
        try:
            # Use PostgreSQL database from environment variable
            database_url = os.environ.get('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not found")
            
            self.engine = create_engine(database_url)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Create tables if they don't exist
            self.create_tables()
            return True
            
        except Exception as e:
            st.error(f"❌ Database connection failed: {str(e)}")
            return False
    
    def create_tables(self):
        """Create database tables ONLY if they don't exist - PRODUCTION SAFE"""
        try:
            # Check if tables already exist to prevent data loss
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            
            # If key tables exist, only run safe migrations
            if 'unified_sales_data' in existing_tables or 'master_clients' in existing_tables:
                logging.info("Production tables detected - running safe schema updates only")
                logging.info(f"Production protection status: {self.protection._get_protection_status()}")
                self._run_safe_database_migrations()
                return
                
            logging.info("Creating database tables (no existing data detected)")
            self._create_all_tables()
            
        except Exception as e:
            st.error(f"❌ Error creating database tables: {str(e)}")
    
    def _create_all_tables(self):
        """Create all database tables for new installations"""
        try:
            # Historical data table
            historical_data_table = Table(
                'historical_data',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('dataset_name', String(255)),
                Column('date_column', String(100)),
                Column('demand_column', String(100)),
                Column('grouping_columns', String(500)),  # JSON string
                Column('upload_timestamp', DateTime, default=datetime.utcnow),
                Column('data_summary', String(1000)),  # JSON string
                extend_existing=True
            )
            
            # Forecasts table
            forecasts_table = Table(
                'forecasts',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('dataset_id', Integer),
                Column('model_type', String(100)),
                Column('parameters', String(1000)),  # JSON string
                Column('forecast_periods', Integer),
                Column('accuracy_metrics', String(500)),  # JSON string
                Column('created_timestamp', DateTime, default=datetime.utcnow),
                extend_existing=True
            )
            
            # Scenarios table
            scenarios_table = Table(
                'scenarios',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('forecast_id', Integer),
                Column('scenario_name', String(255)),
                Column('scenario_type', String(100)),
                Column('scenario_params', String(1000)),  # JSON string
                Column('period_range', String(100)),  # JSON string
                Column('impact_summary', String(1000)),  # JSON string
                Column('created_timestamp', DateTime, default=datetime.utcnow),
                extend_existing=True
            )
            
            # Data points table (for storing actual data values)
            data_points_table = Table(
                'data_points',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('dataset_id', Integer),
                Column('date_value', DateTime),
                Column('demand_value', Float),
                Column('grouping_value', String(255)),
                extend_existing=True
            )
            
            # Forecast points table (for storing forecast values)
            forecast_points_table = Table(
                'forecast_points',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('forecast_id', Integer),
                Column('period', Integer),
                Column('forecast_value', Float),
                Column('confidence_lower', Float),
                Column('confidence_upper', Float),
                extend_existing=True
            )
            
            # Scenario points table (for storing scenario values)
            scenario_points_table = Table(
                'scenario_points',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('scenario_id', Integer),
                Column('period', Integer),
                Column('base_value', Float),
                Column('scenario_value', Float),
                extend_existing=True
            )
            
            # Create all tables
            self.metadata.create_all(self.engine)
            logging.info("Database tables created successfully")
            
        except Exception as e:
            logging.error(f"Error creating database tables: {str(e)}")
            raise
    
    def save_dataset(self, data, dataset_name, date_column, demand_column, grouping_columns=None):
        """Save dataset to database"""
        try:
            with self.engine.connect() as conn:
                # Save dataset metadata
                data_summary = {
                    'total_records': len(data),
                    'date_range': {
                        'start': str(data.index.min()),
                        'end': str(data.index.max())
                    },
                    'columns': list(data.columns),
                    'frequency': self._detect_frequency(data)
                }
                
                # Insert dataset record
                result = conn.execute(text("""
                    INSERT INTO historical_data 
                    (dataset_name, date_column, demand_column, grouping_columns, data_summary)
                    VALUES (:dataset_name, :date_column, :demand_column, :grouping_columns, :data_summary)
                    RETURNING id
                """), {
                    'dataset_name': dataset_name,
                    'date_column': date_column,
                    'demand_column': demand_column,
                    'grouping_columns': json.dumps(grouping_columns) if grouping_columns else None,
                    'data_summary': json.dumps(data_summary)
                })
                
                dataset_id = result.fetchone()[0]
                
                # Save data points
                data_points = []
                for idx, row in data.iterrows():
                    grouping_value = ""
                    if grouping_columns:
                        grouping_value = "_".join([str(row.get(col, "")) for col in grouping_columns])
                    
                    data_points.append({
                        'dataset_id': dataset_id,
                        'date_value': idx,
                        'demand_value': float(row[demand_column]),
                        'grouping_value': grouping_value
                    })
                
                # Batch insert data points
                if data_points:
                    conn.execute(text("""
                        INSERT INTO data_points (dataset_id, date_value, demand_value, grouping_value)
                        VALUES (:dataset_id, :date_value, :demand_value, :grouping_value)
                    """), data_points)
                
                conn.commit()
                return dataset_id
                
        except Exception as e:
            st.error(f"❌ Error saving dataset: {str(e)}")
            return None
    
    def save_forecast(self, dataset_id, model_type, parameters, forecast_periods, forecast_data, accuracy_metrics=None):
        """Save forecast results to database"""
        try:
            with self.engine.connect() as conn:
                # Insert forecast record
                result = conn.execute(text("""
                    INSERT INTO forecasts 
                    (dataset_id, model_type, parameters, forecast_periods, accuracy_metrics)
                    VALUES (:dataset_id, :model_type, :parameters, :forecast_periods, :accuracy_metrics)
                    RETURNING id
                """), {
                    'dataset_id': dataset_id,
                    'model_type': model_type,
                    'parameters': json.dumps(parameters),
                    'forecast_periods': forecast_periods,
                    'accuracy_metrics': json.dumps(accuracy_metrics) if accuracy_metrics else None
                })
                
                forecast_id = result.fetchone()[0]
                
                # Save forecast points
                forecast_points = []
                forecast_values = forecast_data['forecast']
                confidence_interval = forecast_data.get('confidence_interval')
                
                for i, value in enumerate(forecast_values):
                    forecast_points.append({
                        'forecast_id': forecast_id,
                        'period': i + 1,
                        'forecast_value': float(value),
                        'confidence_lower': float(confidence_interval['lower'].iloc[i]) if confidence_interval else None,
                        'confidence_upper': float(confidence_interval['upper'].iloc[i]) if confidence_interval else None
                    })
                
                # Batch insert forecast points
                if forecast_points:
                    conn.execute(text("""
                        INSERT INTO forecast_points 
                        (forecast_id, period, forecast_value, confidence_lower, confidence_upper)
                        VALUES (:forecast_id, :period, :forecast_value, :confidence_lower, :confidence_upper)
                    """), forecast_points)
                
                conn.commit()
                return forecast_id
                
        except Exception as e:
            st.error(f"❌ Error saving forecast: {str(e)}")
            return None
    
    def save_scenario(self, forecast_id, scenario_name, scenario_type, scenario_params, period_range, scenario_data, impact_summary=None):
        """Save scenario to database"""
        try:
            with self.engine.connect() as conn:
                # Insert scenario record
                result = conn.execute(text("""
                    INSERT INTO scenarios 
                    (forecast_id, scenario_name, scenario_type, scenario_params, period_range, impact_summary)
                    VALUES (:forecast_id, :scenario_name, :scenario_type, :scenario_params, :period_range, :impact_summary)
                    RETURNING id
                """), {
                    'forecast_id': forecast_id,
                    'scenario_name': scenario_name,
                    'scenario_type': scenario_type,
                    'scenario_params': json.dumps(scenario_params),
                    'period_range': json.dumps(period_range),
                    'impact_summary': json.dumps(impact_summary) if impact_summary else None
                })
                
                scenario_id = result.fetchone()[0]
                
                # Save scenario points
                scenario_points = []
                base_forecast = scenario_data['base_forecast']
                scenario_forecast = scenario_data['scenario_forecast']
                
                for i in range(len(base_forecast)):
                    scenario_points.append({
                        'scenario_id': scenario_id,
                        'period': i + 1,
                        'base_value': float(base_forecast.iloc[i]),
                        'scenario_value': float(scenario_forecast.iloc[i])
                    })
                
                # Batch insert scenario points
                if scenario_points:
                    conn.execute(text("""
                        INSERT INTO scenario_points 
                        (scenario_id, period, base_value, scenario_value)
                        VALUES (:scenario_id, :period, :base_value, :scenario_value)
                    """), scenario_points)
                
                conn.commit()
                return scenario_id
                
        except Exception as e:
            st.error(f"❌ Error saving scenario: {str(e)}")
            return None
    
    def get_datasets(self):
        """Retrieve all datasets from database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, dataset_name, upload_timestamp, data_summary
                    FROM historical_data
                    ORDER BY upload_timestamp DESC
                """))
                
                datasets = []
                for row in result:
                    summary = json.loads(row[3]) if row[3] else {}
                    datasets.append({
                        'id': row[0],
                        'name': row[1],
                        'upload_time': row[2],
                        'total_records': summary.get('total_records', 0),
                        'date_range': summary.get('date_range', {}),
                        'frequency': summary.get('frequency', 'Unknown')
                    })
                
                return datasets
                
        except Exception as e:
            st.error(f"❌ Error retrieving datasets: {str(e)}")
            return []
    
    def get_forecasts(self, dataset_id=None):
        """Retrieve forecasts from database"""
        try:
            with self.engine.connect() as conn:
                query = """
                    SELECT f.id, f.dataset_id, f.model_type, f.forecast_periods, 
                           f.created_timestamp, f.accuracy_metrics, h.dataset_name
                    FROM forecasts f
                    JOIN historical_data h ON f.dataset_id = h.id
                """
                params = {}
                
                if dataset_id:
                    query += " WHERE f.dataset_id = :dataset_id"
                    params['dataset_id'] = dataset_id
                
                query += " ORDER BY f.created_timestamp DESC"
                
                result = conn.execute(text(query), params)
                
                forecasts = []
                for row in result:
                    accuracy = json.loads(row[5]) if row[5] else {}
                    forecasts.append({
                        'id': row[0],
                        'dataset_id': row[1],
                        'model_type': row[2],
                        'forecast_periods': row[3],
                        'created_time': row[4],
                        'accuracy_metrics': accuracy,
                        'dataset_name': row[6]
                    })
                
                return forecasts
                
        except Exception as e:
            st.error(f"❌ Error retrieving forecasts: {str(e)}")
            return []
    
    def get_scenarios(self, forecast_id=None):
        """Retrieve scenarios from database"""
        try:
            with self.engine.connect() as conn:
                query = """
                    SELECT s.id, s.forecast_id, s.scenario_name, s.scenario_type, 
                           s.created_timestamp, s.impact_summary, f.model_type
                    FROM scenarios s
                    JOIN forecasts f ON s.forecast_id = f.id
                """
                params = {}
                
                if forecast_id:
                    query += " WHERE s.forecast_id = :forecast_id"
                    params['forecast_id'] = forecast_id
                
                query += " ORDER BY s.created_timestamp DESC"
                
                result = conn.execute(text(query), params)
                
                scenarios = []
                for row in result:
                    impact = json.loads(row[5]) if row[5] else {}
                    scenarios.append({
                        'id': row[0],
                        'forecast_id': row[1],
                        'scenario_name': row[2],
                        'scenario_type': row[3],
                        'created_time': row[4],
                        'impact_summary': impact,
                        'model_type': row[6]
                    })
                
                return scenarios
                
        except Exception as e:
            st.error(f"❌ Error retrieving scenarios: {str(e)}")
            return []
    
    def load_dataset_data(self, dataset_id):
        """Load dataset data from database"""
        try:
            with self.engine.connect() as conn:
                # Get dataset metadata
                dataset_result = conn.execute(text("""
                    SELECT dataset_name, date_column, demand_column, grouping_columns
                    FROM historical_data
                    WHERE id = :dataset_id
                """), {'dataset_id': dataset_id})
                
                dataset_row = dataset_result.fetchone()
                if not dataset_row:
                    return None
                
                # Get data points
                data_result = conn.execute(text("""
                    SELECT date_value, demand_value, grouping_value
                    FROM data_points
                    WHERE dataset_id = :dataset_id
                    ORDER BY date_value
                """), {'dataset_id': dataset_id})
                
                data_rows = data_result.fetchall()
                if not data_rows:
                    return None
                
                # Convert to DataFrame
                data_dict = {
                    'date': [row[0] for row in data_rows],
                    dataset_row[2]: [row[1] for row in data_rows]  # demand column
                }
                
                if dataset_row[3]:  # grouping columns exist
                    grouping_cols = json.loads(dataset_row[3])
                    for col in grouping_cols:
                        data_dict[col] = [row[2] for row in data_rows]
                
                df = pd.DataFrame(data_dict)
                df.set_index('date', inplace=True)
                
                return {
                    'data': df,
                    'dataset_name': dataset_row[0],
                    'date_column': dataset_row[1],
                    'demand_column': dataset_row[2],
                    'grouping_columns': json.loads(dataset_row[3]) if dataset_row[3] else None
                }
                
        except Exception as e:
            st.error(f"❌ Error loading dataset: {str(e)}")
            return None
    
    def _detect_frequency(self, data):
        """Helper method to detect data frequency"""
        try:
            if len(data) < 2:
                return "Unknown"
            
            time_diffs = data.index.to_series().diff().dropna()
            most_common_diff = time_diffs.mode().iloc[0]
            
            if most_common_diff.days == 1:
                return "Daily"
            elif most_common_diff.days == 7:
                return "Weekly"
            elif 28 <= most_common_diff.days <= 31:
                return "Monthly"
            elif 90 <= most_common_diff.days <= 92:
                return "Quarterly"
            elif 365 <= most_common_diff.days <= 366:
                return "Yearly"
            else:
                return f"Every {most_common_diff.days} days"
                
        except Exception:
            return "Unknown"
    
    def check_connection(self):
        """Check if database connection is active"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def get_database_stats(self):
        """Get database statistics"""
        try:
            with self.engine.connect() as conn:
                stats = {}
                
                # Count records in each table
                tables = ['historical_data', 'forecasts', 'scenarios', 'data_points']
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    stats[table] = result.fetchone()[0]
                
                return stats
                
        except Exception as e:
            st.error(f"❌ Error getting database stats: {str(e)}")
            return {}
    
    def _run_safe_database_migrations(self):
        """Run safe schema migrations that don't affect existing data"""
        try:
            # Create a more robust connection for migrations
            database_url = os.environ.get('DATABASE_URL')
            conn = psycopg2.connect(
                database_url,
                connect_timeout=30,
                keepalives_idle=600,
                keepalives_interval=30,
                keepalives_count=3
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Safe migrations for database tables (simplified to avoid SSL issues)
            # First check if data_points table exists
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'data_points';")
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                # Only create index if table exists
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_points_dataset_date ON data_points(dataset_id, date_value);")
                    logging.info("Index created successfully")
                except Exception as migration_error:
                    logging.warning(f"Index creation failed (non-critical): {migration_error}")
            else:
                logging.info("data_points table does not exist, skipping index creation")
                        
            conn.close()
            logging.info("Safe database migrations completed")
            
        except Exception as e:
            logging.error(f"Error running safe database migrations: {str(e)}")
            # Don't raise - migrations are optional and shouldn't break the app
            pass