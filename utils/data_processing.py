import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime
import io

class DataProcessor:
    """Handle data loading, validation, and preprocessing"""
    
    def __init__(self):
        self.supported_date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S'
        ]
    
    def load_data(self, uploaded_file):
        """Load and initially process uploaded CSV file"""
        try:
            # Read CSV file
            data = pd.read_csv(uploaded_file)
            
            if data.empty:
                st.error("❌ The uploaded file is empty.")
                return None
            
            # Try to identify and parse date column
            date_column = self._identify_date_column(data)
            
            if date_column is None:
                st.error("❌ No valid date column found. Please ensure your data contains a date column.")
                return None
            
            # Parse dates and set as index
            data[date_column] = self._parse_dates(data[date_column])
            data = data.set_index(date_column).sort_index()
            
            # Remove any non-numeric columns that aren't needed
            numeric_columns = data.select_dtypes(include=[np.number]).columns
            non_numeric_columns = data.select_dtypes(exclude=[np.number]).columns
            
            if len(numeric_columns) == 0:
                st.error("❌ No numeric columns found for demand/sales data.")
                return None
            
            # Keep numeric columns and categorical columns that might be useful for grouping
            data = data[list(numeric_columns) + list(non_numeric_columns)]
            
            return data
            
        except Exception as e:
            st.error(f"❌ Error loading data: {str(e)}")
            return None
    
    def _identify_date_column(self, data):
        """Identify the date column in the dataset"""
        potential_date_columns = []
        
        for column in data.columns:
            # Check column names
            if any(keyword in column.lower() for keyword in ['date', 'time', 'day', 'month', 'year']):
                potential_date_columns.append(column)
                continue
            
            # Check data types and content
            sample_values = data[column].dropna().head(10).astype(str)
            date_like_count = 0
            
            for value in sample_values:
                if self._is_date_like(value):
                    date_like_count += 1
            
            if date_like_count >= len(sample_values) * 0.8:  # 80% of values look like dates
                potential_date_columns.append(column)
        
        # Return the first potential date column
        return potential_date_columns[0] if potential_date_columns else None
    
    def _is_date_like(self, value):
        """Check if a value looks like a date"""
        for date_format in self.supported_date_formats:
            try:
                datetime.strptime(str(value), date_format)
                return True
            except ValueError:
                continue
        
        # Check for common date patterns
        value_str = str(value)
        if any(char in value_str for char in ['-', '/', ':']):
            return True
        
        return False
    
    def _parse_dates(self, date_series):
        """Parse dates using multiple format attempts"""
        parsed_dates = []
        
        for date_value in date_series:
            parsed_date = None
            
            # Try each format
            for date_format in self.supported_date_formats:
                try:
                    parsed_date = datetime.strptime(str(date_value), date_format)
                    break
                except ValueError:
                    continue
            
            # If no format worked, try pandas to_datetime
            if parsed_date is None:
                try:
                    parsed_date = pd.to_datetime(date_value, infer_datetime_format=True)
                except:
                    parsed_date = pd.NaT
            
            parsed_dates.append(parsed_date)
        
        return pd.Series(parsed_dates)
    
    def validate_data(self, data, demand_column, grouping_columns=None):
        """Validate the processed data"""
        try:
            # Check if demand column exists and is numeric
            if demand_column not in data.columns:
                st.error(f"❌ Column '{demand_column}' not found in data.")
                return None
            
            if not pd.api.types.is_numeric_dtype(data[demand_column]):
                st.error(f"❌ Column '{demand_column}' is not numeric.")
                return None
            
            # Check for missing values
            missing_values = data[demand_column].isnull().sum()
            if missing_values > 0:
                st.warning(f"⚠️ Found {missing_values} missing values in demand column. They will be forward-filled.")
                data[demand_column] = data[demand_column].fillna(method='ffill')
            
            # Check for negative values
            negative_values = (data[demand_column] < 0).sum()
            if negative_values > 0:
                st.warning(f"⚠️ Found {negative_values} negative values in demand column.")
            
            # Check for duplicated timestamps
            duplicated_dates = data.index.duplicated().sum()
            if duplicated_dates > 0:
                st.warning(f"⚠️ Found {duplicated_dates} duplicate dates. Taking the mean of duplicated values.")
                data = data.groupby(data.index).mean()
            
            # Validate grouping columns
            if grouping_columns:
                for col in grouping_columns:
                    if col not in data.columns:
                        st.error(f"❌ Grouping column '{col}' not found in data.")
                        return None
            
            st.success("✅ Data validation completed successfully!")
            return data
            
        except Exception as e:
            st.error(f"❌ Error validating data: {str(e)}")
            return None
    
    def detect_frequency(self, data):
        """Detect the frequency of the time series data"""
        try:
            if len(data) < 2:
                return "Unknown"
            
            # Calculate the most common time difference
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
    
    def prepare_for_forecasting(self, data, demand_column, grouping_columns=None):
        """Prepare data for forecasting by handling grouping and aggregation"""
        try:
            if grouping_columns and len(grouping_columns) > 0:
                # Group data and return multiple series
                grouped_data = {}
                for group_values, group_data in data.groupby(grouping_columns):
                    group_name = "_".join([str(v) for v in group_values]) if isinstance(group_values, tuple) else str(group_values)
                    grouped_data[group_name] = group_data[demand_column].sort_index()
                return grouped_data
            else:
                # Return single series
                return {demand_column: data[demand_column].sort_index()}
                
        except Exception as e:
            st.error(f"❌ Error preparing data for forecasting: {str(e)}")
            return None
