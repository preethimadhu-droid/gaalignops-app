import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime
import re

class SalesDashboardProcessor:
    """Process sales dashboard data from the specific CSV format"""
    
    def __init__(self):
        self.processed_data = None
        self.metadata = {}
    
    def load_sales_dashboard_data(self, file_path):
        """Load and process the sales dashboard CSV file"""
        try:
            # Read the CSV file - handling the specific format with multi-level headers
            raw_data = pd.read_csv(file_path, header=None)
            
            # The data has a specific structure:
            # Row 0: Year information (2025-04, 2025-04, etc.)
            # Row 1: Metric types (Planned, Booked, Billed, Remaining, Forecasted)
            # Row 2: Combined headers (Account, Identifier, etc. + date-metric combinations)
            # Row 3+: Actual data
            
            # Use row 2 (index 2) as the main headers
            headers = raw_data.iloc[2].tolist()
            
            # Get the actual data starting from row 3
            data = raw_data.iloc[3:].copy()
            data.columns = headers
            
            # Reset index and remove any completely empty rows
            data = data.reset_index(drop=True)
            data = data.dropna(how='all')
            
            # Process the data
            processed_data = self._process_sales_data(data)
            
            return processed_data
            
        except Exception as e:
            print(f"Error loading sales dashboard data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _process_sales_data(self, data):
        """Process the sales data into structured format"""
        try:
            # Extract basic account information
            account_info_cols = ['Account', 'Identifier ', 'Account-Track', 'Owner', 'Source', 
                               'Domain', 'Region', 'LoB', 'Offering', 'Confidence']
            
            account_info = data[account_info_cols].copy()
            
            # Extract time series data (all columns with date patterns)
            time_series_cols = [col for col in data.columns if col not in account_info_cols]
            time_series_data = data[time_series_cols].copy()
            
            # Parse time series columns and convert to long format
            parsed_data = []
            
            for idx, row in data.iterrows():
                account = row['Account']
                identifier = row['Identifier ']
                owner = row['Owner']
                domain = row['Domain']
                region = row['Region']
                lob = row['LoB']
                offering = row['Offering']
                confidence = row['Confidence']
                
                # Process each time series column
                for col in time_series_cols:
                    if pd.isna(col) or col == '':
                        continue
                        
                    # Parse the column name to extract date and metric type
                    date_metric = self._parse_column_name(col)
                    if date_metric:
                        date_str, metric_type = date_metric
                        value = row[col]
                        
                        # Clean and convert value
                        clean_value = self._clean_monetary_value(value)
                        
                        parsed_data.append({
                            'Account': account,
                            'Identifier': identifier,
                            'Owner': owner,
                            'Domain': domain,
                            'Region': region,
                            'LoB': lob,
                            'Offering': offering,
                            'Confidence': confidence,
                            'Date': date_str,
                            'Metric_Type': metric_type,
                            'Value': clean_value
                        })
            
            # Convert to DataFrame
            processed_df = pd.DataFrame(parsed_data)
            
            # Convert Date column to datetime
            processed_df['Date'] = pd.to_datetime(processed_df['Date'], format='%Y-%B', errors='coerce')
            
            # Filter out rows with invalid dates or missing values
            processed_df = processed_df.dropna(subset=['Date', 'Value'])
            processed_df = processed_df[processed_df['Value'] != 0]  # Remove zero values
            
            return processed_df
            
        except Exception as e:
            st.error(f"Error processing sales data: {str(e)}")
            return None
    
    def _parse_column_name(self, col_name):
        """Parse column name to extract date and metric type"""
        try:
            if pd.isna(col_name) or col_name == '':
                return None
                
            # Pattern: YYYY-Month_MetricType
            pattern = r'(\d{4})-(\w+)_(\w+)'
            match = re.match(pattern, str(col_name))
            
            if match:
                year, month, metric = match.groups()
                date_str = f"{year}-{month}"
                return date_str, metric
            
            return None
            
        except Exception:
            return None
    
    def _clean_monetary_value(self, value):
        """Clean monetary values and convert to float"""
        try:
            if pd.isna(value) or value == '':
                return 0.0
            
            # Convert to string and remove currency symbols and commas
            value_str = str(value).replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
            
            # Handle negative values
            if value_str.startswith('-'):
                return -float(value_str[1:])
            
            return float(value_str)
            
        except (ValueError, TypeError):
            return 0.0
    
    def get_summary_metrics(self, processed_data):
        """Calculate summary metrics from processed data"""
        try:
            if processed_data is None or processed_data.empty:
                return {}
            
            # Group by metric type and calculate totals
            metric_summary = processed_data.groupby('Metric_Type')['Value'].sum().to_dict()
            
            # Account summaries
            account_count = processed_data['Account'].nunique()
            region_count = processed_data['Region'].nunique()
            domain_count = processed_data['Domain'].nunique()
            
            # Time range
            date_range = {
                'start': processed_data['Date'].min(),
                'end': processed_data['Date'].max()
            }
            
            # Top accounts by total value
            top_accounts = processed_data.groupby('Account')['Value'].sum().nlargest(10).to_dict()
            
            # Region breakdown
            region_breakdown = processed_data.groupby('Region')['Value'].sum().to_dict()
            
            # Domain breakdown
            domain_breakdown = processed_data.groupby('Domain')['Value'].sum().to_dict()
            
            return {
                'metric_summary': metric_summary,
                'account_count': account_count,
                'region_count': region_count,
                'domain_count': domain_count,
                'date_range': date_range,
                'top_accounts': top_accounts,
                'region_breakdown': region_breakdown,
                'domain_breakdown': domain_breakdown
            }
            
        except Exception as e:
            st.error(f"Error calculating summary metrics: {str(e)}")
            return {}
    
    def prepare_time_series_data(self, processed_data, metric_type='Planned'):
        """Prepare time series data for forecasting"""
        try:
            if processed_data is None or processed_data.empty:
                return None
            
            # Filter by metric type
            filtered_data = processed_data[processed_data['Metric_Type'] == metric_type].copy()
            
            if filtered_data.empty:
                return None
            
            # Group by date and sum values
            time_series = filtered_data.groupby('Date')['Value'].sum().reset_index()
            time_series = time_series.sort_values('Date')
            time_series.set_index('Date', inplace=True)
            
            return time_series
            
        except Exception as e:
            st.error(f"Error preparing time series data: {str(e)}")
            return None
    
    def get_account_performance_data(self, processed_data):
        """Get account performance data for detailed analysis"""
        try:
            if processed_data is None or processed_data.empty:
                return None
            
            # Calculate performance metrics per account
            account_metrics = processed_data.groupby(['Account', 'Metric_Type'])['Value'].sum().unstack(fill_value=0)
            
            # Calculate derived metrics
            if 'Planned' in account_metrics.columns and 'Billed' in account_metrics.columns:
                account_metrics['Achievement_Rate'] = (account_metrics['Billed'] / account_metrics['Planned'] * 100).round(2)
            
            if 'Forecasted' in account_metrics.columns:
                account_metrics['Forecast_vs_Planned'] = (account_metrics['Forecasted'] / account_metrics['Planned'] * 100).round(2)
            
            # Add account details
            account_details = processed_data.groupby('Account').agg({
                'Domain': 'first',
                'Region': 'first',
                'Owner': 'first',
                'LoB': 'first',
                'Confidence': 'first'
            })
            
            # Merge metrics with details
            performance_data = account_metrics.merge(account_details, left_index=True, right_index=True)
            
            return performance_data
            
        except Exception as e:
            st.error(f"Error calculating account performance data: {str(e)}")
            return None