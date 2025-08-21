#!/usr/bin/env python3
"""
Cycle Time and Wait Time Analytics for Candidate Pipeline
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

class CycleTimeAnalyzer:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()
    
    def get_stage_transition_data(self, where_clause="", params=None):
        """Get all status transitions with cycle times"""
        if params is None:
            params = []
            
        query = f"""
        SELECT 
            csh.candidate_id,
            cd.candidate_name,
            cd.role,
            mc.client_name,
            csh.previous_status,
            csh.new_status,
            csh.status_change_date,
            csh.cycle_time_days,
            csh.change_source,
            cd.data_source
        FROM candidate_status_history csh
        JOIN candidate_data cd ON csh.candidate_id = cd.id
        LEFT JOIN master_clients mc ON cd.hire_for_client_id = mc.master_client_id
        WHERE cd.data_source = 'import' {where_clause}
        ORDER BY csh.candidate_id, csh.status_change_date
        """
        
        self.cursor.execute(query, params)
        results = self.cursor.fetchall()
        
        if results:
            return pd.DataFrame(results, columns=[
                'candidate_id', 'candidate_name', 'role', 'client_name',
                'previous_status', 'new_status', 'status_change_date',
                'cycle_time_days', 'change_source', 'data_source'
            ])
        return pd.DataFrame()
    
    def calculate_average_cycle_times(self, df):
        """Calculate average cycle times for each status transition"""
        if df.empty:
            return pd.DataFrame()
            
        # Define meaningful status transitions
        transition_map = {
            '01 - Profile received': 'Profile Received',
            '02 - Profile Screening by GA': 'GA Screening',
            '03 - Profile Screening by Vendor': 'Vendor Screening',
            '04 - GA Interview Round 1': 'GA Interview 1',
            '05 - GA Interview Round 2': 'GA Interview 2',
            '06 - Client Screening': 'Client Screening',
            '07 - Client Interview Round 1': 'Client Interview 1',
            '08 - Client Interview Round 2': 'Client Interview 2',
            '09 - Staffed': 'Staffed',
            '10 - GA - Screen Rejected': 'GA Rejected',
            '11 - GA - Interview Round 1 Rejected': 'GA Int1 Rejected',
            '12 - GA - Interview Rejected': 'GA Int Rejected',
            '13 - Client - Screen Rejected': 'Client Screen Rejected',
            '14 - Client - Interview Rejected': 'Client Int Rejected'
        }
        
        # Map status names
        df['previous_stage'] = df['previous_status'].map(transition_map).fillna(df['previous_status'])
        df['new_stage'] = df['new_status'].map(transition_map).fillna(df['new_status'])
        
        # Calculate average cycle times by transition
        cycle_summary = df.groupby(['previous_stage', 'new_stage']).agg({
            'cycle_time_days': ['mean', 'median', 'count', 'std'],
            'candidate_id': 'nunique'
        }).round(1)
        
        cycle_summary.columns = ['avg_days', 'median_days', 'total_transitions', 'std_days', 'unique_candidates']
        cycle_summary = cycle_summary.reset_index()
        
        return cycle_summary.sort_values('avg_days', ascending=False)
    
    def get_current_stage_wait_times(self, where_clause="", params=None):
        """Get current wait times for candidates in active stages"""
        if params is None:
            params = []
            
        query = f"""
        SELECT 
            cd.id,
            cd.candidate_name,
            cd.role,
            mc.client_name,
            cd.status,
            cd.status_last_changed,
            EXTRACT(DAY FROM (NOW() - cd.status_last_changed)) as days_in_current_stage,
            cd.created_date,
            EXTRACT(DAY FROM (NOW() - cd.created_date)) as total_pipeline_days
        FROM candidate_data cd
        LEFT JOIN master_clients mc ON cd.hire_for_client_id = mc.master_client_id
        WHERE cd.data_source = 'import' 
        AND cd.status NOT IN ('09 - Staffed', '10 - GA - Screen Rejected', '12 - GA - Interview Rejected', 
                             '13 - Client - Screen Rejected', '14 - Client - Interview Rejected',
                             '19 - Candidate RNR/Dropped', '20 - Internal Dropped')
        {where_clause}
        ORDER BY days_in_current_stage DESC
        """
        
        self.cursor.execute(query, params)
        results = self.cursor.fetchall()
        
        if results:
            return pd.DataFrame(results, columns=[
                'candidate_id', 'candidate_name', 'role', 'client_name',
                'current_status', 'status_last_changed', 'days_in_current_stage',
                'created_date', 'total_pipeline_days'
            ])
        return pd.DataFrame()
    
    def get_bottleneck_analysis(self):
        """Identify stages with longest wait times"""
        query = """
        SELECT 
            cd.status,
            COUNT(*) as candidates_count,
            AVG(EXTRACT(DAY FROM (NOW() - cd.status_last_changed))) as avg_wait_days,
            MAX(EXTRACT(DAY FROM (NOW() - cd.status_last_changed))) as max_wait_days,
            MIN(EXTRACT(DAY FROM (NOW() - cd.status_last_changed))) as min_wait_days
        FROM candidate_data cd
        WHERE cd.data_source = 'import'
        AND cd.status NOT IN ('09 - Staffed', '10 - GA - Screen Rejected', '12 - GA - Interview Rejected',
                             '13 - Client - Screen Rejected', '14 - Client - Interview Rejected',
                             '19 - Candidate RNR/Dropped', '20 - Internal Dropped')
        GROUP BY cd.status
        ORDER BY avg_wait_days DESC
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        if results:
            return pd.DataFrame(results, columns=[
                'status', 'candidates_count', 'avg_wait_days', 'max_wait_days', 'min_wait_days'
            ]).round(1)
        return pd.DataFrame()
    
    def create_cycle_time_visualization(self, cycle_data):
        """Create cycle time visualization"""
        if cycle_data.empty:
            return None
            
        # Filter for meaningful transitions with sufficient data
        significant_transitions = cycle_data[cycle_data['total_transitions'] >= 3]
        
        if significant_transitions.empty:
            return None
            
        # Create bar chart for average cycle times
        fig = go.Figure()
        
        # Create transition labels
        significant_transitions['transition'] = (
            significant_transitions['previous_stage'] + ' → ' + significant_transitions['new_stage']
        )
        
        fig.add_trace(go.Bar(
            x=significant_transitions['transition'],
            y=significant_transitions['avg_days'],
            name='Average Days',
            text=significant_transitions['avg_days'].round(1),
            textposition='auto',
            marker_color='lightblue'
        ))
        
        fig.update_layout(
            title='Average Cycle Time by Stage Transition',
            xaxis_title='Stage Transition',
            yaxis_title='Average Days',
            xaxis_tickangle=-45,
            height=500,
            showlegend=False
        )
        
        return fig
    
    def create_wait_time_heatmap(self, wait_data):
        """Create wait time heatmap by role and status"""
        if wait_data.empty:
            return None
            
        # Create pivot table for heatmap
        heatmap_data = wait_data.pivot_table(
            values='days_in_current_stage',
            index='role',
            columns='current_status',
            aggfunc='mean',
            fill_value=0
        ).round(1)
        
        if heatmap_data.empty:
            return None
            
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            colorscale='Reds',
            text=heatmap_data.values,
            texttemplate="%{text:.1f}",
            textfont={"size": 10},
            colorbar=dict(title="Average Wait Days")
        ))
        
        fig.update_layout(
            title='Average Wait Time Heatmap (Days in Current Stage)',
            xaxis_title='Current Status',
            yaxis_title='Role',
            height=400
        )
        
        return fig
    
    def get_performance_summary(self):
        """Get overall pipeline performance metrics"""
        query = """
        SELECT 
            COUNT(*) as total_active_candidates,
            AVG(EXTRACT(DAY FROM (NOW() - cd.status_last_changed))) as avg_current_wait,
            AVG(EXTRACT(DAY FROM (NOW() - cd.created_date))) as avg_total_pipeline_time,
            COUNT(CASE WHEN cd.status = '09 - Staffed' THEN 1 END) as staffed_count,
            COUNT(CASE WHEN cd.status LIKE '%Rejected%' OR cd.status LIKE '%Dropped%' THEN 1 END) as rejected_count
        FROM candidate_data cd
        WHERE cd.data_source = 'import'
        """
        
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        
        if result:
            total, avg_wait, avg_pipeline, staffed, rejected = result
            success_rate = (staffed / total * 100) if total > 0 else 0
            rejection_rate = (rejected / total * 100) if total > 0 else 0
            
            return {
                'total_candidates': total or 0,
                'avg_current_wait_days': round(avg_wait or 0, 1),
                'avg_pipeline_days': round(avg_pipeline or 0, 1),
                'success_rate': round(success_rate, 1),
                'rejection_rate': round(rejection_rate, 1),
                'staffed_count': staffed or 0,
                'rejected_count': rejected or 0
            }
        return {}