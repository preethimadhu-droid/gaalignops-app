import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

class Visualizer:
    """Handle all visualization and plotting functionality"""
    
    def __init__(self):
        self.colors = {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e',
            'success': '#2ca02c',
            'warning': '#d62728',
            'info': '#9467bd',
            'light': '#17becf',
            'dark': '#8c564b'
        }
    
    def plot_forecast(self, historical_data, forecast_data, confidence_interval=None):
        """Plot historical data with forecast and confidence intervals"""
        fig = go.Figure()
        
        # Historical data
        fig.add_trace(go.Scatter(
            x=historical_data.index,
            y=historical_data.values,
            mode='lines',
            name='Historical Data',
            line=dict(color=self.colors['primary'], width=2)
        ))
        
        # Generate forecast dates
        if len(historical_data) > 0:
            start_date = historical_data.index[-1]
            forecast_dates = pd.date_range(
                start=start_date + pd.Timedelta(days=1),
                periods=len(forecast_data),
                freq='D'
            )
        else:
            forecast_dates = pd.date_range(
                start=pd.Timestamp.now(),
                periods=len(forecast_data),
                freq='D'
            )
        
        # Forecast data
        fig.add_trace(go.Scatter(
            x=forecast_dates,
            y=forecast_data.values,
            mode='lines',
            name='Forecast',
            line=dict(color=self.colors['secondary'], width=2, dash='dash')
        ))
        
        # Confidence interval
        if confidence_interval is not None:
            fig.add_trace(go.Scatter(
                x=forecast_dates,
                y=confidence_interval['upper'].values,
                mode='lines',
                name='Upper Confidence',
                line=dict(color=self.colors['light'], width=1),
                showlegend=False
            ))
            
            fig.add_trace(go.Scatter(
                x=forecast_dates,
                y=confidence_interval['lower'].values,
                mode='lines',
                name='Confidence Interval',
                line=dict(color=self.colors['light'], width=1),
                fill='tonexty',
                fillcolor='rgba(23, 190, 207, 0.2)'
            ))
        
        # Add vertical line to separate historical and forecast
        if len(historical_data) > 0:
            fig.add_vline(
                x=historical_data.index[-1],
                line_dash="dot",
                line_color="gray",
                annotation_text="Forecast Start",
                annotation_position="top"
            )
        
        fig.update_layout(
            title='Demand Forecast',
            xaxis_title='Date',
            yaxis_title='Demand',
            hovermode='x unified',
            showlegend=True,
            height=500
        )
        
        return fig
    
    def plot_historical_trend(self, data):
        """Plot historical trend with moving averages"""
        fig = go.Figure()
        
        # Original data
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name='Actual Demand',
            line=dict(color=self.colors['primary'], width=1)
        ))
        
        # 7-day moving average
        if len(data) >= 7:
            ma_7 = data.rolling(window=7).mean()
            fig.add_trace(go.Scatter(
                x=ma_7.index,
                y=ma_7.values,
                mode='lines',
                name='7-Day MA',
                line=dict(color=self.colors['secondary'], width=2)
            ))
        
        # 30-day moving average
        if len(data) >= 30:
            ma_30 = data.rolling(window=30).mean()
            fig.add_trace(go.Scatter(
                x=ma_30.index,
                y=ma_30.values,
                mode='lines',
                name='30-Day MA',
                line=dict(color=self.colors['success'], width=2)
            ))
        
        fig.update_layout(
            title='Historical Demand Trend',
            xaxis_title='Date',
            yaxis_title='Demand',
            hovermode='x unified',
            height=400
        )
        
        return fig
    
    def plot_distribution(self, data):
        """Plot demand distribution histogram"""
        fig = go.Figure()
        
        fig.add_trace(go.Histogram(
            x=data.values,
            nbinsx=30,
            name='Demand Distribution',
            marker_color=self.colors['primary'],
            opacity=0.7
        ))
        
        # Add mean line
        mean_value = data.mean()
        fig.add_vline(
            x=mean_value,
            line_dash="dash",
            line_color=self.colors['warning'],
            annotation_text=f"Mean: {mean_value:.2f}",
            annotation_position="top right"
        )
        
        fig.update_layout(
            title='Demand Distribution',
            xaxis_title='Demand Value',
            yaxis_title='Frequency',
            height=400
        )
        
        return fig
    
    def plot_seasonal_pattern(self, data):
        """Plot seasonal patterns"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Monthly Pattern', 'Weekly Pattern', 'Daily Pattern', 'Quarterly Pattern'],
            specs=[[{"type": "scatter"}, {"type": "scatter"}],
                   [{"type": "scatter"}, {"type": "scatter"}]]
        )
        
        # Monthly pattern
        if len(data) >= 30:
            monthly_avg = data.groupby(data.index.month).mean()
            fig.add_trace(
                go.Scatter(
                    x=monthly_avg.index,
                    y=monthly_avg.values,
                    mode='lines+markers',
                    name='Monthly',
                    line=dict(color=self.colors['primary'])
                ),
                row=1, col=1
            )
        
        # Weekly pattern
        if len(data) >= 7:
            weekly_avg = data.groupby(data.index.dayofweek).mean()
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            fig.add_trace(
                go.Scatter(
                    x=[days[i] for i in weekly_avg.index],
                    y=weekly_avg.values,
                    mode='lines+markers',
                    name='Weekly',
                    line=dict(color=self.colors['secondary'])
                ),
                row=1, col=2
            )
        
        # Daily pattern (hour of day if datetime available)
        if hasattr(data.index, 'hour'):
            hourly_avg = data.groupby(data.index.hour).mean()
            fig.add_trace(
                go.Scatter(
                    x=hourly_avg.index,
                    y=hourly_avg.values,
                    mode='lines+markers',
                    name='Hourly',
                    line=dict(color=self.colors['success'])
                ),
                row=2, col=1
            )
        else:
            # Day of month pattern
            daily_avg = data.groupby(data.index.day).mean()
            fig.add_trace(
                go.Scatter(
                    x=daily_avg.index,
                    y=daily_avg.values,
                    mode='lines+markers',
                    name='Day of Month',
                    line=dict(color=self.colors['success'])
                ),
                row=2, col=1
            )
        
        # Quarterly pattern
        if len(data) >= 90:
            quarterly_avg = data.groupby(data.index.quarter).mean()
            quarters = ['Q1', 'Q2', 'Q3', 'Q4']
            fig.add_trace(
                go.Scatter(
                    x=[quarters[i-1] for i in quarterly_avg.index],
                    y=quarterly_avg.values,
                    mode='lines+markers',
                    name='Quarterly',
                    line=dict(color=self.colors['info'])
                ),
                row=2, col=2
            )
        
        fig.update_layout(
            title='Seasonal Patterns',
            height=500,
            showlegend=False
        )
        
        return fig
    
    def plot_scenario_comparison(self, historical_data, scenarios):
        """Plot comparison of multiple scenarios"""
        fig = go.Figure()
        
        # Historical data
        fig.add_trace(go.Scatter(
            x=historical_data.index,
            y=historical_data.values,
            mode='lines',
            name='Historical Data',
            line=dict(color=self.colors['dark'], width=2)
        ))
        
        # Generate forecast dates
        if len(historical_data) > 0:
            start_date = historical_data.index[-1]
        else:
            start_date = pd.Timestamp.now()
        
        colors = [self.colors['primary'], self.colors['secondary'], self.colors['success'], 
                 self.colors['warning'], self.colors['info'], self.colors['light']]
        
        for i, (scenario_name, scenario_data) in enumerate(scenarios.items()):
            forecast_dates = pd.date_range(
                start=start_date + pd.Timedelta(days=1),
                periods=len(scenario_data['scenario_forecast']),
                freq='D'
            )
            
            # Base forecast (dotted line)
            if i == 0:  # Only show base forecast once
                fig.add_trace(go.Scatter(
                    x=forecast_dates,
                    y=scenario_data['base_forecast'].values,
                    mode='lines',
                    name='Base Forecast',
                    line=dict(color='gray', width=1, dash='dot')
                ))
            
            # Scenario forecast
            color = colors[i % len(colors)]
            fig.add_trace(go.Scatter(
                x=forecast_dates,
                y=scenario_data['scenario_forecast'].values,
                mode='lines',
                name=scenario_name,
                line=dict(color=color, width=2)
            ))
        
        # Add vertical line to separate historical and forecast
        if len(historical_data) > 0:
            fig.add_vline(
                x=historical_data.index[-1],
                line_dash="dot",
                line_color="gray",
                annotation_text="Forecast Start",
                annotation_position="top"
            )
        
        fig.update_layout(
            title='Scenario Comparison',
            xaxis_title='Date',
            yaxis_title='Demand',
            hovermode='x unified',
            height=500
        )
        
        return fig
    
    def plot_forecast_accuracy(self, actual_data, forecast_data, model_name):
        """Plot forecast accuracy comparison"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Actual vs Forecast', 'Residuals', 'Residual Distribution', 'Cumulative Error'],
            specs=[[{"type": "scatter"}, {"type": "scatter"}],
                   [{"type": "histogram"}, {"type": "scatter"}]]
        )
        
        # Actual vs Forecast
        fig.add_trace(
            go.Scatter(
                x=actual_data.values,
                y=forecast_data.values,
                mode='markers',
                name='Forecast vs Actual',
                marker=dict(color=self.colors['primary'])
            ),
            row=1, col=1
        )
        
        # Perfect forecast line
        min_val = min(actual_data.min(), forecast_data.min())
        max_val = max(actual_data.max(), forecast_data.max())
        fig.add_trace(
            go.Scatter(
                x=[min_val, max_val],
                y=[min_val, max_val],
                mode='lines',
                name='Perfect Forecast',
                line=dict(color='red', dash='dash')
            ),
            row=1, col=1
        )
        
        # Residuals over time
        residuals = actual_data - forecast_data
        fig.add_trace(
            go.Scatter(
                x=actual_data.index,
                y=residuals.values,
                mode='lines+markers',
                name='Residuals',
                line=dict(color=self.colors['warning'])
            ),
            row=1, col=2
        )
        
        # Zero line for residuals
        fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=2)
        
        # Residual distribution
        fig.add_trace(
            go.Histogram(
                x=residuals.values,
                name='Residual Distribution',
                marker_color=self.colors['info']
            ),
            row=2, col=1
        )
        
        # Cumulative error
        cumulative_error = residuals.cumsum()
        fig.add_trace(
            go.Scatter(
                x=actual_data.index,
                y=cumulative_error.values,
                mode='lines',
                name='Cumulative Error',
                line=dict(color=self.colors['success'])
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            title=f'Forecast Accuracy Analysis - {model_name}',
            height=600,
            showlegend=False
        )
        
        return fig
