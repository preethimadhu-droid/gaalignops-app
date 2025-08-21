import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime

class ScenarioBuilder:
    """Build and manage what-if scenarios for demand forecasting"""
    
    def __init__(self):
        self.scenario_types = {
            "percentage": self._apply_percentage_change,
            "absolute": self._apply_absolute_change,
            "seasonal": self._apply_seasonal_adjustment,
            "event": self._apply_market_event
        }
    
    def create_scenario(self, base_forecast_data, scenario_name, scenario_params, start_period, end_period):
        """Create a new what-if scenario based on base forecast"""
        try:
            base_forecast = base_forecast_data['forecast'].copy()
            
            # Validate period ranges
            if start_period < 1 or start_period > len(base_forecast):
                st.error("❌ Invalid start period.")
                return None
            
            if end_period < start_period or end_period > len(base_forecast):
                st.error("❌ Invalid end period.")
                return None
            
            # Apply scenario transformation
            scenario_type = scenario_params.get('type')
            if scenario_type not in self.scenario_types:
                st.error(f"❌ Unknown scenario type: {scenario_type}")
                return None
            
            scenario_forecast = self.scenario_types[scenario_type](
                base_forecast,
                scenario_params,
                start_period - 1,  # Convert to 0-based index
                end_period - 1
            )
            
            if scenario_forecast is None:
                return None
            
            # Create scenario result
            scenario_result = {
                'scenario_name': scenario_name,
                'base_forecast': base_forecast,
                'scenario_forecast': scenario_forecast,
                'scenario_params': scenario_params,
                'period_range': (start_period, end_period),
                'impact_summary': self._calculate_impact_summary(base_forecast, scenario_forecast),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return scenario_result
            
        except Exception as e:
            st.error(f"❌ Error creating scenario: {str(e)}")
            return None
    
    def _apply_percentage_change(self, base_forecast, params, start_idx, end_idx):
        """Apply percentage change to forecast"""
        try:
            change_percent = params.get('value', 0) / 100.0
            scenario_forecast = base_forecast.copy()
            
            # Apply percentage change to specified period range
            scenario_forecast.iloc[start_idx:end_idx+1] *= (1 + change_percent)
            
            return scenario_forecast
            
        except Exception as e:
            st.error(f"❌ Error applying percentage change: {str(e)}")
            return None
    
    def _apply_absolute_change(self, base_forecast, params, start_idx, end_idx):
        """Apply absolute change to forecast"""
        try:
            change_value = params.get('value', 0)
            scenario_forecast = base_forecast.copy()
            
            # Apply absolute change to specified period range
            scenario_forecast.iloc[start_idx:end_idx+1] += change_value
            
            # Ensure non-negative values
            scenario_forecast = scenario_forecast.clip(lower=0)
            
            return scenario_forecast
            
        except Exception as e:
            st.error(f"❌ Error applying absolute change: {str(e)}")
            return None
    
    def _apply_seasonal_adjustment(self, base_forecast, params, start_idx, end_idx):
        """Apply seasonal adjustment to forecast"""
        try:
            seasonal_factor = params.get('factor', 1.0)
            scenario_forecast = base_forecast.copy()
            
            # Create seasonal pattern (sine wave)
            period_length = end_idx - start_idx + 1
            seasonal_pattern = np.sin(np.linspace(0, 2 * np.pi, period_length)) * 0.5 + 1
            seasonal_pattern *= seasonal_factor
            
            # Apply seasonal adjustment
            scenario_forecast.iloc[start_idx:end_idx+1] *= seasonal_pattern
            
            return scenario_forecast
            
        except Exception as e:
            st.error(f"❌ Error applying seasonal adjustment: {str(e)}")
            return None
    
    def _apply_market_event(self, base_forecast, params, start_idx, end_idx):
        """Apply market event impact to forecast"""
        try:
            event_impact = params.get('impact', 0) / 100.0
            event_duration = params.get('duration', 7)
            
            scenario_forecast = base_forecast.copy()
            
            # Calculate actual duration within the specified range
            actual_duration = min(event_duration, end_idx - start_idx + 1)
            
            # Create impact curve (stronger at beginning, tapering off)
            impact_curve = np.exp(-np.linspace(0, 3, actual_duration))
            impact_curve = impact_curve / impact_curve[0]  # Normalize to start at 1
            
            # Apply event impact
            for i in range(actual_duration):
                if start_idx + i <= end_idx:
                    current_impact = event_impact * impact_curve[i]
                    scenario_forecast.iloc[start_idx + i] *= (1 + current_impact)
            
            # Ensure non-negative values
            scenario_forecast = scenario_forecast.clip(lower=0)
            
            return scenario_forecast
            
        except Exception as e:
            st.error(f"❌ Error applying market event: {str(e)}")
            return None
    
    def _calculate_impact_summary(self, base_forecast, scenario_forecast):
        """Calculate summary of scenario impact"""
        try:
            base_total = base_forecast.sum()
            scenario_total = scenario_forecast.sum()
            
            # Overall impact
            total_impact = ((scenario_total - base_total) / base_total) * 100 if base_total != 0 else 0
            
            # Period-by-period impact
            period_impacts = ((scenario_forecast - base_forecast) / base_forecast) * 100
            period_impacts = period_impacts.fillna(0)
            
            # Summary statistics
            max_positive_impact = period_impacts.max()
            max_negative_impact = period_impacts.min()
            avg_impact = period_impacts.mean()
            
            return {
                'total_impact_percent': total_impact,
                'base_total': base_total,
                'scenario_total': scenario_total,
                'max_positive_impact': max_positive_impact,
                'max_negative_impact': max_negative_impact,
                'average_impact': avg_impact,
                'affected_periods': len(period_impacts[period_impacts != 0])
            }
            
        except Exception as e:
            st.warning(f"⚠️ Could not calculate impact summary: {str(e)}")
            return {}
    
    def compare_scenarios(self, scenarios):
        """Compare multiple scenarios and provide insights"""
        try:
            comparison_data = []
            
            for scenario_name, scenario_data in scenarios.items():
                impact_summary = scenario_data.get('impact_summary', {})
                
                comparison_data.append({
                    'Scenario': scenario_name,
                    'Total Impact (%)': f"{impact_summary.get('total_impact_percent', 0):.2f}%",
                    'Base Total': f"{impact_summary.get('base_total', 0):.2f}",
                    'Scenario Total': f"{impact_summary.get('scenario_total', 0):.2f}",
                    'Max Impact (%)': f"{impact_summary.get('max_positive_impact', 0):.2f}%",
                    'Min Impact (%)': f"{impact_summary.get('max_negative_impact', 0):.2f}%",
                    'Avg Impact (%)': f"{impact_summary.get('average_impact', 0):.2f}%"
                })
            
            return pd.DataFrame(comparison_data)
            
        except Exception as e:
            st.error(f"❌ Error comparing scenarios: {str(e)}")
            return pd.DataFrame()
    
    def generate_scenario_insights(self, scenario_data):
        """Generate insights for a specific scenario"""
        try:
            insights = []
            impact_summary = scenario_data.get('impact_summary', {})
            
            total_impact = impact_summary.get('total_impact_percent', 0)
            
            # Overall impact assessment
            if total_impact > 10:
                insights.append(f"📈 **Significant Positive Impact**: This scenario increases total demand by {total_impact:.1f}%")
            elif total_impact > 0:
                insights.append(f"📊 **Moderate Positive Impact**: This scenario increases total demand by {total_impact:.1f}%")
            elif total_impact < -10:
                insights.append(f"📉 **Significant Negative Impact**: This scenario decreases total demand by {abs(total_impact):.1f}%")
            elif total_impact < 0:
                insights.append(f"📊 **Moderate Negative Impact**: This scenario decreases total demand by {abs(total_impact):.1f}%")
            else:
                insights.append("➡️ **Neutral Impact**: This scenario has minimal impact on total demand")
            
            # Volatility assessment
            max_positive = impact_summary.get('max_positive_impact', 0)
            max_negative = impact_summary.get('max_negative_impact', 0)
            
            if max_positive > 25 or abs(max_negative) > 25:
                insights.append("⚠️ **High Volatility**: This scenario introduces significant demand fluctuations")
            
            # Affected periods
            affected_periods = impact_summary.get('affected_periods', 0)
            if affected_periods > 0:
                insights.append(f"📅 **Duration**: Impact affects {affected_periods} forecast periods")
            
            return insights
            
        except Exception as e:
            st.warning(f"⚠️ Could not generate scenario insights: {str(e)}")
            return ["No insights available"]
    
    def export_scenario_analysis(self, scenarios):
        """Export detailed scenario analysis"""
        try:
            analysis_data = []
            
            for scenario_name, scenario_data in scenarios.items():
                base_forecast = scenario_data['base_forecast']
                scenario_forecast = scenario_data['scenario_forecast']
                
                for i in range(len(base_forecast)):
                    period_impact = ((scenario_forecast.iloc[i] - base_forecast.iloc[i]) / base_forecast.iloc[i]) * 100 if base_forecast.iloc[i] != 0 else 0
                    
                    analysis_data.append({
                        'Scenario': scenario_name,
                        'Period': i + 1,
                        'Base_Forecast': base_forecast.iloc[i],
                        'Scenario_Forecast': scenario_forecast.iloc[i],
                        'Absolute_Difference': scenario_forecast.iloc[i] - base_forecast.iloc[i],
                        'Percentage_Impact': period_impact
                    })
            
            return pd.DataFrame(analysis_data)
            
        except Exception as e:
            st.error(f"❌ Error exporting scenario analysis: {str(e)}")
            return pd.DataFrame()
