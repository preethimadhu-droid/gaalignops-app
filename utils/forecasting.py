import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timedelta
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.seasonal import seasonal_decompose

class ForecastingEngine:
    """Handle various forecasting models and accuracy calculations"""
    
    def __init__(self):
        self.models = {
            "Simple Moving Average": self._simple_moving_average,
            "Exponential Smoothing": self._exponential_smoothing,
            "Double Exponential Smoothing": self._double_exponential_smoothing,
            "Triple Exponential Smoothing": self._triple_exponential_smoothing
        }
    
    def generate_forecast(self, data, demand_column, model_type, forecast_periods, model_params):
        """Generate forecast using specified model"""
        try:
            # Extract time series
            ts_data = data[demand_column].dropna()
            
            if len(ts_data) < 10:
                st.error("❌ Insufficient data for forecasting. Need at least 10 data points.")
                return None
            
            # Generate forecast
            forecast_func = self.models.get(model_type)
            if forecast_func is None:
                st.error(f"❌ Unknown model type: {model_type}")
                return None
            
            forecast_result = forecast_func(ts_data, forecast_periods, model_params)
            
            if forecast_result is None:
                return None
            
            # Calculate accuracy metrics on historical data
            accuracy_metrics = self._calculate_accuracy_metrics(ts_data, model_type, model_params)
            
            # Prepare result
            result = {
                'forecast': forecast_result['forecast'],
                'confidence_interval': forecast_result.get('confidence_interval'),
                'model_type': model_type,
                'parameters': model_params,
                'accuracy_metrics': accuracy_metrics,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return result
            
        except Exception as e:
            st.error(f"❌ Error generating forecast: {str(e)}")
            return None
    
    def _simple_moving_average(self, ts_data, forecast_periods, params):
        """Simple Moving Average forecasting"""
        try:
            window = params.get('window', 7)
            
            if len(ts_data) < window:
                st.error(f"❌ Insufficient data for moving average with window {window}.")
                return None
            
            # Calculate moving average
            moving_avg = ts_data.rolling(window=window).mean()
            last_avg = moving_avg.iloc[-1]
            
            # Generate forecast (constant forecast equal to last moving average)
            forecast = pd.Series([last_avg] * forecast_periods)
            
            # Simple confidence interval (±10% of the forecast value)
            confidence_interval = {
                'lower': forecast * 0.9,
                'upper': forecast * 1.1
            }
            
            return {
                'forecast': forecast,
                'confidence_interval': confidence_interval
            }
            
        except Exception as e:
            st.error(f"❌ Error in moving average forecasting: {str(e)}")
            return None
    
    def _exponential_smoothing(self, ts_data, forecast_periods, params):
        """Single Exponential Smoothing"""
        try:
            alpha = params.get('alpha', 0.3)
            
            # Manual exponential smoothing
            smoothed_values = [ts_data.iloc[0]]
            
            for i in range(1, len(ts_data)):
                smoothed_value = alpha * ts_data.iloc[i] + (1 - alpha) * smoothed_values[-1]
                smoothed_values.append(smoothed_value)
            
            # Forecast is the last smoothed value repeated
            last_smoothed = smoothed_values[-1]
            forecast = pd.Series([last_smoothed] * forecast_periods)
            
            # Calculate confidence interval based on historical errors
            residuals = ts_data - pd.Series(smoothed_values, index=ts_data.index)
            std_error = residuals.std()
            
            confidence_interval = {
                'lower': forecast - 1.96 * std_error,
                'upper': forecast + 1.96 * std_error
            }
            
            return {
                'forecast': forecast,
                'confidence_interval': confidence_interval
            }
            
        except Exception as e:
            st.error(f"❌ Error in exponential smoothing: {str(e)}")
            return None
    
    def _double_exponential_smoothing(self, ts_data, forecast_periods, params):
        """Double Exponential Smoothing (Holt's method)"""
        try:
            alpha = params.get('alpha', 0.3)
            beta = params.get('beta', 0.3)
            
            # Initialize
            level = ts_data.iloc[0]
            trend = ts_data.iloc[1] - ts_data.iloc[0] if len(ts_data) > 1 else 0
            
            forecasts = []
            levels = [level]
            trends = [trend]
            
            # Apply Holt's method
            for i in range(1, len(ts_data)):
                last_level = level
                level = alpha * ts_data.iloc[i] + (1 - alpha) * (level + trend)
                trend = beta * (level - last_level) + (1 - beta) * trend
                
                levels.append(level)
                trends.append(trend)
            
            # Generate forecast
            forecast = pd.Series([level + trend * (h + 1) for h in range(forecast_periods)])
            
            # Calculate confidence interval
            fitted_values = pd.Series([levels[i] + trends[i] for i in range(len(levels))])
            residuals = ts_data - fitted_values[:len(ts_data)]
            std_error = residuals.std()
            
            confidence_interval = {
                'lower': forecast - 1.96 * std_error,
                'upper': forecast + 1.96 * std_error
            }
            
            return {
                'forecast': forecast,
                'confidence_interval': confidence_interval
            }
            
        except Exception as e:
            st.error(f"❌ Error in double exponential smoothing: {str(e)}")
            return None
    
    def _triple_exponential_smoothing(self, ts_data, forecast_periods, params):
        """Triple Exponential Smoothing (Holt-Winters)"""
        try:
            alpha = params.get('alpha', 0.3)
            beta = params.get('beta', 0.3)
            gamma = params.get('gamma', 0.3)
            seasonal_periods = params.get('seasonal_periods', 12)
            
            if len(ts_data) < 2 * seasonal_periods:
                st.warning(f"⚠️ Insufficient data for seasonal forecasting. Using double exponential smoothing instead.")
                return self._double_exponential_smoothing(ts_data, forecast_periods, params)
            
            # Use statsmodels for Holt-Winters
            model = ExponentialSmoothing(
                ts_data,
                trend='add',
                seasonal='add',
                seasonal_periods=seasonal_periods
            )
            
            fitted_model = model.fit(
                smoothing_level=alpha,
                smoothing_trend=beta,
                smoothing_seasonal=gamma
            )
            
            # Generate forecast
            forecast = fitted_model.forecast(steps=forecast_periods)
            
            # Calculate confidence interval
            residuals = ts_data - fitted_model.fittedvalues
            std_error = residuals.std()
            
            confidence_interval = {
                'lower': forecast - 1.96 * std_error,
                'upper': forecast + 1.96 * std_error
            }
            
            return {
                'forecast': forecast,
                'confidence_interval': confidence_interval
            }
            
        except Exception as e:
            st.warning(f"⚠️ Error in triple exponential smoothing: {str(e)}. Falling back to double exponential smoothing.")
            return self._double_exponential_smoothing(ts_data, forecast_periods, params)
    
    def _calculate_accuracy_metrics(self, ts_data, model_type, params):
        """Calculate accuracy metrics using cross-validation"""
        try:
            if len(ts_data) < 20:
                return {}
            
            # Use last 20% of data for validation
            split_point = int(len(ts_data) * 0.8)
            train_data = ts_data[:split_point]
            test_data = ts_data[split_point:]
            
            # Generate forecast for test period
            forecast_func = self.models.get(model_type)
            forecast_result = forecast_func(train_data, len(test_data), params)
            
            if forecast_result is None:
                return {}
            
            forecast_values = forecast_result['forecast']
            
            # Calculate metrics
            mae = mean_absolute_error(test_data, forecast_values)
            rmse = np.sqrt(mean_squared_error(test_data, forecast_values))
            mape = np.mean(np.abs((test_data - forecast_values) / test_data)) * 100
            
            # R-squared (coefficient of determination)
            r2 = r2_score(test_data, forecast_values) if len(test_data) > 1 else 0
            
            return {
                'mae': mae,
                'rmse': rmse,
                'mape': mape,
                'r2': r2
            }
            
        except Exception as e:
            st.warning(f"⚠️ Could not calculate accuracy metrics: {str(e)}")
            return {}
    
    def detect_seasonality(self, ts_data, seasonal_periods=None):
        """Detect seasonality in time series data"""
        try:
            if len(ts_data) < 24:  # Need at least 2 seasonal cycles
                return False, None
            
            # Try different seasonal periods if not specified
            if seasonal_periods is None:
                test_periods = [7, 12, 24, 52]  # Weekly, monthly, bi-monthly, yearly
            else:
                test_periods = [seasonal_periods]
            
            for period in test_periods:
                if len(ts_data) >= 2 * period:
                    try:
                        decomposition = seasonal_decompose(ts_data, model='additive', period=period)
                        
                        # Check if seasonal component has significant variation
                        seasonal_var = decomposition.seasonal.var()
                        total_var = ts_data.var()
                        
                        if seasonal_var / total_var > 0.1:  # Seasonal component explains >10% of variance
                            return True, period
                    except:
                        continue
            
            return False, None
            
        except Exception:
            return False, None
