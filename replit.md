# Demand Planning & Forecasting Application

## Overview
This Streamlit-based web application provides a comprehensive solution for demand planning and forecasting. It enables users to upload historical sales data, generate forecasts, create and analyze "what-if" scenarios, and visualize results. The system aims to streamline business planning, optimize resource allocation, and provide actionable insights for strategic decision-making.

## User Preferences
Preferred communication style: Simple, everyday language.

**CRITICAL CONSTRAINT**: Do NOT make changes to Demand screen, Target Setting, Demand Tweaking, Editable Plan View, Pipeline Management, and Pipeline Calculation Logic unless explicitly requested by user. These core functionalities must remain stable.

**PIPELINE CALCULATION PROTECTION**: The pipeline calculation logic using formula `target ÷ (conversion_rate ÷ 100)` has been fixed and verified. Do NOT modify this calculation logic or the pipeline generation interface without explicit user permission.

**ENVIRONMENT SEPARATION**: Production and development are completely separate environments with different codebases AND different data. Development environment should only use development data, production should only use production data. Never mix data, configurations, or changes between environments. All environment-specific operations must respect the current environment context.

**USER REQUIREMENT**: "Do not make changes to interface, fields and data without checking with me first" - Always confirm before modifying any working functionality.

**CRITICAL USER POLICY**: "DO NOT CREATE ANY TEST DATA WITHOUT ASKING ME" - Never generate, insert, or create any sample, test, or mock data in the database without explicit user permission. Always use existing production data only.

**CRITICAL ID-BASED OPERATIONS**: "Use client id all updates should be based on ID and not name" - All database operations must use client_id/master_client_id as primary keys instead of client names for data consistency and integrity.

**USER REQUIREMENT**: "Screen cleared after save" - confirmed current requirement that forms should ALWAYS clear completely after successful save operations, both in new and edit modes.

**DATA SOURCE REQUIREMENTS**:
- Client data: Use unified_sales_data (demand database) for client dropdowns; master_clients table for metadata (confidence, status) - cleaned of test entries
- Talent data: Always use talent_supply table (Supply Table) for all talent information and dropdowns
- Never use hardcoded or mock data - all dropdowns and selections must be populated from these database tables

## System Architecture

### Frontend
- **Framework**: Streamlit for the user interface.
- **Layout**: Multi-page design with sidebar navigation and tabbed interfaces.
- **State Management**: Streamlit session state for data persistence.
- **Visualization**: Plotly for interactive charts.
- **UI/UX Decisions**: Emphasis on interactive elements, professional appearance, and clear data representation.

### Backend
- **Structure**: Modular, utility-based architecture.
- **Core Modules**: `DataProcessor`, `ForecastingEngine`, `ScenarioBuilder`, `Visualizer`, and `DatabaseManager`.
- **Data Processing**: Supports CSV uploads, automatic date detection and parsing, data cleaning, and persistence to PostgreSQL.
- **Forecasting Models**: Includes Simple Moving Average and Exponential Smoothing.
- **Scenario Types**: Supports percentage changes, absolute adjustments, seasonal modifications, and market event impacts.
- **Financial Year Structure**: Hard-coded to April 1st of current year to March 31st of next year. Q1 (April-June), Q2 (July-Sept), Q3 (Oct-Dec), Q4 (Jan-March of following year) for all calculations. Centralized through FinancialYearManager.
- **User Management**: Role-Based Access Control (RBAC) with user, role, and role group management, including permission inheritance and login tracking.
- **Data Protection**: Multi-layer protection system for critical data, including PostgreSQL triggers for synchronization and overwrite prevention.
- **Performance Optimization**: Asynchronous data fetching, lazy loading, intelligent caching, database connection pooling, and session state optimization.
- **Data Standardization**: Automatic standardization of status names and source mapping during data import and consolidation.
- **Candidate Tracking**: Real-time mapping system connecting candidate statuses to pipeline stages for accurate "Actual #" calculations.
- **Data Transformation Policy**: APPEND-ONLY transformations - all field transformations (status, source, flags) apply only to NEW records during dataaggregator import. Existing candidate records are never modified, preserving complete historical data integrity.
- **Status Standardization**: Centralized candidate status configuration with 7 core standardized statuses: Dropped, On Boarded, On Hold, Rejected, Screening, Selected, Tech Round. Each status can be flagged as Client-managed or Greyamp-managed for process ownership clarity.
- **ID-Based Operations**: All critical database operations (updates, deletions, confidence management) use client_id/master_client_id as primary keys instead of client names for enhanced data integrity and consistency.
- **Deletion Logic**: Comprehensive deletion functionality that removes demand_supply_assignments and reduces client confidence to 50% when demand records are deleted from Existing or Ready for Staffing views, maintaining workflow integrity.
- **Actual vs Planned Tracking**: Enhanced demand_metadata table with actual_start_date and actual_end_date columns for tracking project execution against planned timelines. Includes automatic duration calculations and discrepancy notifications in Demand Tweaking interface with color-coded severity indicators.
- **Pipeline Configuration Testing**: Comprehensive test suite integrated into build pipeline to validate Pipeline Add/Edit functionality. Tests automatically fail build if pipeline management features are broken, ensuring deployment safety. Includes 6 core tests covering pipeline creation, stage management, special stage behavior, editing functionality, and environment isolation.
- **Edit Pipeline Interface**: Redesigned Edit Pipeline interface matching New Pipeline Creation layout for consistency. Features visual workflow display with colored boxes (green for mapped stages, red for unmapped), summary tables, and comprehensive stage management.
- **Pipeline Status Management**: New pipelines default to "Inactive" status when created and automatically activate when linked to Supply Plans. Pipeline status is dynamically managed based on Supply Plan associations.
- **SQL Template Variable Guardian System**: Implemented comprehensive guardian system to prevent SQL template variable errors by automatically detecting and fixing unresolved `{env_table_*}` variables. Integrated guardian checks into build pipeline.
- **Build Pipeline Testing Integration**: Enhanced build pipeline with comprehensive testing including SQL template guardian checks, basic pipeline functionality tests, and guardian system validation.
- **Pipeline Calculation Logic**: Corrected fundamental mathematical error in reverse pipeline calculation to `target ÷ (conversion_rate ÷ 100)`.
- **JSON Serialization**: Resolved critical TypeError in pipeline data save functionality related to `datetime.date` serialization.
- **Add Role Form**: Resolved duplicate "Add Role" form issue and added "Keep form open to add more roles" checkbox functionality.
- **Database Save for New Roles**: Fixed critical bug where new roles added to existing staffing plans were only saved to session state but not persisted to database.
- **Pipeline Data Persistence**: Resolved critical issue where pipeline calculations were not saving to database by storing calculated pipeline results in session state.

### Database
- **Type**: PostgreSQL for persistent storage.
- **Key Tables**: `master_clients`, `talent_supply`, `unified_sales_data`, `demand_metadata`, `demand_supply_assignments`, `roles`, `role_groups`, `user_role_mappings`, `talent_pipelines`, `pipeline_stages`, `pipeline_plan_actuals`, `staffing_plans`, `assignment_history`, `availability_history`.
- **Environment Segregation**: Complete separation between production and development environments with different codebases and data. Production uses clean table names, development uses `dev_` prefixed tables, controlled by `ENVIRONMENT` variable and auto-detection.

## External Dependencies

### Core Libraries
- **Streamlit**: Web application framework.
- **Pandas**: Data manipulation and analysis.
- **NumPy**: Numerical operations.
- **Plotly**: Interactive data visualization.
- **FinancialYearManager**: Centralized financial year logic and month ordering (April-March cycle).

### Statistical/ML Libraries
- **Statsmodels**: Time series analysis and forecasting.
- **Scikit-learn**: Machine learning utilities.

### Utility Libraries
- **DateTime**: Date and time handling.
- **Psycopg2**: PostgreSQL adapter for Python.

### Third-Party Integrations
- **Google Sheets API**: For incremental data synchronization to PostgreSQL, with OAuth2 and Service Account authentication.
- **Google OAuth2**: For secure user authentication with domain restrictions.