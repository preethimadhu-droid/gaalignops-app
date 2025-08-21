import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import io
import base64
import time
try:
    import psycopg2
except ImportError:
    psycopg2 = None
import os
import logging
import threading
from typing import Optional, Dict, Any, List, Tuple, Union

# Load environment variables from .env file FIRST
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')

# Import utility modules
from utils.data_processing import DataProcessor
from utils.forecasting import ForecastingEngine
from utils.visualization import Visualizer
from utils.scenario_builder import ScenarioBuilder
from utils.database import DatabaseManager
from utils.environment_manager import EnvironmentManager
from utils.sales_dashboard_processor import SalesDashboardProcessor
from utils.sales_data_manager import SalesDataManager
from utils.unified_data_manager import UnifiedDataManager
from utils.cycle_time_analyzer import CycleTimeAnalyzer
from auth import check_auth, login_page, user_header, require_auth, load_user_permissions

# Import performance optimization modules
from utils.performance_manager import get_performance_manager
from utils.session_optimizer import optimize_session, get_connection_manager

# Initialize environment manager
env_manager = EnvironmentManager()

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Import scheduler for automatic Google Sheets sync
try:
    from utils.scheduler import DataSyncScheduler, data_sync_scheduler
except ImportError as e:
    logger.warning(f"Scheduler import failed: {e}. Scheduler features will be disabled.")
    class DummyScheduler:
        def start_scheduler(self): pass
        def get_scheduler_status(self): return {'is_running': False, 'status': 'disabled'}
    data_sync_scheduler = DummyScheduler()

# Page configuration will be done in main()

def handle_oauth_callback():
    """Handle OAuth callback with automatic authorization code detection"""
    try:
        # Check for OAuth callback parameters in URL
        query_params = st.query_params.to_dict() if hasattr(st, 'query_params') else {}
        
        # Look for authorization code in URL parameters
        auth_code = query_params.get('code')
        state = query_params.get('state')
        
        if auth_code:
            logger.info(f"OAuth callback detected with authorization code: {auth_code[:20]}...")
            
            # Store the authorization code in session state for processing
            st.session_state.oauth_auth_code = auth_code
            st.session_state.oauth_state = state
            st.session_state.oauth_callback_detected = True
            
            # Clear URL parameters to clean up the URL
            st.query_params.clear()
            
            # Redirect to Settings page with Google Sheets tab
            st.session_state.settings_page = "Settings"
            st.session_state.settings_tab = "google_sheets"
            st.session_state.oauth_auto_process = True
            
            return True
            
    except Exception as e:
        logger.error(f"Error handling OAuth callback: {e}")
        
    return False

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = None
if 'forecasts' not in st.session_state:
    st.session_state.forecasts = {}
if 'scenarios' not in st.session_state:
    st.session_state.scenarios = {}
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = DatabaseManager()
if 'sales_db_manager' not in st.session_state:
    st.session_state.sales_db_manager = SalesDataManager(env_manager)
if 'current_dataset_id' not in st.session_state:
    st.session_state.current_dataset_id = None

# Permission checking helpers
def has_module_access(module_name, sub_page=None, permission_type='view'):
    """Check if current user has access to a module/sub-page"""
    user_email = st.session_state.get('user_email')
    permission_manager = st.session_state.get('permission_manager')
    
    if not user_email or not permission_manager:
        return False
    
    if sub_page:
        return permission_manager.has_permission(user_email, module_name, sub_page, permission_type)
    else:
        return permission_manager.can_access_module(user_email, module_name)

def show_permission_error(module_name, action="access"):
    """Display permission denied message"""
    st.error(f"🚫 Access Denied: You don't have permission to {action} {module_name}.")
    st.info("Please contact your administrator if you need access to this feature.")

def permission_protected_button(label, module_name, sub_page, permission_type='add', **kwargs):
    """Create a button that only shows if user has permission"""
    if has_module_access(module_name, sub_page, permission_type):
        return st.button(label, **kwargs)
    else:
        st.info(f"🔒 {permission_type.title()} access required for this feature")
        return False

def staffing_plans_section():
    """Staffing Plans form section for creating/editing staffing plans"""
    st.markdown("#### 📝 Create/Edit Staffing Plan")
    
    # Initialize necessary managers
    from utils.supply_data_manager import SupplyDataManager
    from utils.staffing_plans_manager import StaffingPlansManager
    
    supply_manager = SupplyDataManager(env_manager)
    staffing_manager = StaffingPlansManager(env_manager)
    
    # Check if we're in edit mode
    edit_plan_id = st.session_state.get('edit_staffing_plan_id')
    is_edit_mode = edit_plan_id is not None
    
    if is_edit_mode:
        # Editing mode - load existing plan data
        # Load existing plan data
        try:
            plan_data = staffing_manager.get_staffing_plan(edit_plan_id)
            if plan_data:
                # Pre-populate session state with existing data
                st.session_state.current_plan_name = plan_data.get('plan_name', '')
                st.session_state.selected_client_name = plan_data.get('client_name', '')
                st.session_state.planned_positions = plan_data.get('planned_positions', 0)
                st.session_state.target_hires = plan_data.get('target_hires', 0)
                st.session_state.current_plan_id = edit_plan_id
                
                # Load pipeline information if available
                pipeline_name = plan_data.get('pipeline_name')
                pipeline_id = plan_data.get('pipeline_id')
                if pipeline_name:
                    st.session_state.selected_pipeline_name = pipeline_name
                if pipeline_id:
                    st.session_state.selected_pipeline_id = pipeline_id
                
                # Load existing staffing plan rows from database
                if 'staffing_plan_rows' not in st.session_state:
                    existing_rows = staffing_manager.load_staffing_plan_rows(edit_plan_id)
                    if existing_rows:
                        st.session_state.staffing_plan_rows = existing_rows
                        st.info(f"✅ Loaded {len(existing_rows)} existing roles from database")
                    else:
                        st.info("ℹ️ No existing roles found for this plan")
                        st.session_state.staffing_plan_rows = []
                
                # Load existing generated pipeline plans if available
                if 'show_generated_plans' not in st.session_state:
                    try:
                        existing_plans = staffing_manager.load_generated_pipeline_plan(edit_plan_id)
                        if existing_plans:
                            st.session_state.show_generated_plans = True
                            st.session_state.generated_pipeline_data = existing_plans
                    except Exception as e:
                        # Table might not exist yet, that's okay
                        pass
                
                # Handle date loading - only set if not already in session state (to preserve user changes)
                if 'current_from_date' not in st.session_state or 'current_to_date' not in st.session_state:
                    from_date = plan_data.get('target_start_date') or plan_data.get('from_date')
                    to_date = plan_data.get('target_end_date') or plan_data.get('to_date')
                    
                    # Handle date conversion and ensure dates are within valid range
                    if isinstance(from_date, str):
                        parsed_from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
                    elif from_date:
                        parsed_from_date = from_date
                    else:
                        parsed_from_date = datetime.now().date()
                        
                    if isinstance(to_date, str):
                        parsed_to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
                    elif to_date:
                        parsed_to_date = to_date
                    else:
                        parsed_to_date = datetime.now().date() + timedelta(days=90)
                    
                    # Ensure dates are not in the past to avoid validation errors
                    today = datetime.now().date()
                    if parsed_from_date < today:
                        parsed_from_date = today
                    if parsed_to_date < parsed_from_date:
                        parsed_to_date = parsed_from_date + timedelta(days=90)
                    
                    # Only set if not already set (preserves user changes during form interaction)
                    if 'current_from_date' not in st.session_state:
                        st.session_state.current_from_date = parsed_from_date
                    if 'current_to_date' not in st.session_state:
                        st.session_state.current_to_date = parsed_to_date
        except Exception as e:
            st.error(f"Error loading plan data: {str(e)}")
    
    # Step 1: Client Selection
    st.markdown("**Step 1: Client Selection**")
    
    # Get client options FROM master_clients table
    try:
        import psycopg2
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        master_clients_table = env_manager.get_table_name('master_clients')
        cursor.execute(f"SELECT DISTINCT client_name FROM {master_clients_table} ORDER BY client_name")
        client_options = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if client_options:
            # Add a default "Select a client" option at the beginning
            client_options_with_default = ["-- Select a client --"] + client_options
            
            default_client = st.session_state.get('selected_client_name', '')
            client_index = 0  # Default to "-- Select a client --"
            
            # Set index if we have a valid saved client (for both edit mode and new with auto-selection)
            if default_client and default_client in client_options:
                client_index = client_options_with_default.index(default_client)
            
            selected_client = st.selectbox(
                "Select Client", 
                client_options_with_default,
                index=client_index,
                key="staffing_client_select"
            )
            
            # Only store the client name if it's not the default option
            if selected_client != "-- Select a client --":
                st.session_state.selected_client_name = selected_client
            else:
                # Clear the selection if default is selected
                if 'selected_client_name' in st.session_state:
                    del st.session_state['selected_client_name']
        else:
            st.error("No clients found in database")
            return
            
    except Exception as e:
        st.error(f"Error loading clients: {str(e)}")
        return
    
    # Step 2: Plan Details
    st.markdown("**Step 2: Plan Details**")
    
    col1, col2 = st.columns(2)
    with col1:
        plan_name = st.text_input(
            "Plan Name", 
            value=st.session_state.get('current_plan_name', ''),
            placeholder="e.g., Q3 2025 Software Engineers",
            key="staffing_plan_name"
        )
        st.session_state.current_plan_name = plan_name
    
    with col2:
        # In edit mode, use existing planned positions; in new mode, auto-calculate
        if is_edit_mode and 'planned_positions' in st.session_state:
            # Use existing planned positions from database in edit mode
            calculated_positions = st.session_state.planned_positions
        else:
            # Auto-calculate planned positions from demand data for new plans
            calculated_positions = 1  # Default fallback
            
            # Get selected client and calculate Booked - Billed positions
            selected_client = st.session_state.get('selected_client_name')
            if selected_client and selected_client != "-- Select a client --":
                try:
                    import psycopg2
                    conn = psycopg2.connect(os.environ['DATABASE_URL'])
                    cursor = conn.cursor()
                    
                    # Calculate Planned Positions = Booked - Billed from demand data
                    unified_sales_data_table = env_manager.get_table_name('unified_sales_data')
                    cursor.execute(f"""
                        WITH client_booked AS (
                            SELECT COALESCE(SUM(value), 0) as total_booked
                            FROM {unified_sales_data_table}
                            WHERE client_name = %s AND metric_type = 'Booked'
                        ),
                        client_billed AS (
                            SELECT COALESCE(SUM(value), 0) as total_billed
                            FROM {unified_sales_data_table}
                            WHERE client_name = %s AND metric_type = 'Billed'
                        )
                        SELECT 
                            COALESCE(cb.total_booked, 0) as total_booked,
                            COALESCE(cbl.total_billed, 0) as total_billed
                        FROM client_booked cb, client_billed cbl
                    """, (selected_client, selected_client))
                    
                    result = cursor.fetchone()
                    if result:
                        total_booked, total_billed = result
                        calculated_positions = max(1, int(total_booked - total_billed))  # Ensure at least 1
                        

                    
                    conn.close()
                except Exception as e:
                    st.error(f"Error calculating positions: {str(e)}")
        
        planned_positions = st.number_input(
            "Planned Positions", 
            min_value=1.0, 
            value=float(calculated_positions),
            key="staffing_planned_positions",
            help=f"{'Saved from database' if is_edit_mode else 'Auto-calculated from demand data (Booked - Billed positions)'}",
            disabled=True
        )
        st.session_state.planned_positions = planned_positions
    
    # Add Target Hires field in a new row
    col1, col2 = st.columns(2)
    with col1:
        target_hires = st.number_input(
            "Target Hires", 
            min_value=0, 
            value=st.session_state.get('target_hires', 0),
            key="staffing_target_hires",
            help="Number of people to be hired for this plan"
        )
        st.session_state.target_hires = target_hires
    
    with col2:
        st.write("")  # Spacer for alignment
    
    # Step 3: Time Period
    st.markdown("**Step 3: Time Period**")
    
    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input(
            "From Date", 
            value=st.session_state.get('current_from_date', datetime.now().date()),
            key="staffing_from_date"
        )
        st.session_state.current_from_date = from_date
        
    with col2:
        to_date = st.date_input(
            "To Date", 
            value=st.session_state.get('current_to_date', datetime.now().date() + timedelta(days=90)),
            key="staffing_to_date"
        )
        st.session_state.current_to_date = to_date
    
    # Calculate and display duration
    if from_date and to_date and to_date > from_date:
        duration_days = (to_date - from_date).days
        duration_months = round(duration_days / 30.44, 1)  # Average days per month
        st.caption(f"📅 Staffing Plan Duration: {duration_months} month(s) total (From {from_date.strftime('%B %Y')} to {to_date.strftime('%B %Y')})")
    
    # Step 4: Detailed Staffing Plan Builder
    st.markdown("**Step 4: Detailed Staffing Plan Builder**")
    
    # Initialize staffing plan rows in session state if not exists
    if 'staffing_plan_rows' not in st.session_state:
        st.session_state.staffing_plan_rows = []
    
    # Summary Panel at the top
    st.markdown("### 📊 Summary Panel")
    
    # Debug: Show session state info
    if st.checkbox("🔍 Debug Info", key="debug_checkbox"):
        st.info(f"**Session State Debug:**")
        st.write(f"- staffing_plan_rows count: {len(st.session_state.get('staffing_plan_rows', []))}")
        st.write(f"- current_plan_id: {st.session_state.get('current_plan_id', 'None')}")
        st.write(f"- edit_staffing_plan_id: {st.session_state.get('edit_staffing_plan_id', 'None')}")
        if st.session_state.get('staffing_plan_rows'):
            st.write(f"- Roles in session: {[row.get('role', 'Unknown') for row in st.session_state.staffing_plan_rows]}")
    
    # Calculate totals from existing rows
    total_planned = sum(row.get('positions', 0) for row in st.session_state.staffing_plan_rows)
    balance_positions = target_hires - total_planned
    
    # Display demand context first
    if planned_positions and planned_positions != target_hires:
        st.info(f"📋 **Demand Requirement**: {planned_positions} total positions needed (from Demand Planning: Booked - Billed)")
        st.caption(f"**This staffing plan** targets {target_hires} hires for the period {from_date.strftime('%b %Y')} to {to_date.strftime('%b %Y')}")
        st.markdown("---")
    
    # Calculate number of roles linked
    roles_linked_count = len(st.session_state.staffing_plan_rows)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Target Hires", target_hires, help="Number of hires planned for this specific time period")
    with col2:
        st.metric("Roles Planned", total_planned, help="Sum of positions from detailed role breakdown below")
    with col3:
        st.metric("# of Roles Linked", roles_linked_count, help="Number of different roles configured in this staffing plan")
    with col4:
        color = "normal" if balance_positions >= 0 else "inverse"
        st.metric("Balance", balance_positions, delta=None, help="Target Hires - Roles Planned")
        if balance_positions < 0:
            st.error(f"⚠️ Over-planned by {abs(balance_positions)} positions!")
        elif balance_positions > 0:
            st.warning(f"💡 {balance_positions} positions remaining to plan")
        else:
            st.success("✅ All target positions planned!")
    
    st.markdown("---")
    
    # Get available data for dropdowns
    try:
        import psycopg2
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        # Get pipeline configurations for the selected client
        selected_client = st.session_state.get('selected_client_name')
        pipeline_configs = []
        pipeline_options = ["-- Select a pipeline --"]
        
        if selected_client and selected_client != "-- Select a client --":
            # Get client_id
            master_clients_table = env_manager.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", (selected_client,))
            client_result = cursor.fetchone()
            
            if client_result:
                client_id = client_result[0]
                talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
                cursor.execute(f"""
                    SELECT tp.id, tp.name, tp.description, tp.is_active 
                    FROM {talent_pipelines_table} tp 
                    WHERE tp.client_id = %s
                    ORDER BY tp.name
                """, (client_id,))
                pipeline_configs = cursor.fetchall()
                pipeline_options = ["-- Select a pipeline --"] + [f"{config[1]}" for config in pipeline_configs]
        
        # Get talent data for owner dropdown
        talent_supply_table = env_manager.get_table_name('talent_supply')
        cursor.execute(f"""
            SELECT id, name 
            FROM {talent_supply_table} 
            WHERE employment_status = 'Active' AND type = 'FTE' 
            ORDER BY name
        """)
        talent_data = cursor.fetchall()
        owner_options = ["-- Select Owner --"] + [talent[1] for talent in talent_data]
        
        conn.close()
        
        # Display existing staffing plan rows
        if st.session_state.staffing_plan_rows:
            st.markdown("### 📋 Staffing Plan Details")
            
            for idx, row in enumerate(st.session_state.staffing_plan_rows):
                with st.expander(f"📍 Role {idx + 1}: {row.get('role', 'Untitled')} ({row.get('positions', 0)} positions)", expanded=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.text_input("Role", value=row.get('role', ''), disabled=True, key=f"view_role_{idx}")
                        st.text_input("Skills", value=row.get('skills', ''), disabled=True, key=f"view_skills_{idx}")
                        st.number_input("# of Positions", value=row.get('positions', 0), disabled=True, key=f"view_positions_{idx}")
                    
                    with col2:
                        st.date_input("To be Staffed By", value=row.get('staffed_by_date', from_date), disabled=True, key=f"view_date_{idx}")
                        st.selectbox("Pipeline", [row.get('pipeline', '-- Select a pipeline --')], disabled=True, key=f"view_pipeline_{idx}")
                        st.selectbox("Owner", [row.get('owner', '-- Select Owner --')], disabled=True, key=f"view_owner_{idx}")
                    
                    # Edit and Delete buttons
                    col_edit, col_delete = st.columns([1, 1])
                    with col_edit:
                        if st.button(f"✏️ Edit", key=f"edit_row_{idx}"):
                            st.session_state.editing_row = idx
                            st.rerun()
                    with col_delete:
                        if st.button(f"🗑️ Delete", key=f"delete_row_{idx}"):
                            del st.session_state.staffing_plan_rows[idx]
                            
                            # Save the updated staffing plan rows to database immediately
                            current_plan_id = st.session_state.get('edit_staffing_plan_id') or st.session_state.get('current_plan_id')
                            if current_plan_id:
                                try:
                                    success = staffing_manager.save_staffing_plan_rows(current_plan_id, st.session_state.staffing_plan_rows)
                                    if success:
                                        st.success("✅ Role deleted and saved to database!")
                                    else:
                                        st.warning("⚠️ Role deleted but could not save to database")
                                except Exception as e:
                                    st.error(f"❌ Error saving deletion to database: {str(e)}")
                            else:
                                st.success("Row deleted!")
                            
                            st.rerun()
        
        # Add Role Button or Edit Form
        st.markdown("---")
        editing_idx = st.session_state.get('editing_row', None)
        show_add_form = st.session_state.get('show_add_role_form', False)
        
        # Show Add Role button only if not editing and not already showing form
        if editing_idx is None and not show_add_form:
            if st.button("➕ Add Role", type="primary"):
                st.session_state.show_add_role_form = True
                st.rerun()
        
        # Show form if editing or add form is requested
        if editing_idx is not None or show_add_form:
            form_title = f"✏️ Edit Row {editing_idx + 1}" if editing_idx is not None else "➕ Add New Staffing Plan Row"
            st.markdown(f"### {form_title}")
            
            # Pre-populate form if editing
            if editing_idx is not None and editing_idx < len(st.session_state.staffing_plan_rows):
                edit_row = st.session_state.staffing_plan_rows[editing_idx]
                default_role = edit_row.get('role', '')
                default_skills = edit_row.get('skills', '')
                default_positions = edit_row.get('positions', 1)
                default_date = edit_row.get('staffed_by_date', from_date)
                default_pipeline = edit_row.get('pipeline', '-- Select a pipeline --')
                default_owner = edit_row.get('owner', '-- Select Owner --')
            else:
                default_role = ''
                default_skills = ''
                default_positions = 1
                default_date = from_date
                default_pipeline = '-- Select a pipeline --'
                default_owner = '-- Select Owner --'
            
            with st.form("staffing_plan_row_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    new_role = st.text_input("Role *", value=default_role, help="Enter the job role/position title")
                    new_skills = st.text_input("Skills *", value=default_skills, help="Enter required skills and qualifications")
                    new_positions = st.number_input("# of Positions *", min_value=1, value=default_positions, help="Number of positions for this role")
                
                with col2:
                    # Ensure default_date is within valid range
                    if default_date < from_date:
                        default_date = from_date
                    elif default_date > to_date:
                        default_date = to_date
                        
                    new_staffed_by_date = st.date_input(
                        "To be Staffed By *", 
                        value=default_date,
                        min_value=from_date,
                        max_value=to_date,
                        help=f"Date must be between {from_date} and {to_date}"
                    )
                    
                    # Pipeline dropdown with proper index
                    pipeline_index = 0
                    if default_pipeline in pipeline_options:
                        pipeline_index = pipeline_options.index(default_pipeline)
                    new_pipeline = st.selectbox("Pipeline Applicable *", pipeline_options, index=pipeline_index, help="Select the recruitment pipeline for this role")
                    
                    # Owner dropdown with proper index
                    owner_index = 0
                    if default_owner in owner_options:
                        owner_index = owner_options.index(default_owner)
                    new_owner = st.selectbox("Owner *", owner_options, index=owner_index, help="Select the talent manager responsible for this role")
                
                # Form validation and submission
                col_submit, col_cancel = st.columns([1, 1])
                with col_submit:
                    submit_button = st.form_submit_button("💾 Save Row", type="primary")
                with col_cancel:
                    cancel_button = st.form_submit_button("❌ Cancel")
            
                if submit_button:
                    # Validation
                    errors = []
                    
                    if not new_role.strip():
                        errors.append("Role is required")
                    if not new_skills.strip():
                        errors.append("Skills are required")
                    if new_positions <= 0:
                        errors.append("Number of positions must be greater than 0")
                    if new_pipeline == "-- Select a pipeline --":
                        errors.append("Pipeline selection is required")
                    if new_owner == "-- Select Owner --":
                        errors.append("Owner selection is required")
                    
                    # Check total positions don't exceed target hires
                    current_total = total_planned
                    if editing_idx is not None:
                        # Subtract the current row's positions if editing
                        current_total -= st.session_state.staffing_plan_rows[editing_idx].get('positions', 0)
                    
                    if current_total + new_positions > target_hires:
                        excess = (current_total + new_positions) - target_hires
                        errors.append(f"Total positions would exceed Target Hires by {excess}. Reduce by {excess} positions.")
                    
                    if errors:
                        st.error("Please fix the following errors:")
                        for error in errors:
                            st.error(f"• {error}")
                    else:
                        # Create new row data
                        new_row = {
                            'role': new_role.strip(),
                            'skills': new_skills.strip(),
                            'positions': new_positions,
                            'staffed_by_date': new_staffed_by_date,
                            'pipeline': new_pipeline,
                            'owner': new_owner
                        }
                        
                        # Add or update row
                        if editing_idx is not None:
                            st.session_state.staffing_plan_rows[editing_idx] = new_row
                            st.session_state.editing_row = None
                            st.success("Row updated successfully!")
                        else:
                            st.session_state.staffing_plan_rows.append(new_row)
                            st.session_state.show_add_role_form = False  # Hide form after adding
                            st.success("New row added successfully!")
                        
                        # Save the updated staffing plan rows to database immediately
                        current_plan_id = st.session_state.get('edit_staffing_plan_id') or st.session_state.get('current_plan_id')
                        if current_plan_id:
                            try:
                                success = staffing_manager.save_staffing_plan_rows(current_plan_id, st.session_state.staffing_plan_rows)
                                if success:
                                    st.success("✅ Role saved to database successfully!")
                                else:
                                    st.warning("⚠️ Role added but could not save to database")
                            except Exception as e:
                                st.error(f"❌ Error saving role to database: {str(e)}")
                        
                        st.rerun()
                
                if cancel_button:
                    if editing_idx is not None:
                        st.session_state.editing_row = None
                    else:
                        st.session_state.show_add_role_form = False  # Hide form when canceling add
                    st.rerun()
        
        # Action Buttons
        st.markdown("---")
        st.markdown("### 🎯 Actions")
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.write("")  # Spacer
        
        with col2:
            generate_disabled = len(st.session_state.staffing_plan_rows) == 0
            generate_help = "Generate pipeline plans for all roles" if not generate_disabled else "Add roles first to generate plans"
            
            if st.button("🚀 Generate Plan", disabled=generate_disabled, help=generate_help):
                if st.session_state.staffing_plan_rows:
                    # Generate pipeline plans for each row and store for database persistence
                    generated_data = []
                    for idx, row in enumerate(st.session_state.staffing_plan_rows):
                        try:
                            # Get pipeline configuration
                            pipeline_name = row['pipeline']
                            selected_pipeline_id = None
                            
                            # Debug: Show available pipeline configs
                            st.info(f"🔍 DEBUG: Looking for pipeline '{pipeline_name}' in available configs: {[config[1] for config in pipeline_configs]}")
                            
                            for config in pipeline_configs:
                                if config[1] == pipeline_name:
                                    selected_pipeline_id = config[0]
                                    st.success(f"✅ Found pipeline '{pipeline_name}' with ID {selected_pipeline_id}")
                                    break
                            
                            if selected_pipeline_id:
                                from utils.pipeline_manager import PipelineManager
                                pipeline_mgr = PipelineManager(env_manager)
                                
                                # Debug: Show what we're querying
                                st.info(f"🔍 DEBUG: Querying pipeline ID {selected_pipeline_id} for role {row['role']}")
                                st.info(f"🔍 DEBUG: Using table names - talent_pipelines: {env_manager.get_table_name('talent_pipelines')}, pipeline_stages: {env_manager.get_table_name('pipeline_stages')}")
                                
                                # Calculate reverse pipeline using role positions and staffed_by_date
                                try:
                                    pipeline_results = pipeline_mgr.calculate_reverse_pipeline(
                                        selected_pipeline_id, row['positions'], row['staffed_by_date']
                                    )
                                    
                                    if pipeline_results:
                                        generated_data.append({
                                            'role': row['role'],
                                            'pipeline_id': selected_pipeline_id,
                                            'pipeline_name': pipeline_name,
                                            'pipeline_owner': row.get('owner', ''),
                                            'pipeline_results': pipeline_results
                                        })
                                        st.success(f"✅ Generated pipeline for {row['role']} successfully!")
                                    else:
                                        st.warning(f"⚠️ Could not generate pipeline for {row['role']} - no pipeline stages found")
                                        st.info(f"🔍 DEBUG: Pipeline results returned None - this means either no stages found or calculation failed")
                                except Exception as e:
                                    st.error(f"❌ Error generating pipeline for {row['role']}: {str(e)}")
                                    st.error(f"🔍 DEBUG: Exception type: {type(e).__name__}")
                                    import traceback
                                    st.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
                            else:
                                st.warning(f"⚠️ No pipeline ID found for {row['role']} with pipeline '{pipeline_name}'")
                        except Exception as e:
                            st.error(f"❌ Could not generate plan for {row.get('role', 'Unknown role')}: {str(e)}")
                    
                    # Store generated data for database persistence
                    st.session_state.generated_pipeline_data = generated_data
                    st.session_state.show_generated_plans = True
                    
                    # Save generated plans to database if we have a plan_id
                    current_plan_id = st.session_state.get('edit_staffing_plan_id') or st.session_state.get('current_plan_id')
                    
                    if current_plan_id and generated_data:
                        try:
                            # Debug: Log the data structure being sent
                            st.info(f"🔍 DEBUG: Generated data structure: {generated_data}")
                            
                            pipeline_data = {
                                'generated_plans': generated_data,
                                'created_by': st.session_state.get('user_info', {}).get('email', 'admin')
                            }
                            
                            st.info(f"🔍 DEBUG: Pipeline data being sent: {pipeline_data}")
                            
                            success = staffing_manager.save_generated_pipeline_plan(current_plan_id, pipeline_data)
                            if success:
                                st.success("✅ Pipeline plans generated and saved to database!")
                                # Clear session state to force reload of fresh data
                                if 'show_generated_plans' in st.session_state:
                                    del st.session_state['show_generated_plans']
                                if 'generated_pipeline_data' in st.session_state:
                                    del st.session_state['generated_pipeline_data']
                            else:
                                st.warning("⚠️ Pipeline plans generated but could not save to database")
                        except Exception as e:
                            st.error(f"❌ Pipeline plans generated but database save failed: {str(e)}")
                            st.error(f"❌ Error type: {type(e).__name__}")
                            import traceback
                            st.error(f"❌ Full error: {traceback.format_exc()}")
                    else:
                        if not current_plan_id:
                            st.info("💡 Pipeline plans generated! Save the staffing plan first to persist these plans.")
                        else:
                            st.success("✅ Pipeline plans generated successfully!")
                    
                    st.rerun()
        
        with col3:
            if st.button("❌ Cancel"):
                # Clear all staffing plan rows and form states
                st.session_state.staffing_plan_rows = []
                if 'editing_row' in st.session_state:
                    del st.session_state['editing_row']
                if 'show_generated_plans' in st.session_state:
                    del st.session_state['show_generated_plans']
                if 'show_add_role_form' in st.session_state:
                    del st.session_state['show_add_role_form']
                st.info("Staffing plan builder cleared")
                st.rerun()
        
        # Add a reload button to manually refresh roles from database
        if st.button("🔄 Reload Roles from Database"):
            current_plan_id = st.session_state.get('edit_staffing_plan_id') or st.session_state.get('current_plan_id')
            if current_plan_id:
                try:
                    existing_rows = staffing_manager.load_staffing_plan_rows(current_plan_id)
                    if existing_rows:
                        st.session_state.staffing_plan_rows = existing_rows
                        st.success(f"✅ Reloaded {len(existing_rows)} roles from database!")
                    else:
                        st.info("ℹ️ No roles found in database for this plan")
                        st.session_state.staffing_plan_rows = []
                except Exception as e:
                    st.error(f"❌ Error reloading roles: {str(e)}")
            else:
                st.warning("⚠️ No plan ID available to reload roles")
            st.rerun()
        
        # Show Generated Pipeline Plans
        if st.session_state.get('show_generated_plans', False) and st.session_state.staffing_plan_rows:
            st.markdown("---")
            st.markdown("### 📈 Generated Pipeline Plans")
            
            # Check if we have saved pipeline data first
            saved_pipeline_data = st.session_state.get('generated_pipeline_data', [])
            
            for idx, row in enumerate(st.session_state.staffing_plan_rows):
                with st.expander(f"📊 Pipeline Plan for {row['role']} - {row['owner']}", expanded=True):
                    try:
                        # First try to use saved pipeline data
                        pipeline_results = None
                        role_name = row['role']
                        
                        # Look for saved data for this role AND owner (from session state or database)
                        saved_data_for_role = None
                        role_name = row['role']
                        owner_name = row['owner']
                        
                        # First check session state
                        for saved_plan in saved_pipeline_data:
                            if (saved_plan.get('role') == role_name and 
                                saved_plan.get('pipeline_owner') == owner_name):
                                saved_data_for_role = saved_plan
                                break
                        
                        # If not in session state, check database for existing plans by role AND owner
                        if not saved_data_for_role and is_edit_mode:
                            existing_plans = st.session_state.get('loaded_pipeline_plans', [])
                            for existing_plan in existing_plans:
                                if (existing_plan.get('role') == role_name and 
                                    existing_plan.get('pipeline_owner') == owner_name):
                                    saved_data_for_role = existing_plan
                                    break
                        
                        if saved_data_for_role and saved_data_for_role.get('pipeline_results'):
                            # Use saved pipeline results
                            pipeline_results = saved_data_for_role['pipeline_results']
                        else:
                            # Generate new pipeline results if no saved data
                            pipeline_name = row['pipeline']
                            selected_pipeline_id = None
                            
                            for config in pipeline_configs:
                                if config[1] == pipeline_name:
                                    selected_pipeline_id = config[0]
                                    break
                            
                            if selected_pipeline_id:
                                from utils.pipeline_manager import PipelineManager
                                pipeline_mgr = PipelineManager(env_manager)
                                
                                # Calculate reverse pipeline using role positions and staffed_by_date
                                pipeline_results = pipeline_mgr.calculate_reverse_pipeline(
                                    selected_pipeline_id, row['positions'], row['staffed_by_date']
                                )
                                
                                # Store the calculated pipeline data in session state for saving
                                if pipeline_results:
                                    if 'generated_pipeline_data' not in st.session_state:
                                        st.session_state.generated_pipeline_data = []
                                    
                                    # Check if this role's data already exists and update/add it
                                    existing_data = st.session_state.generated_pipeline_data
                                    role_found = False
                                    for i, existing_plan in enumerate(existing_data):
                                        if (existing_plan.get('role') == row['role'] and 
                                            existing_plan.get('pipeline_owner') == row.get('owner', '')):
                                            # Update existing role data
                                            existing_data[i] = {
                                                'role': row['role'],
                                                'pipeline_id': selected_pipeline_id,
                                                'pipeline_name': pipeline_name,
                                                'pipeline_owner': row.get('owner', ''),
                                                'pipeline_results': pipeline_results
                                            }
                                            role_found = True
                                            break
                                    
                                    if not role_found:
                                        # Add new role data
                                        existing_data.append({
                                            'role': row['role'],
                                            'pipeline_id': selected_pipeline_id,
                                            'pipeline_name': pipeline_name,
                                            'pipeline_owner': row.get('owner', ''),
                                            'pipeline_results': pipeline_results
                                        })
                                    
                                    st.session_state.generated_pipeline_data = existing_data
                                    st.session_state.show_generated_plans = True
                        
                        if pipeline_results:
                            # Create pipeline plan table for this role
                            import pandas as pd
                            
                            def format_date(date_value):
                                """Format date value handling both string and date objects"""
                                if isinstance(date_value, str):
                                    # Try to parse string date and reformat
                                    try:
                                        parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
                                        return parsed_date.strftime('%m/%d/%Y')
                                    except:
                                        return date_value  # Return as-is if parsing fails
                                elif hasattr(date_value, 'strftime'):
                                    return date_value.strftime('%m/%d/%Y')
                                else:
                                    return str(date_value)
                            
                            pipeline_data = []
                            
                            for result in pipeline_results:
                                pipeline_data.append({
                                    'Stage': result['stage_name'],
                                    'Profiles Planned': result['profiles_converted'],
                                    'Planned Conversion Rate': f"{result['conversion_rate']:.1f}%",
                                    'Planned TAT': result['tat_days'],
                                    'Needed By Date': format_date(result['needed_by_date'])
                                })
                            
                            pipeline_df = pd.DataFrame(pipeline_data)
                            st.dataframe(pipeline_df, use_container_width=True, hide_index=True)
                            
                            # Format staffed_by_date safely too
                            staffed_by_formatted = format_date(row['staffed_by_date'])
                            st.info(f"📋 **Role:** {row['role']} | **Skills:** {row['skills']} | **Owner:** {row['owner']} | **Target:** {row['positions']} hires by {staffed_by_formatted}")
                        else:
                            st.warning(f"Could not generate pipeline plan for {row['role']}")
                            
                    except Exception as e:
                        st.error(f"Error displaying pipeline plan for {row['role']}: {str(e)}")
        
    except Exception as e:
        st.error(f"Error loading staffing plan builder: {str(e)}")
    
    # Action buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.write("")  # Spacer
    
    with col2:
        if st.button("💾 Save Plan", type="primary", key="save_staffing_plan"):
            if plan_name and selected_client and from_date and to_date:
                try:
                    # Get client_id from selected client name
                    client_id = None
                    try:
                        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                        cursor = conn.cursor()
                        master_clients_table = env_manager.get_table_name('master_clients')
                        cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", (selected_client,))
                        result = cursor.fetchone()
                        if result:
                            client_id = result[0]
                        conn.close()
                    except Exception as e:
                        st.error(f"Error getting client ID: {str(e)}")
                    
                    plan_data = {
                        'plan_name': plan_name,
                        'client_name': selected_client,
                        'client_id': client_id,  # Required field for update
                        'planned_positions': planned_positions,
                        'target_hires': target_hires,
                        'from_date': from_date,  # For create_staffing_plan
                        'to_date': to_date,      # For create_staffing_plan
                        'target_start_date': from_date,  # Required field for update
                        'target_end_date': to_date,      # Required field for update
                        'pipeline_id': st.session_state.get('selected_pipeline_id'),
                        'pipeline_name': st.session_state.get('selected_pipeline_name'),
                        'safety_buffer_pct': 0.0,  # Default safety buffer percentage
                        'staffed_positions': 0,     # Default staffed positions
                        'created_by': st.session_state.get('user_email', st.session_state.get('user_info', {}).get('email', 'admin')),
                        'staffing_plan_rows': st.session_state.get('staffing_plan_rows', [])
                    }
                    
                    staffing_plan_id = None
                    if is_edit_mode:
                        # Update existing plan
                        try:
                            success = staffing_manager.update_staffing_plan(edit_plan_id, plan_data)
                            staffing_plan_id = edit_plan_id
                            if success:
                                st.success("✅ Staffing plan updated successfully!")
                            else:
                                st.error("❌ Error updating staffing plan - update operation failed")
                        except Exception as e:
                            st.error(f"❌ Error updating staffing plan: {str(e)}")
                            st.error(f"❌ Error type: {type(e).__name__}")
                            import traceback
                            st.error(f"❌ Full error: {traceback.format_exc()}")
                            success = False
                    else:
                        # Create new plan
                        try:
                            staffing_plan_id = staffing_manager.create_staffing_plan(plan_data)
                            success = staffing_plan_id is not None
                            if success:
                                st.success("✅ Staffing plan created successfully!")
                            else:
                                st.error("❌ Error creating staffing plan - creation operation failed")
                        except Exception as e:
                            st.error(f"❌ Error creating staffing plan: {str(e)}")
                            st.error(f"❌ Error type: {type(e).__name__}")
                            import traceback
                            st.error(f"❌ Full error: {traceback.format_exc()}")
                            success = False
                            staffing_plan_id = None
                    
                    if success and staffing_plan_id:
                        # Set current plan ID for future pipeline generation saves
                        st.session_state.current_plan_id = staffing_plan_id
                        
                        # Save detailed staffing plan rows if available
                        staffing_plan_rows = st.session_state.get('staffing_plan_rows', [])
                        
                        if staffing_plan_rows:
                            st.info(f"💾 Saving {len(staffing_plan_rows)} detailed staffing role(s)...")
                            try:
                                staffing_manager.save_staffing_plan_rows(staffing_plan_id, staffing_plan_rows)
                                st.success(f"✅ Saved {len(staffing_plan_rows)} role details to database")
                            except Exception as e:
                                st.error(f"❌ Error saving role details: {str(e)}")
                        
                        # Save generated pipeline plans if available
                        if st.session_state.get('show_generated_plans', False):
                            try:
                                # Store pipeline plan data for this staffing plan
                                pipeline_plan_data = {
                                    'generated_plans': st.session_state.get('generated_pipeline_data', []),
                                    'created_by': st.session_state.get('user_email', 'admin')
                                }
                                staffing_manager.save_generated_pipeline_plan(staffing_plan_id, pipeline_plan_data)
                                st.success("✅ Saved generated pipeline plans to database")
                            except Exception as e:
                                st.error(f"❌ Error saving pipeline plans: {str(e)}")
                        
                        # Clear form and return to list  
                        keys_to_clear = [
                            'show_staffing_form', 'edit_staffing_plan_id',
                            'selected_client_name', 'planned_positions', 'current_plan_name',
                            'current_from_date', 'current_to_date', 'pipeline_planning_data',
                            'pipeline_plan_data', 'staffing_plan_rows', 'editing_row', 'show_generated_plans',
                            'show_add_role_form'
                        ]
                        for key in keys_to_clear:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Error saving staffing plan: {str(e)}")
            else:
                st.error("Please fill in all required fields")
    
    with col3:
        if st.button("❌ Cancel", key="cancel_staffing_plan"):
            # Clear form and return to list
            keys_to_clear = [
                'show_staffing_form', 'edit_staffing_plan_id',
                'selected_client_name', 'planned_positions', 'current_plan_name',
                'current_from_date', 'current_to_date', 'pipeline_planning_data',
                'staffing_plan_rows', 'editing_row', 'show_generated_plans', 'show_add_role_form'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

def main():
    logger.info("Application startup initiated")
    
    # Initialize performance optimization system EARLY
    performance_manager = get_performance_manager()
    
    # Optimize session state and preload critical data
    optimize_session()
    
    # Handle OAuth callback FIRST - before any page configuration
    # This ensures we can process authorization codes immediately
    # Handle OAuth callback FIRST - detect authorization code in URL
    query_params = st.query_params.to_dict() if hasattr(st, 'query_params') else {}
    auth_code = query_params.get('code')
    
    if auth_code:
        # OAuth callback detected - process authentication immediately
        logger.info(f"OAuth callback detected with authorization code: {auth_code[:20]}...")
        
        # Set page config before handling OAuth
        st.set_page_config(
            page_title="GA AlignOps - OAuth Processing",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Import and call handle_oauth_callback to process authentication
        from auth import handle_oauth_callback
        oauth_success = handle_oauth_callback()
        
        if oauth_success:
            # OAuth authentication successful - redirect to main app
            logger.info("OAuth authentication successful - redirecting to main app")
            st.session_state.current_page = "Demand Planning"
            st.success("✅ Successfully authenticated! Redirecting to application...")
            st.rerun()
        else:
            # OAuth authentication failed - show error and continue to login
            st.error("❌ OAuth authentication failed. Please try again.")
            
        # Return early to prevent normal flow
        return
    
    # Set development environment for proper table separation
    import os
    if 'ENVIRONMENT' not in os.environ:
        os.environ['ENVIRONMENT'] = 'development'
    
    # Initialize Environment Manager
    from utils.environment_manager import EnvironmentManager
    if 'env_manager' not in st.session_state:
        st.session_state.env_manager = EnvironmentManager()
    
    env_manager = st.session_state.env_manager
    
    # Configure page with environment indicator
    env_suffix = "[DEVELOPMENT]" if env_manager.is_development() else "[PRODUCTION]"
    st.set_page_config(
        page_title=f"GA AlignOps {env_suffix}",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    logger.info("Streamlit page configuration completed")
    
    # Initialize Google Sheets data sync scheduler
    # Only start scheduler in development to avoid conflicts in production deployment
    if env_manager.is_development():
        try:
            if 'scheduler_initialized' not in st.session_state:
                data_sync_scheduler.start_scheduler()
                st.session_state.scheduler_initialized = True
                logger.info("Google Sheets sync scheduler started - will run daily at 8 PM IST")
        except Exception as e:
            logger.warning(f"Failed to start scheduler: {e}")
    
    # Hide streamlit menu but keep sidebar
    st.markdown("""
        <style>
        #MainMenu {visibility: hidden;}
        .stDeployButton {display:none;}
        footer {visibility: hidden;}
        </style>
        """, unsafe_allow_html=True)
    
    # Direct Access Mode - Auto-login Admin (SSL Certificate Workaround)
    # Initialize User Manager with Environment Manager
    from utils.user_manager import UserManager
    if 'user_manager' not in st.session_state:
        st.session_state.user_manager = UserManager()
    
    user_manager = st.session_state.user_manager
    
    # Authentication with SSL fallback handling
    from auth import check_auth, login_page
    
    # Check if user is authenticated
    if not check_auth():
        logger.info("User not authenticated - showing login page")
        login_page()
        return
    
    # Initialize user_info if not present but user is authenticated
    if 'user_info' not in st.session_state:
        # Get username from session and rebuild user info
        username = st.session_state.get('username', 'admin')
        # Fallback default user info for authenticated users
        user_info = {
            "email": st.session_state.get('user_email', 'preethi.madhu@greyamp.com'),
            "name": st.session_state.get('username', 'Preethi Madhu'), 
            "role": "Administrator",
            "is_admin": True
        }
        
        # Get actual role from role group mapping
        try:
            import psycopg2
            conn = psycopg2.connect(os.getenv('DATABASE_URL'))
            cursor = conn.cursor()
            
            # Get user's role group
            users_table = env_manager.get_table_name('users')
            query = f'''
            SELECT rg.group_name
            FROM {users_table} u
            JOIN user_role_mappings urm ON u.username = urm.user_name
            JOIN role_groups rg ON urm.role_group_id = rg.id
            WHERE u.email = %s AND rg.status = 'Active'
            LIMIT 1
            '''
            
            cursor.execute(query, (user_info['email'],))
            role_result = cursor.fetchone()
            
            if role_result:
                # Update role to actual role group name
                user_info['role'] = role_result[0]
                user_info['is_admin'] = role_result[0] in ['Super Admins', 'Administrator']
            
            conn.close()
        except Exception as e:
            logger.warning(f"Could not fetch role group for user: {e}")
        
        st.session_state.user_info = user_info
    
    logger.info(f"User authenticated: {st.session_state.user_info.get('email', 'Unknown')}")
    
    # Set user_email in session state for RBAC system (always sync with current user)
    st.session_state.user_email = st.session_state.user_info.get('email', '')
    
    # Show user info with enhanced display
    st.markdown(f"""
    <div style='display: flex; justify-content: space-between; align-items: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px; margin-bottom: 10px;'>
        <div>
            <div style='font-weight: bold; color: #333;'>👤 {st.session_state.user_info.get("name", "User")}</div>
            <div style='font-size: 0.9em; color: #666;'>{st.session_state.user_info.get("email", "")}</div>
            <div style='font-size: 0.8em; color: #007ACC; font-weight: bold;'>🔑 {st.session_state.user_info.get("role", "User")}</div>
        </div>
        <div style='text-align: right;'>
            <div style='font-size: 0.8em; color: #666;'>🔐 Direct Access Mode</div>
            <div style='font-size: 0.7em; color: #999;'>OAuth ready when SSL resolved</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Logout functionality
    if st.button("🚪 Logout", key="logout_main"):
        logger.info(f"User logout initiated: {st.session_state.user_info.get('email', 'Unknown')}")
        # Clear all authentication-related session state
        auth_keys = ['authenticated', 'user_info', 'user_email', 'username']
        for key in auth_keys:
            if key in st.session_state:
                del st.session_state[key]
        logger.info("User session cleared successfully")
        st.rerun()
    
    # Environment Banner
    if env_manager.is_development():
        st.markdown("""
        <div style='background-color: #28a745; color: white; padding: 10px; border-radius: 5px; margin-bottom: 10px; text-align: center; font-weight: bold;'>
            🟢 DEVELOPMENT ENVIRONMENT - Safe Testing Zone
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='background-color: #dc3545; color: white; padding: 10px; border-radius: 5px; margin-bottom: 10px; text-align: center; font-weight: bold;'>
            🔴 PRODUCTION ENVIRONMENT - Live Business Data
        </div>
        """, unsafe_allow_html=True)
    
    # App title
    st.title("🏢 GA AlignOps")
    st.markdown("---")
    
    # App-style navigation with buttons
    st.sidebar.title("📋 Navigation")
    
    # Initialize page in session state - check URL parameters first
    url_page = st.query_params.get("page")
    if 'current_page' not in st.session_state:
        if url_page and url_page in ["Demand Planning", "Supply Planning", "Demand - Supply Mapping", "Insights & Reporting", "Settings"]:
            st.session_state.current_page = url_page
        else:
            st.session_state.current_page = "Demand Planning"
    elif url_page and url_page in ["Demand Planning", "Supply Planning", "Demand - Supply Mapping", "Insights & Reporting", "Settings"]:
        # Update page if URL parameter is different (for OAuth redirects)
        st.session_state.current_page = url_page
    
    # Navigation buttons with app-style design
    pages = [
        ("📋", "Demand Planning"),
        ("🔄", "Supply Planning"), 
        ("🔗", "Demand - Supply Mapping"),
        ("📊", "Insights & Reporting"),
        ("⚙️", "Settings")
    ]
    
    # Initialize Permission Manager first
    from utils.permission_manager import PermissionManager
    if 'permission_manager' not in st.session_state:
        st.session_state.permission_manager = PermissionManager()
    
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    for icon, page_name in pages:
        # Check if user has permission to access this page
        if page_name == "Demand Planning":
            # For Demand Planning, check if user has access to any sub-page
            has_target_setting = permission_manager.has_permission(current_user_email, "Demand Planning", "Target Setting", "view")
            has_demand_tweaking = permission_manager.has_permission(current_user_email, "Demand Planning", "Demand Tweaking", "view")
            has_editable_plan = permission_manager.has_permission(current_user_email, "Demand Planning", "Editable Plan View", "view")
            has_access = has_target_setting or has_demand_tweaking or has_editable_plan
        elif page_name == "Supply Planning":
            has_access = permission_manager.has_permission(current_user_email, "Supply Planning", "Talent Management", "view")
            # Debug logging for permission check
            if current_user_email == "anna.pauly@greyamp.com":
                logger.info(f"DEBUG: Anna Pauly Supply Planning access check: {has_access}")
        elif page_name == "Demand - Supply Mapping":
            has_access = permission_manager.has_permission(current_user_email, "Demand - Supply Mapping", "Add New Mapping", "view")
            # Debug logging for permission check
            if current_user_email == "anna.pauly@greyamp.com":
                logger.info(f"DEBUG: Anna Pauly Demand-Supply Mapping access check: {has_access}")
        elif page_name == "Insights & Reporting":
            has_access = permission_manager.has_permission(current_user_email, "Insights & Reporting", "Analytics Dashboard", "view")
        elif page_name == "Settings":
            has_access = permission_manager.has_permission(current_user_email, "Settings", "User Management", "view")
        else:
            has_access = False
        
        # Create uniform navigation buttons using only Streamlit buttons
        is_current = st.session_state.current_page == page_name
        
        if has_access:
            # Use button with consistent styling
            button_clicked = st.sidebar.button(
                f"{icon}  {page_name}", 
                key=f"nav_{page_name}", 
                use_container_width=True,
                type="primary" if is_current else "secondary"
            )
            
            if button_clicked and not is_current:
                st.session_state.current_page = page_name
                st.rerun()
        else:
            # Show disabled button with tooltip for restricted access
            st.sidebar.button(
                f"{icon}  {page_name}", 
                key=f"nav_{page_name}_disabled", 
                use_container_width=True,
                disabled=True,
                help="🔒 Access restricted - Contact administrator for permissions"
            )
    
    # Add professional CSS for navigation buttons
    st.sidebar.markdown("""
        <style>
        /* Sidebar navigation styling */
        div[data-testid="stSidebar"] .stButton {
            margin: 8px 0 !important;
            width: 100% !important;
        }
        
        div[data-testid="stSidebar"] {
            padding: 1rem 1rem !important;
        }
        
        div[data-testid="stSidebar"] .stButton > button {
            width: 100% !important;
            height: 52px !important;
            min-height: 52px !important;
            max-height: 52px !important;
            font-family: 'Source Sans Pro', sans-serif !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            text-align: left !important;
            padding: 14px 16px !important;
            border-radius: 8px !important;
            border: 1px solid #e1e4e8 !important;
            transition: all 0.2s ease !important;
            background-color: #ffffff !important;
            color: #24292e !important;
            line-height: 1.3 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: flex-start !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            box-sizing: border-box !important;
        }
        
        /* Secondary button (inactive) styling */
        div[data-testid="stSidebar"] .stButton > button[kind="secondary"] {
            background-color: #ffffff !important;
            color: #24292e !important;
            border-color: #e1e4e8 !important;
        }
        
        div[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {
            background-color: #f6f8fa !important;
            border-color: #0066cc !important;
            color: #0066cc !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 2px 8px rgba(0, 102, 204, 0.15) !important;
        }
        
        /* Primary button (active) styling */
        div[data-testid="stSidebar"] .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #0066cc 0%, #004499 100%) !important;
            color: white !important;
            border-color: #0066cc !important;
            box-shadow: 0 2px 4px rgba(0, 102, 204, 0.2) !important;
        }
        
        div[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
            background: linear-gradient(135deg, #0052a3 0%, #003366 100%) !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 4px 12px rgba(0, 102, 204, 0.3) !important;
        }
        
        /* Remove focus outline */
        div[data-testid="stSidebar"] .stButton > button:focus {
            outline: none !important;
            box-shadow: 0 0 0 2px rgba(0, 102, 204, 0.3) !important;
        }
        
        /* Sidebar container styling */
        .css-1d391kg {
            padding-top: 1.5rem !important;
        }
        
        /* Navigation title spacing */
        div[data-testid="stSidebar"] h1 {
            margin-bottom: 1.5rem !important;
            font-size: 18px !important;
            font-weight: 600 !important;
            color: #24292e !important;
        }
        
        /* Ensure consistent button text alignment */
        div[data-testid="stSidebar"] .stButton > button p {
            margin: 0 !important;
            padding: 0 !important;
            text-align: left !important;
            width: 100% !important;
        }
        
        /* Fix any text wrapping issues */
        div[data-testid="stSidebar"] .stButton > button * {
            text-align: left !important;
            word-wrap: break-word !important;
            hyphens: none !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    page = st.session_state.current_page
    logger.info(f"Navigating to page: {page}")
    
    # Permission manager already initialized above in navigation section
    permission_manager = st.session_state.permission_manager
    
    # Apply role-based access control to each page
    current_user_email = st.session_state.get('user_email', '')
    
    if page == "Demand Planning":
        # Check if user has access to any Demand Planning sub-pages
        has_target_setting = permission_manager.has_permission(current_user_email, "Demand Planning", "Target Setting", "view")
        has_demand_tweaking = permission_manager.has_permission(current_user_email, "Demand Planning", "Demand Tweaking", "view")
        has_editable_plan = permission_manager.has_permission(current_user_email, "Demand Planning", "Editable Plan View", "view")
        
        logger.info(f"Demand Planning access for '{current_user_email}': Target Setting={has_target_setting}, Demand Tweaking={has_demand_tweaking}, Editable Plan={has_editable_plan}")
        
        if has_target_setting or has_demand_tweaking or has_editable_plan:
            # Load data efficiently with performance manager
            with st.spinner("Loading Demand Planning data..."):
                page_data = performance_manager.load_page_data("Demand Planning")
            demand_planning_main_page()
        else:
            permission_manager.show_access_denied_message("Demand Planning", "any sub-pages")
    elif page == "Supply Planning":
        if permission_manager.protect_page_access(current_user_email, "Supply Planning", "Talent Management"):
            # Load data efficiently with performance manager
            with st.spinner("Loading Supply Planning data..."):
                page_data = performance_manager.load_page_data("Supply Planning")
            supply_planning_page()
    elif page == "Demand - Supply Mapping":
        if permission_manager.protect_page_access(current_user_email, "Demand - Supply Mapping", "Add New Mapping"):
            # Load data efficiently with performance manager
            with st.spinner("Loading Mapping data..."):
                page_data = performance_manager.load_page_data("Demand - Supply Mapping")
            demand_supply_mapping_page()
    elif page == "Insights & Reporting":
        if permission_manager.protect_page_access(current_user_email, "Insights & Reporting", "Analytics Dashboard"):
            # Load analytics data efficiently
            with st.spinner("Loading Analytics data..."):
                page_data = performance_manager.load_page_data("Insights & Reporting")
            insights_reporting_page(page_data)
            logger.info("Insights & Reporting page displayed")
    elif page == "Settings":
        if permission_manager.protect_page_access(current_user_email, "Settings", "User Management"):
            settings_page()

def insights_reporting_page(page_data):
    """Enhanced Insights & Reporting page with performance-optimized data loading"""
    st.header("📊 Insights & Reporting")
    
    # Create tabs for different insights
    analytics_tab, performance_tab = st.tabs(["📈 Analytics Dashboard", "⚡ Performance Monitor"])
    
    with analytics_tab:
        try:
            # Use cached data from performance manager
            if page_data and 'analytics_data' in page_data:
                analytics_data = page_data['analytics_data']
                dashboard_metrics = page_data.get('dashboard_metrics', {})
                
                # Display key metrics
                if dashboard_metrics:
                    st.subheader("Key Performance Indicators")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Revenue", f"${dashboard_metrics.get('total_revenue', 0):,.0f}")
                    with col2:
                        st.metric("Total Accounts", f"{dashboard_metrics.get('total_accounts', 0):,}")
                    with col3:
                        st.metric("Active Regions", f"{dashboard_metrics.get('active_regions', 0):,}")
                    with col4:
                        if dashboard_metrics.get('top_performers'):
                            top_performer = max(dashboard_metrics['top_performers'], key=dashboard_metrics['top_performers'].get)
                            st.metric("Top Performer", top_performer)
                
                # Display analytics charts if data is available
                if not analytics_data.empty:
                    st.subheader("Revenue Trends by Owner")
                    
                    # Create interactive chart
                    import plotly.express as px
                    fig = px.bar(
                        analytics_data, 
                        x='owner', 
                        y='value', 
                        color='month',
                        title="Revenue by Owner and Month",
                        labels={'value': 'Revenue ($)', 'owner': 'Owner'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No analytics data available at this time.")
            else:
                st.info("Loading analytics data...")
                
        except Exception as e:
            st.error(f"Error loading analytics data: {str(e)}")
    
    with performance_tab:
        st.subheader("Application Performance Monitor")
        
        # Get performance stats from performance manager
        try:
            performance_manager = get_performance_manager()
            perf_stats = performance_manager.get_performance_stats()
            
            # Display performance metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Page Load Times**")
                metrics = perf_stats.get('metrics', {})
                for page, stats in metrics.items():
                    load_time = stats.get('load_time', 0)
                    st.metric(f"{page}", f"{load_time:.2f}s")
            
            with col2:
                st.markdown("**Cache Statistics**")
                cache_stats = perf_stats.get('cache_stats', {})
                st.metric("Cached Items", cache_stats.get('cached_items', 0))
                st.metric("Session Keys", cache_stats.get('session_keys', 0))
            
            # Performance optimization controls
            st.markdown("**Optimization Controls**")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("Clear Cache"):
                    performance_manager.clear_cache()
                    st.success("Cache cleared successfully!")
                    st.rerun()
            
            with col2:
                if st.button("Optimize Session"):
                    optimize_session()
                    st.success("Session optimized!")
            
            with col3:
                if st.button("Refresh Stats"):
                    st.rerun()
                    
        except Exception as e:
            st.error(f"Error loading performance stats: {str(e)}")

def demand_supply_mapping_page():
    """Demand - Supply Mapping page with Add New functionality and permission enforcement"""
    
    logging.info("Demand - Supply Mapping page displayed")
    
    # Check permissions for demand-supply mapping
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    # Check if user can view demand-supply mapping
    if not permission_manager.has_permission(current_user_email, "Demand - Supply Mapping", "Add New Mapping", "view"):
        permission_manager.show_access_denied_message("Demand - Supply Mapping", "Add New Mapping")
        return
    
    st.title("🔗 Demand - Supply Mapping")
    st.markdown("Match demand requirements with available talent")
    
    # Check if we're in edit mode (edit button was clicked)
    is_edit_mode = 'edit_client' in st.session_state and 'mapping_action' in st.session_state
    
    if is_edit_mode:
        # Show edit mode interface directly
        st.info(f"✏️ Edit Mode: Editing assignments for {st.session_state.edit_client}")
        
        # Show back button
        if st.button("← Back to View Assignments"):
            # Clear edit mode session state
            if 'edit_client' in st.session_state:
                del st.session_state.edit_client
            if 'mapping_action' in st.session_state:
                del st.session_state.mapping_action
            if 'saved_demand_info' in st.session_state:
                del st.session_state.saved_demand_info
            if 'supply_rows' in st.session_state:
                del st.session_state.supply_rows
            st.rerun()
        
        # Import and use the 2-panel interface for editing
        # TODO: Replace with actual mapping interface when two_panel_functions is available
        st.info("🔄 Mapping interface is being updated. Please check back later.")
        st.write("This section will allow you to create new demand-supply mappings.")
        
    else:
        # Normal tab interface when not in edit mode
        mapping_tab, existing_tab = st.tabs(["🆕 Add New Mapping", "📋 Existing Mappings"])
        
        with mapping_tab:
            # Real new mapping interface
            show_add_new_mapping_tab()
        
        with existing_tab:
            # Real mapping interface with Ready for Staffing and Current Staffing tabs
            staffing_tab1, staffing_tab2 = st.tabs(["🟢 Ready for Staffing", "🔵 Current Staffing"])
            
            with staffing_tab1:
                show_ready_for_staffing_tab()
            
            with staffing_tab2:
                show_current_staffing_tab()


def show_add_new_mapping_tab():
    """Show the Add New Mapping interface"""
    st.subheader("➕ Create New Demand-Supply Mapping")
    
    # Get environment manager
    env_manager = st.session_state.env_manager
    
    # Get available clients
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    clients = []
    try:
        cursor = conn.cursor()
        master_clients_table = env_manager.get_table_name('master_clients')
        cursor.execute(f"SELECT master_client_id, client_name FROM {master_clients_table} ORDER BY client_name")
        clients = cursor.fetchall()
    finally:
        conn.close()
    
    # Get available talent
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    talent = []
    try:
        cursor = conn.cursor()
        talent_supply_table = env_manager.get_table_name('talent_supply')
        cursor.execute(f"SELECT id, name, talent_type, skills FROM {talent_supply_table} WHERE assignment_status != 'Fully Assigned' ORDER BY name")
        talent = cursor.fetchall()
    finally:
        conn.close()
    
    # Create the mapping form
    with st.form("new_mapping_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🏢 Client Information")
            client_options = [""] + [f"{client[1]}" for client in clients]
            selected_client = st.selectbox("Select Client", client_options, key="new_mapping_client")
            
            # Get client ID for selected client
            client_id = None
            if selected_client:
                for client in clients:
                    if client[1] == selected_client:
                        client_id = client[0]
                        break
            
            # Show client details if selected
            if client_id:
                st.info(f"**Client ID:** {client_id}")
        
        with col2:
            st.markdown("#### 👤 Talent Information")
            talent_options = [""] + [f"{t[1]} ({t[2]})" for t in talent]
            selected_talent = st.selectbox("Select Talent", talent_options, key="new_mapping_talent")
            
            # Get talent ID for selected talent
            talent_id = None
            if selected_talent:
                for t in talent:
                    if f"{t[1]} ({t[2]})" == selected_talent:
                        talent_id = t[0]
                        break
            
            # Show talent details if selected
            if talent_id:
                st.info(f"**Talent ID:** {talent_id}")
        
        # Assignment details
        st.markdown("#### 📋 Assignment Details")
        col3, col4, col5 = st.columns(3)
        
        with col3:
            assignment_percentage = st.slider("Assignment Percentage", 0, 100, 50, key="new_mapping_percentage")
        
        with col4:
            duration_months = st.number_input("Duration (Months)", min_value=1, max_value=24, value=3, key="new_mapping_duration")
        
        with col5:
            start_date = st.date_input("Start Date", key="new_mapping_start")
        
        # Skills and notes
        skills = st.text_area("Required Skills", placeholder="Enter specific skills required for this assignment", key="new_mapping_skills")
        notes = st.text_area("Additional Notes", placeholder="Any additional information about this mapping", key="new_mapping_notes")
        
        # Submit button
        if st.form_submit_button("💾 Create Mapping", type="primary"):
            if selected_client and selected_talent and client_id and talent_id:
                # Create the mapping
                success = create_demand_supply_mapping(
                    client_id, talent_id, assignment_percentage, 
                    duration_months, start_date, skills, notes
                )
                if success:
                    st.success("✅ Mapping created successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to create mapping. Please try again.")
            else:
                st.error("❌ Please select both client and talent before creating mapping.")

def show_ready_for_staffing_tab():
    """Show the Ready for Staffing tab with available assignments"""
    st.subheader("🟢 Ready for Staffing")
    
    # Get environment manager
    env_manager = st.session_state.env_manager
    
    # Get ready for staffing assignments
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    ready_assignments = []
    try:
        cursor = conn.cursor()
        demand_supply_table = env_manager.get_table_name('demand_supply_assignments')
        master_clients_table = env_manager.get_table_name('master_clients')
        talent_supply_table = env_manager.get_table_name('talent_supply')
        
        # Query for ready assignments (status = 'Ready' or similar)
        query = f"""
            SELECT 
                dsa.id, dsa.client_id, dsa.talent_id, dsa.assignment_percentage,
                dsa.duration_months, dsa.start_date, dsa.end_date, dsa.status,
                mc.client_name, ts.name as talent_name, ts.talent_type, ts.skills
            FROM {demand_supply_table} dsa
            LEFT JOIN {master_clients_table} mc ON dsa.client_id = mc.master_client_id
            LEFT JOIN {talent_supply_table} ts ON dsa.talent_id = ts.id
            WHERE dsa.status = 'Ready' OR dsa.status = 'Pending'
            ORDER BY dsa.start_date DESC
        """
        cursor.execute(query)
        ready_assignments = cursor.fetchall()
    finally:
        conn.close()
    
    if ready_assignments:
        # Display ready assignments in a table
        st.markdown("#### 📋 Available Assignments")
        
        # Create DataFrame for display
        import pandas as pd
        df_data = []
        for assignment in ready_assignments:
            df_data.append({
                'ID': assignment[0],
                'Client': assignment[8] or 'Unknown',
                'Talent': assignment[9] or 'Unknown',
                'Type': assignment[10] or 'Unknown',
                'Percentage': f"{assignment[3]}%",
                'Duration': f"{assignment[4]} months",
                'Start Date': assignment[5] or 'TBD',
                'Status': assignment[7] or 'Unknown',
                'Skills': assignment[11] or 'N/A'
            })
        
        df = pd.DataFrame(df_data)
        st.dataframe(df, use_container_width=True)
        
        # Action buttons
        st.markdown("#### 🎯 Actions")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Export to Excel", key="export_ready"):
                # Export functionality would go here
                st.info("Export functionality coming soon!")
        
        with col2:
            if st.button("🔄 Refresh Data", key="refresh_ready"):
                st.rerun()
    else:
        st.info("ℹ️ No assignments are currently ready for staffing.")
        st.write("Assignments will appear here when they are marked as 'Ready' or 'Pending'.")

def show_current_staffing_tab():
    """Show the Current Staffing tab with active assignments"""
    st.subheader("🔵 Current Staffing")
    
    # Get environment manager
    env_manager = st.session_state.env_manager
    
    # Get current active assignments
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    active_assignments = []
    try:
        cursor = conn.cursor()
        demand_supply_table = env_manager.get_table_name('demand_supply_assignments')
        master_clients_table = env_manager.get_table_name('master_clients')
        talent_supply_table = env_manager.get_table_name('talent_supply')
        
        # Query for active assignments
        query = f"""
            SELECT 
                dsa.id, dsa.client_id, dsa.talent_id, dsa.assignment_percentage,
                dsa.duration_months, dsa.start_date, dsa.end_date, dsa.status,
                mc.client_name, ts.name as talent_name, ts.talent_type, ts.skills,
                dsa.notes
            FROM {demand_supply_table} dsa
            LEFT JOIN {master_clients_table} mc ON dsa.client_id = mc.master_client_id
            LEFT JOIN {talent_supply_table} ts ON dsa.talent_id = ts.id
            WHERE dsa.status = 'Active' OR dsa.status = 'In Progress'
            ORDER BY dsa.start_date DESC
        """
        cursor.execute(query)
        active_assignments = cursor.fetchall()
    finally:
        conn.close()
    
    if active_assignments:
        # Display active assignments in a table
        st.markdown("#### 📋 Active Assignments")
        
        # Create DataFrame for display
        import pandas as pd
        df_data = []
        for assignment in active_assignments:
            df_data.append({
                'ID': assignment[0],
                'Client': assignment[8] or 'Unknown',
                'Talent': assignment[9] or 'Unknown',
                'Type': assignment[10] or 'Unknown',
                'Percentage': f"{assignment[3]}%",
                'Duration': f"{assignment[4]} months",
                'Start Date': assignment[5] or 'TBD',
                'End Date': assignment[6] or 'TBD',
                'Status': assignment[7] or 'Unknown',
                'Skills': assignment[11] or 'N/A',
                'Notes': assignment[12] or 'N/A'
            })
        
        df = pd.DataFrame(df_data)
        st.dataframe(df, use_container_width=True)
        
        # Action buttons
        st.markdown("#### 🎯 Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📊 Export to Excel", key="export_current"):
                st.info("Export functionality coming soon!")
        
        with col2:
            if st.button("🔄 Refresh Data", key="refresh_current"):
                st.rerun()
        
        with col3:
            if st.button("📈 View Analytics", key="analytics_current"):
                st.info("Analytics functionality coming soon!")
    else:
        st.info("ℹ️ No active assignments found.")
        st.write("Active assignments will appear here when they are created and marked as 'Active' or 'In Progress'.")

def create_demand_supply_mapping(client_id, talent_id, assignment_percentage, duration_months, start_date, skills, notes):
    """Create a new demand-supply mapping"""
    try:
        import psycopg2
        from datetime import datetime, timedelta
        
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        cursor = conn.cursor()
        
        # Calculate end date
        end_date = start_date + timedelta(days=duration_months * 30)
        
        # Get environment manager
        env_manager = st.session_state.env_manager
        demand_supply_table = env_manager.get_table_name('demand_supply_assignments')
        
        # Insert new mapping
        cursor.execute(f"""
            INSERT INTO {demand_supply_table} 
            (client_id, master_client_id, talent_id, assignment_percentage, duration_months, 
             start_date, end_date, status, notes, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            client_id, client_id, talent_id, assignment_percentage, duration_months,
            start_date, end_date, 'Ready', notes, datetime.now()
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        st.error(f"Error creating mapping: {str(e)}")
        return False

def demand_planning_main_page():
    st.header("📋 Demand Planning")
    
    # Get current user permissions
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    # Check permissions for each sub-page
    has_target_setting = permission_manager.has_permission(current_user_email, "Demand Planning", "Target Setting", "view")
    has_demand_tweaking = permission_manager.has_permission(current_user_email, "Demand Planning", "Demand Tweaking", "view")
    has_editable_plan = permission_manager.has_permission(current_user_email, "Demand Planning", "Editable Plan View", "view")
    has_demand_forecast = permission_manager.has_permission(current_user_email, "Demand Planning", "Demand Tweaking", "view")  # Use same permission as Demand Tweaking
    has_pipeline_config = permission_manager.has_permission(current_user_email, "Demand Planning", "Demand Tweaking", "view")  # Use same permission as Demand Tweaking
    
    # Build tab list based on permissions
    available_tabs = []
    tab_functions = []
    
    if has_target_setting:
        available_tabs.append("Target Setting")
        tab_functions.append(target_setting_page)
    
    if has_demand_tweaking:
        available_tabs.append("Demand Tweaking") 
        tab_functions.append(demand_tweaking_page)
    
    if has_demand_forecast:
        available_tabs.append("Demand Forecast")
        tab_functions.append(demand_forecast_managing_page)
        
        # Add new Forecast Management tab
        available_tabs.append("Forecast Management")
        tab_functions.append(forecast_management_page)
    
    if has_pipeline_config:
        available_tabs.append("Demand Pipeline Configuration")
        tab_functions.append(demand_pipeline_configuration_page)
    
    if has_editable_plan:
        available_tabs.append("Editable Plan View")
        tab_functions.append(editable_plan_view_page)
    
    # Show available tabs
    if available_tabs:
        tabs = st.tabs(available_tabs)
        
        for i, (tab, tab_function) in enumerate(zip(tabs, tab_functions)):
            with tab:
                tab_function()
    else:
        st.error("❌ You don't have permission to access any Demand Planning features.")
        st.info("Contact your administrator to request access to Target Setting, Demand Tweaking, Demand Forecast, Forecast Management, or Editable Plan View.")

def demand_forecast_managing_page():
    """Demand Forecast page showing quarterly forecast overview by owner"""
    st.subheader("📈 Demand Forecast - Quarterly Overview")
    
    # Initialize unified database manager
    unified_db = UnifiedDataManager()
    
    try:
        # Use the same data source as Demand Tweaking for consistency
        all_data = unified_db.get_all_data()
        
        # Check if all_data is a DataFrame
        if not isinstance(all_data, pd.DataFrame):
            st.error("Unable to load forecast data - invalid data format")
            return
            
        # Filter for forecast data - include only Planned and Billed metrics
        forecast_data = all_data[all_data['metric_type'].isin(['Planned', 'Billed'])].copy()
        
        # Ensure we have the required columns
        required_columns = ['account_name', 'owner', 'year', 'month', 'metric_type', 'value', 'lob', 'region']
        missing_columns = [col for col in required_columns if col not in forecast_data.columns]
        if missing_columns:
            st.error(f"Missing required columns: {missing_columns}")
            return
        
        if forecast_data.empty:
            st.warning("No forecast or billed data available from planning and target table.")
            return
            
        # Year selection
        col1, col2 = st.columns([1, 3])
        with col1:
            available_years_series = forecast_data['year'].dropna()
            available_years = sorted(available_years_series.unique()) if isinstance(available_years_series, pd.Series) else []
            if not available_years:
                st.error("No years available in the data")
                return
            current_year = datetime.now().year
            if current_year in available_years:
                default_year_idx = available_years.index(current_year)
            else:
                default_year_idx = 0
            selected_year = st.selectbox("Year", available_years, index=default_year_idx, key="forecast_year_select")
        
        # Filter data for selected year
        year_data = forecast_data[forecast_data['year'] == selected_year].copy()
        
        # Calculate quarterly metrics based on Financial Year (April-March)
        def get_quarter_from_month(month):
            """Convert month to quarter - Financial Year: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar"""
            # Financial Year Quarter Mapping
            month_to_quarter = {
                'April': 'Q1', 'Apr': 'Q1', 'april': 'Q1', 'apr': 'Q1',
                'May': 'Q1', 'may': 'Q1',
                'June': 'Q1', 'Jun': 'Q1', 'june': 'Q1', 'jun': 'Q1',
                'July': 'Q2', 'Jul': 'Q2', 'july': 'Q2', 'jul': 'Q2',
                'August': 'Q2', 'Aug': 'Q2', 'august': 'Q2', 'aug': 'Q2',
                'September': 'Q2', 'Sep': 'Q2', 'september': 'Q2', 'sep': 'Q2',
                'October': 'Q3', 'Oct': 'Q3', 'october': 'Q3', 'oct': 'Q3',
                'November': 'Q3', 'Nov': 'Q3', 'november': 'Q3', 'nov': 'Q3',
                'December': 'Q3', 'Dec': 'Q3', 'december': 'Q3', 'dec': 'Q3',
                'January': 'Q4', 'Jan': 'Q4', 'january': 'Q4', 'jan': 'Q4',
                'February': 'Q4', 'Feb': 'Q4', 'february': 'Q4', 'feb': 'Q4',
                'March': 'Q4', 'Mar': 'Q4', 'march': 'Q4', 'mar': 'Q4'
            }
            return month_to_quarter.get(month, 'Unknown')
        
        # Add quarter column
        if isinstance(year_data, pd.DataFrame):
            year_data['quarter'] = year_data['month'].apply(get_quarter_from_month)
            
            # Get unique owners
            owners_series = year_data['owner'].dropna()
            owners = sorted([x for x in owners_series.unique() if x]) if isinstance(owners_series, pd.Series) else []
        else:
            st.error("Invalid data format for processing")
            return
        
        if not owners:
            st.warning("No owner data available for the selected year.")
            return
        
        # Get target data
        try:
            if psycopg2 is None:
                st.error("Database connection not available")
                return
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()
            
            # Get quarterly targets - convert numpy types to native Python types
            owner_targets_table = env_manager.get_table_name('owner_targets')
            cursor.execute(f"""
                SELECT owner_name, quarter, target_amount
                FROM {owner_targets_table} 
                WHERE year = %s
                ORDER BY owner_name, quarter
            """, (int(selected_year),))
            
            target_data = cursor.fetchall()
            target_dict = {}
            for row in target_data:
                owner, quarter, target_amount = row
                if owner not in target_dict:
                    target_dict[owner] = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
                target_dict[owner][quarter] = float(target_amount) if target_amount else 0
            
            conn.close()
            
        except Exception as e:
            st.error(f"Error loading target data: {str(e)}")
            target_dict = {}
        
        # Calculate total forecast by owner for sorting
        owner_totals = {}
        for owner in owners:
            owner_data = year_data[year_data['owner'] == owner].copy()
            total_forecast = owner_data[owner_data['metric_type'] == 'Planned']['value'].sum()
            owner_totals[owner] = total_forecast
        
        # Sort owners by total forecast in descending order
        sorted_owners = sorted(owner_totals.keys(), key=lambda x: owner_totals[x], reverse=True)
        
        # Display owner cards by quarter
        st.markdown(f"### 📊 {selected_year} Quarterly Forecast Overview")
        
        for owner in sorted_owners:
            # Get owner's data
            owner_data = year_data[year_data['owner'] == owner].copy()
            
            # Calculate owner's total forecast and booked amounts
            total_forecast = owner_data[owner_data['metric_type'] == 'Planned']['value'].sum()
            total_booked = owner_data[owner_data['metric_type'] == 'Billed']['value'].sum()
            
            # Create collapsible owner section
            with st.expander(f"👤 {owner} - Total Forecast: ${total_forecast:,.0f} | Total Booked: ${total_booked:,.0f}", expanded=False):
                # Create quarter cards - each quarter as a full-width row
                quarters = ['Q1', 'Q2', 'Q3', 'Q4']
                
                for quarter in quarters:
                    # Get quarter data
                    quarter_data = owner_data[owner_data['quarter'] == quarter].copy()
                    
                    # Calculate metrics
                    planned_data = quarter_data[quarter_data['metric_type'] == 'Planned']
                    billed_data = quarter_data[quarter_data['metric_type'] == 'Billed']
                    
                    quarterly_forecast = planned_data['value'].sum() if not planned_data.empty else 0
                    quarterly_booked = billed_data['value'].sum() if not billed_data.empty else 0
                    
                    # Get target
                    target_amount = target_dict.get(owner, {}).get(quarter, 0)
                    
                    # Count accounts with forecast > 0
                    accounts_with_forecast = len(planned_data[planned_data['value'] > 0]['account_name'].unique()) if not planned_data.empty else 0
                    
                    # Calculate average ticket size
                    if accounts_with_forecast > 0:
                        avg_ticket_size = quarterly_forecast / accounts_with_forecast
                    else:
                        avg_ticket_size = 0
                    
                    # Calculate realized (Booked - Target)
                    total_realized = quarterly_booked - target_amount
                    
                    # Create full-width expandable card for each quarter
                    with st.expander(f"🗂️ {quarter} {selected_year} - Target: ${target_amount:,.0f} | Forecast: ${quarterly_forecast:,.0f} | Booked: ${quarterly_booked:,.0f}", expanded=False):
                        # Display metrics in columns within the expanded card
                        metric_cols = st.columns(6)
                        
                        with metric_cols[0]:
                            st.metric("Target", f"${target_amount:,.0f}")
                        
                        with metric_cols[1]:
                            st.metric("Total Forecast", f"${quarterly_forecast:,.0f}")
                        
                        with metric_cols[2]:
                            st.metric("Total Booked", f"${quarterly_booked:,.0f}")
                        
                        with metric_cols[3]:
                            st.metric("# of Accounts", f"{accounts_with_forecast}")
                        
                        with metric_cols[4]:
                            st.metric("Avg Ticket Size", f"${avg_ticket_size:,.0f}")
                        
                        with metric_cols[5]:
                            # Color code realized amount (Booked - Target)
                            if total_realized > 0:
                                st.metric("Total Realized (Booked - Target)", f"${total_realized:,.0f}", delta="Over Target", delta_color="normal")
                            elif total_realized == 0:
                                st.metric("Total Realized (Booked - Target)", "$0", delta="Target Met", delta_color="normal")
                            else:
                                # Show negative amounts in red using HTML
                                st.markdown("**Total Realized (Booked - Target)**")
                                st.markdown(f"<h3 style='color: red; margin: 0;'>${total_realized:,.0f}</h3>", unsafe_allow_html=True)
                                st.markdown("<small style='color: red;'>⬇ Below Target</small>", unsafe_allow_html=True)
                        
                        # Show account details if available
                        if not quarter_data.empty:
                            st.markdown("---")
                            st.markdown("**📋 Account Details:**")
                            account_summary = quarter_data.groupby(['account_name', 'metric_type'])['value'].sum().unstack(fill_value=0)
                            
                            # Display accounts in a more structured way
                            for account in account_summary.index:
                                planned_val = account_summary.loc[account].get('Planned', 0)
                                billed_val = account_summary.loc[account].get('Billed', 0)
                                if planned_val > 0 or billed_val > 0:
                                    account_cols = st.columns([2, 1, 1])
                                    with account_cols[0]:
                                        st.markdown(f"**{account}**")
                                    with account_cols[1]:
                                        st.markdown(f"Forecast: ${planned_val:,.0f}")
                                    with account_cols[2]:
                                        st.markdown(f"Booked: ${billed_val:,.0f}")
                        else:
                            st.markdown("---")
                            st.info(f"No forecast or billed data available for {quarter} {selected_year}")
        

                
    except Exception as e:
        st.error(f"Error loading forecast data: {str(e)}")
        st.expander("Error Details").write(str(e))

def demand_tweaking_page():
    st.subheader("📊 Demand Tweaking - Advanced Analytics & Filtering")
    
    # Initialize unified database manager
    unified_db = UnifiedDataManager()
    
    # Get all data for filtering
    try:
        all_data = unified_db.get_all_data()
        
        if all_data.empty:
            st.warning("No data available. Please add data first in the data entry section.")
            return
            
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return
    
    # Get current year and quarter for defaults
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_quarter = f"Q{(current_month - 1) // 3 + 1}"
    
    # Filter Panel
    st.markdown("### 🔍 Filter Panel")
    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Year filter
            available_years = sorted(all_data['year'].dropna().unique())
            if current_year in available_years:
                default_year_idx = available_years.index(current_year)
            else:
                default_year_idx = 0
            selected_year = st.selectbox("Year", available_years, index=default_year_idx)
            
            # Owner filter - default to "All"
            available_owners = sorted([x for x in all_data['owner'].dropna().unique() if x])
            selected_owner = st.selectbox("Owner", ["All"] + available_owners, index=0)
            
        with col2:
            # Quarter filter - default to "All"
            available_quarters = ["All", "Q1", "Q2", "Q3", "Q4"]
            selected_quarter = st.selectbox("Quarter", available_quarters, index=0)
            
            # LoB filter - default to "All"
            available_lobs = sorted([x for x in all_data['lob'].dropna().unique() if x])
            selected_lob = st.selectbox("LoB", ["All"] + available_lobs, index=0)
            
        with col3:
            # Region filter - default to "All"
            available_regions = sorted([x for x in all_data['region'].dropna().unique() if x])
            selected_region = st.selectbox("Region", ["All"] + available_regions, index=0)
            
            # Source filter - default to "All"
            available_sources = sorted([x for x in all_data['source'].dropna().unique() if x])
            selected_source = st.selectbox("Source", ["All"] + available_sources, index=0)
            
        with col4:
            # Advanced Metric Type Filter
            st.markdown("**Advanced Filter:**")
            forecast_filter = st.checkbox("Forecast > 0 only", value=False, help="Show only records where Planned value is greater than 0")
    
    # Apply filters automatically (no search button needed)
    filtered_data = all_data[all_data['year'] == selected_year].copy()
    
    if selected_owner != "All":
        filtered_data = filtered_data[filtered_data['owner'] == selected_owner]
    if selected_lob != "All":
        filtered_data = filtered_data[filtered_data['lob'] == selected_lob]
    if selected_region != "All":
        filtered_data = filtered_data[filtered_data['region'] == selected_region]
    if selected_source != "All":
        filtered_data = filtered_data[filtered_data['source'] == selected_source]
    
    # Apply forecast filter if selected
    if forecast_filter:
        # Show only records where Planned (Forecasted) value > 0
        forecast_records = filtered_data[
            (filtered_data['metric_type'] == 'Planned') & 
            (filtered_data['value'] > 0)
        ]
        if not forecast_records.empty:
            # Get unique identifiers for records with forecast > 0
            forecast_accounts = forecast_records[['account_name', 'month', 'year']].drop_duplicates()
            # Filter all data to only include these account-month combinations
            filtered_data = filtered_data.merge(
                forecast_accounts, 
                on=['account_name', 'month', 'year'], 
                how='inner'
            )
        else:
            # No records match the forecast filter
            filtered_data = filtered_data.iloc[0:0]  # Empty dataframe with same structure
    
    # Filter by quarter months (Financial Year: April-March)
    quarter_months = {
        "Q1": ["April", "May", "June"],
        "Q2": ["July", "August", "September"], 
        "Q3": ["October", "November", "December"],
        "Q4": ["January", "February", "March"]
    }
    
    # Only filter by quarter if not "All"
    if selected_quarter != "All":
        selected_months = quarter_months[selected_quarter]
        filtered_data = filtered_data[filtered_data['month'].isin(selected_months)]
    
    # Display selected owner name
    if selected_owner != "All":
        st.markdown(f"### 👤 Owner Selected: **{selected_owner}**")
    else:
        st.markdown("### 👥 All Owners Selected")
    
    # View/Edit Panel
    st.markdown("### 📋 View/Edit Panel")
    
    # Summary Panel
    st.markdown("#### 📊 Summary Panel")
    
    # Use filtered data for summary (this includes all applied search criteria)
    summary_data = filtered_data.copy()
    
    # Determine summary title based on filters
    filter_parts = []
    if selected_quarter != "All":
        filter_parts.append(f"{selected_quarter} ({', '.join(quarter_months[selected_quarter])})")
    else:
        filter_parts.append(f"All Quarters (FY {selected_year})")
    
    if forecast_filter:
        filter_parts.append("Forecast > 0")
        
    if selected_owner != "All":
        filter_parts.append(f"Owner: {selected_owner}")
        
    summary_title = f"Summary for {' | '.join(filter_parts)}"
    
    # Add visual demarcation for Summary Panel
    st.markdown("---")
    st.markdown(f"### 📊 {summary_title}")
    st.markdown("---")
    
    # Helper function to get target data for owner-quarter combination
    def get_target_for_owner_quarter(owner, quarter, year):
        """Get target data for specific owner-quarter combination"""
        try:
            if psycopg2 is None:
                return 0.0
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            
            # If owner is "All", get company-wide target FROM annual_targets
            if not owner or owner == "All":
                annual_table = env_manager.get_table_name('annual_targets')
                annual_query = f"""
                SELECT q1_target, q2_target, q3_target, q4_target 
                FROM {annual_table} 
                WHERE year = %s
                """
                annual_result = pd.read_sql_query(annual_query, conn, params=[int(year)])
                
                if not annual_result.empty:
                    quarter_col = f"{quarter.lower()}_target"
                    if quarter_col in annual_result.columns:
                        target_value = annual_result[quarter_col].iloc[0]
                        if pd.notna(target_value) and target_value > 0:
                            conn.close()
                            return float(target_value)
            else:
                # Get owner-specific target only
                owner_table = env_manager.get_table_name('owner_targets')
                owner_query = f"""
                SELECT target_amount 
                FROM {owner_table} 
                WHERE owner_name = %s AND quarter = %s AND year = %s
                """
                owner_result = pd.read_sql_query(owner_query, conn, params=[owner, quarter, int(year)])
                
                if not owner_result.empty:
                    target_value = owner_result['target_amount'].iloc[0]
                    if pd.notna(target_value) and target_value > 0:
                        conn.close()
                        return float(target_value)
            
            conn.close()
            return 0.0
                
        except Exception as e:
            print(f"DEBUG: Target calculation error: {e}")
            return 0.0
        
    if not summary_data.empty:
        # Quarterly totals will be calculated from actual table data later
        quarterly_totals = {}
        
        # Overall totals will be calculated dynamically based on the selected view data
    # Complete Financial Year View
    st.markdown("#### 📋 Complete Financial Year View")
    
    # Show all data for the selected year (complete financial year)
    complete_year_data = all_data[all_data['year'] == selected_year].copy()
    
    if selected_owner != "All":
        complete_year_data = complete_year_data[complete_year_data['owner'] == selected_owner]
    if selected_lob != "All":
        complete_year_data = complete_year_data[complete_year_data['lob'] == selected_lob]
    if selected_region != "All":
        complete_year_data = complete_year_data[complete_year_data['region'] == selected_region]
    if selected_source != "All":
        complete_year_data = complete_year_data[complete_year_data['source'] == selected_source]
    
    # Apply forecast filter if selected
    if forecast_filter:
        print(f"DEBUG: Forecast filter enabled. Before filtering: {len(complete_year_data)} records")
        # Show only records where Planned (Forecasted) value > 0
        forecast_records = complete_year_data[
            (complete_year_data['metric_type'] == 'Planned') & 
            (complete_year_data['value'] > 0)
        ]
        print(f"DEBUG: Found {len(forecast_records)} forecast records > 0")
        if not forecast_records.empty:
            # Get unique identifiers for records with forecast > 0
            forecast_accounts = forecast_records[['account_name', 'month', 'year']].drop_duplicates()
            print(f"DEBUG: Found {len(forecast_accounts)} unique account-month combinations with forecast > 0")
            # Filter all data to only include these account-month combinations
            complete_year_data = complete_year_data.merge(
                forecast_accounts, 
                on=['account_name', 'month', 'year'], 
                how='inner'
            )
            print(f"DEBUG: After forecast filter: {len(complete_year_data)} records")
        else:
            # No records match the forecast filter
            print("DEBUG: No forecast records found, returning empty dataset")
            complete_year_data = complete_year_data.iloc[0:0]  # Empty dataframe with same structure
    
    if not complete_year_data.empty:
        import pandas as pd
        
        # UI Design Options
        view_option = st.radio(
            "Select View Format:",
            ["📊 Master Table (Complete Year)", "🎯 Owner-Account Hierarchy", "📈 Account Summary Cards"],
            horizontal=True
        )
        
        if view_option == "📊 Master Table (Complete Year)":
            # Calculate Overall Totals from the actual table data
            
            # Build the table data first to get the actual values
            table_data = []
            id_mapping = {}
            
            # Determine which months to display based on quarter selection
            if selected_quarter == "All":
                display_months = []
                for quarter in ["Q1", "Q2", "Q3", "Q4"]:
                    display_months.extend(quarter_months[quarter])
            else:
                display_months = quarter_months[selected_quarter]
            
            for account in sorted(complete_year_data['account_name'].unique()):
                account_data = complete_year_data[complete_year_data['account_name'] == account]
                account_info = account_data.iloc[0]
                
                # Calculate Duration - count of months where Planned > 0 (matching Monthly data view)
                duration_count = 0
                planned_data = account_data[account_data['metric_type'] == 'Planned']
                if not planned_data.empty:
                    # Count months where planned value > 0
                    positive_planned = planned_data[pd.to_numeric(planned_data['value'], errors='coerce') > 0]
                    duration_count = len(positive_planned['month'].unique())
                
                # Get confidence level FROM master_clients table
                confidence_level = 0
                try:
                    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                    cursor = conn.cursor()
                    master_clients_table = env_manager.get_table_name('master_clients')
                    cursor.execute(f"SELECT confidence_level FROM {master_clients_table} WHERE client_name = %s", (account,))
                    result = cursor.fetchone()
                    if result and result[0] is not None:
                        confidence_level = result[0]
                    conn.close()
                except Exception as e:
                    print(f"Error getting confidence for {account}: {e}")
                
                # Base row
                row = {
                    'Account': account,
                    'Owner': account_info['owner'],
                    'LoB': account_info['lob'],
                    'Region': account_info['region'],
                    'Status': account_info['status'],
                    'Duration': duration_count,
                    'Confidence': confidence_level
                }
                
                # Store IDs for each metric type and month for this account
                account_ids = {}
                
                # Running totals for this account
                total_f = 0
                total_b = 0
                total_bl = 0
                
                # Add monthly data for the selected quarter or all quarters
                for month in display_months:
                    month_data = account_data[account_data['month'] == month]
                    month_abbr = month[:3]  # Apr, May, Jun, etc.
                    
                    # Get metric values and IDs
                    planned_data = month_data[month_data['metric_type'] == 'Planned']
                    booked_data = month_data[month_data['metric_type'] == 'Booked']
                    billed_data = month_data[month_data['metric_type'] == 'Billed']
                    
                    forecasted = planned_data['value'].sum() if not planned_data.empty else 0
                    booked = booked_data['value'].sum() if not booked_data.empty else 0
                    billed = billed_data['value'].sum() if not billed_data.empty else 0
                    
                    # Store IDs for updating
                    if not planned_data.empty:
                        account_ids[f'{month_abbr}_F'] = planned_data.iloc[0]['id']
                    if not booked_data.empty:
                        account_ids[f'{month_abbr}_B'] = booked_data.iloc[0]['id']
                    if not billed_data.empty:
                        account_ids[f'{month_abbr}_Bl'] = billed_data.iloc[0]['id']
                    
                    # Add columns
                    row[f'{month_abbr}_F'] = forecasted
                    row[f'{month_abbr}_B'] = booked
                    row[f'{month_abbr}_Bl'] = billed
                    
                    # Add to totals
                    total_f += forecasted
                    total_b += booked
                    total_bl += billed
                
                # Add row totals
                row['Total_Forecasted'] = total_f
                row['Total_Booked'] = total_b
                row['Total_Billed'] = total_bl
                
                table_data.append(row)
                id_mapping[account] = account_ids
            
            import pandas as pd
            edited_df = pd.DataFrame(table_data)
            
            # Sort by Total_Forecasted in descending order (highest to lowest)
            if not edited_df.empty:
                edited_df = edited_df.sort_values('Total_Forecasted', ascending=False)
            
            # Quarterly Summary will be calculated after data editing using the same logic as Overall Totals
            
            st.markdown("##### Editable Monthly Data View")
            
            # Use the data already calculated above
            df = edited_df.copy()
            
            # Overall Totals will be calculated after data editing for consistency with Summary Statistics
            
            # Column configuration for editable data editor
            column_config = {}
            
            # Fixed columns - pinned/frozen up to Confidence column
            fixed_cols = ['Account', 'Owner', 'LoB', 'Region', 'Status', 'Duration', 'Confidence']
            for col in fixed_cols:
                if col == 'Status':
                    column_config[col] = st.column_config.SelectboxColumn(
                        col,
                        width="small",
                        options=["Active Lead", "Dropped"],
                        pinned=True
                    )
                elif col == 'Duration':
                    column_config[col] = st.column_config.NumberColumn(
                        col,
                        width="small",
                        min_value=0,
                        max_value=24,
                        step=1,
                        pinned=True
                    )
                elif col == 'Confidence':
                    column_config[col] = st.column_config.NumberColumn(
                        col,
                        width="small",
                        min_value=0,
                        max_value=100,
                        step=5,
                        pinned=True,
                        help="Confidence percentage (0-100%)"
                    )
                else:
                    column_config[col] = st.column_config.TextColumn(
                        col,
                        width="medium" if col == 'Account' else "small",
                        pinned=True
                    )
            
            # Metric columns - make them editable
            for col in df.columns:
                if col not in fixed_cols:
                    if col.startswith('Total_'):
                        column_config[col] = st.column_config.NumberColumn(
                            col,
                            format="$%.0f",
                            width="medium",
                            disabled=True  # Total columns are calculated, not editable
                        )
                    else:
                        column_config[col] = st.column_config.NumberColumn(
                            col,
                            format="$%.0f",
                            width="small",
                            min_value=0,
                            step=1000
                        )
            
            st.info("💡 Edit Forecast (F), Booked (B), and Billed (Bl) values directly in the table.")
            
            # Create editable data editor
            if not df.empty:
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    height=600,
                    column_config=column_config,
                    key="master_table_editor"
                )
                
                # Recalculate totals for edited data
                for idx in range(len(edited_df)):
                    total_f = 0
                    total_b = 0
                    total_bl = 0
                    
                    for col in edited_df.columns:
                        if col.endswith('_F') and not col.startswith('Total_'):
                            total_f += edited_df.iloc[idx][col]
                        elif col.endswith('_B') and not col.startswith('Total_'):
                            total_b += edited_df.iloc[idx][col]
                        elif col.endswith('_Bl') and not col.startswith('Total_'):
                            total_bl += edited_df.iloc[idx][col]
                    
                    # Update the totals in the edited dataframe
                    edited_df.at[idx, 'Total_Forecasted'] = total_f
                    edited_df.at[idx, 'Total_Booked'] = total_b
                    edited_df.at[idx, 'Total_Billed'] = total_bl
                
                # Save and action buttons
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    if st.button("💾 Save All Changes", type="primary"):
                        try:
                            changes_made = False
                            
                            # Compare original and edited data
                            for idx, account in enumerate(edited_df['Account']):
                                if account in id_mapping:
                                    original_row = df[df['Account'] == account].iloc[0]
                                    edited_row = edited_df.iloc[idx]
                                    
                                    # Check each editable column for changes
                                    for col in edited_df.columns:
                                        if col not in fixed_cols and not col.startswith('Total_') and '_' in col:
                                            original_val = original_row[col]
                                            edited_val = edited_row[col]
                                            
                                            if abs(float(original_val) - float(edited_val)) > 0.01:  # Allow for small float differences
                                                # Parse column name to get month and metric type
                                                month_abbr, metric_suffix = col.rsplit('_', 1)
                                                
                                                # Map suffix to database metric type
                                                metric_mapping = {
                                                    'F': 'Planned',
                                                    'B': 'Booked', 
                                                    'Bl': 'Billed'
                                                }
                                                
                                                if metric_suffix in metric_mapping:
                                                    metric_type = metric_mapping[metric_suffix]
                                                    import calendar
                                                    month_name = calendar.month_name[list(calendar.month_abbr).index(month_abbr)]
                                                    
                                                    # Find record in the existing data
                                                    matching_records = complete_year_data[
                                                        (complete_year_data['account_name'] == account) &
                                                        (complete_year_data['month'] == month_name) &
                                                        (complete_year_data['metric_type'] == metric_type)
                                                    ]
                                                    
                                                    if not matching_records.empty:
                                                        record_id = matching_records.iloc[0]['id']
                                                        updated_data = {'value': float(edited_val)}
                                                        
                                                        result = unified_db.update_record(record_id, updated_data)
                                                        if result:
                                                            changes_made = True
                                    
                                    # Check for account info changes
                                    for col in ['Owner', 'LoB', 'Region', 'Status']:
                                        if str(original_row[col]) != str(edited_row[col]):
                                            # Update all records for this account
                                            account_records = complete_year_data[complete_year_data['account_name'] == account]
                                            for _, record in account_records.iterrows():
                                                updated_data = {
                                                    'owner': edited_row['Owner'],
                                                    'lob': edited_row['LoB'],
                                                    'region': edited_row['Region'],
                                                    'status': edited_row['Status']
                                                }
                                                unified_db.update_record(record['id'], updated_data)
                                            changes_made = True
                                            break
                                    
                                    # Check for confidence level changes
                                    if 'Confidence' in edited_row and str(original_row['Confidence']) != str(edited_row['Confidence']):
                                        try:
                                            # Get database connection using the centralized utility
                                            from utils.database_connection import get_database_connection
                                            conn = get_database_connection()
                                            cursor = conn.cursor()
                                            # Get env_manager from session state
                                            env_manager = st.session_state.get('env_manager')
                                            if env_manager:
                                                master_clients_table = env_manager.get_table_name('master_clients')
                                                cursor.execute(f"""
                                                    UPDATE {master_clients_table}
                                                    SET confidence_level = %s 
                                                    WHERE client_name = %s
                                                """, (int(edited_row['Confidence']), account))
                                            else:
                                                st.error("Environment manager not found")
                                            conn.commit()
                                            conn.close()
                                            changes_made = True
                                        except Exception as e:
                                            print(f"Error updating confidence for {account}: {e}")
                            
                            if changes_made:
                                st.success("✅ All changes saved successfully!")
                                # Clear any cached data to force refresh
                                if 'demand_tweaking_data' in st.session_state:
                                    del st.session_state['demand_tweaking_data']
                                # Force refresh to recalculate all totals including quarterly summary cards
                                time.sleep(0.2)
                                st.rerun()
                            else:
                                st.info("ℹ️ No changes detected to save.")
                                
                        except Exception as e:
                            st.error(f"❌ Error saving changes: {str(e)}")
                
                with col2:
                    if st.button("🔄 Refresh Data"):
                        st.rerun()
                
                with col3:
                    if st.button("📥 Export Data"):
                        csv = edited_df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"monthly_data_{selected_year}_{selected_quarter}.csv",
                            mime="text/csv"
                        )
                
                # Summary statistics
                # Overall Totals will be calculated after Quarterly Summary to ensure all variables are available
                
                # Calculate Quarterly Summary from the same edited data
                st.markdown("---")
                st.markdown("#### 📊 Quarterly Summary")
                
                # Define quarter months mapping
                quarter_months = {
                    "Q1": ["April", "May", "June"],
                    "Q2": ["July", "August", "September"], 
                    "Q3": ["October", "November", "December"],
                    "Q4": ["January", "February", "March"]
                }
                
                # Calculate quarterly totals from the edited data
                quarterly_totals = {}
                for quarter in ["Q1", "Q2", "Q3", "Q4"]:
                    qtr_months = quarter_months[quarter]
                    
                    # Get target based on selected owner and quarter
                    if selected_owner != "All":
                        target_value = get_target_for_owner_quarter(selected_owner, quarter, selected_year)
                    else:
                        # For "All" owners, get the company-wide target (pass None for company-wide)
                        target_value = get_target_for_owner_quarter(None, quarter, selected_year)
                    
                    # Calculate totals from edited data by quarter
                    qtr_forecasted = 0
                    qtr_booked = 0
                    qtr_billed = 0
                    
                    # Sum up values from all monthly columns for this quarter from edited_df
                    for month_name in qtr_months:
                        month_abbr = month_name[:3]
                        if f'{month_abbr}_F' in edited_df.columns:
                            qtr_forecasted += edited_df[f'{month_abbr}_F'].sum()
                        if f'{month_abbr}_B' in edited_df.columns:
                            qtr_booked += edited_df[f'{month_abbr}_B'].sum()
                        if f'{month_abbr}_Bl' in edited_df.columns:
                            qtr_billed += edited_df[f'{month_abbr}_Bl'].sum()
                    
                    quarterly_totals[quarter] = {
                        'forecasted': qtr_forecasted,
                        'booked': qtr_booked,
                        'billed': qtr_billed,
                        'target': target_value
                    }
                
                # Calculate total target for deficit calculation (needed for Overall Totals)
                total_target = sum(quarterly_totals[quarter]['target'] for quarter in quarterly_totals if quarterly_totals[quarter]['target'] > 0)
                
                # Display quarterly breakdown using the same edited data as Overall Totals
                qtr_cols = st.columns(4)
                for i, quarter in enumerate(["Q1", "Q2", "Q3", "Q4"]):
                    with qtr_cols[i]:
                        qtr_data = quarterly_totals[quarter]
                        target_display = f"${qtr_data['target']:,.0f}" if qtr_data['target'] > 0 else "N/A"
                        st.markdown(f"""
                        <div style='background-color: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; border: 2px solid #007bff; margin-bottom: 10px; color: #333;'>
                        <h4 style='color: #007bff; margin: 0 0 10px 0;'>{quarter}</h4>
                        <div style='color: #28a745; font-weight: bold; margin: 5px 0;'>F: ${qtr_data['forecasted']:,.0f}</div>
                        <div style='color: #dc3545; font-weight: bold; margin: 5px 0;'>B: ${qtr_data['booked']:,.0f}</div>
                        <div style='color: #6c757d; font-weight: bold; margin: 5px 0;'>Bl: ${qtr_data['billed']:,.0f}</div>
                        <div style='color: #fd7e14; font-weight: bold; margin: 5px 0;'>T: {target_display}</div>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Now calculate Overall Totals using the same edited data and total_target from quarterly calculations
                st.markdown("---")
                st.markdown("### 📈 Overall Totals")
                st.markdown("---")
                
                # Calculate totals from the same edited data as Quarterly Summary
                current_totals = {
                    'forecasted': edited_df['Total_Forecasted'].sum() if not edited_df.empty else 0,
                    'booked': edited_df['Total_Booked'].sum() if not edited_df.empty else 0,
                    'billed': edited_df['Total_Billed'].sum() if not edited_df.empty else 0,
                    'accounts': len(edited_df) if not edited_df.empty else 0,
                    'owners': len(edited_df['Owner'].unique()) if not edited_df.empty else 0
                }
                
                # Calculate target deficit using total_target from quarterly calculations
                target_deficit = current_totals['billed'] - total_target
                deficit_color = "color: green;" if target_deficit >= 0 else "color: red;"
                deficit_text = f"${target_deficit:,.0f}"
                
                col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
                
                with col1:
                    st.metric("Accounts", f"{current_totals['accounts']}")
                with col2:
                    st.metric("Owners", f"{current_totals['owners']}")
                with col3:
                    st.metric("Total Forecasted", f"${current_totals['forecasted']:,.0f}")
                with col4:
                    st.metric("Total Booked", f"${current_totals['booked']:,.0f}")
                with col5:
                    st.metric("Total Billed", f"${current_totals['billed']:,.0f}")
                with col6:
                    st.metric("Total Target", f"${total_target:,.0f}")
                with col7:
                    st.markdown(f"""
                    <div style="text-align: center; padding: 10px;">
                        <div style="font-size: 14px; color: #808495; margin-bottom: 4px;">Target Deficit</div>
                        <div style="font-size: 24px; font-weight: 600; {deficit_color}">
                            {deficit_text}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("##### Summary Statistics")
                
                # Use the same data for Summary Statistics (redundant but keeping for user familiarity)
                total_accounts = len(edited_df)
                total_planned = edited_df['Total_Forecasted'].sum()
                total_booked = edited_df['Total_Booked'].sum()
                total_billed = edited_df['Total_Billed'].sum()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Accounts", total_accounts)
                with col2:
                    st.metric("Total Planned", f"${total_planned:,.0f}")
                with col3:
                    st.metric("Total Booked", f"${total_booked:,.0f}")
                with col4:
                    st.metric("Total Billed", f"${total_billed:,.0f}")
            
            else:
                st.warning("No data available for the selected filters.")
            
            # Add legend for the table
            if selected_quarter != "All":
                structure_text = f"**Financial Year Structure (FY {selected_year}):** Q1: Apr-Jun | Q2: Jul-Sep | Q3: Oct-Dec | Q4: Jan-Mar"
            else:
                structure_text = f"**Complete Financial Year View (FY {selected_year} - April 2025 - March 2026)"
            
            st.markdown(f"""
            {structure_text}
            - **Legend:** F=Forecasted, B=Booked, Bl=Billed
            - **Editable:** All F, B, Bl values and account details (Owner, LoB, Region, Status)
            - **Row Totals:** Automatically calculated from monthly values
            """)
            

        
        elif view_option == "🎯 Owner-Account Hierarchy":
            st.markdown("##### Owner → Account → Quarterly Breakdown")
            
            # Calculate owner totals for sorting
            owner_totals = []
            for owner in complete_year_data['owner'].unique():
                owner_data = complete_year_data[complete_year_data['owner'] == owner]
                owner_f = owner_data[owner_data['metric_type'] == 'Planned']['value'].sum()
                owner_totals.append((owner, owner_f))
            
            # Sort owners by forecasted amount (highest to lowest)
            owner_totals.sort(key=lambda x: x[1], reverse=True)
            
            # Group by owner first (sorted by forecast amount)
            for owner, _ in owner_totals:
                owner_data = complete_year_data[complete_year_data['owner'] == owner]
                
                # Calculate owner totals
                owner_f = owner_data[owner_data['metric_type'] == 'Planned']['value'].sum()
                owner_b = owner_data[owner_data['metric_type'] == 'Booked']['value'].sum()
                owner_bl = owner_data[owner_data['metric_type'] == 'Billed']['value'].sum()
                
                with st.expander(f"👤 {owner} | F: ${owner_f:,.0f} | B: ${owner_b:,.0f} | Bl: ${owner_bl:,.0f}", expanded=False):
                    
                    # Calculate account totals for sorting
                    account_totals = []
                    for account in owner_data['account_name'].unique():
                        account_data = owner_data[owner_data['account_name'] == account]
                        acc_f = account_data[account_data['metric_type'] == 'Planned']['value'].sum()
                        account_totals.append((account, acc_f))
                    
                    # Sort accounts by forecasted amount (highest to lowest)
                    account_totals.sort(key=lambda x: x[1], reverse=True)
                    
                    # Show accounts under this owner (sorted by forecast amount)
                    for account, _ in account_totals:
                        account_data = owner_data[owner_data['account_name'] == account]
                        
                        # Account totals and status
                        acc_f = account_data[account_data['metric_type'] == 'Planned']['value'].sum()
                        acc_b = account_data[account_data['metric_type'] == 'Booked']['value'].sum()
                        acc_bl = account_data[account_data['metric_type'] == 'Billed']['value'].sum()
                        account_status = account_data['status'].iloc[0]
                        
                        st.markdown(f"**🏢 {account}** | Status: {account_status} | F: ${acc_f:,.0f} | B: ${acc_b:,.0f} | Bl: ${acc_bl:,.0f}")
                        
                        # Quarterly breakdown
                        cols = st.columns(4)
                        for i, quarter in enumerate(["Q1", "Q2", "Q3", "Q4"]):
                            with cols[i]:
                                qtr_months = quarter_months[quarter]
                                qtr_data = account_data[account_data['month'].isin(qtr_months)]
                                
                                qtr_f = qtr_data[qtr_data['metric_type'] == 'Planned']['value'].sum()
                                qtr_b = qtr_data[qtr_data['metric_type'] == 'Booked']['value'].sum()
                                qtr_bl = qtr_data[qtr_data['metric_type'] == 'Billed']['value'].sum()
                                
                                st.markdown(f"""
                                <div style='background-color: #f8f9fa; padding: 12px; border-radius: 8px; text-align: center; border: 1px solid #007bff; margin: 5px; color: #333;'>
                                <h6 style='color: #007bff; margin: 0 0 8px 0;'>{quarter}</h6>
                                <div style='color: #28a745; font-weight: bold; margin: 3px 0; font-size: 13px;'>F: ${qtr_f:,.0f}</div>
                                <div style='color: #dc3545; font-weight: bold; margin: 3px 0; font-size: 13px;'>B: ${qtr_b:,.0f}</div>
                                <div style='color: #6c757d; font-weight: bold; margin: 3px 0; font-size: 13px;'>Bl: ${qtr_bl:,.0f}</div>
                                </div>
                                """, unsafe_allow_html=True)
                        
                        st.markdown("---")
        
        elif view_option == "📈 Account Summary Cards":
            st.markdown("##### Account Performance Cards")
            
            # Calculate account totals for sorting
            account_totals = []
            for account in complete_year_data['account_name'].unique():
                account_data = complete_year_data[complete_year_data['account_name'] == account]
                total_f = account_data[account_data['metric_type'] == 'Planned']['value'].sum()
                account_totals.append((account, total_f))
            
            # Sort accounts by forecasted amount (highest to lowest)
            account_totals.sort(key=lambda x: x[1], reverse=True)
            accounts = [account for account, _ in account_totals]
            
            for i in range(0, len(accounts), 2):
                cols = st.columns(2)
                
                for j, col in enumerate(cols):
                    if i + j < len(accounts):
                        account = accounts[i + j]
                        account_data = complete_year_data[complete_year_data['account_name'] == account]
                        account_info = account_data.iloc[0]
                        
                        with col:
                            # Calculate totals
                            total_f = account_data[account_data['metric_type'] == 'Planned']['value'].sum()
                            total_b = account_data[account_data['metric_type'] == 'Booked']['value'].sum()
                            total_bl = account_data[account_data['metric_type'] == 'Billed']['value'].sum()
                            
                            # Create card
                            st.markdown(f"""
                            <div style='border: 1px solid #ddd; padding: 15px; border-radius: 8px; margin-bottom: 10px;'>
                            <h4>{account}</h4>
                            <p><strong>Owner:</strong> {account_info['owner']}</p>
                            <p><strong>LoB:</strong> {account_info['lob']} | <strong>Region:</strong> {account_info['region']}</p>
                            <p><strong>Status:</strong> {account_info['status']}</p>
                            <hr>
                            <div style='display: flex; justify-content: space-between;'>
                            <div><strong>Forecasted:</strong><br>${total_f:,.0f}</div>
                            <div><strong>Booked:</strong><br>${total_b:,.0f}</div>
                            <div><strong>Billed:</strong><br>${total_bl:,.0f}</div>
                            </div>
                            </div>
                            """, unsafe_allow_html=True)
        
        # Export functionality
        if st.button("📥 Export Complete Year Data"):
            if 'df' in locals():
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"complete_fy_data_{selected_year}.csv",
                    mime="text/csv"
                )
    else:
        st.info("No data available for the selected filters.")
    


def demand_management_page():
    st.subheader("📝 Demand Management - Data Entry")
    
    # Initialize unified database manager
    unified_db = UnifiedDataManager()
    
    # Data is already loaded into the unified database table
    # No need to reload CSV data as it's been restructured
    
    # Get dropdown options from unified database
    try:
        all_data = unified_db.get_all_data()
        
        if not all_data.empty:
            # Get unique values and remove None/NaN values, then sort
            owners = sorted([x for x in all_data['owner'].dropna().unique() if x])
            sources = sorted([x for x in all_data['source'].dropna().unique() if x])
            industries = sorted([x for x in all_data['industry'].dropna().unique() if x])
            regions = sorted([x for x in all_data['region'].dropna().unique() if x])
            lobs = sorted([x for x in all_data['lob'].dropna().unique() if x])
            offerings = sorted([x for x in all_data['offering'].dropna().unique() if x])
            metric_types = sorted([x for x in all_data['metric_type'].dropna().unique() if x])
        else:
            # Default options if no data
            owners = ["Avinash", "Madhu", "AP"]
            sources = ["Direct Sales", "Reference", "Specmatic", "Partner"]
            industries = ["BFSI", "Energy", "Technology"]
            regions = ["India", "Philippines", "Indonesia", "Australia"]
            lobs = ["Digital Engineering Consulting", "Digital Engineering Delivery", "Digital Biz Consulting"]
            offerings = ["AInsightOps", "TC", "Cloud and Devops", "Technical Enablement"]
            metric_types = ["Planned", "Booked", "Billed", "Forecasted"]
        
    except Exception as e:
        st.error(f"Error loading dropdown options: {str(e)}")
        # Use default options from the CSV sample
        owners = ["Avinash", "Madhu", "AP"]
        sources = ["Direct Sales", "Reference", "Specmatic", "Partner"]
        industries = ["BFSI", "Energy", "Technology"]
        regions = ["India", "Philippines", "Indonesia", "Australia"]
        lobs = ["Digital Engineering Consulting", "Digital Engineering Delivery", "Digital Biz Consulting"]
        offerings = ["AInsightOps", "TC", "Cloud and Devops", "Technical Enablement"]
        metric_types = ["Planned", "Booked", "Billed", "Forecasted"]
    
    # Initialize session state for custom industries
    if 'custom_industries' not in st.session_state:
        st.session_state.custom_industries = []
    
    # Data entry form
    with st.form("demand_entry_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            account_name = st.text_input("Account Name *", placeholder="Enter account name")
            track = st.text_input("Track", placeholder="Enter track information")
            connect_name = st.text_input("Connect Name", placeholder="Enter connect name")
            partner_connect = st.text_input("Partner Connect", placeholder="Enter partner connect information")
            partner_org = st.text_input("Partner Org", placeholder="Enter partner organization")
            
            # Status dropdown
            status_options = ["Active Lead", "Dropped"]
            status = st.selectbox("Status *", options=status_options, index=0)
            
            # Owner dropdown
            owner = st.selectbox("Owner *", options=[""] + owners, index=0)
            
            # Source dropdown
            source = st.selectbox("Source *", options=[""] + sources, index=0)
            
            # Industry dropdown with inline add functionality
            industry_col1, industry_col2 = st.columns([5, 1])
            
            with industry_col1:
                # Combine existing industries with any custom ones from session
                all_industries = industries + st.session_state.custom_industries
                industry = st.selectbox("Industry *", options=[""] + sorted(all_industries), index=0, key="industry_select")
            
            with industry_col2:
                st.markdown("<div style='height: 32px;'></div>", unsafe_allow_html=True)
                add_industry = st.form_submit_button(
                    "＋", 
                    help="Add new industry",
                    use_container_width=True
                )
            
            # Region dropdown
            region = st.selectbox("Region *", options=[""] + regions, index=0)
        
        with col2:
            # LoB dropdown
            lob = st.selectbox("LoB *", options=[""] + lobs, index=0)
            
            # Offering dropdown
            offering = st.selectbox("Offering *", options=[""] + offerings, index=0)
            
            # Financial year dropdown (current year and future years)
            import datetime
            current_year = datetime.datetime.now().year
            fy_options = [f"FY{year}" for year in range(current_year, current_year + 6)]
            financial_year = st.selectbox("Financial Year *", options=fy_options, index=0)
            
            # Expected to Start from dropdown
            months = ['April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December', 'January', 'February', 'March']
            expected_start_month = st.selectbox("Expected to Start from *", options=[""] + months, index=0)
            
            # Duration input
            duration_months = st.number_input("Duration (in months) *", min_value=1, max_value=12, value=1, step=1)
            
            # Metric Type dropdown
            metric_type = st.selectbox("Metric Type *", options=[""] + metric_types, index=0)
            
            # Revenue per month field
            revenue_per_month = st.number_input("Revenue per month (USD) *", min_value=0.0, step=1000.0, format="%.2f")
            
            # Total Revenue calculation and display
            total_revenue = revenue_per_month * duration_months
            if revenue_per_month > 0:
                st.info(f"**Total Revenue:** ${total_revenue:,.2f} USD over {duration_months} months")
            
            # Currency display (read-only)
            st.text_input("Currency", value="USD", disabled=True)
        
        # Next button
        next_clicked = st.form_submit_button("➡️ Next", type="primary")
        
        # Handle Add Industry button
        if add_industry:
            # Show modal for adding new industry
            st.session_state.show_add_industry_modal = True
            st.rerun()
        
        if next_clicked:
            # Validate required fields
            required_fields = {
                "Account Name": account_name,
                "Owner": owner,
                "Source": source,
                "Industry": industry,
                "Region": region,
                "LoB": lob,
                "Offering": offering,
                "Expected to Start from": expected_start_month,
                "Metric Type": metric_type
            }
            
            missing_fields = [field for field, value in required_fields.items() if not value]
            
            if missing_fields:
                st.error(f"Please fill in the following required fields: {', '.join(missing_fields)}")
            elif revenue_per_month <= 0:
                st.error("Revenue per month must be greater than 0")
            else:
                # Store form data in session state for the table view
                st.session_state.form_data = {
                    'account_name': account_name,
                    'track': track,
                    'connect_name': connect_name,
                    'partner_connect': partner_connect,
                    'partner_org': partner_org,
                    'status': status,
                    'owner': owner,
                    'source': source,
                    'industry': industry,
                    'region': region,
                    'lob': lob,
                    'offering': offering,
                    'financial_year': financial_year,
                    'expected_start_month': expected_start_month,
                    'duration_months': duration_months,
                    'metric_type': metric_type,
                    'revenue_per_month': revenue_per_month,
                    'total_revenue': total_revenue
                }
                st.session_state.show_monthly_breakdown = True
                st.rerun()

    # Initialize session state for monthly breakdown
    if 'show_monthly_breakdown' not in st.session_state:
        st.session_state.show_monthly_breakdown = False
    
    # Monthly breakdown table
    if st.session_state.show_monthly_breakdown and 'form_data' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Monthly Revenue Breakdown")
        
        form_data = st.session_state.form_data
        
        # Generate monthly data
        months = ['April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December', 'January', 'February', 'March']
        start_idx = months.index(form_data['expected_start_month'])
        
        monthly_data = []
        for i in range(form_data['duration_months']):
            month_idx = (start_idx + i) % 12
            monthly_data.append({
                'Month': months[month_idx],
                'Expected Revenue (USD)': form_data['revenue_per_month']
            })
        
        # Convert to DataFrame for editing
        df_monthly = pd.DataFrame(monthly_data)
        
        # Editable data table
        edited_df = st.data_editor(
            df_monthly,
            num_rows="fixed",
            use_container_width=True,
            column_config={
                "Month": st.column_config.TextColumn("Month", disabled=True),
                "Expected Revenue (USD)": st.column_config.NumberColumn(
                    "Expected Revenue (USD)",
                    min_value=0,
                    step=1000,
                    format="$%.2f"
                )
            }
        )
        
        # Save Details button
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button("💾 Save Details", type="primary", use_container_width=True):
                try:
                    # Save each monthly record to unified database
                    success_count = 0
                    
                    for _, row in edited_df.iterrows():
                        month = row['Month']
                        revenue = row['Expected Revenue (USD)']
                        
                        try:
                            # Calculate month number and year
                            month_number = months.index(month) + 1
                            fy_year = int(form_data['financial_year'].replace("FY", ""))
                            
                            # Determine actual year based on financial year cycle
                            if month_number >= 4:  # April to December
                                actual_year = fy_year
                            else:  # January to March
                                actual_year = fy_year + 1
                            
                            # Debug output
                            st.write(f"Debug: Saving {month} ({month_number}) for {form_data['account_name']} - ${revenue}")
                            
                            # Add record to unified database
                            record_data = {
                                'account_name': form_data['account_name'],
                                'account_track': form_data['track'] or "",
                                'connect_name': form_data['connect_name'] or "",
                                'partner_connect': form_data['partner_connect'] or "",
                                'partner_org': form_data['partner_org'] or "",
                                'status': form_data['status'],
                                'owner': form_data['owner'],
                                'source': form_data['source'],
                                'industry': form_data['industry'],
                                'region': form_data['region'],
                                'lob': form_data['lob'],
                                'offering': form_data['offering'],
                                'financial_year': form_data['financial_year'],
                                'year': actual_year,
                                'month': month,
                                'metric_type': form_data['metric_type'],
                                'value': revenue,
                                'confidence': 50  # Default confidence for new records
                            }
                            
                            success = unified_db.add_new_record(record_data)
                            
                            if success:
                                success_count += 1
                                st.write(f"✅ Success: {month} record saved")
                            else:
                                st.write(f"❌ Failed: {month} record not saved")
                                
                        except Exception as e:
                            st.error(f"Error processing {month}: {str(e)}")
                    
                    if success_count == len(edited_df):
                        st.success(f"✅ Successfully saved {success_count} monthly records to database!")
                        st.balloons()
                        # Clear the form data
                        st.session_state.show_monthly_breakdown = False
                        del st.session_state.form_data
                        st.rerun()
                    else:
                        st.error(f"Only {success_count} out of {len(edited_df)} records were saved successfully.")
                        
                except Exception as e:
                    st.error(f"Error saving records: {str(e)}")

    # Modal for adding new industry
    if st.session_state.get('show_add_industry_modal', False):
        with st.container():
            st.write("---")
            st.subheader("➕ Add New Industry")
            
            new_industry_name = st.text_input("Industry Name", placeholder="Enter new industry name", key="new_industry_modal")
            
            modal_col1, modal_col2, modal_col3 = st.columns([1, 1, 2])
            
            with modal_col1:
                if st.button("Save"):
                    if new_industry_name and new_industry_name not in st.session_state.custom_industries:
                        st.session_state.custom_industries.append(new_industry_name)
                        st.session_state.show_add_industry_modal = False
                        st.success(f"Added '{new_industry_name}' to industry list!")
                        st.rerun()
                    elif not new_industry_name:
                        st.error("Please enter an industry name")
                    else:
                        st.error("Industry already exists")
            
            with modal_col2:
                if st.button("Cancel"):
                    st.session_state.show_add_industry_modal = False
                    st.rerun()

def editable_plan_view_page():
    st.subheader("📊 Editable Plan View")
    
    # Use the unified data manager for simpler, more reliable data persistence
    unified_db = UnifiedDataManager()
    
    # Show database statistics
    stats = unified_db.get_database_stats()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", stats.get('total_records', 0))
    with col2:
        st.metric("Unique Accounts", stats.get('unique_accounts', 0))
    with col3:
        st.metric("Unique Tracks", stats.get('unique_tracks', 0))
    
    # Display database structure information
    with st.expander("📊 Database Structure Info"):
        st.markdown("""
        **Database Tables Created:**
        - **accounts**: Account information with owner, region, LoB, confidence
        - **sales_data**: Individual records with extracted year, month, month_number, metric_type, and value
        
        **Date Components Extracted:**
        - Year (e.g., 2025, 2026)
        - Month Name (e.g., April, May, June)
        - Month Number (e.g., 4, 5, 6)
        - Metric Type (Planned, Booked, Billed, Forecasted)
        """)
    
    # Get all data from unified database
    try:
        editable_data = unified_db.get_all_data()
        if editable_data.empty:
            st.warning("No data available in unified database.")
            return
        
        # Remove the ID column from display (keep for updates)
        display_data = editable_data.drop(columns=['id']).copy()
        
        # Show data summary
        st.write(f"📊 **{display_data.shape[0]}** records loaded from unified database")
        
    except Exception as e:
        st.error(f"Error retrieving data: {str(e)}")
        return
    
    # Create editable plan view
    st.subheader("📈 Editable Plan View (Database-Driven)")
    
    try:
        # Simple message
        st.info("💡 Edit any cell directly. Use 'Save Changes' button to persist to database.")
        
        # Use data_editor directly with display data to prevent refresh issues
        edited_data = st.data_editor(
            display_data,
            use_container_width=True,
            height=600,
            key="plan_editor",
            column_config={
                "account_name": st.column_config.TextColumn("Account", width="medium"),
                "account_track": st.column_config.TextColumn("Track", width="medium"),
                "connect_name": st.column_config.TextColumn("Connect Name", width="medium"),
                "partner_connect": st.column_config.TextColumn("Partner Connect", width="medium"),
                "partner_org": st.column_config.TextColumn("Partner Org", width="medium"),
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    width="small",
                    options=["Active Lead", "Dropped"]
                ),
                "duration": st.column_config.NumberColumn(
                    "Duration (Months)",
                    width="small",
                    min_value=0,
                    max_value=24,
                    step=1
                ),
                "owner": st.column_config.TextColumn("Owner", width="small"),
                "source": st.column_config.TextColumn("Source", width="small"),
                "industry": st.column_config.TextColumn("Industry", width="small"),
                "region": st.column_config.TextColumn("Region", width="small"),
                "lob": st.column_config.TextColumn("LoB", width="medium"),
                "offering": st.column_config.TextColumn("Offering", width="medium"),
                "confidence": st.column_config.NumberColumn(
                    "Confidence %", 
                    width="small", 
                    min_value=0, 
                    max_value=100,
                    step=1
                ),
                "financial_year": st.column_config.TextColumn("Financial Year", width="small"),
                "year": st.column_config.NumberColumn("Year", width="small", min_value=2020, max_value=2030),
                "month": st.column_config.SelectboxColumn(
                    "Month", 
                    width="small",
                    options=['April', 'May', 'June', 'July', 'August', 'September', 
                            'October', 'November', 'December', 'January', 'February', 'March']
                ),
                "metric_type": st.column_config.SelectboxColumn(
                    "Metric Type", 
                    width="medium",
                    options=['Planned', 'Booked', 'Billed', 'Forecasted']
                ),
                "value": st.column_config.NumberColumn(
                    "Value", 
                    width="medium", 
                    format="$%.0f", 
                    min_value=0,
                    step=1000
                )
            },
            hide_index=True
        )
        
        # No session state updates to prevent refresh
        
        # Save functionality
        col1, col2 = st.columns([3, 1])
        
        with col1:
            if st.button("💾 Save All Changes to Database", type="primary", use_container_width=True):
                try:
                    # Get fresh IDs from database
                    original_data = unified_db.get_all_data()
                    updates_list = []
                    
                    for idx, row in edited_data.iterrows():
                        if idx < len(original_data):
                            record_id = original_data.iloc[idx]['id']
                            update_data = {col: row[col] for col in edited_data.columns if col != 'id'}
                            updates_list.append({'id': record_id, 'data': update_data})
                    
                    if updates_list:
                        successful_saves = unified_db.bulk_update_records(updates_list)
                        st.success(f"✅ Saved {successful_saves} records to database!")
                        st.balloons()
                    else:
                        st.info("ℹ️ No records to save.")
                except Exception as e:
                    st.error(f"Error during save: {str(e)}")
        
        with col2:
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                st.rerun()
        
        # Export functionality
        st.write("---")
        st.subheader("📤 Export Data")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📊 Export to CSV", use_container_width=True):
                csv_data = edited_data.to_csv(index=False)
                st.download_button(
                    label="💾 Download CSV",
                    data=csv_data,
                    file_name=f"sales_data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col2:
            st.info("💡 Use the form above to edit individual records. Changes are saved immediately to the database.")
        
        # Optional raw data view
        if st.checkbox("Show Raw Database Data"):
            raw_data = unified_db.get_all_data()
            st.subheader("📋 Raw Database Records")
            st.dataframe(raw_data, use_container_width=True, height=300)
    
    except Exception as e:
        st.error(f"Error creating editable view: {str(e)}")
        st.dataframe(editable_data.head(20) if 'editable_data' in locals() else pd.DataFrame(), use_container_width=True)

def target_setting_page():
    """Target Setting page with View/Edit mode and Annual Target and Quarterly breakdown"""
    st.subheader("🎯 Target Setting")
    
    # Initialize the target database manager
    unified_db = UnifiedDataManager()
    
    try:
        # Create targets table if it doesn't exist
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS annual_targets (
                id SERIAL PRIMARY KEY,
                year INTEGER UNIQUE,
                annual_target REAL,
                q1_target REAL DEFAULT 0,
                q2_target REAL DEFAULT 0,
                q3_target REAL DEFAULT 0,
                q4_target REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create owner targets table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owner_targets (
                id SERIAL PRIMARY KEY,
                year INTEGER,
                owner_name TEXT,
                quarter TEXT,
                target_amount REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(year, owner_name, quarter)
            )
        """)
        
        # Create owner target splits table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owner_target_splits (
                id SERIAL PRIMARY KEY,
                owner_target_id INTEGER,
                split_type TEXT,  -- 'Market' or 'LoB'
                split_name TEXT,
                split_amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (owner_target_id) REFERENCES owner_targets (id)
            )
        """)
        conn.commit()
        conn.close()
        
        # Get current year
        current_year = datetime.now().year
        
        # Year selection and edit mode toggle
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            year_input = st.selectbox(
                "Year:",
                options=list(range(current_year - 2, current_year + 3)),
                index=2,  # Default to current year
                key="target_year_select"
            )
        
        # Load existing data for selected year
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cursor = conn.cursor()
        annual_table = env_manager.get_table_name('annual_targets')
        cursor.execute(f"SELECT * FROM {annual_table} WHERE year = %s", (year_input,))
        existing_data = cursor.fetchone()
        conn.close()
        
        # Initialize form values and check if data exists
        if existing_data:
            annual_target = existing_data[2] if existing_data[2] else 0.0
            q1_target = existing_data[3] if existing_data[3] else 0.0
            q2_target = existing_data[4] if existing_data[4] else 0.0
            q3_target = existing_data[5] if existing_data[5] else 0.0
            q4_target = existing_data[6] if existing_data[6] else 0.0
            has_saved_data = True
        else:
            annual_target = 0.0
            q1_target = 0.0
            q2_target = 0.0
            q3_target = 0.0
            q4_target = 0.0
            has_saved_data = False
        
        # Initialize edit mode in session state
        if 'edit_mode' not in st.session_state:
            st.session_state.edit_mode = not has_saved_data  # Edit mode if no saved data
        
        # Edit mode toggle
        if has_saved_data:
            with col3:
                if st.button("✏️ Edit" if not st.session_state.edit_mode else "👁️ View", 
                           use_container_width=True):
                    st.session_state.edit_mode = not st.session_state.edit_mode
                    st.rerun()
        
        # Display mode based on edit_mode and saved data
        if has_saved_data and not st.session_state.edit_mode:
            # VIEW MODE - Show compact summary
            display_target_view_mode(year_input, annual_target, q1_target, q2_target, q3_target, q4_target, unified_db)
        else:
            # EDIT MODE - Show full editing interface
            display_target_edit_mode(year_input, annual_target, q1_target, q2_target, q3_target, q4_target, unified_db, current_year)
        
    except Exception as e:
        st.error(f"Error in target setting page: {str(e)}")

def display_target_view_mode(year, annual_target, q1_target, q2_target, q3_target, q4_target, unified_db):
    """Display targets in compact view mode"""
    
    # Calculate balance
    total_quarterly = q1_target + q2_target + q3_target + q4_target
    balance = annual_target - total_quarterly
    
    # Compact summary with smaller fonts
    st.markdown(f"<h4>📊 FY {year} Target Summary</h4>", unsafe_allow_html=True)
    
    # Metrics in smaller format
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown(f"<div style='text-align: center;'><b>Annual</b><br/>${annual_target:,.0f}</div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div style='text-align: center;'><b>Q1</b><br/>${q1_target:,.0f}</div>", unsafe_allow_html=True)
    with col3:
        st.markdown(f"<div style='text-align: center;'><b>Q2</b><br/>${q2_target:,.0f}</div>", unsafe_allow_html=True)
    with col4:
        st.markdown(f"<div style='text-align: center;'><b>Q3</b><br/>${q3_target:,.0f}</div>", unsafe_allow_html=True)
    with col5:
        st.markdown(f"<div style='text-align: center;'><b>Q4</b><br/>${q4_target:,.0f}</div>", unsafe_allow_html=True)
    
    # Balance status
    if abs(balance) < 0.01:
        st.markdown("<p style='color: green; font-size: 14px;'>✅ Targets are perfectly balanced</p>", unsafe_allow_html=True)
    else:
        st.markdown(f"<p style='color: orange; font-size: 14px;'>⚠️ Balance: ${balance:,.2f}</p>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Show owner targets
    display_owner_targets_view(year, unified_db, q1_target, q2_target, q3_target, q4_target)

def display_target_edit_mode(year, annual_target, q1_target, q2_target, q3_target, q4_target, unified_db, current_year):
    """Display targets in full edit mode"""
    
    st.info(f"💰 Setting targets for Financial Year {year}")
    
    # Annual Target Section
    st.subheader("📊 Annual Target")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        annual_target_input = st.number_input(
            "Annual Target Amount",
            min_value=0.0,
            value=float(annual_target),
            step=10000.0,
            format="%.2f",
            key="annual_target"
        )
    
    with col2:
        st.markdown("**Currency: USD**")
        st.markdown(f"**${annual_target_input:,.2f}**")
    
    st.markdown("---")
    
    # Quarterly Breakdown Section
    st.subheader("📅 Quarterly Target Breakdown")
    
    # Create 4 columns for quarters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("**Q1 Target**")
        q1_input = st.number_input(
            "Q1 Amount (USD)",
            min_value=0.0,
            value=float(q1_target),
            step=1000.0,
            format="%.2f",
            key="q1_target"
        )
        st.markdown(f"**${q1_input:,.2f}**")
    
    with col2:
        st.markdown("**Q2 Target**")
        q2_input = st.number_input(
            "Q2 Amount (USD)",
            min_value=0.0,
            value=float(q2_target),
            step=1000.0,
            format="%.2f",
            key="q2_target"
        )
        st.markdown(f"**${q2_input:,.2f}**")
    
    with col3:
        st.markdown("**Q3 Target**")
        q3_input = st.number_input(
            "Q3 Amount (USD)",
            min_value=0.0,
            value=float(q3_target),
            step=1000.0,
            format="%.2f",
            key="q3_target"
        )
        st.markdown(f"**${q3_input:,.2f}**")
    
    with col4:
        st.markdown("**Q4 Target**")
        q4_input = st.number_input(
            "Q4 Amount (USD)",
            min_value=0.0,
            value=float(q4_target),
            step=1000.0,
            format="%.2f",
            key="q4_target"
        )
        st.markdown(f"**${q4_input:,.2f}**")
    
    # Calculate balance
    total_quarterly = q1_input + q2_input + q3_input + q4_input
    balance = annual_target_input - total_quarterly
    
    # Show balance message
    st.markdown("---")
    if abs(balance) < 0.01:  # Small threshold for floating point comparison
        st.success("**Target is perfectly balanced**")
    else:
        if balance > 0:
            st.warning(f"⚠️ **Balance Pending: ${balance:,.2f}**")
            st.markdown("*You still have target amount to allocate across quarters*")
        else:
            st.error(f"❌ **Balance Pending: ${abs(balance):,.2f}**")
            st.markdown("*Quarterly targets exceed annual target*")
    
    # Summary section
    st.markdown("---")
    st.subheader("📈 Target Summary")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Annual Target", f"${annual_target_input:,.2f}")
    with col2:
        st.metric("Total Quarterly", f"${total_quarterly:,.2f}")
    with col3:
        if abs(balance) > 0.01:
            st.metric("Balance", f"${balance:,.2f}", delta=f"{balance:+,.2f}")
        else:
            st.metric("Balance", "$0.00", delta="Balanced ✅")
    
    # Check if values have changed to show save button
    values_changed = (
        annual_target_input != annual_target or
        q1_input != q1_target or
        q2_input != q2_target or
        q3_input != q3_target or
        q4_input != q4_target
    )
    
    # Save button (only show if values changed)
    if values_changed:
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if st.button("💾 Save Targets", type="primary", use_container_width=True):
                try:
                    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                    cursor = conn.cursor()
                    
                    # Check if record exists for selected year
                    annual_table = env_manager.get_table_name('annual_targets')
                    cursor.execute(f"SELECT id FROM {annual_table} WHERE year = %s", (year,))
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        # Update existing record
                        cursor.execute(f"""
                            UPDATE {annual_table} 
                            SET annual_target = %s, q1_target = %s, q2_target = %s, q3_target = %s, q4_target = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE year = %s
                        """, (annual_target_input, q1_input, q2_input, q3_input, q4_input, year))
                    else:
                        # Insert new record
                        cursor.execute(f"""
                            INSERT INTO {annual_table} (year, annual_target, q1_target, q2_target, q3_target, q4_target)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (year, annual_target_input, q1_input, q2_input, q3_input, q4_input))
                    
                    conn.commit()
                    conn.close()
                    
                    st.success(f"✅ Targets saved successfully for {year}!")
                    st.balloons()
                    
                    # Switch to view mode and trigger refresh
                    st.session_state.edit_mode = False
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error saving targets: {str(e)}")
        
        with col2:
            if st.button("🔄 Reset", type="secondary", use_container_width=True):
                st.rerun()
    
    # Always show owner targets section
    st.markdown("---")
    display_owner_targets_edit(year, unified_db, q1_input, q2_input, q3_input, q4_input)

def display_owner_targets_view(year, unified_db, q1_target, q2_target, q3_target, q4_target):
    """Display owner targets in view mode"""
    
    st.markdown(f"<h4>📊 Individual Performance</h4>", unsafe_allow_html=True)
    
    # Load existing owner targets and billing data from database
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        owner_targets_table = env_manager.get_table_name('owner_targets')
        existing_owner_targets = pd.read_sql_query(f"""
            SELECT owner_name, quarter, target_amount 
            FROM {owner_targets_table} 
            WHERE year = %s
        """, conn, params=[year])
        
        # Load billing data from unified_sales_data
        unified_sales_data_table = env_manager.get_table_name('unified_sales_data')
        billing_data = pd.read_sql_query(f"""
            SELECT owner, 
                   CASE 
                       WHEN month_number IN (4,5,6) THEN 'Q1'
                       WHEN month_number IN (7,8,9) THEN 'Q2'
                       WHEN month_number IN (10,11,12) THEN 'Q3'
                       WHEN month_number IN (1,2,3) THEN 'Q4'
                   END as quarter,
                   SUM(CASE WHEN metric_type = 'Billed' THEN value ELSE 0 END) as total_billing
            FROM {unified_sales_data_table} 
            WHERE year IN (%s, %s)
                  AND owner IS NOT NULL 
                  AND owner != ''
            GROUP BY owner, quarter
        """, conn, params=[year, year+1])
        conn.close()
            
    except Exception as e:
        st.error(f"Error loading performance data: {str(e)}")
        existing_owner_targets = pd.DataFrame()
        billing_data = pd.DataFrame()
    
    # Show compact view of owner performance in specific order
    specific_owners = ["Madhu", "Avinash", "AP"]
    
    # Get owners that have either targets or billing data
    owners_with_targets = existing_owner_targets['owner_name'].unique() if not existing_owner_targets.empty else []
    owners_with_billing = billing_data['owner'].unique() if not billing_data.empty else []
    all_relevant_owners = set(list(owners_with_targets) + list(owners_with_billing))
    
    available_owners = [owner for owner in specific_owners if owner in all_relevant_owners]
    
    if available_owners:
        for owner in available_owners:
            with st.expander(f"👤 {owner}", expanded=False):
                owner_data = existing_owner_targets[existing_owner_targets['owner_name'] == owner]
                
                # Create table-like layout with row headings
                # Header row with quarters
                cols = st.columns([1, 1, 1, 1, 1])  # First column for row labels
                cols[0].markdown("")  # Empty for alignment
                cols[1].markdown("**Q1**")
                cols[2].markdown("**Q2**") 
                cols[3].markdown("**Q3**")
                cols[4].markdown("**Q4**")
                
                # Target row
                cols = st.columns([1, 1, 1, 1, 1])
                cols[0].markdown("**Target**")
                
                for i, quarter in enumerate(['Q1', 'Q2', 'Q3', 'Q4']):
                    quarter_data = owner_data[owner_data['quarter'] == quarter]
                    target_amount = quarter_data['target_amount'].iloc[0] if not quarter_data.empty else 0
                    cols[i+1].markdown(f"<div style='text-align: center; color: #ffffff; font-weight: bold; font-size: 18px; background-color: #333333; padding: 5px; border-radius: 4px;'>${target_amount:,.0f}</div>", unsafe_allow_html=True)
                
                # Total Billing row
                cols = st.columns([1, 1, 1, 1, 1])
                cols[0].markdown("**Total Billing**")
                
                for i, quarter in enumerate(['Q1', 'Q2', 'Q3', 'Q4']):
                    quarter_data = owner_data[owner_data['quarter'] == quarter]
                    target_amount = quarter_data['target_amount'].iloc[0] if not quarter_data.empty else 0
                    
                    # Get billing data for this owner and quarter
                    billing_amount = 0
                    if not billing_data.empty:
                        billing_row = billing_data[
                            (billing_data['owner'] == owner) & 
                            (billing_data['quarter'] == quarter)
                        ]
                        billing_amount = billing_row['total_billing'].iloc[0] if not billing_row.empty else 0
                    
                    # Determine color based on billing vs target comparison
                    if billing_amount == 0:
                        billing_color = "black"
                        billing_display = "0"
                    elif billing_amount >= target_amount:
                        billing_color = "green"
                        billing_display = f"${billing_amount:,.0f}"
                    else:
                        billing_color = "red"
                        billing_display = f"${billing_amount:,.0f}"
                    
                    # Style billing with color-coded background
                    if billing_color == "green":
                        bg_color = "#28a745"  # Green background
                        text_color = "#ffffff"
                    else:  # Both red and zero (no billing) show as red
                        bg_color = "#dc3545"  # Red background for below target or zero
                        text_color = "#ffffff"
                    
                    cols[i+1].markdown(f"<div style='text-align: center; color: {text_color}; font-weight: bold; font-size: 18px; background-color: {bg_color}; padding: 5px; border-radius: 4px;'>{billing_display}</div>", unsafe_allow_html=True)
    else:
        st.info("No individual performance data available yet. Set up targets in edit mode or ensure billing data exists.")

def display_owner_targets_edit(year, unified_db, q1_target, q2_target, q3_target, q4_target):
    """Display owner targets in edit mode"""
    
    st.header("👥 Individual Owner Plans")
    st.info("💡 Allocate quarterly targets to individual owners and split by Market/LoB")
    
    # Load existing owner targets from database
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        existing_owner_targets = pd.read_sql_query("""
            SELECT owner_name, quarter, target_amount 
            FROM f"{env_manager.get_table_name('owner_targets')}" 
            WHERE year = %s
        """, conn, params=[year])
        conn.close()
    except:
        existing_owner_targets = pd.DataFrame()
    
    # Define quarters with their targets
    quarters = [("Q1", q1_target), ("Q2", q2_target), ("Q3", q3_target), ("Q4", q4_target)]
    
    # Function to calculate used targets for a quarter
    def get_used_target(quarter):
        if not existing_owner_targets.empty:
            quarter_data = existing_owner_targets[existing_owner_targets['quarter'] == quarter]
            return quarter_data['target_amount'].sum()
        return 0.0
    
    # Display target availability for each quarter
    st.subheader("📊 Target Availability by Quarter")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        used_q1 = get_used_target("Q1")
        balance_q1 = q1_target - used_q1
        if balance_q1 > 0:
            st.markdown(f"<div style='color: red;'><b>Q1 Balance Pending: ${balance_q1:,.2f}</b></div>", unsafe_allow_html=True)
        elif balance_q1 == 0:
            st.markdown(f"<div style='color: green;'><b>Q1 Target versus allocated is well balanced</b></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div style='color: orange;'><b>Q1 Over-allocated by: ${abs(balance_q1):,.2f}</b></div>", unsafe_allow_html=True)
        
    with col2:
        used_q2 = get_used_target("Q2")
        balance_q2 = q2_target - used_q2
        if balance_q2 > 0:
            st.markdown(f"<div style='color: red;'><b>Q2 Balance Pending: ${balance_q2:,.2f}</b></div>", unsafe_allow_html=True)
        elif balance_q2 == 0:
            st.markdown(f"<div style='color: green;'><b>Q2 Target versus allocated is well balanced</b></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div style='color: orange;'><b>Q2 Over-allocated by: ${abs(balance_q2):,.2f}</b></div>", unsafe_allow_html=True)
        
    with col3:
        used_q3 = get_used_target("Q3")
        balance_q3 = q3_target - used_q3
        if balance_q3 > 0:
            st.markdown(f"<div style='color: red;'><b>Q3 Balance Pending: ${balance_q3:,.2f}</b></div>", unsafe_allow_html=True)
        elif balance_q3 == 0:
            st.markdown(f"<div style='color: green;'><b>Q3 Target versus allocated is well balanced</b></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div style='color: orange;'><b>Q3 Over-allocated by: ${abs(balance_q3):,.2f}</b></div>", unsafe_allow_html=True)
        
    with col4:
        used_q4 = get_used_target("Q4")
        balance_q4 = q4_target - used_q4
        if balance_q4 > 0:
            st.markdown(f"<div style='color: red;'><b>Q4 Balance Pending: ${balance_q4:,.2f}</b></div>", unsafe_allow_html=True)
        elif balance_q4 == 0:
            st.markdown(f"<div style='color: green;'><b>Q4 Target versus allocated is well balanced</b></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div style='color: orange;'><b>Q4 Over-allocated by: ${abs(balance_q4):,.2f}</b></div>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Get unique owners from database
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        unified_sales_data_table = env_manager.get_table_name('unified_sales_data')
        owners_df = pd.read_sql_query(f"SELECT DISTINCT owner FROM {unified_sales_data_table} WHERE owner IS NOT NULL AND owner != ''", conn)
        available_owners = sorted(owners_df['owner'].tolist()) if not owners_df.empty else ["Owner 1", "Owner 2", "Owner 3"]
        conn.close()
    except:
        available_owners = ["Owner 1", "Owner 2", "Owner 3"]
    
    # Get LoB, Region and Source data for splits
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        unified_sales_data_table = env_manager.get_table_name('unified_sales_data')
        lob_df = pd.read_sql_query(f"SELECT DISTINCT lob FROM {unified_sales_data_table} WHERE lob IS NOT NULL AND lob != ''", conn)
        region_df = pd.read_sql_query(f"SELECT DISTINCT region FROM {unified_sales_data_table} WHERE region IS NOT NULL AND region != ''", conn)
        source_df = pd.read_sql_query(f"SELECT DISTINCT source FROM {unified_sales_data_table} WHERE source IS NOT NULL AND source != ''", conn)
        available_lobs = sorted(lob_df['lob'].tolist()) if not lob_df.empty else []
        available_regions = sorted(region_df['region'].tolist()) if not region_df.empty else []
        available_sources = sorted(source_df['source'].tolist()) if not source_df.empty else []
        conn.close()
    except:
        available_lobs = []
        available_regions = []
        available_sources = []
    

    
    # Owner Cards - 3 columns layout with specific owners
    st.subheader("📊 Individual Performance")
    
    # Enhanced Save All Changes button with professional styling
    st.markdown("""
    <div style="text-align: center; margin: 25px 0;">
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("💾 Save All Owner Changes", type="primary", use_container_width=True):
        changes_saved = 0
        debug_info = []
        try:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cursor = conn.cursor()
            
            # Save changes for all three owners
            specific_owners = ["Madhu", "Avinash", "AP"]
            for owner_idx, owner_name in enumerate(specific_owners):
                for quarter, _ in quarters:
                    target_key = f"target_{owner_name}_{quarter}_{owner_idx}"
                    splits_key = f"quarter_splits_{owner_name}_{quarter}_{owner_idx}"
                    
                    debug_info.append(f"Checking {owner_name} {quarter}: target_key={target_key}, splits_key={splits_key}")
                    
                    if target_key in st.session_state and st.session_state[target_key] > 0:
                        target_amount = st.session_state[target_key]
                        debug_info.append(f"Target amount for {owner_name} {quarter}: ${target_amount}")
                        
                        # Save owner target
                        cursor.execute("""
                            INSERT INTO f"{env_manager.get_table_name('owner_targets')}" (owner_name, quarter, year, target_amount)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (owner_name, quarter, year) DO UPDATE SET target_amount = %s
                        """, (owner_name, quarter, year, target_amount, target_amount))
                        
                        # Get owner_target_id
                        cursor.execute("""
                            SELECT id FROM f"{env_manager.get_table_name('owner_targets')}" 
                            WHERE owner_name = %s AND quarter = %s AND year = %s
                        """, (owner_name, quarter, year))
                        owner_target_id = cursor.fetchone()[0]
                        debug_info.append(f"Owner target ID: {owner_target_id}")
                        
                        # Delete existing splits
                        cursor.execute("""
                            DELETE FROM owner_target_splits WHERE owner_target_id = %s
                        """, (owner_target_id,))
                        
                        # Save splits
                        if splits_key in st.session_state and st.session_state[splits_key]:
                            debug_info.append(f"Saving {len(st.session_state[splits_key])} splits for {owner_name} {quarter}")
                            for split in st.session_state[splits_key]:
                                debug_info.append(f"Split: {split}")
                                cursor.execute("""
                                    INSERT INTO owner_target_splits (owner_target_id, split_type, split_name, split_amount)
                                    VALUES (%s, %s, %s, %s)
                                """, (owner_target_id, split['split_type'], split['split_name'], split['split_amount']))
                        else:
                            debug_info.append(f"No splits found for {owner_name} {quarter}")
                        
                        changes_saved += 1
            
            conn.commit()
            conn.close()
            
            if changes_saved > 0:
                st.success(f"✅ All owner targets saved successfully!")
                with st.expander("🔍 Debug Info", expanded=False):
                    for info in debug_info:
                        st.text(info)
                st.balloons()
                # Clear session state to force reload of data
                for key in list(st.session_state.keys()):
                    if 'quarter_splits_' in key:
                        del st.session_state[key]
                st.rerun()
            else:
                st.warning("No changes to save")
                with st.expander("🔍 Debug Info", expanded=True):
                    for info in debug_info:
                        st.text(info)
                
        except Exception as e:
            st.error(f"Error saving: {str(e)}")
    
    # Clean section divider
    st.markdown("""
    <hr style="margin: 40px 0; border: none; height: 2px; 
               background: linear-gradient(to right, #667eea, #764ba2);">
    """, unsafe_allow_html=True)
    
    # Create clean 3-column layout with proper spacing
    owner_cols = st.columns(3, gap="large")
    specific_owners = ["Madhu", "Avinash", "AP"]
    
    for owner_idx in range(3):
        with owner_cols[owner_idx]:
            # Fixed owner names in specific order
            selected_owner = specific_owners[owner_idx]
            
            if selected_owner:
                # Elegant owner header with professional gradient styling
                st.markdown(f"""
                <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                           padding: 24px; border-radius: 15px; margin-bottom: 25px; 
                           box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3);
                           border: 1px solid rgba(255, 255, 255, 0.2);'>
                    <h3 style='color: white; margin: 0; font-weight: 600; text-align: center;
                              font-size: 1.4rem; letter-spacing: 0.5px;'>
                        {selected_owner}
                    </h3>
                </div>
                """, unsafe_allow_html=True)
                
                # Create elegant quarter cards with professional styling
                for quarter, max_target in quarters:
                    # Professional quarter card styling
                    st.markdown(f"""
                    <div style='background: linear-gradient(145deg, #f8f9fa, #e9ecef); 
                               padding: 18px; border-radius: 12px; margin-bottom: 20px;
                               box-shadow: 0 4px 20px rgba(0,0,0,0.08);
                               border: 1px solid rgba(0,0,0,0.05);'>
                        <h4 style='color: #495057; margin: 0 0 15px 0; font-weight: 600;
                                  text-align: center; font-size: 1.1rem;'>
                            {quarter}
                        </h4>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.container():
                        
                        # Get existing target for this owner/quarter
                        existing_target = 0.0
                        if not existing_owner_targets.empty:
                            existing_data = existing_owner_targets[
                                (existing_owner_targets['owner_name'] == selected_owner) & 
                                (existing_owner_targets['quarter'] == quarter)
                            ]
                            if not existing_data.empty:
                                existing_target = existing_data['target_amount'].iloc[0]
                        
                        # Target input
                        target_input = st.number_input(
                            f"Target Amount",
                            min_value=0.0,
                            max_value=max_target,
                            value=float(existing_target),
                            step=1000.0,
                            format="%.2f",
                            key=f"target_{selected_owner}_{quarter}_{owner_idx}",
                            label_visibility="collapsed"
                        )
                        
                        # Define quarter_splits_key early to avoid unbound errors
                        quarter_splits_key = f"splits_{selected_owner}_{quarter}"
                        
                        # Show balance if target > 0
                        if target_input > 0:
                            st.caption(f"Assigned: ${target_input:,.2f}")
                            
                            # Initialize splits
                            if quarter_splits_key not in st.session_state:
                                st.session_state[quarter_splits_key] = []
                                # Load from database
                                try:
                                    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                                    existing_splits = pd.read_sql_query("""
                                        SELECT ots.id, ots.split_type, ots.split_name, ots.split_amount
                                        FROM owner_target_splits ots
                                        JOIN f"{env_manager.get_table_name('owner_targets')}" ot ON ots.owner_target_id = ot.id
                                        WHERE ot.owner_name = %s AND ot.quarter = %s AND ot.year = %s
                                    """, conn, params=[selected_owner, quarter, year])
                                    conn.close()
                                    if not existing_splits.empty:
                                        st.session_state[quarter_splits_key] = existing_splits.to_dict('records')
                                except:
                                    pass
                            
                            # Calculate balance
                            total_splits = sum(float(s['split_amount']) for s in st.session_state[quarter_splits_key])
                            balance = float(target_input) - total_splits
                            
                            if abs(balance) < 0.01:
                                st.caption("✅ Well balanced!")
                            elif balance > 0:
                                st.caption(f"⚖️ Balance: ${balance:,.2f}")
                            else:
                                st.caption(f"⚠️ Over by: ${abs(balance):,.2f}")
                        
                        # Split section under each quarter
                        if target_input > 0:
                            with st.expander(f"🎯 Split {quarter} Target", expanded=False):
                                # Use consistent session state key
                                current_splits = st.session_state.get(quarter_splits_key, [])
                                total_splits = sum(float(s['split_amount']) for s in current_splits)
                                
                                # Display current splits
                                if current_splits:
                                    st.markdown("**Current Splits:**")
                                    
                                    # Create splits table with delete functionality
                                    for i, split in enumerate(current_splits):
                                        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                                        with col1:
                                            st.text(split['split_type'])
                                        with col2:
                                            st.text(split['split_name'])
                                        with col3:
                                            st.text(f"${split['split_amount']:,.2f}")
                                        with col4:
                                            # Create unique delete button key
                                            delete_btn_key = f"del_btn_{quarter_splits_key}_{i}"
                                            if st.button("🗑️", key=delete_btn_key):
                                                # Delete from database immediately
                                                try:
                                                    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                                                    cursor = conn.cursor()
                                                    
                                                    # Get owner_target_id
                                                    cursor.execute("""
                                                        SELECT id FROM f"{env_manager.get_table_name('owner_targets')}" 
                                                        WHERE owner_name = %s AND quarter = %s AND year = %s
                                                    """, (selected_owner, quarter, year))
                                                    result = cursor.fetchone()
                                                    
                                                    if result:
                                                        # Delete by split ID if available, otherwise use combination
                                                        if 'id' in split:
                                                            cursor.execute("""
                                                                DELETE FROM owner_target_splits WHERE id = %s
                                                            """, (split['id'],))
                                                        else:
                                                            owner_target_id = result[0]
                                                            cursor.execute("""
                                                                DELETE FROM owner_target_splits 
                                                                WHERE owner_target_id = %s AND split_type = %s AND split_name = %s AND split_amount = %s
                                                            """, (owner_target_id, split['split_type'], split['split_name'], split['split_amount']))
                                                        
                                                        conn.commit()
                                                    conn.close()
                                                except Exception as e:
                                                    st.error(f"Error deleting split: {str(e)}")
                                                
                                                # Remove from session state
                                                st.session_state[quarter_splits_key].pop(i)
                                                st.success(f"✅ Deleted: {split['split_type']} - {split['split_name']}")
                                                st.rerun()
                                    

                                    
                                    # Show totals and balance
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.metric("Total Split", f"${total_splits:,.2f}")
                                    with col2:
                                        balance = float(target_input) - total_splits
                                        if abs(balance) < 0.01:
                                            st.success("✅ Well balanced!")
                                        elif balance > 0:
                                            st.metric("Balance", f"${balance:,.2f}")
                                        else:
                                            st.error(f"Over: ${abs(balance):,.2f}")
                                    
                                    # Clear all button
                                    if st.button(f"Clear All Splits", key=f"clear_{quarter_splits_key}"):
                                        # Clear from database
                                        try:
                                            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                                            cursor = conn.cursor()
                                            
                                            # Get owner_target_id and clear all splits
                                            cursor.execute("""
                                                SELECT id FROM f"{env_manager.get_table_name('owner_targets')}" 
                                                WHERE owner_name = %s AND quarter = %s AND year = %s
                                            """, (selected_owner, quarter, year))
                                            result = cursor.fetchone()
                                            
                                            if result:
                                                owner_target_id = result[0]
                                                cursor.execute("""
                                                    DELETE FROM owner_target_splits WHERE owner_target_id = %s
                                                """, (owner_target_id,))
                                                conn.commit()
                                            
                                            conn.close()
                                        except Exception as e:
                                            st.error(f"Error clearing splits: {str(e)}")
                                        
                                        # Clear from session state
                                        st.session_state[quarter_splits_key] = []
                                        st.success("✅ Cleared all splits")
                                        st.rerun()
                                    
                                    st.markdown("---")
                                
                                # Add new split form (only if under target)
                                if total_splits < target_input:
                                    st.markdown("**Add New Split:**")
                                    
                                    # First row - Quarter, Type, Value
                                    col1, col2, col3 = st.columns([1, 2, 2])
                                    
                                    with col1:
                                        st.markdown(f"**{quarter}**")
                                    
                                    with col2:
                                        split_type = st.selectbox(
                                            "Type:",
                                            ["", "LoB", "Region", "Source"],
                                            key=f"type_{quarter_splits_key}"
                                        )
                                    
                                    with col3:
                                        if split_type == "LoB":
                                            split_name = st.selectbox("Value:", [""] + available_lobs, key=f"name_{quarter_splits_key}")
                                        elif split_type == "Region":
                                            split_name = st.selectbox("Value:", [""] + available_regions, key=f"name_{quarter_splits_key}")
                                        elif split_type == "Source":
                                            split_name = st.selectbox("Value:", [""] + available_sources, key=f"name_{quarter_splits_key}")
                                        else:
                                            split_name = ""
                                    
                                    # Second row - Amount field (more visible)
                                    st.markdown("**Amount:**")
                                    split_amount = st.number_input(
                                        "Enter split amount:",
                                        min_value=0.0,
                                        max_value=float(target_input - total_splits),
                                        value=0.0,
                                        step=1000.0,
                                        format="%.2f",
                                        key=f"amount_{quarter_splits_key}",
                                        help=f"Maximum available: ${target_input - total_splits:,.2f}"
                                    )
                                    
                                    if st.button(f"Add Split", key=f"add_{quarter_splits_key}"):
                                        if split_type and split_name and split_amount > 0:
                                            # Add split to session state
                                            new_split = {
                                                'split_type': split_type,
                                                'split_name': split_name,
                                                'split_amount': split_amount
                                            }
                                            st.session_state[quarter_splits_key].append(new_split)
                                            
                                            # Save to database immediately
                                            try:
                                                conn = psycopg2.connect(os.getenv("DATABASE_URL"))
                                                cursor = conn.cursor()
                                                
                                                # Get or create owner_target record
                                                cursor.execute("""
                                                    INSERT INTO f"{env_manager.get_table_name('owner_targets')}" (owner_name, quarter, year, target_amount)
                                                    VALUES (%s, %s, %s, %s)
                                                    ON CONFLICT (owner_name, quarter, year) DO UPDATE SET target_amount = %s
                                                """, (selected_owner, quarter, year, target_input, target_input))
                                                
                                                cursor.execute("""
                                                    SELECT id FROM f"{env_manager.get_table_name('owner_targets')}" 
                                                    WHERE owner_name = %s AND quarter = %s AND year = %s
                                                """, (selected_owner, quarter, year))
                                                owner_target_id = cursor.fetchone()[0]
                                                
                                                # Insert split record
                                                cursor.execute("""
                                                    INSERT INTO owner_target_splits (owner_target_id, split_type, split_name, split_amount)
                                                    VALUES (%s, %s, %s, %s)
                                                """, (owner_target_id, split_type, split_name, split_amount))
                                                
                                                conn.commit()
                                                conn.close()
                                                
                                                st.success(f"✅ Added: {split_type} - {split_name} = ${split_amount:,.2f}")
                                                st.rerun()
                                            except Exception as e:
                                                st.error(f"Error saving split: {str(e)}")
                                        else:
                                            st.error("Please fill all fields")
                                

                                

                        
                        st.markdown("---")

# Legacy functions - keeping for reference but not used in new app structure
def data_upload_section():
    st.header("📁 Data Upload & Management")
    
    # Database connection status
    db_status = st.session_state.db_manager.check_connection()
    if db_status:
        st.success("🔗 Database connected successfully")
    else:
        st.error("❌ Database connection failed")
        return
    
    # Create tabs for upload and manage
    upload_tab, manage_tab = st.tabs(["Upload New Data", "Manage Existing Data"])
    
    with upload_tab:
        st.markdown("""
        Upload your historical sales/demand data in CSV format. The data should contain:
        - **Date column**: Time series data (daily, weekly, or monthly)
        - **Demand/Sales column**: Numerical values representing demand or sales
        - **Optional**: Additional columns for product categories, regions, etc.
        """)
        
        # Dataset name input
        dataset_name = st.text_input(
            "Dataset Name",
            value=f"Dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            help="Enter a name for this dataset"
        )
        
        # File upload
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type="csv",
            help="Upload your historical demand/sales data"
        )
        
        if uploaded_file is not None:
            try:
                with st.spinner("Processing uploaded file..."):
                    # Read and process data
                    processor = DataProcessor()
                    data = processor.load_data(uploaded_file)
                
                if data is not None:
                    st.success("✅ File processed successfully!")
                    
                    # Data preview
                    st.subheader("📊 Data Preview")
                    st.dataframe(data.head(10), use_container_width=True)
                    
                    # Data summary
                    st.subheader("📈 Data Summary")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Records", len(data))
                    with col2:
                        try:
                            date_range = f"{data.index.min().strftime('%Y-%m-%d')} to {data.index.max().strftime('%Y-%m-%d')}"
                        except:
                            date_range = "Date range unavailable"
                        st.metric("Date Range", date_range)
                    with col3:
                        st.metric("Frequency", processor.detect_frequency(data))
                    
                    # Column configuration
                    st.subheader("⚙️ Column Configuration")
                    columns = data.columns.tolist()
                    
                    if len(columns) == 0:
                        st.error("No columns found in the data")
                        return
                    
                    demand_column = st.selectbox(
                        "Select Demand/Sales Column",
                        columns,
                        help="Choose the column containing demand or sales values"
                    )
                    
                    # Optional grouping columns
                    available_grouping_columns = [col for col in columns if col != demand_column]
                    grouping_columns = st.multiselect(
                        "Select Grouping Columns (Optional)",
                        available_grouping_columns,
                        help="Select columns for grouping (e.g., product, region)"
                    )
                    
                    # Show selected configuration
                    st.info(f"Selected demand column: **{demand_column}**")
                    if grouping_columns:
                        st.info(f"Selected grouping columns: **{', '.join(grouping_columns)}**")
                    
                    # Validate and store data
                    if st.button("Validate & Save to Database", type="primary"):
                        with st.spinner("Validating and saving data..."):
                            validated_data = processor.validate_data(data, demand_column, grouping_columns)
                            if validated_data is not None:
                                # Save to database
                                dataset_id = st.session_state.db_manager.save_dataset(
                                    validated_data, 
                                    dataset_name, 
                                    "date",  # date column name
                                    demand_column, 
                                    grouping_columns
                                )
                                
                                if dataset_id:
                                    st.session_state.data = validated_data
                                    st.session_state.demand_column = demand_column
                                    st.session_state.grouping_columns = grouping_columns
                                    st.session_state.current_dataset_id = dataset_id
                                    st.success(f"✅ Data validated and saved to database! Dataset ID: {dataset_id}")
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error("❌ Failed to save data to database")
                            else:
                                st.error("❌ Data validation failed")
                else:
                    st.error("❌ Failed to process the uploaded file")
                    
            except Exception as e:
                st.error(f"❌ Error processing file: {str(e)}")
                st.expander("Error Details").write(str(e))
    
    with manage_tab:
        st.subheader("📋 Existing Datasets")
        
        # Refresh button
        if st.button("🔄 Refresh Datasets", help="Refresh the list of datasets from database"):
            st.rerun()
        
        # Get datasets from database
        datasets = st.session_state.db_manager.get_datasets()
        
        if datasets:
            st.success(f"Found {len(datasets)} dataset(s) in database")
            
            # Display datasets in a table
            dataset_df = pd.DataFrame([{
                'ID': d['id'],
                'Name': d['name'],
                'Records': d['total_records'],
                'Frequency': d['frequency'],
                'Upload Time': d['upload_time'].strftime('%Y-%m-%d %H:%M') if d['upload_time'] else 'Unknown'
            } for d in datasets])
            
            st.dataframe(dataset_df, use_container_width=True)
            
            # Dataset selection
            selected_dataset = st.selectbox(
                "Select Dataset to Load",
                [(d['id'], d['name']) for d in datasets],
                format_func=lambda x: f"{x[1]} (ID: {x[0]})"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if selected_dataset and st.button("Load Selected Dataset", type="primary"):
                    with st.spinner("Loading dataset..."):
                        dataset_data = st.session_state.db_manager.load_dataset_data(selected_dataset[0])
                        if dataset_data:
                            st.session_state.data = dataset_data['data']
                            st.session_state.demand_column = dataset_data['demand_column']
                            st.session_state.grouping_columns = dataset_data['grouping_columns']
                            st.session_state.current_dataset_id = selected_dataset[0]
                            st.success(f"✅ Dataset '{dataset_data['dataset_name']}' loaded successfully!")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ Failed to load dataset")
            
            with col2:
                if selected_dataset and st.button("🗑️ Delete Dataset", help="Delete this dataset from database"):
                    st.warning("This will permanently delete the dataset and all associated forecasts and scenarios!")
                    if st.button("Confirm Delete", type="secondary"):
                        # Note: Would implement delete functionality here
                        st.info("Delete functionality will be implemented in future version")
        else:
            st.info("📭 No datasets found in database. Upload your first dataset using the 'Upload New Data' tab.")
            st.markdown("**Steps to get started:**")
            st.markdown("1. Go to 'Upload New Data' tab")
            st.markdown("2. Choose a CSV file with date and demand columns")
            st.markdown("3. Configure columns and save to database")
    
    # Display current data status
    if st.session_state.data is not None:
        st.subheader("Current Dataset Status")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"📊 Dataset loaded with {len(st.session_state.data)} records")
        with col2:
            if st.session_state.current_dataset_id:
                st.info(f"🗄️ Database ID: {st.session_state.current_dataset_id}")

def forecasting_section():
    st.header("🔮 Demand Forecasting")
    
    if st.session_state.data is None:
        st.warning("⚠️ Please upload data first in the Data Upload page.")
        return
    
    # Initialize forecasting engine
    forecasting_engine = ForecastingEngine()
    
    # Forecasting parameters
    st.subheader("Forecasting Parameters")
    
    col1, col2 = st.columns(2)
    
    with col1:
        forecast_periods = st.slider(
            "Forecast Periods",
            min_value=1,
            max_value=365,
            value=30,
            help="Number of periods to forecast"
        )
        
        model_type = st.selectbox(
            "Forecasting Model",
            ["Simple Moving Average", "Exponential Smoothing", "Double Exponential Smoothing", "Triple Exponential Smoothing"],
            help="Select the forecasting model to use"
        )
    
    with col2:
        if model_type == "Simple Moving Average":
            window = st.slider("Moving Average Window", 3, 30, 7)
            model_params = {"window": window}
        elif model_type == "Exponential Smoothing":
            alpha = st.slider("Smoothing Parameter (α)", 0.1, 1.0, 0.3, 0.1)
            model_params = {"alpha": alpha}
        elif model_type == "Double Exponential Smoothing":
            alpha = st.slider("Level Smoothing (α)", 0.1, 1.0, 0.3, 0.1)
            beta = st.slider("Trend Smoothing (β)", 0.1, 1.0, 0.3, 0.1)
            model_params = {"alpha": alpha, "beta": beta}
        else:  # Triple Exponential Smoothing
            alpha = st.slider("Level Smoothing (α)", 0.1, 1.0, 0.3, 0.1)
            beta = st.slider("Trend Smoothing (β)", 0.1, 1.0, 0.3, 0.1)
            gamma = st.slider("Seasonal Smoothing (γ)", 0.1, 1.0, 0.3, 0.1)
            seasonal_periods = st.number_input("Seasonal Periods", 1, 52, 12)
            model_params = {"alpha": alpha, "beta": beta, "gamma": gamma, "seasonal_periods": seasonal_periods}
    
    # Generate forecast
    if st.button("Generate Forecast"):
        with st.spinner("Generating forecast..."):
            try:
                forecast_result = forecasting_engine.generate_forecast(
                    st.session_state.data,
                    st.session_state.demand_column,
                    model_type,
                    forecast_periods,
                    model_params
                )
                
                if forecast_result is not None:
                    # Save to session state
                    forecast_key = f"{model_type}_{datetime.now().strftime('%H%M%S')}"
                    st.session_state.forecasts[forecast_key] = forecast_result
                    
                    # Save to database if we have a current dataset
                    if st.session_state.current_dataset_id:
                        forecast_id = st.session_state.db_manager.save_forecast(
                            st.session_state.current_dataset_id,
                            model_type,
                            model_params,
                            forecast_periods,
                            forecast_result,
                            forecast_result.get('accuracy_metrics')
                        )
                        if forecast_id:
                            st.success(f"✅ Forecast generated and saved to database! Forecast ID: {forecast_id}")
                        else:
                            st.success("✅ Forecast generated successfully!")
                    else:
                        st.success("✅ Forecast generated successfully!")
                    
                    # Display forecast results
                    st.subheader("Forecast Results")
                    
                    # Visualize forecast
                    visualizer = Visualizer()
                    fig = visualizer.plot_forecast(
                        st.session_state.data[st.session_state.demand_column],
                        forecast_result['forecast'],
                        forecast_result.get('confidence_interval')
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Forecast accuracy metrics
                    if 'accuracy_metrics' in forecast_result:
                        st.subheader("Forecast Accuracy Metrics")
                        metrics = forecast_result['accuracy_metrics']
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("MAPE", f"{metrics.get('mape', 0):.2f}%")
                        with col2:
                            st.metric("MAE", f"{metrics.get('mae', 0):.2f}")
                        with col3:
                            st.metric("RMSE", f"{metrics.get('rmse', 0):.2f}")
                        with col4:
                            st.metric("R²", f"{metrics.get('r2', 0):.3f}")
                
            except Exception as e:
                st.error(f"❌ Error generating forecast: {str(e)}")
    
    # Display existing forecasts
    if st.session_state.forecasts:
        st.subheader("Generated Forecasts")
        forecast_names = list(st.session_state.forecasts.keys())
        selected_forecast = st.selectbox("Select Forecast to View", forecast_names)
        
        if selected_forecast:
            forecast_data = st.session_state.forecasts[selected_forecast]
            
            # Show forecast details
            st.write(f"**Model:** {forecast_data['model_type']}")
            st.write(f"**Generated:** {forecast_data['timestamp']}")
            st.write(f"**Forecast Periods:** {len(forecast_data['forecast'])}")

def scenarios_section():
    st.header("🎯 What-If Scenarios")
    
    if st.session_state.data is None:
        st.warning("⚠️ Please upload data first in the Data Upload page.")
        return
    
    if not st.session_state.forecasts:
        st.warning("⚠️ Please generate a forecast first in the Forecasting page.")
        return
    
    # Initialize scenario builder
    scenario_builder = ScenarioBuilder()
    
    st.subheader("Create New Scenario")
    
    # Base forecast selection
    forecast_names = list(st.session_state.forecasts.keys())
    base_forecast = st.selectbox("Select Base Forecast", forecast_names)
    
    # Scenario parameters
    col1, col2 = st.columns(2)
    
    with col1:
        scenario_name = st.text_input("Scenario Name", value=f"Scenario_{datetime.now().strftime('%H%M%S')}")
        scenario_type = st.selectbox(
            "Scenario Type",
            ["Percentage Change", "Absolute Change", "Seasonal Adjustment", "Market Event"]
        )
    
    with col2:
        if scenario_type == "Percentage Change":
            change_percent = st.slider("Percentage Change (%)", -50, 100, 0)
            scenario_params = {"type": "percentage", "value": change_percent}
        elif scenario_type == "Absolute Change":
            change_value = st.number_input("Absolute Change", value=0.0)
            scenario_params = {"type": "absolute", "value": change_value}
        elif scenario_type == "Seasonal Adjustment":
            seasonal_factor = st.slider("Seasonal Factor", 0.5, 2.0, 1.0, 0.1)
            scenario_params = {"type": "seasonal", "factor": seasonal_factor}
        else:  # Market Event
            event_impact = st.slider("Event Impact (%)", -30, 50, 0)
            event_duration = st.slider("Event Duration (periods)", 1, 30, 7)
            scenario_params = {"type": "event", "impact": event_impact, "duration": event_duration}
    
    # Additional parameters
    st.subheader("Additional Parameters")
    col1, col2 = st.columns(2)
    
    with col1:
        apply_from_period = st.number_input("Apply from Period", 1, 365, 1)
    with col2:
        apply_to_period = st.number_input("Apply to Period", apply_from_period, 365, 30)
    
    # Create scenario
    if st.button("Create Scenario"):
        try:
            base_forecast_data = st.session_state.forecasts[base_forecast]
            scenario_result = scenario_builder.create_scenario(
                base_forecast_data,
                scenario_name,
                scenario_params,
                apply_from_period,
                apply_to_period
            )
            
            if scenario_result is not None:
                st.session_state.scenarios[scenario_name] = scenario_result
                st.success(f"✅ Scenario '{scenario_name}' created successfully!")
        
        except Exception as e:
            st.error(f"❌ Error creating scenario: {str(e)}")
    
    # Display and compare scenarios
    if st.session_state.scenarios:
        st.subheader("Scenario Comparison")
        
        scenario_names = list(st.session_state.scenarios.keys())
        selected_scenarios = st.multiselect(
            "Select Scenarios to Compare",
            scenario_names,
            default=scenario_names[:3] if len(scenario_names) >= 3 else scenario_names
        )
        
        if selected_scenarios and st.button("Compare Scenarios"):
            # Create comparison visualization
            visualizer = Visualizer()
            comparison_fig = visualizer.plot_scenario_comparison(
                st.session_state.data[st.session_state.demand_column],
                {name: st.session_state.scenarios[name] for name in selected_scenarios}
            )
            st.plotly_chart(comparison_fig, use_container_width=True)
            
            # Scenario impact analysis
            st.subheader("Impact Analysis")
            impact_data = []
            
            for scenario_name in selected_scenarios:
                scenario = st.session_state.scenarios[scenario_name]
                base_total = scenario['base_forecast'].sum()
                scenario_total = scenario['scenario_forecast'].sum()
                impact = ((scenario_total - base_total) / base_total) * 100
                
                impact_data.append({
                    "Scenario": scenario_name,
                    "Base Total": f"{base_total:.2f}",
                    "Scenario Total": f"{scenario_total:.2f}",
                    "Impact (%)": f"{impact:.2f}%"
                })
            
            impact_df = pd.DataFrame(impact_data)
            st.dataframe(impact_df, use_container_width=True)

def dashboard_section():
    st.header("📈 Dashboard & Insights")
    
    if st.session_state.data is None:
        st.warning("⚠️ Please upload data first in the Data Upload page.")
        return
    
    # Key Performance Indicators
    st.subheader("Key Performance Indicators")
    
    data = st.session_state.data[st.session_state.demand_column]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        current_avg = data.tail(30).mean()
        previous_avg = data.tail(60).head(30).mean()
        change = ((current_avg - previous_avg) / previous_avg) * 100 if previous_avg != 0 else 0
        st.metric("30-Day Average", f"{current_avg:.2f}", f"{change:.1f}%")
    
    with col2:
        total_demand = data.sum()
        st.metric("Total Demand", f"{total_demand:.0f}")
    
    with col3:
        volatility = data.std()
        st.metric("Volatility (σ)", f"{volatility:.2f}")
    
    with col4:
        trend = "📈" if data.tail(10).mean() > data.head(10).mean() else "📉"
        st.metric("Trend", trend)
    
    # Visualizations
    visualizer = Visualizer()
    
    # Historical trend
    st.subheader("Historical Demand Trend")
    trend_fig = visualizer.plot_historical_trend(data)
    st.plotly_chart(trend_fig, use_container_width=True)
    
    # Distribution analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Demand Distribution")
        dist_fig = visualizer.plot_distribution(data)
        st.plotly_chart(dist_fig, use_container_width=True)
    
    with col2:
        st.subheader("Seasonal Pattern")
        seasonal_fig = visualizer.plot_seasonal_pattern(data)
        st.plotly_chart(seasonal_fig, use_container_width=True)
    
    # Forecast summary if available
    if st.session_state.forecasts:
        st.subheader("Forecast Summary")
        
        forecast_summary = []
        for name, forecast in st.session_state.forecasts.items():
            avg_forecast = forecast['forecast'].mean()
            forecast_summary.append({
                "Model": forecast['model_type'],
                "Average Forecast": f"{avg_forecast:.2f}",
                "Periods": len(forecast['forecast']),
                "Generated": forecast['timestamp']
            })
        
        summary_df = pd.DataFrame(forecast_summary)
        st.dataframe(summary_df, use_container_width=True)
    
    # Business insights
    st.subheader("Business Insights")
    
    insights = []
    
    # Trend analysis
    recent_trend = data.tail(30).mean() / data.head(30).mean() - 1
    if recent_trend > 0.1:
        insights.append("📈 **Growing Demand**: Recent demand shows significant upward trend (+{:.1f}%)".format(recent_trend * 100))
    elif recent_trend < -0.1:
        insights.append("📉 **Declining Demand**: Recent demand shows downward trend ({:.1f}%)".format(recent_trend * 100))
    else:
        insights.append("📊 **Stable Demand**: Demand remains relatively stable with minimal variation")
    
    # Volatility analysis
    cv = (data.std() / data.mean()) * 100
    if cv > 30:
        insights.append("⚠️ **High Volatility**: Demand shows high variability (CV: {:.1f}%)".format(cv))
    elif cv < 10:
        insights.append("✅ **Low Volatility**: Demand is relatively stable (CV: {:.1f}%)".format(cv))
    
    # Seasonality detection
    if len(data) >= 365:
        monthly_avg = data.groupby(data.index.month).mean()
        seasonal_var = monthly_avg.std() / monthly_avg.mean()
        if seasonal_var > 0.2:
            insights.append("🌊 **Seasonal Pattern**: Strong seasonal patterns detected")
    
    for insight in insights:
        st.info(insight)

def export_section():
    st.header("📤 Export Results")
    
    if st.session_state.data is None:
        st.warning("⚠️ No data available to export.")
        return
    
    st.subheader("Export Options")
    
    # Export historical data
    if st.button("Export Historical Data"):
        csv_data = st.session_state.data.to_csv()
        st.download_button(
            label="Download Historical Data (CSV)",
            data=csv_data,
            file_name=f"historical_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # Export forecasts
    if st.session_state.forecasts:
        st.subheader("Export Forecasts")
        
        forecast_names = list(st.session_state.forecasts.keys())
        selected_forecast = st.selectbox("Select Forecast to Export", forecast_names)
        
        if st.button("Export Selected Forecast"):
            forecast_data = st.session_state.forecasts[selected_forecast]
            
            # Create export dataframe
            export_df = pd.DataFrame({
                'Date': pd.date_range(
                    start=st.session_state.data.index[-1] + timedelta(days=1),
                    periods=len(forecast_data['forecast']),
                    freq='D'
                ),
                'Forecast': forecast_data['forecast'],
                'Model': forecast_data['model_type']
            })
            
            csv_data = export_df.to_csv(index=False)
            st.download_button(
                label="Download Forecast (CSV)",
                data=csv_data,
                file_name=f"forecast_{selected_forecast}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Export scenarios
    if st.session_state.scenarios:
        st.subheader("Export Scenarios")
        
        if st.button("Export All Scenarios"):
            all_scenarios_data = []
            
            for scenario_name, scenario_data in st.session_state.scenarios.items():
                scenario_df = pd.DataFrame({
                    'Date': pd.date_range(
                        start=st.session_state.data.index[-1] + timedelta(days=1),
                        periods=len(scenario_data['scenario_forecast']),
                        freq='D'
                    ),
                    'Scenario': scenario_name,
                    'Base_Forecast': scenario_data['base_forecast'],
                    'Scenario_Forecast': scenario_data['scenario_forecast']
                })
                all_scenarios_data.append(scenario_df)
            
            combined_scenarios = pd.concat(all_scenarios_data, ignore_index=True)
            csv_data = combined_scenarios.to_csv(index=False)
            
            st.download_button(
                label="Download All Scenarios (CSV)",
                data=csv_data,
                file_name=f"scenarios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Export summary report
    st.subheader("Summary Report")
    
    if st.button("Generate Summary Report"):
        report_data = {
            "Data Summary": {
                "Total Records": len(st.session_state.data),
                "Date Range": f"{st.session_state.data.index.min()} to {st.session_state.data.index.max()}",
                "Average Demand": st.session_state.data[st.session_state.demand_column].mean(),
                "Total Demand": st.session_state.data[st.session_state.demand_column].sum()
            },
            "Forecasts Generated": len(st.session_state.forecasts),
            "Scenarios Created": len(st.session_state.scenarios),
            "Generated On": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Convert to readable format
        report_text = "DEMAND PLANNING & FORECASTING SUMMARY REPORT\n"
        report_text += "=" * 50 + "\n\n"
        
        for section, content in report_data.items():
            report_text += f"{section}:\n"
            if isinstance(content, dict):
                for key, value in content.items():
                    report_text += f"  - {key}: {value}\n"
            else:
                report_text += f"  {content}\n"
            report_text += "\n"
        
        st.download_button(
            label="Download Summary Report (TXT)",
            data=report_text,
            file_name=f"summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain"
        )

def reporting_insights_page():
    st.header("📊 Reporting & Insights")
    st.markdown("---")
    
    st.info("🚧 **Coming Soon**: Advanced reporting and insights features")
    
    st.markdown("""
    This section will include:
    - **Advanced Analytics**: Deep dive into demand patterns and trends
    - **Performance Reports**: Model accuracy and forecast performance metrics  
    - **Executive Dashboards**: High-level KPIs and business insights
    - **Custom Reports**: Build your own reports with flexible filters
    - **Automated Alerts**: Set up notifications for demand anomalies
    """)



def settings_page():
    st.header("⚙️ Settings")
    st.markdown("---")
    
    # Initialize User Manager
    from utils.user_manager import UserManager
    if 'user_manager' not in st.session_state:
        st.session_state.user_manager = UserManager()
    
    user_manager = st.session_state.user_manager
    current_user_email = st.session_state.user_info.get('email', '')
    
    # Check admin status - first check session state for simple auth, then database
    user_info = st.session_state.get('user_info', {})
    is_admin = user_info.get('is_admin', False) or user_manager.is_admin(current_user_email)
    
    # Show RBAC authentication status
    permission_manager = st.session_state.get('permission_manager')
    if permission_manager:
        accessible_modules = permission_manager.get_accessible_modules(current_user_email)
        st.info(f"🔐 Role-Based Access Control Active - Access to {len(accessible_modules)} modules")
        st.success(f"✅ Accessible modules: {', '.join(accessible_modules) if accessible_modules else 'Limited access'}")
    else:
        st.warning("⚠️ Permission manager not initialized")
    
    # Create tabs for different settings sections
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["👥 User Management", "🔐 Roles & Role Groups", "🗄️ Database", "⚙️ Application", "📊 Export", "🌍 Environment", "📊 Google Sheets"])
    
    # Tab 1: User Management (Permission-Based)
    with tab1:
        # Check RBAC permission for User Management
        if permission_manager and permission_manager.has_permission(current_user_email, "Settings", "User Management", "view"):
            # Additional check for admin operations
            is_admin = user_info.get('is_admin', False) or user_manager.is_admin(current_user_email)
            
            if not is_admin:
                st.warning("🔒 Administrative operations require admin privileges.")
                st.info(f"Current user: {current_user_email} - View access only")
            else:
                st.subheader("👥 User Access Management")
            
            # User Statistics
            user_stats = user_manager.get_user_stats()
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Users", user_stats.get('total_users', 0))
            with col2:
                st.metric("Active Users", user_stats.get('active_users', 0))
            with col3:
                st.metric("Inactive Users", user_stats.get('inactive_users', 0))
            
            st.markdown("---")
            
            # Sub-tabs for user operations
            user_tab1, user_tab2, user_tab3 = st.tabs(["➕ Add User", "✏️ Edit User", "📋 View Users"])
            
            # Add User Tab
            with user_tab1:
                st.subheader("➕ Add New User")
                with st.form("add_user_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_username = st.text_input("Username", placeholder="Enter username")
                        new_email = st.text_input("Email", placeholder="user@greyamp.com")
                    with col2:
                        new_password = st.text_input("Password", type="password", placeholder="Enter secure password")
                    
                    st.caption("Note: Users will be required to change their password on first login")
                    
                    add_submitted = st.form_submit_button("👤 Create User", type="primary")
                    
                    if add_submitted:
                        if not all([new_username, new_email, new_password]):
                            st.error("Please fill in all fields")
                        elif not new_email.endswith("@greyamp.com"):
                            st.error("Email must be from greyamp.com domain")
                        elif len(new_password) < 6:
                            st.error("Password must be at least 6 characters long")
                        else:
                            success, message = user_manager.add_user(
                                new_username, new_email, new_password, 'team_member', current_user_email
                            )
                            if success:
                                st.success(f"✅ {message}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")
            
            # Edit User Tab
            with user_tab2:
                st.subheader("✏️ Edit Existing User")
                
                # Get all users for selection
                users_df = user_manager.get_all_users()
                if len(users_df) > 0:
                    user_options = [f"{row['username']} ({row['email']})" 
                                  for _, row in users_df.iterrows()]
                    
                    selected_user_idx = st.selectbox("Select User to Edit", 
                                                   range(len(user_options)),
                                                   format_func=lambda x: user_options[x])
                    
                    if selected_user_idx is not None:
                        selected_user = users_df.iloc[selected_user_idx]
                        
                        with st.form("edit_user_form"):
                            st.info(f"Editing: {selected_user['username']} ({selected_user['email']})")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                edit_username = st.text_input("Username", value=selected_user['username'])
                                edit_email = st.text_input("Email", value=selected_user['email'])
                            with col2:
                                edit_password = st.text_input("New Password (leave empty to keep current)", 
                                                            type="password", placeholder="Enter new password")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                update_submitted = st.form_submit_button("💾 Update User", type="primary")
                            with col2:
                                deactivate_submitted = st.form_submit_button("🚫 Deactivate User", type="secondary")
                            
                            if update_submitted:
                                # Collect all fields that might need updating
                                username_to_update = edit_username.strip() if edit_username and edit_username != selected_user['username'] else None
                                email_to_update = edit_email.strip() if edit_email and edit_email != selected_user['email'] else None
                                password_to_update = edit_password.strip() if edit_password and edit_password.strip() else None
                                
                                # Check if any changes were made
                                if any([username_to_update, email_to_update, password_to_update]):
                                    # Validate email domain if changed
                                    if email_to_update and not email_to_update.endswith("@greyamp.com"):
                                        st.error("Email must be from greyamp.com domain")
                                    else:
                                        success, message = user_manager.update_user(
                                            int(selected_user['id']), 
                                            username=username_to_update,
                                            email=email_to_update,
                                            password=password_to_update,
                                            profile=None,
                                            updated_by_email=current_user_email
                                        )
                                        if success:
                                            st.success(f"✅ {message}")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {message}")
                                else:
                                    st.info("No changes detected")
                            
                            if deactivate_submitted:
                                if selected_user['email'] == current_user_email:
                                    st.error("Cannot deactivate your own account")
                                else:
                                    success, message = user_manager.delete_user(int(selected_user['id']))
                                    if success:
                                        st.success(f"✅ {message}")
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {message}")
                else:
                    st.info("No users found")
            
            # View Users Tab
            with user_tab3:
                st.subheader("📋 Current Users")
                users_df = user_manager.get_all_users()
                
                if len(users_df) > 0:
                    st.markdown("**User Management with Actions:**")
                    
                    for index, user in users_df.iterrows():
                        col1, col2, col3, col4, col5 = st.columns([2, 2, 1.5, 1, 1])
                        
                        with col1:
                            st.write(f"**{user['username']}**")
                            st.write(f"_{user['email']}_")
                        
                        with col2:
                            # Get actual role group from database using flexible username matching
                            role_group_assigned = "No Role Group Assigned"
                            if user['email']:
                                try:
                                    # Query the role group using flexible username matching
                                    if psycopg2 is None:
                                        continue
                                    conn = psycopg2.connect(DATABASE_URL)
                                    cursor = conn.cursor()
                                    
                                    # Try multiple username variants for lookup with debug
                                    email_str = str(user['email']) if user['email'] is not None else ''
                                    username_variants = [
                                        user['username'],  # Direct username (try first)
                                        email_str,  # Full email
                                        email_str.split('@')[0] if '@' in email_str else email_str,  # Email prefix
                                        email_str.split('@')[0].replace('.', ' ').title() if '@' in email_str else email_str  # Formatted name
                                    ]
                                    
                                    for username_variant in username_variants:
                                        cursor.execute("""
                                            SELECT rg.group_name 
                                            FROM user_role_mappings urm
                                            JOIN role_groups rg ON urm.role_group_id = rg.id
                                            WHERE urm.user_name = %s AND urm.status = 'active'
                                        """, (username_variant,))
                                        result = cursor.fetchone()
                                        if result:
                                            role_group_assigned = result[0]
                                            break
                                    
                                    conn.close()
                                except Exception as e:
                                    pass  # Keep default value
                            
                            st.write(f"Role Group: {role_group_assigned}")
                            created_date_val = user['created_date']
                            if created_date_val is not None and pd.notna(created_date_val):
                                try:
                                    created_date = pd.to_datetime(created_date_val).strftime('%Y-%m-%d')
                                    st.write(f"Created: {created_date}")
                                except (ValueError, AttributeError):
                                    st.write("Created: N/A")
                            else:
                                st.write("Created: N/A")
                        
                        with col3:
                            last_login_val = user['last_login']
                            if last_login_val is not None and pd.notna(last_login_val):
                                try:
                                    last_login = pd.to_datetime(last_login_val).strftime('%Y-%m-%d')
                                    st.write(f"Last Login: {last_login}")
                                except (ValueError, AttributeError):
                                    st.write("Last Login: Never")
                            else:
                                st.write("Last Login: Never")
                            st.write(f"Created by: {user['created_by']}")
                        
                        with col4:
                            if permission_manager.has_permission(current_user_email, "Settings", "User Management", "edit"):
                                if st.button(f"✏️ Edit", key=f"user_mgmt_edit_{user['id']}"):
                                    st.session_state[f'editing_user_{user["id"]}'] = True
                                    st.rerun()
                        
                        with col5:
                            if permission_manager.has_permission(current_user_email, "Settings", "User Management", "delete"):
                                # Don't allow deleting own account
                                if user['email'] != current_user_email:
                                    if st.button(f"🗑️ Delete", key=f"user_mgmt_delete_{user['id']}"):
                                        success, message = user_manager.delete_user(int(user['id']))
                                        if success:
                                            st.success(message)
                                            st.rerun()
                                        else:
                                            st.error(message)
                                else:
                                    st.write("_Own account_")
                        
                        # Edit form for this user
                        if st.session_state.get(f'editing_user_{user["id"]}', False):
                            with st.form(f"user_mgmt_edit_form_{user['id']}"):
                                st.subheader(f"Edit User: {user['username']}")
                                edit_username = st.text_input("Username", value=user['username'])
                                edit_email = st.text_input("Email", value=user['email'])

                                edit_password = st.text_input("New Password (leave blank to keep current)", type="password")
                                
                                col_save, col_cancel = st.columns(2)
                                with col_save:
                                    if st.form_submit_button("💾 Save Changes"):
                                        # Validate email domain
                                        if not edit_email or not edit_email.endswith("@greyamp.com"):
                                            st.error("Email must be from greyamp.com domain")
                                        else:
                                            success, message = user_manager.update_user(
                                                int(user['id']), 
                                                username=edit_username if edit_username != user['username'] else None,
                                                email=edit_email if edit_email != user['email'] else None,
                                                password=edit_password if edit_password else None,
                                                profile=None,
                                                updated_by_email=current_user_email
                                            )
                                            if success:
                                                st.success(message)
                                                st.session_state[f'editing_user_{user["id"]}'] = False
                                                st.rerun()
                                            else:
                                                st.error(message)
                                
                                with col_cancel:
                                    if st.form_submit_button("❌ Cancel"):
                                        st.session_state[f'editing_user_{user["id"]}'] = False
                                        st.rerun()
                        
                        st.divider()
                else:
                    st.info("No users found")
        else:
            # Show permission denied message for User Management
            if permission_manager:
                permission_manager.show_access_denied_message("Settings", "User Management")
            else:
                st.error("Permission manager not available")
    
    # Tab 2: Roles & Role Groups (RBAC Management)
    with tab2:
        if permission_manager and permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "view"):
            st.subheader("🔐 Role-Based Access Control Management")
            
            # Import role manager
            from utils.role_manager import RoleManager
            from utils.environment_manager import EnvironmentManager
            if 'role_manager' not in st.session_state:
                local_env_manager = EnvironmentManager()
                st.session_state.role_manager = RoleManager(local_env_manager)
            
            role_manager = st.session_state.role_manager
            
            # Role management sub-tabs
            role_tab1, role_tab2, role_tab3 = st.tabs(["🎭 Roles", "👥 Role Groups", "🔗 User Mappings"])
            
            # Roles tab
            with role_tab1:
                st.subheader("🎭 Roles Management")
                
                if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "add"):
                    # Show form toggle button
                    if 'show_add_role_form' not in st.session_state:
                        st.session_state['show_add_role_form'] = False
                    
                    if not st.session_state['show_add_role_form']:
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if st.button("➕ Add New Role", type="primary", use_container_width=True, key="show_add_role_btn"):
                                st.session_state['show_add_role_form'] = True
                                st.rerun()
                    else:
                        # Show close button when form is open
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if st.button("❌ Close Form", type="secondary", use_container_width=True, key="close_add_role_btn"):
                                st.session_state['show_add_role_form'] = False
                                st.rerun()
                    
                    st.markdown("---")
                
                # Show Add New Role form if requested
                if 'show_add_role_form' in st.session_state and st.session_state['show_add_role_form']:
                    st.markdown("### ➕ Create New Role")
                    
                    with st.form("roles_add_form"):
                        col1, col2 = st.columns(2)
                        with col1:
                            new_role_name = st.text_input("Role Name*", placeholder="e.g., Team Lead, Developer")
                            new_role_status = st.selectbox("Status", ["Active", "Inactive"])
                        with col2:
                            new_role_description = st.text_area("Description", placeholder="Describe the role's responsibilities and purpose")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            create_role = st.form_submit_button("✅ Create Role", type="primary")
                        with col2:
                            cancel_create = st.form_submit_button("❌ Cancel")
                        
                        if create_role and new_role_name:
                            success, message = role_manager.create_role(new_role_name, new_role_description, new_role_status.lower())
                            if success:
                                st.success(message)
                                st.session_state['show_add_role_form'] = False
                                st.rerun()
                            else:
                                st.error(message)
                        elif create_role and not new_role_name:
                            st.error("Please enter a role name")
                        
                        if cancel_create:
                            st.session_state['show_add_role_form'] = False
                            st.rerun()
                    
                    st.markdown("---")
                
                # Display existing roles
                roles = role_manager.get_all_roles()
                if not roles.empty:
                    st.markdown("**Current Roles:**")
                    
                    # Add table headers
                    header_col1, header_col2, header_col3, header_col4, header_col5, header_col6, header_col7 = st.columns([1, 2, 3, 1, 2, 1, 1])
                    with header_col1:
                        st.markdown("**ID**")
                    with header_col2:
                        st.markdown("**Role Name**")
                    with header_col3:
                        st.markdown("**Description**")
                    with header_col4:
                        st.markdown("**Status**")
                    with header_col5:
                        st.markdown("**Created Date**")
                    with header_col6:
                        st.markdown("**Edit**")
                    with header_col7:
                        st.markdown("**Delete**")
                    
                    st.markdown("---")
                    
                    # Create interactive table with edit/delete buttons
                    for index, row in roles.iterrows():
                        col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 2, 3, 1, 2, 1, 1])
                        
                        with col1:
                            st.write(str(row['id']))
                        with col2:
                            st.write(row['role_name'])
                        with col3:
                            st.write(row['description'] if pd.notna(row['description']) else "No description")
                        with col4:
                            status_color = "🟢" if row['status'] == 'Active' else "🔴"
                            st.write(f"{status_color} {row['status']}")
                        with col5:
                            if pd.notna(row['created_date']):
                                try:
                                    created_date = pd.to_datetime(row['created_date']).strftime('%Y-%m-%d %H:%M')
                                    st.write(created_date)
                                except (ValueError, AttributeError):
                                    st.write("N/A")
                            else:
                                st.write("N/A")
                        with col6:
                            # Edit button
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "edit"):
                                edit_key = f"edit_role_{row['id']}"
                                if st.button("⚙️", key=f"role_edit_btn_{row['id']}", help="Edit Role"):
                                    st.session_state[edit_key] = True
                                    st.rerun()
                        with col7:
                            # Delete button
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "delete"):
                                confirm_key = f"confirm_delete_role_{row['id']}"
                                
                                if confirm_key in st.session_state and st.session_state[confirm_key]:
                                    # Show confirmation buttons
                                    if st.button("✅", key=f"role_yes_del_{row['id']}", help="Confirm Delete"):
                                        success, message = role_manager.delete_role(row['id'])
                                        if success:
                                            st.success(message)
                                            del st.session_state[confirm_key]
                                            st.rerun()
                                        else:
                                            st.error(message)
                                            del st.session_state[confirm_key]
                                    
                                    if st.button("❌", key=f"role_no_del_{row['id']}", help="Cancel Delete"):
                                        del st.session_state[confirm_key]
                                        st.rerun()
                                else:
                                    if st.button("🗑️", key=f"role_del_btn_{row['id']}", help="Delete Role"):
                                        st.session_state[confirm_key] = True
                                        st.rerun()
                    
                    st.markdown("---")
                    
                    # Show edit forms if requested
                    for index, row in roles.iterrows():
                        edit_key = f"edit_role_{row['id']}"
                        if edit_key in st.session_state and st.session_state[edit_key]:
                            st.markdown(f"### ⚙️ Edit Role: **{row['role_name']}**")
                            
                            with st.form(f"roles_edit_form_{row['id']}"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    # Role name
                                    edit_role_name = st.text_input(
                                        "Role Name",
                                        value=row['role_name'],
                                        key=f"role_edit_name_{row['id']}"
                                    )
                                    
                                    # Status
                                    current_status = row.get('status', 'Active')
                                    status_index = 0 if current_status == 'Active' else 1
                                    edit_status = st.selectbox(
                                        "Status",
                                        ["Active", "Inactive"],
                                        index=status_index,
                                        key=f"role_edit_status_{row['id']}"
                                    )
                                
                                with col2:
                                    # Description
                                    edit_description = st.text_area(
                                        "Description",
                                        value=row['description'] if pd.notna(row['description']) else "",
                                        key=f"role_edit_desc_{row['id']}"
                                    )
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    save_role = st.form_submit_button("💾 Save Changes", type="primary")
                                with col2:
                                    cancel_role = st.form_submit_button("❌ Cancel")
                                
                                if save_role:
                                    success, message = role_manager.update_role(
                                        row['id'],
                                        edit_role_name,
                                        edit_description,
                                        edit_status.lower()
                                    )
                                    if success:
                                        st.success(message)
                                        del st.session_state[edit_key]
                                        st.rerun()
                                    else:
                                        st.error(message)
                                
                                if cancel_role:
                                    del st.session_state[edit_key]
                                    st.rerun()
                            
                            st.markdown("---")
                            break  # Only show one edit form at a time
                else:
                    st.info("No roles found")
            
            # Role Groups tab
            with role_tab2:
                st.subheader("👥 Role Groups Management")
                
                # Add New Role Group Action Button at the top
                # Check if user has permission to add role groups
                user_email = st.session_state.get('user_email', '')
                if user_email and hasattr(permission_manager, 'has_permission'):
                    try:
                        has_add_permission = permission_manager.has_permission(user_email, "Settings", "Roles & Role Groups", "add")
                    except:
                        # Fallback for Super Admin
                        has_add_permission = user_email == "preethi.madhu@greyamp.com"
                else:
                    # Default permission for authenticated users
                    has_add_permission = True
                
                if has_add_permission:
                    # Show form toggle button
                    if 'show_add_role_group_form' not in st.session_state:
                        st.session_state['show_add_role_group_form'] = False
                    
                    if not st.session_state['show_add_role_group_form']:
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if st.button("➕ Add New Role Group", type="primary", use_container_width=True, key="show_add_form_btn"):
                                st.session_state['show_add_role_group_form'] = True
                                st.rerun()
                    else:
                        # Show close button when form is open
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if st.button("❌ Close Form", type="secondary", use_container_width=True, key="close_add_form_btn"):
                                st.session_state['show_add_role_group_form'] = False
                                st.rerun()
                    
                    st.markdown("---")
                
                # Show Add New Role Group form if requested
                if 'show_add_role_group_form' in st.session_state and st.session_state['show_add_role_group_form']:
                    st.markdown("### ➕ Create New Role Group")
                    
                    with st.form("add_role_group_form_detailed"):
                        col1, col2 = st.columns(2)
                        with col1:
                            group_name = st.text_input("Group Name*", placeholder="e.g., Sales Team, Managers")
                            group_status = st.selectbox("Status", ["Active", "Inactive"])
                        with col2:
                            group_description = st.text_area("Description", placeholder="Describe the role group's purpose and responsibilities")
                        
                        st.markdown("---")
                        st.markdown("**🔐 Set Permissions for this Role Group:**")
                        
                        # Define available modules and sub-pages
                        modules_config = {
                            "Demand Planning": ["Target Setting", "Demand Tweaking", "Editable Plan View"],
                            "Supply Planning": ["Talent Management", "Pipeline Configuration", "Staffing Plans"],
                            "Demand - Supply Mapping": ["Add New Mapping", "View Mappings"],
                            "Insights & Reporting": ["Analytics Dashboard", "Export Functions"],
                            "Settings": ["User Management", "Roles & Role Groups", "Database Status", "Application Settings", "Export Settings", "Environment"]
                        }
                        
                        permissions_data = []
                        
                        for module, sub_pages in modules_config.items():
                            st.markdown(f"**{module}**")
                            for sub_page in sub_pages:
                                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                                with col1:
                                    st.write(f"  • {sub_page}")
                                with col2:
                                    can_view = st.checkbox("View", value=False, key=f"new_view_{module}_{sub_page}")
                                with col3:
                                    can_add = st.checkbox("Add", value=False, key=f"new_add_{module}_{sub_page}")
                                with col4:
                                    can_edit = st.checkbox("Edit", value=False, key=f"new_edit_{module}_{sub_page}")
                                with col5:
                                    can_delete = st.checkbox("Delete", value=False, key=f"new_delete_{module}_{sub_page}")
                                
                                permissions_data.append({
                                    'Module': module,
                                    'Sub-Page': sub_page,
                                    'View': can_view,
                                    'Add': can_add,
                                    'Edit': can_edit,
                                    'Delete': can_delete
                                })
                        
                        st.markdown("---")
                        col1, col2, col3 = st.columns([1, 1, 2])
                        with col1:
                            create_group = st.form_submit_button("✅ Create Role Group", type="primary")
                        with col2:
                            cancel_create = st.form_submit_button("❌ Cancel")
                        
                        if create_group and group_name:
                            try:
                                # Create the role group first
                                success, message = role_manager.create_role_group(group_name, group_description, [], group_status.lower())
                                if success:
                                    # Get the newly created group ID
                                    import psycopg2
                                    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT id FROM role_groups WHERE group_name = %s ORDER BY created_date DESC LIMIT 1", (group_name,))
                                    group_id = cursor.fetchone()[0]
                                    
                                    # Save permissions for the new group
                                    for perm in permissions_data:
                                        if perm['View'] or perm['Add'] or perm['Edit'] or perm['Delete']:  # Only save if at least one permission is set
                                            cursor.execute('''
                                            INSERT INTO role_group_permissions (group_id, module_name, sub_page, can_add, can_edit, can_delete, can_view, created_date)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                            ''', (group_id, perm['Module'], perm['Sub-Page'], perm['Add'], perm['Edit'], perm['Delete'], perm['View'], datetime.now()))
                                    
                                    conn.commit()
                                    conn.close()
                                    
                                    st.success(f"Role group '{group_name}' created successfully with permissions!")
                                    st.session_state['show_add_role_group_form'] = False
                                    st.rerun()
                                else:
                                    st.error(message)
                            except Exception as e:
                                st.error(f"Error creating role group: {str(e)}")
                        elif create_group and not group_name:
                            st.error("Please enter a group name")
                        
                        if cancel_create:
                            st.session_state['show_add_role_group_form'] = False
                            st.rerun()
                    
                    st.markdown("---")
                
                # Display existing role groups in table format
                role_groups = role_manager.get_all_role_groups()
                if not role_groups.empty:
                    st.markdown("**Current Role Groups:**")
                    
                    # Add table headers
                    header_col1, header_col2, header_col3, header_col4, header_col5, header_col6 = st.columns([2, 3, 1, 2, 1, 1])
                    with header_col1:
                        st.markdown("**Group Name**")
                    with header_col2:
                        st.markdown("**Description**")
                    with header_col3:
                        st.markdown("**Status**")
                    with header_col4:
                        st.markdown("**Roles**")
                    with header_col5:
                        st.markdown("**Permissions**")
                    with header_col6:
                        st.markdown("**Delete**")
                    
                    st.markdown("---")
                    
                    # Create interactive table with edit/delete buttons
                    for index, row in role_groups.iterrows():
                        col1, col2, col3, col4, col5, col6 = st.columns([2, 3, 1, 2, 1, 1])
                        
                        with col1:
                            st.write(f"**{row['group_name']}**")
                        with col2:
                            st.write(row['description'] if pd.notna(row['description']) else "No description")
                        with col3:
                            status_color = "🟢" if row['status'] == 'Active' else "🔴"
                            st.write(f"{status_color} {row['status']}")
                        with col4:
                            roles_text = row['roles'] if pd.notna(row['roles']) else "No roles assigned"
                            st.write(roles_text)
                        with col5:
                            # Permissions Configuration button
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "edit"):
                                permissions_key = f"configure_permissions_{row['id']}"
                                if st.button("⚙️", key=f"permissions_{row['id']}", help="Edit Role Group"):
                                    st.session_state[permissions_key] = True
                                    st.rerun()
                        with col6:
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "delete"):
                                # Check if we're in delete confirmation mode for this specific group
                                confirm_key = f"confirm_delete_group_{row['id']}"
                                
                                if confirm_key in st.session_state and st.session_state[confirm_key]:
                                    # Show confirmation buttons
                                    if st.button("✅", key=f"yes_delete_{row['id']}", help="Confirm Delete"):
                                        success, message = role_manager.delete_role_group(row['id'])
                                        if success:
                                            st.success(message)
                                            # Clear confirmation state
                                            del st.session_state[confirm_key]
                                            st.rerun()
                                        else:
                                            st.error(message)
                                            del st.session_state[confirm_key]
                                    
                                    if st.button("❌", key=f"no_delete_{row['id']}", help="Cancel Delete"):
                                        # Clear confirmation state
                                        del st.session_state[confirm_key]
                                        st.rerun()
                                else:
                                    # Show delete button
                                    if st.button("🗑️", key=f"delete_group_{row['id']}", help="Delete Role Group"):
                                        st.session_state[confirm_key] = True
                                        st.rerun()
                
                # Show role group configuration interface if requested
                for index, row in role_groups.iterrows():
                    permissions_key = f"configure_permissions_{row['id']}"
                    if permissions_key in st.session_state and st.session_state[permissions_key]:
                        st.markdown("---")
                        st.markdown(f"### ⚙️ Edit Role Group: **{row['group_name']}**")
                        
                        # Create tabs for different edit sections
                        role_edit_tab1, role_edit_tab2, role_edit_tab3 = st.tabs(["📝 Basic Info", "🎭 Assigned Roles", "🔐 Permissions"])
                        
                        # Tab 1: Basic Information
                        with role_edit_tab1:
                            with st.form(f"basic_info_form_{row['id']}"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    edit_group_name = st.text_input(
                                        "Group Name", 
                                        value=row['group_name'],
                                        key=f"edit_group_name_{row['id']}"
                                    )
                                    current_status = row.get('status', 'Active')
                                    status_index = 0 if current_status == 'Active' else 1
                                    edit_group_status = st.selectbox(
                                        "Status",
                                        ["Active", "Inactive"],
                                        index=status_index,
                                        key=f"edit_group_status_{row['id']}"
                                    )
                                with col2:
                                    edit_group_description = st.text_area(
                                        "Description",
                                        value=row['description'] if pd.notna(row['description']) else "",
                                        key=f"edit_group_desc_{row['id']}"
                                    )
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    save_basic_info = st.form_submit_button("💾 Save Basic Info", type="primary")
                                with col2:
                                    cancel_basic_info = st.form_submit_button("❌ Cancel")
                                
                                if save_basic_info:
                                    # Get current roles to maintain them
                                    current_roles = role_manager.get_roles_for_group(row['id'])
                                    role_names = [r['role_name'] for r in current_roles]
                                    
                                    success, message = role_manager.update_role_group(
                                        row['id'],
                                        edit_group_name,
                                        edit_group_description,
                                        role_names,
                                        edit_group_status.lower()
                                    )
                                    if success:
                                        st.success(message)
                                        st.rerun()
                                    else:
                                        st.error(message)
                                
                                if cancel_basic_info:
                                    del st.session_state[permissions_key]
                                    st.rerun()
                        
                        # Tab 2: Role Assignment Management
                        with role_edit_tab2:
                            st.markdown("**Manage Roles Assigned to this Group:**")
                            
                            # Get currently assigned roles
                            assigned_roles = role_manager.get_roles_for_group(row['id'])
                            all_available_roles = role_manager.get_available_roles_for_dropdown()
                            
                            # Display currently assigned roles
                            if assigned_roles:
                                st.markdown("**Currently Assigned Roles:**")
                                for assigned_role in assigned_roles:
                                    col1, col2 = st.columns([4, 1])
                                    with col1:
                                        st.write(f"• {assigned_role['role_name']}")
                                    with col2:
                                        if st.button("🗑️", key=f"remove_role_{row['id']}_{assigned_role['id']}", help="Remove Role"):
                                            # Remove this role from the group
                                            remaining_roles = [r['role_name'] for r in assigned_roles if r['id'] != assigned_role['id']]
                                            success, message = role_manager.update_role_group(
                                                row['id'],
                                                row['group_name'],
                                                row['description'] if pd.notna(row['description']) else "",
                                                remaining_roles,
                                                row['status']
                                            )
                                            if success:
                                                st.success(f"Role '{assigned_role['role_name']}' removed from group")
                                                st.rerun()
                                            else:
                                                st.error(message)
                            else:
                                st.info("No roles currently assigned to this group")
                            
                            st.markdown("---")
                            
                            # Add new role form
                            with st.form(f"add_role_form_{row['id']}"):
                                st.markdown("**Add New Role to Group:**")
                                
                                # Filter out already assigned roles
                                assigned_role_ids = [r['id'] for r in assigned_roles]
                                available_roles = [(role_id, role_name) for role_id, role_name in all_available_roles 
                                                 if role_id not in assigned_role_ids]
                                
                                if available_roles:
                                    role_options = ["-- Select Role --"] + [role_name for _, role_name in available_roles]
                                    selected_role = st.selectbox(
                                        "Available Roles",
                                        role_options,
                                        key=f"new_role_select_{row['id']}"
                                    )
                                else:
                                    st.info("All available roles are already assigned to this group")
                                    selected_role = "-- No Roles Available --"
                                
                                # Always include submit button to avoid form error
                                add_role = st.form_submit_button("➕ Add Role to Group", type="primary", disabled=(not available_roles))
                                
                                if add_role and available_roles and selected_role != "-- Select Role --":
                                    # Add the selected role to the group
                                    current_role_names = [r['role_name'] for r in assigned_roles]
                                    updated_roles = current_role_names + [selected_role]
                                    
                                    success, message = role_manager.update_role_group(
                                        row['id'],
                                        row['group_name'],
                                        row['description'] if pd.notna(row['description']) else "",
                                        updated_roles,
                                        row['status']
                                    )
                                    if success:
                                        st.success(f"Role '{selected_role}' added to group")
                                        st.rerun()
                                    else:
                                        st.error(message)
                                elif add_role and available_roles:
                                    st.error("Please select a role to add")
                        
                        # Tab 3: Permissions Configuration
                        with role_edit_tab3:
                            st.markdown("**Configure Module Permissions:**")
                            
                            # Get existing permissions for this group
                            try:
                                import psycopg2
                                conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                                existing_perms_query = '''
                                SELECT module_name, sub_page, can_add, can_edit, can_delete, can_view
                                FROM role_group_permissions 
                                WHERE group_id = %s
                                ORDER BY module_name, sub_page
                                '''
                                existing_permissions = pd.read_sql_query(existing_perms_query, conn, params=(row['id'],))
                                conn.close()
                            except Exception as e:
                                st.error(f"Error loading permissions: {str(e)}")
                                existing_permissions = pd.DataFrame()
                            
                            # Create permissions form
                            with st.form(f"permissions_form_{row['id']}"):
                                st.markdown("**Module Permissions:**")
                                
                                # Define available modules and sub-pages (moved Supply pipeline management to Talent Management)
                                modules_config = {
                                    "Demand Planning": ["Target Setting", "Demand Tweaking", "Editable Plan View"],
                                    "Supply Planning": ["Talent Management", "Pipeline Configuration", "Supply Management"],
                                    "Demand - Supply Mapping": ["Add New Mapping", "View Mappings"],
                                    "Insights & Reporting": ["Analytics Dashboard", "Export Functions"],
                                    "Settings": ["User Management", "Roles & Role Groups", "Database Status", "Application Settings", "Export Settings", "Environment"]
                                }
                                
                                permissions_data = []
                                
                                for module, sub_pages in modules_config.items():
                                    st.markdown(f"**{module}**")
                                    for sub_page in sub_pages:
                                        # Check if permission exists
                                        existing_perm = existing_permissions[
                                            (existing_permissions['module_name'] == module) & 
                                            (existing_permissions['sub_page'] == sub_page)
                                        ]
                                        
                                        if not existing_perm.empty:
                                            current_add = existing_perm.iloc[0]['can_add']
                                            current_edit = existing_perm.iloc[0]['can_edit']
                                            current_delete = existing_perm.iloc[0]['can_delete']
                                            current_view = existing_perm.iloc[0]['can_view']
                                        else:
                                            current_add = current_edit = current_delete = current_view = False
                                        
                                        col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                                        with col1:
                                            st.write(f"  • {sub_page}")
                                        with col2:
                                            can_view = st.checkbox("View", value=current_view, key=f"view_{row['id']}_{module}_{sub_page}")
                                        with col3:
                                            can_add = st.checkbox("Add", value=current_add, key=f"add_{row['id']}_{module}_{sub_page}")
                                        with col4:
                                            can_edit = st.checkbox("Edit", value=current_edit, key=f"edit_{row['id']}_{module}_{sub_page}")
                                        with col5:
                                            can_delete = st.checkbox("Delete", value=current_delete, key=f"delete_{row['id']}_{module}_{sub_page}")
                                        
                                        permissions_data.append({
                                            'Module': module,
                                            'Sub-Page': sub_page,
                                            'View': can_view,
                                            'Add': can_add,
                                            'Edit': can_edit,
                                            'Delete': can_delete
                                        })
                                
                                col1, col2, col3 = st.columns([1, 1, 2])
                                with col1:
                                    save_permissions = st.form_submit_button("💾 Save Permissions", type="primary")
                                with col2:
                                    cancel_permissions = st.form_submit_button("❌ Cancel")
                                
                                if save_permissions:
                                    try:
                                        # Save permissions to database
                                        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                                        cursor = conn.cursor()
                                        
                                        # Clear existing permissions
                                        cursor.execute('DELETE FROM role_group_permissions WHERE group_id = %s', (row['id'],))
                                        
                                        # Insert new permissions
                                        for perm in permissions_data:
                                            cursor.execute('''
                                            INSERT INTO role_group_permissions (group_id, module_name, sub_page, can_add, can_edit, can_delete, can_view, created_date)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                            ''', (row['id'], perm['Module'], perm['Sub-Page'], perm['Add'], perm['Edit'], perm['Delete'], perm['View'], datetime.now()))
                                        
                                        conn.commit()
                                        conn.close()
                                        st.success(f"Permissions saved for role group '{row['group_name']}'!")
                                        del st.session_state[permissions_key]
                                        st.rerun()
                                        
                                    except Exception as e:
                                        st.error(f"Error saving permissions: {str(e)}")
                                
                                if cancel_permissions:
                                    del st.session_state[permissions_key]
                                    st.rerun()
                        
                        break  # Only show one configuration form at a time
                
                if role_groups.empty:
                    st.info("No role groups found. Create your first role group using the button above.")
            
            # User Mappings tab
            with role_tab3:
                st.subheader("🔗 User-Role Mappings")
                
                # Display current mappings with action buttons
                mappings = role_manager.get_all_user_role_mappings()
                if not mappings.empty:
                    # Add table headers
                    header_col1, header_col2, header_col3, header_col4, header_col5, header_col6, header_col7, header_col8 = st.columns([1, 2, 3, 2, 1.5, 1, 1, 1])
                    with header_col1:
                        st.markdown("**ID**")
                    with header_col2:
                        st.markdown("**User Name**")
                    with header_col3:
                        st.markdown("**Email**")
                    with header_col4:
                        st.markdown("**Role Group**")
                    with header_col5:
                        st.markdown("**Team**")
                    with header_col6:
                        st.markdown("**Status**")
                    with header_col7:
                        st.markdown("**Edit**")
                    with header_col8:
                        st.markdown("**Delete**")
                    
                    st.markdown("---")
                    
                    # Create interactive table with edit/delete buttons
                    for index, row in mappings.iterrows():
                        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 2, 3, 2, 1.5, 1, 1, 1])
                        
                        with col1:
                            st.write(str(row['mapping_id']))
                        with col2:
                            st.write(row['user_name'])
                        with col3:
                            st.write(row['email'])
                        with col4:
                            st.write(row['group_name'])
                        with col5:
                            team_value = row.get('team', 'Not Set')
                            st.write(team_value if team_value else 'Not Set')
                        with col6:
                            status_color = "🟢" if row['mapping_status'] == 'Active' else "🔴"
                            st.write(f"{status_color} {row['mapping_status']}")
                        with col7:
                            # Edit button
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "edit"):
                                edit_key = f"edit_mapping_{row['mapping_id']}"
                                if st.button("⚙️", key=f"edit_btn_{row['mapping_id']}", help="Edit Mapping"):
                                    st.session_state[edit_key] = True
                                    st.rerun()
                        with col8:
                            # Delete button
                            if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "delete"):
                                confirm_key = f"confirm_delete_mapping_{row['mapping_id']}"
                                
                                if confirm_key in st.session_state and st.session_state[confirm_key]:
                                    # Show confirmation buttons
                                    if st.button("✅", key=f"yes_del_{row['mapping_id']}", help="Confirm Delete"):
                                        success, message = role_manager.delete_user_role_mapping(row['mapping_id'])
                                        if success:
                                            st.success(message)
                                            del st.session_state[confirm_key]
                                            st.rerun()
                                        else:
                                            st.error(message)
                                            del st.session_state[confirm_key]
                                    
                                    if st.button("❌", key=f"no_del_{row['mapping_id']}", help="Cancel Delete"):
                                        del st.session_state[confirm_key]
                                        st.rerun()
                                else:
                                    if st.button("🗑️", key=f"del_btn_{row['mapping_id']}", help="Delete Mapping"):
                                        st.session_state[confirm_key] = True
                                        st.rerun()
                    
                    st.markdown("---")
                    
                    # Show edit forms if requested
                    for index, row in mappings.iterrows():
                        edit_key = f"edit_mapping_{row['mapping_id']}"
                        if edit_key in st.session_state and st.session_state[edit_key]:
                            st.markdown(f"### ⚙️ Edit Mapping for: **{row['user_name']}**")
                            
                            # Get available users and role groups for dropdowns
                            all_users = role_manager.get_all_users()
                            all_role_groups = role_manager.get_all_role_groups()
                            
                            with st.form(f"edit_mapping_form_{row['mapping_id']}"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    # User dropdown
                                    user_options = [f"{user['name']} ({user['email']})" for _, user in all_users.iterrows()]
                                    current_user_option = f"{row['user_name']} ({row['email']})"
                                    try:
                                        user_index = user_options.index(current_user_option)
                                    except ValueError:
                                        user_index = 0
                                    
                                    selected_user = st.selectbox(
                                        "Select User",
                                        user_options,
                                        index=user_index,
                                        key=f"mapping_edit_user_{row['mapping_id']}"
                                    )
                                    
                                    # Extract user ID from selection
                                    selected_user_email = selected_user.split('(')[1].split(')')[0]
                                    selected_user_row = all_users[all_users['email'] == selected_user_email]
                                    selected_user_id = selected_user_row.iloc[0]['id'] if not selected_user_row.empty else None
                                
                                with col2:
                                    # Role group dropdown
                                    group_options = all_role_groups['group_name'].tolist()
                                    try:
                                        group_index = group_options.index(row['group_name'])
                                    except ValueError:
                                        group_index = 0
                                    
                                    selected_group = st.selectbox(
                                        "Select Role Group",
                                        group_options,
                                        index=group_index,
                                        key=f"mapping_edit_group_{row['mapping_id']}"
                                    )
                                    
                                    # Get group ID
                                    selected_group_row = all_role_groups[all_role_groups['group_name'] == selected_group]
                                    selected_group_id = selected_group_row.iloc[0]['id'] if not selected_group_row.empty else None
                                
                                # Team and Status in second row
                                col3, col4 = st.columns(2)
                                with col3:
                                    # Team dropdown
                                    team_options = ["Sales", "Talent", "Professional Services", "Biz Support"]
                                    current_team = row.get('team', '')
                                    try:
                                        team_index = team_options.index(current_team) if current_team in team_options else 0
                                    except (ValueError, TypeError):
                                        team_index = 0
                                    
                                    selected_team = st.selectbox(
                                        "Select Team",
                                        team_options,
                                        index=team_index,
                                        key=f"mapping_edit_team_{row['mapping_id']}"
                                    )
                                
                                with col4:
                                    # Status
                                    mapping_status = st.selectbox(
                                        "Status",
                                        ["Active", "Inactive"],
                                        index=0 if row['mapping_status'] == 'Active' else 1,
                                        key=f"mapping_edit_status_{row['mapping_id']}"
                                    )
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    save_mapping = st.form_submit_button("💾 Save Changes", type="primary")
                                with col2:
                                    cancel_mapping = st.form_submit_button("❌ Cancel")
                                
                                if save_mapping and selected_user_id and selected_group_id:
                                    success, message = role_manager.update_user_role_mapping(
                                        row['mapping_id'],
                                        selected_user_id,
                                        selected_group_id,
                                        mapping_status.lower(),
                                        selected_team
                                    )
                                    if success:
                                        st.success(message)
                                        del st.session_state[edit_key]
                                        st.rerun()
                                    else:
                                        st.error(message)
                                elif save_mapping:
                                    st.error("Please select valid user and role group")
                                
                                if cancel_mapping:
                                    del st.session_state[edit_key]
                                    st.rerun()
                            
                            st.markdown("---")
                            break  # Only show one edit form at a time
                    
                    # Add new mapping form
                    if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "add"):
                        st.markdown("### ➕ Add New User-Role Mapping")
                        
                        # Get available users and role groups
                        all_users = role_manager.get_all_users()
                        all_role_groups = role_manager.get_all_role_groups()
                        
                        if not all_users.empty and not all_role_groups.empty:
                            with st.form("add_mapping_form"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    # User dropdown - start with empty selection
                                    user_options = ["-- Select User --"] + [f"{user['name']} ({user['email']})" for _, user in all_users.iterrows()]
                                    selected_user = st.selectbox("Select User", user_options, index=0, key="add_mapping_user_select")
                                    
                                    # Extract user ID only if valid selection made
                                    selected_user_id = None
                                    if selected_user != "-- Select User --":
                                        selected_user_email = selected_user.split('(')[1].split(')')[0]
                                        selected_user_row = all_users[all_users['email'] == selected_user_email]
                                        selected_user_id = selected_user_row.iloc[0]['id'] if not selected_user_row.empty else None
                                
                                with col2:
                                    # Role group dropdown - start with empty selection
                                    group_options = ["-- Select Role Group --"] + all_role_groups['group_name'].tolist()
                                    selected_group = st.selectbox("Select Role Group", group_options, index=0, key="add_mapping_group_select")
                                    
                                    # Get group ID only if valid selection made
                                    selected_group_id = None
                                    if selected_group != "-- Select Role Group --":
                                        selected_group_row = all_role_groups[all_role_groups['group_name'] == selected_group]
                                        selected_group_id = selected_group_row.iloc[0]['id'] if not selected_group_row.empty else None
                                
                                # Team and Status in second row
                                col3, col4 = st.columns(2)
                                with col3:
                                    # Team dropdown
                                    team_options = ["Sales", "Talent", "Professional Services", "Biz Support"]
                                    selected_team = st.selectbox("Select Team", team_options, index=0, key="add_mapping_team_select")
                                
                                with col4:
                                    # Status
                                    mapping_status = st.selectbox("Status", ["Active", "Inactive"], index=0, key="add_mapping_status_select")
                                
                                if st.form_submit_button("➕ Add Mapping", type="primary"):
                                    if selected_user_id and selected_group_id:
                                        success, message = role_manager.create_user_role_mapping(
                                            selected_user_id,
                                            selected_group_id,
                                            mapping_status.lower(),
                                            selected_team
                                        )
                                        if success:
                                            st.success(message)
                                            # Clear any session state that might persist form data
                                            for key in list(st.session_state.keys()):
                                                if 'add_mapping_form' in key:
                                                    del st.session_state[key]
                                            st.rerun()
                                        else:
                                            st.error(message)
                                    else:
                                        st.error("Please select both user and role group to create mapping")
                        else:
                            st.warning("⚠️ No users or role groups available. Please create users and role groups first.")
                else:
                    st.info("No user-role mappings found. Add your first mapping using the form below.")
                    
                    # Add new mapping form when no mappings exist
                    if permission_manager.has_permission(current_user_email, "Settings", "Roles & Role Groups", "add"):
                        st.markdown("### ➕ Add New User-Role Mapping")
                        
                        # Get available users and role groups
                        all_users = role_manager.get_all_users()
                        all_role_groups = role_manager.get_all_role_groups()
                        
                        if not all_users.empty and not all_role_groups.empty:
                            with st.form("add_first_mapping_form"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    # User dropdown - start with empty selection
                                    user_options = ["-- Select User --"] + [f"{user['name']} ({user['email']})" for _, user in all_users.iterrows()]
                                    selected_user = st.selectbox("Select User", user_options, index=0, key="first_form_user")
                                    
                                    # Extract user ID only if valid selection made
                                    selected_user_id = None
                                    if selected_user != "-- Select User --":
                                        selected_user_email = selected_user.split('(')[1].split(')')[0]
                                        selected_user_row = all_users[all_users['email'] == selected_user_email]
                                        selected_user_id = selected_user_row.iloc[0]['id'] if not selected_user_row.empty else None
                                
                                with col2:
                                    # Role group dropdown - start with empty selection
                                    group_options = ["-- Select Role Group --"] + all_role_groups['group_name'].tolist()
                                    selected_group = st.selectbox("Select Role Group", group_options, index=0, key="first_form_group")
                                    
                                    # Get group ID only if valid selection made
                                    selected_group_id = None
                                    if selected_group != "-- Select Role Group --":
                                        selected_group_row = all_role_groups[all_role_groups['group_name'] == selected_group]
                                        selected_group_id = selected_group_row.iloc[0]['id'] if not selected_group_row.empty else None
                                
                                # Team and Status in second row
                                col3, col4 = st.columns(2)
                                with col3:
                                    # Team dropdown
                                    team_options = ["Sales", "Talent", "Professional Services", "Biz Support"]
                                    selected_team = st.selectbox("Select Team", team_options, index=0, key="first_form_team")
                                
                                with col4:
                                    # Status
                                    mapping_status = st.selectbox("Status", ["Active", "Inactive"], index=0, key="first_form_status")
                                
                                if st.form_submit_button("➕ Add Mapping", type="primary"):
                                    if selected_user_id and selected_group_id:
                                        success, message = role_manager.create_user_role_mapping(
                                            selected_user_id,
                                            selected_group_id,
                                            mapping_status.lower(),
                                            selected_team
                                        )
                                        if success:
                                            st.success(message)
                                            # Clear any session state that might persist form data
                                            for key in list(st.session_state.keys()):
                                                if 'first_form' in key or 'add_first_mapping_form' in key:
                                                    del st.session_state[key]
                                            st.rerun()
                                        else:
                                            st.error(message)
                                    else:
                                        st.error("Please select both user and role group to create mapping")
                        else:
                            st.warning("⚠️ No users or role groups available. Please create users and role groups first.")
            

        else:
            permission_manager.show_access_denied_message("Settings", "Roles & Role Groups")
    
    # Tab 3: Database Status
    with tab3:
        st.subheader("🗄️ Database Status")
        col1, col2 = st.columns(2)
        
        with col1:
            # Database connection status
            db_status = st.session_state.db_manager.check_connection()
            if db_status:
                st.success("🔗 Database Connected")
            else:
                st.error("❌ Database Disconnected")
        
        with col2:
            # Database statistics
            db_stats = st.session_state.db_manager.get_database_stats()
            if db_stats:
                st.metric("Total Datasets", db_stats.get('historical_data', 0))
                st.metric("Total Forecasts", db_stats.get('forecasts', 0))
                st.metric("Total Scenarios", db_stats.get('scenarios', 0))
    
    # Tab 4: Application Settings  
    with tab4:
        st.subheader("⚙️ Application Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Data Settings**")
            default_forecast_periods = st.number_input("Default Forecast Periods", value=30, min_value=1, max_value=365)
            auto_save = st.checkbox("Auto-save data", value=True)
            data_validation = st.checkbox("Strict data validation", value=True)
        
        with col2:
            st.markdown("**Display Settings**")
            chart_theme = st.selectbox("Chart Theme", ["Light", "Dark", "Auto"])
            show_confidence_intervals = st.checkbox("Show confidence intervals by default", value=True)
            decimal_places = st.slider("Decimal places for metrics", 0, 5, 2)
        
        st.subheader("Advanced Settings")
        max_data_points = st.number_input("Maximum data points to process", value=10000, min_value=100)
        calculation_precision = st.selectbox("Calculation Precision", ["Standard", "High", "Maximum"])
        
        if st.button("💾 Save Application Settings"):
            st.success("Settings saved successfully!")
            st.session_state.settings = {
                'default_forecast_periods': default_forecast_periods,
                'auto_save': auto_save,
                'data_validation': data_validation,
                'chart_theme': chart_theme,
                'show_confidence_intervals': show_confidence_intervals,
                'decimal_places': decimal_places,
                'max_data_points': max_data_points,
                'calculation_precision': calculation_precision
            }
    
    # Tab 5: Export Settings
    with tab5:
        st.subheader("📊 Export Settings")
        
        st.markdown("**Export Options:**")
        col1, col2 = st.columns(2)
        
        with col1:
            export_format = st.selectbox("Default Export Format", ["CSV", "Excel", "JSON"])
            include_metadata = st.checkbox("Include metadata in exports", value=True)
            
        with col2:
            date_format = st.selectbox("Date Format", ["YYYY-MM-DD", "DD/MM/YYYY", "MM/DD/YYYY"])
            encoding = st.selectbox("File Encoding", ["UTF-8", "UTF-16", "ASCII"])
        
        st.markdown("**Export Data Options:**")
        export_historical = st.checkbox("Export historical data", value=True)
        export_forecasts = st.checkbox("Export forecast data", value=True)
        export_scenarios = st.checkbox("Export scenario data", value=True)
        
        if st.button("💾 Save Export Settings"):
            st.success("Export settings saved successfully!")
    
    # Tab 6: Environment Settings  
    with tab6:
        st.subheader("🌍 Environment Management")
        
        # Import environment manager
        from utils.environment_manager import EnvironmentManager
        env_manager = EnvironmentManager()
        
        # Current environment status
        current_env = "Development" if env_manager.is_development() else "Production"
        table_prefix = env_manager.get_table_prefix()
        
        st.info(f"**Current Environment:** {current_env}")
        st.info(f"**Table Prefix:** {table_prefix}")
        
        # Environment information
        st.markdown("""
        **Environment Details:**
        - **Development Environment**: Uses `dev_` table prefixes for safe testing
        - **Production Environment**: Uses live data tables without prefixes
        - **Data Isolation**: Development changes do not affect production data
        """)
        
        # Environment switching controls (admin only)
        if is_admin:
            st.markdown("---")
            st.subheader("🔄 Environment Control")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🧪 Switch to Development", 
                           disabled=env_manager.is_development(),
                           help="Switch to development environment for safe testing"):
                    st.info("Environment switching requires deployment restart")
                    
            with col2:
                if st.button("🚀 Switch to Production", 
                           disabled=env_manager.is_production(),
                           help="Switch to production environment - CAUTION: Live data"):
                    st.warning("Environment switching requires deployment restart")
        
        # Database connection info
        st.markdown("---")
        st.subheader("🗄️ Database Information")
        
        database_info = {
            "Environment": current_env,
            "Table Prefix": table_prefix,
            "Database URL": "Connected" if env_manager.get_database_url() else "Not Connected"
        }
        
        info_df = pd.DataFrame(list(database_info.items()), columns=['Setting', 'Value'])
        st.dataframe(info_df, use_container_width=True)
    
    # Tab 7: Google Sheets Integration
    with tab7:
        st.subheader("📊 Google Sheets Data Sync")
        
        # Import Google Sheets manager
        from utils.google_sheets_manager import sheets_manager
        import pytz
        
        # Get sync status
        sync_status = sheets_manager.get_sync_status()
        
        # Display configuration info
        st.markdown("### 📋 Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"""
            **Spreadsheet ID:** {sync_status.get('spreadsheet_id', 'Not configured')}
            **Table Name:** {sync_status.get('table_name', 'Not configured')}
            **Data Records:** {sync_status.get('data_count', 0)}
            """)
        
        with col2:
            st.info(f"""
            **Current Time (IST):** {sync_status.get('current_time', 'Unknown')}
            **Last Sync:** {sync_status.get('last_sync', 'Never')}
            **Next Sync:** {sync_status.get('next_sync', 'Unknown')}
            """)
        
        # Check OAuth credentials status
        has_oauth_secrets = bool(os.getenv('GOOGLE_OAUTH_SECRETS'))
        
        if not has_oauth_secrets:
            st.error("🔑 Google OAuth credentials not configured in environment secrets.")
            st.markdown("""
            **Required Setup:**
            - Add `GOOGLE_OAUTH_SECRETS` to Replit secrets
            - Must contain valid Google Cloud Console OAuth 2.0 credentials
            """)
        else:
            st.success("✅ OAuth credentials configured")
            
            # Authentication status
            if sync_status.get('authenticated', False):
                st.success("✅ Google Sheets API authenticated and ready")
                
                # Add reset option even when authenticated
                if st.button("🔄 Reset Authentication", help="Clear stored credentials and start fresh"):
                    sheets_manager.reset_authentication()
                    st.success("Authentication reset - please authenticate again")
                    st.rerun()
                    
            else:
                st.warning("⚠️ Google Sheets authentication required")
                
                # Check for OAuth callback detected in main app
                if st.session_state.get('oauth_callback_detected', False):
                    st.info("🎉 OAuth callback detected - processing authorization automatically...")
                    
                    auth_code = st.session_state.get('oauth_auth_code')
                    oauth_state = st.session_state.get('oauth_state')
                    
                    if auth_code and oauth_state:
                        with st.spinner("Completing authentication automatically..."):
                            success, message = sheets_manager.complete_oauth_flow(auth_code, oauth_state)
                            if success:
                                st.success(f"✅ {message}")
                                # Clear OAuth callback session state
                                for key in ['oauth_callback_detected', 'oauth_auth_code', 'oauth_state']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error(f"❌ Authentication failed: {message}")
                                st.info("You can try the manual process below if needed.")
                        # Clear the callback detection flag
                        if 'oauth_callback_detected' in st.session_state:
                            del st.session_state['oauth_callback_detected']
                
                # Add reset option for failed authentication
                col_warn1, col_warn2 = st.columns([3, 1])
                with col_warn2:
                    if st.button("🔄 Reset", help="Clear any stuck authentication state"):
                        sheets_manager.reset_authentication()
                        st.success("Authentication reset")
                        st.rerun()
                
                # Show OAuth configuration diagnostic info
                st.markdown("### 🔍 OAuth Configuration Diagnostic")
                
                current_app_url = sheets_manager.get_current_app_url()
                
                st.info(f"""
                **Configured Redirect URI:** `{current_app_url}`
                
                **To fix the "invalid_client" error:**
                1. Go to your Google Cloud Console OAuth 2.0 Client IDs
                2. Ensure this exact URL is in "Authorized redirect URIs": `{current_app_url}`
                3. Save the configuration
                4. Wait 5-10 minutes for changes to propagate
                5. Try authentication again
                
                **Status:** Using hardcoded redirect URI as requested
                """)
                
                # Check if this is OAuth2 or Service Account
                if sheets_manager.oauth_config and sheets_manager.oauth_config.get('type') == 'service_account':
                    if st.button("🔑 Initialize Service Account Access"):
                        success, message = sheets_manager.authenticate_with_existing_flow()
                        if success:
                            st.success(f"✅ {message}")
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                else:
                    # AUTOMATIC OAUTH CALLBACK DETECTION
                    query_params = st.query_params
                    oauth_code = query_params.get('code')
                    
                    # If we have OAuth code, automatically process it 
                    if oauth_code:
                        st.info("🎉 Google OAuth callback detected - processing automatically...")
                        
                        with st.spinner("Completing Google Sheets authentication..."):
                            # Automatically complete OAuth flow
                            success, message = sheets_manager.complete_oauth_flow(oauth_code)
                            if success:
                                st.success(f"✅ {message}")
                                st.balloons()
                                # Clear query parameters and refresh to clean URL
                                st.query_params.clear()
                                st.rerun()
                            else:
                                st.error(f"❌ Authentication failed: {message}")
                                st.info("Please try the authentication process again.")
                    
                    # OAuth2 flow
                    if 'oauth_auth_url' not in st.session_state and not (oauth_code and oauth_state):
                        if st.button("🔑 Get Authentication URL", key="get_auth_url_btn"):
                            success, result = sheets_manager.authenticate_with_existing_flow()
                            if success:
                                st.session_state.oauth_auth_url = result
                                st.success("✅ Authentication URL generated successfully!")
                                st.rerun()
                            else:
                                st.error(f"❌ Authentication setup failed: {result}")
                    
                    if 'oauth_auth_url' in st.session_state:
                        st.info("**Complete these steps to enable Google Sheets access:**")
                        
                        st.markdown(f"""
                        **Step 1:** [Click here to authorize Google Sheets access]({st.session_state.oauth_auth_url})
                        
                        **Step 2:** You'll be redirected to your app URL. Copy the `code=` parameter from the URL
                        
                        Example: If redirected to `https://your-app.replit.app/?code=ABC123&state=xyz`  
                        Copy only the `ABC123` part after `code=`
                        """)
                        
                        # Check if we have auto-filled code from URL
                        default_code = st.session_state.get('auth_code_input', '')
                        authorization_code = st.text_input(
                            "Authorization Code:",
                            value=default_code,
                            placeholder="Will auto-fill when you return from Google authorization",
                            key="auth_code_manual_input",
                            help="After clicking the link above, the code will be automatically filled here"
                        )
                        
                        col1, col2 = st.columns([1, 1])
                        with col1:
                            # Single button for authentication
                            if authorization_code.strip():
                                if st.button("✅ Complete Authentication", key="complete_auth_btn"):
                                    with st.spinner("Completing authentication..."):
                                        # For manual flow, we don't have the state parameter from URL
                                        success, message = sheets_manager.complete_oauth_flow(authorization_code.strip())
                                        if success:
                                            st.success(f"✅ {message}")
                                            # Clear the auth URL and refresh
                                            if 'oauth_auth_url' in st.session_state:
                                                del st.session_state.oauth_auth_url
                                            if 'auth_code_input' in st.session_state:
                                                del st.session_state.auth_code_input
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {message}")
                            else:
                                st.button("✅ Complete Authentication", key="complete_auth_btn_disabled", disabled=True, help="Please paste the authorization code first")
                                
                        with col2:
                            if st.button("🔄 Start Over", key="restart_auth_btn"):
                                # Clear session state and reset authentication
                                sheets_manager.reset_authentication()
                                if 'oauth_auth_url' in st.session_state:
                                    del st.session_state.oauth_auth_url
                                if 'auth_code_input' in st.session_state:
                                    del st.session_state.auth_code_input
                                st.success("Authentication state reset - you can start fresh now")
                                st.rerun()
        
        # Data Operations Section
        if has_oauth_secrets:
            st.markdown("---")
            st.markdown("### 🔄 Data Operations")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if sync_status.get('authenticated', False):
                    if st.button("📥 Sync Data Now", type="primary"):
                        with st.spinner("Syncing data from Google Sheets..."):
                            success, message = sheets_manager.sync_data_now()
                            
                            if success:
                                st.success(f"✅ {message}")
                                # Show preview of synced data
                                preview_df, preview_msg = sheets_manager.get_data_preview()
                                if preview_df is not None:
                                    st.markdown("**Data Preview:**")
                                    st.dataframe(preview_df.head(5), use_container_width=True)
                            else:
                                st.error(f"❌ {message}")
                else:
                    st.info("Complete authentication above to enable sync")
            
            with col2:
                if st.button("🕐 Check Scheduled Sync"):
                    should_run, check_message = sheets_manager.check_scheduled_sync()
                    
                    if should_run:
                        with st.spinner("Running scheduled sync..."):
                            success, sync_message = sheets_manager.sync_data_now()
                            if success:
                                st.success(f"✅ Scheduled sync completed: {sync_message}")
                            else:
                                st.error(f"❌ Scheduled sync failed: {sync_message}")
                    else:
                        st.info(f"ℹ️ {check_message}")
            
            with col3:
                if st.button("📊 View Data Preview"):
                    preview_df, preview_msg = sheets_manager.get_data_preview(limit=20)
                    
                    if preview_df is not None:
                        st.success(f"✅ {preview_msg}")
                        st.dataframe(preview_df, use_container_width=True)
                    else:
                        st.warning(f"⚠️ {preview_msg}")
            
            # Add cleanup option for old records
            st.markdown("---")
            st.markdown("### 🧹 Data Maintenance")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🧹 Clean Old Records"):
                    with st.spinner("Cleaning up old and duplicate records..."):
                        cleanup_success, cleanup_message = sheets_manager.cleanup_old_records()
                        if cleanup_success:
                            st.success(f"✅ {cleanup_message}")
                        else:
                            st.error(f"❌ {cleanup_message}")
            
            with col2:
                st.info("**Note:** Cleanup removes old duplicate records while keeping recent unique data from the last 30 days.")
            
            # Automatic scheduled sync check (runs silently)
            should_run, _ = sheets_manager.check_scheduled_sync()
            if should_run:
                # Run scheduled sync automatically
                success, message = sheets_manager.sync_data_now()
                if success:
                    st.success("🔄 Automated scheduled sync completed successfully!")
                else:
                    st.error(f"❌ Automated sync failed: {message}")
            
            # Data Source Information
            st.markdown("---")
            st.markdown("### 📄 Data Source Information")
            
            st.markdown(f"""
            **Google Sheets URL:** [Open Live Spreadsheet](https://docs.google.com/spreadsheets/d/1HKbqpMo5oGBNy-N-wmjWfruEVawadplTF2h4wrFCKg0/edit?gid=1206139251#gid=1206139251)
            
            **Configuration Details:**
            - **Spreadsheet ID:** `1HKbqpMo5oGBNy-N-wmjWfruEVawadplTF2h4wrFCKg0`
            - **Sheet Range:** `DataAggregator!A:Z` (all columns from DataAggregator sheet)
            - **Database Table:** `DataAggregator`
            - **Sync Schedule:** Daily at 8:00 PM IST (automatic)
            - **Data Processing:** Incremental updates - only new/changed rows are added
            - **Data Refresh:** Smart sync preserves existing data, appends only updates
            """)
            
            # Sync History and Status
            st.markdown("---")
            st.markdown("### 📈 Sync Status")
            
            if sync_status.get('data_count', 0) > 0:
                st.metric("Total Records Synced", sync_status.get('data_count', 0))
                
                # Show last sync details
                last_sync = sync_status.get('last_sync', 'Never')
                if last_sync != 'Never':
                    try:
                        sync_time = datetime.strptime(last_sync.replace(' IST', ''), '%Y-%m-%d %H:%M:%S')
                        time_diff = datetime.now() - sync_time
                        
                        if time_diff.days > 0:
                            time_ago = f"{time_diff.days} days ago"
                        elif time_diff.seconds > 3600:
                            time_ago = f"{time_diff.seconds // 3600} hours ago"
                        else:
                            time_ago = f"{time_diff.seconds // 60} minutes ago"
                        
                        st.info(f"📅 Last synced: {last_sync} ({time_ago})")
                    except:
                        st.info(f"📅 Last synced: {last_sync}")
                else:
                    st.warning("⚠️ No sync history found")
            else:
                st.warning("⚠️ No data synced yet. Click 'Sync Data Now' to start.")
            
            # Show sync schedule
            ist_tz = pytz.timezone('Asia/Kolkata')
            current_ist = datetime.now(ist_tz)
            next_sync = current_ist.replace(hour=20, minute=0, second=0, microsecond=0)
            if current_ist.hour >= 20:
                next_sync += timedelta(days=1)
            
            hours_until_sync = (next_sync - current_ist).total_seconds() / 3600
            st.info(f"⏰ Next automatic sync in {hours_until_sync:.1f} hours at {next_sync.strftime('%Y-%m-%d 20:00 IST')}")
            
            # Show scheduler status
            scheduler_status = data_sync_scheduler.get_scheduler_status()
            if scheduler_status['is_running']:
                st.success(f"✅ **Automatic Scheduler**: Running successfully")
                st.caption(f"Current time: {scheduler_status['current_time']}")
                st.caption(f"Next scheduled sync: {scheduler_status['next_sync']}")
                if scheduler_status['last_sync'] != 'Never':
                    st.caption(f"Last automatic sync: {scheduler_status['last_sync']}")
            else:
                st.warning("⚠️ **Automatic Scheduler**: Not running")
                st.caption("Manual sync only - automatic nightly sync at 8 PM IST is disabled")

def player_boards_page():
    st.header("🏆 Player Boards")
    st.markdown("---")
    
    st.info("🚧 **Coming Soon**: Interactive player boards and gamification features")
    
    st.markdown("""
    This section will include:
    - **Forecast Accuracy Leaderboards**: Compare forecasting performance across teams
    - **Challenges**: Weekly and monthly forecasting competitions
    - **Achievement Badges**: Earn rewards for accurate predictions
    - **Team Collaboration**: Collaborative forecasting and planning
    - **Performance Tracking**: Individual and team performance metrics
    """)
    
    # Placeholder leaderboard
    st.subheader("🎯 Forecast Accuracy Leaderboard (Demo)")
    
    demo_data = pd.DataFrame({
        'Rank': [1, 2, 3, 4, 5],
        'Player': ['Alex Chen', 'Sarah Johnson', 'Mike Rodriguez', 'Emily Davis', 'John Smith'],
        'Team': ['Planning', 'Analytics', 'Planning', 'Sales', 'Analytics'],
        'Accuracy Score': [95.2, 92.8, 90.1, 87.5, 85.3],
        'Forecasts Made': [15, 12, 18, 10, 8],
        'Badge': ['🥇 Master', '🥈 Expert', '🥉 Advanced', '⭐ Proficient', '📈 Learning']
    })
    
    st.dataframe(demo_data, use_container_width=True)

def sales_dashboard_section():
    st.header("📈 Sales Dashboard Analysis")
    
    st.markdown("""
    Upload and analyze your sales dashboard data to extract insights, view performance metrics, 
    and prepare data for forecasting. This section handles complex sales pipeline data with 
    multiple time periods and metric types.
    """)
    
    # Initialize sales dashboard processor
    sales_processor = SalesDashboardProcessor()
    
    # File upload for sales dashboard
    st.subheader("📂 Upload Sales Dashboard Data")
    uploaded_sales_file = st.file_uploader(
        "Choose Sales Dashboard CSV file",
        type="csv",
        help="Upload your sales dashboard data in CSV format",
        key="sales_dashboard_upload"
    )
    
    if uploaded_sales_file is not None:
        try:
            with st.spinner("Processing sales dashboard data..."):
                # Save uploaded file temporarily and process
                temp_file_path = f"temp_sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                with open(temp_file_path, "wb") as f:
                    f.write(uploaded_sales_file.getbuffer())
                
                # Process the data
                processed_data = sales_processor.load_sales_dashboard_data(temp_file_path)
                
                # Clean up temp file
                import os
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
            
            if processed_data is not None and not processed_data.empty:
                st.success(f"✅ Sales dashboard data processed successfully! Found {len(processed_data)} records.")
                
                # Store processed data in session state
                st.session_state.sales_data = processed_data
                
                # Display summary metrics
                st.subheader("📊 Sales Data Summary")
                summary_metrics = sales_processor.get_summary_metrics(processed_data)
                
                if summary_metrics:
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Accounts", summary_metrics.get('account_count', 0))
                    with col2:
                        st.metric("Regions", summary_metrics.get('region_count', 0))
                    with col3:
                        st.metric("Domains", summary_metrics.get('domain_count', 0))
                    with col4:
                        date_range = summary_metrics.get('date_range', {})
                        if date_range:
                            date_span = f"{date_range.get('start', 'N/A').strftime('%Y-%m') if date_range.get('start') else 'N/A'} to {date_range.get('end', 'N/A').strftime('%Y-%m') if date_range.get('end') else 'N/A'}"
                            st.metric("Date Range", date_span)
                
                # Show metric breakdown
                metric_summary = summary_metrics.get('metric_summary', {})
                if metric_summary:
                    st.subheader("💰 Metric Summary")
                    metric_df = pd.DataFrame([
                        {'Metric Type': metric, 'Total Value': f"${value:,.2f}" if value != 0 else "$0.00"}
                        for metric, value in metric_summary.items()
                    ])
                    st.dataframe(metric_df, use_container_width=True)
                
                # Data preview
                st.subheader("🔍 Data Preview")
                st.dataframe(processed_data.head(20), use_container_width=True)
                
                # Regional analysis
                region_breakdown = summary_metrics.get('region_breakdown', {})
                if region_breakdown:
                    st.subheader("🌍 Regional Performance")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        region_df = pd.DataFrame([
                            {'Region': region, 'Total Value': f"${value:,.2f}"}
                            for region, value in region_breakdown.items() if value != 0
                        ]).sort_values('Total Value', ascending=False)
                        st.dataframe(region_df, use_container_width=True)
                    
                    with col2:
                        # Pie chart for regional distribution
                        region_values = {k: v for k, v in region_breakdown.items() if v > 0}
                        if region_values:
                            import plotly.express as px
                            fig = px.pie(
                                values=list(region_values.values()),
                                names=list(region_values.keys()),
                                title="Regional Distribution"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                
                # Domain analysis
                domain_breakdown = summary_metrics.get('domain_breakdown', {})
                if domain_breakdown:
                    st.subheader("🏢 Domain Performance")
                    domain_values = {k: v for k, v in domain_breakdown.items() if v > 0}
                    if domain_values:
                        import plotly.express as px
                        fig = px.bar(
                            x=list(domain_values.keys()),
                            y=list(domain_values.values()),
                            title="Domain Performance",
                            labels={'x': 'Domain', 'y': 'Total Value ($)'}
                        )
                        fig.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)
                
                # Time series analysis options
                st.subheader("📈 Time Series Analysis")
                
                available_metrics = processed_data['Metric_Type'].unique()
                selected_metric = st.selectbox(
                    "Select Metric for Time Series Analysis",
                    available_metrics,
                    help="Choose which metric to analyze over time"
                )
                
                if selected_metric:
                    time_series_data = sales_processor.prepare_time_series_data(processed_data, selected_metric)
                    
                    if time_series_data is not None and not time_series_data.empty:
                        st.subheader(f"📊 {selected_metric} Over Time")
                        
                        # Plot time series
                        import plotly.express as px
                        fig = px.line(
                            x=time_series_data.index,
                            y=time_series_data['Value'],
                            title=f"{selected_metric} Trend Over Time",
                            labels={'x': 'Date', 'y': f'{selected_metric} Value ($)'}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Option to use this data for forecasting
                        st.info("💡 You can use this processed time series data for forecasting in the Forecasting tab!")
                        
                        if st.button("Use for Forecasting", type="primary"):
                            # Prepare data for forecasting
                            forecast_data = time_series_data.copy()
                            forecast_data.columns = [selected_metric]
                            
                            # Store in session state for forecasting
                            st.session_state.data = forecast_data
                            st.session_state.demand_column = selected_metric
                            st.session_state.grouping_columns = []
                            
                            st.success(f"✅ Sales data ({selected_metric}) is now ready for forecasting!")
                            st.balloons()
                    else:
                        st.warning(f"No time series data available for {selected_metric}")
                
                # Account performance analysis
                st.subheader("📋 Account Performance Analysis")
                account_performance = sales_processor.get_account_performance_data(processed_data)
                
                if account_performance is not None and not account_performance.empty:
                    # Display top performing accounts
                    st.markdown("**Top Performing Accounts**")
                    top_accounts = account_performance.head(10)
                    st.dataframe(top_accounts, use_container_width=True)
                    
                    # Export option
                    st.subheader("💾 Export Analysis")
                    if st.button("Export Processed Data"):
                        # Convert to CSV for download
                        csv_data = processed_data.to_csv(index=False)
                        st.download_button(
                            label="Download Processed Sales Data",
                            data=csv_data,
                            file_name=f"processed_sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                        st.success("📁 Data ready for download!")
                
            else:
                st.error("❌ Failed to process sales dashboard data. Please check the file format.")
                
        except Exception as e:
            st.error(f"❌ Error processing sales dashboard: {str(e)}")
            st.expander("Error Details").write(str(e))
    
    # Display existing sales data if available
    if hasattr(st.session_state, 'sales_data') and st.session_state.sales_data is not None:
        st.subheader("📊 Current Sales Data")
        st.info(f"Loaded sales data with {len(st.session_state.sales_data)} records")
        
        # Quick metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Accounts", st.session_state.sales_data['Account'].nunique())
        with col2:
            st.metric("Time Periods", st.session_state.sales_data['Date'].nunique())
        with col3:
            st.metric("Total Value", f"${st.session_state.sales_data['Value'].sum():,.2f}")

def display_pipeline_generation_interface():
    """Display comprehensive pipeline generation UI for each staffing plan role"""
    st.markdown("---")
    st.subheader("🔮 Pipeline Generation for Supply Planning")
    
    # Get pipeline planning data from session state
    pipeline_planning_data = st.session_state.get('pipeline_planning_data', [])
    
    if not pipeline_planning_data:
        st.warning("No pipeline planning data available. Please add roles to the Pipeline Planning Table first.")
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("✅ Done", key="close_pipeline_generation"):
                st.session_state.show_pipeline_generation = False
                if 'supply_plan_auto_loaded' in st.session_state:
                    del st.session_state['supply_plan_auto_loaded']
                if 'current_plan_name' in st.session_state:
                    del st.session_state['current_plan_name']
                if 'current_from_date' in st.session_state:
                    del st.session_state['current_from_date']
                if 'current_to_date' in st.session_state:
                    del st.session_state['current_to_date']
                st.rerun()
        return
    
    # Check if we're in edit mode and auto-load existing supply plan data
    is_edit_mode = 'edit_staffing_plan_id' in st.session_state
    if is_edit_mode and not st.session_state.get('supply_plan_auto_loaded', False):
        edit_plan_id = st.session_state['edit_staffing_plan_id']
        
        # Import StaffingPlansManager early for auto-loading
        from utils.staffing_plans_manager import StaffingPlansManager
        staffing_manager = StaffingPlansManager(env_manager)
        
        # Check if there's already saved supply plan data
        existing_details = staffing_manager.get_pipeline_planning_details(edit_plan_id)
        if existing_details and len(existing_details) > 0:
            st.session_state.supply_plan_auto_loaded = True
    
    # Import required managers
    try:
        from utils.pipeline_manager import PipelineManager
        from utils.staffing_plans_manager import StaffingPlansManager
        
        pipeline_manager = PipelineManager(env_manager)
        staffing_manager = StaffingPlansManager(env_manager)
        
        # Get staffing plan details from current workflow
        plan_name = st.session_state.get('current_plan_name', 'Current Plan')
        from_date = st.session_state.get('current_from_date')
        to_date = st.session_state.get('current_to_date')
        
        # Format dates for display
        from_date_str = from_date.strftime('%Y-%m-%d') if from_date else 'Not Set'
        to_date_str = to_date.strftime('%Y-%m-%d') if to_date else 'Not Set'
        
        st.info(f"**Plan:** {plan_name} | **Period:** {from_date_str} to {to_date_str}")
        
        # Display pipeline generation for each role
        st.markdown("**Supply Plan Generation by Role:**")
        
        for i, role_data in enumerate(pipeline_planning_data):
            role = role_data.get('role', f'Role {i+1}')
            skills = role_data.get('skills', '')
            positions = role_data.get('positions', 1)
            onboard_by = role_data.get('onboard_by')
            pipeline_id = role_data.get('pipeline_id')
            
            with st.expander(f"🎯 {role} - Supply Plan ({positions} position{'s' if positions != 1 else ''})", expanded=True):
                
                # Role Information
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Positions Required", positions)
                with col2:
                    st.metric("Staff By Date", str(onboard_by) if onboard_by else "Not Set")
                with col3:
                    skills_display = skills if skills else "Not specified"
                    st.write(f"**Skills:** {skills_display}")
                
                # Pipeline Configuration Analysis
                if pipeline_id:
                    try:
                        # Get pipeline details
                        pipelines_df = pipeline_manager.get_all_pipelines()
                        pipeline_info = pipelines_df[pipelines_df['id'] == pipeline_id].iloc[0] if not pipelines_df.empty else None
                        
                        if pipeline_info is not None:
                            pipeline_name = pipeline_info.get('name', 'Unknown Pipeline')
                            st.markdown(f"**📋 Selected Pipeline:** {pipeline_name}")
                            
                            # Get pipeline stages for calculation (exclude Reject and On Hold)
                            all_stages = pipeline_manager.get_pipeline_stages(pipeline_id)
                            
                            if all_stages:
                                # Filter out "Reject" and "On Hold" stages
                                stages = [stage for stage in all_stages if stage['stage_name'] not in ['Reject', 'On Hold']]
                                
                                if stages:
                                    # Convert onboard_by to proper date format for calculation
                                    if isinstance(onboard_by, str):
                                        from datetime import datetime
                                        onboard_date = datetime.strptime(str(onboard_by), '%Y-%m-%d').date()
                                    else:
                                        onboard_date = onboard_by
                                    
                                    # Use correct reverse calculation working backwards from target hires
                                    reverse_calculation = pipeline_manager.calculate_reverse_pipeline(
                                        pipeline_id, positions, onboard_date
                                    )
                                    
                                    if reverse_calculation:
                                        # Display pipeline analytics
                                        total_tat = sum(stage['tat_days'] for stage in stages)
                                        first_stage_candidates = reverse_calculation[0]['profiles_in_pipeline']
                                        
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("Total TAT", f"{total_tat} days")
                                        with col2:
                                            st.metric("Active Stages", len(stages))
                                        with col3:
                                            st.metric("Profiles in Pipeline", first_stage_candidates)
                                        with col4:
                                            st.metric("Target Hires", positions)
                                        
                                        # Stage-wise breakdown using your calculation
                                        st.markdown("**📊 Pipeline Requirements (Working Backwards):**")
                                        
                                        # Get current staffing plan ID for saving actual data
                                        current_plan_id = st.session_state.get('edit_staffing_plan_id') or st.session_state.get('current_plan_id')
                                        
                                        # Get existing actual data for this role
                                        existing_actual_data = {}
                                        if current_plan_id:
                                            existing_actual_data = staffing_manager.get_pipeline_requirements_actual(current_plan_id, role)
                                        
                                        # Create editable table with new column structure
                                        pipeline_data = []
                                        actual_stage_data = []
                                        
                                        for stage_result in reverse_calculation:
                                            stage_name = stage_result['stage_name']
                                            profiles_in_pipeline = stage_result['profiles_in_pipeline']
                                            needed_by_date = stage_result['needed_by_date']
                                            
                                            # Get existing actual data or default to 0
                                            existing_actual = existing_actual_data.get(stage_name, {})
                                            current_actual_in_pipeline = existing_actual.get('actual_at_stage', 0)
                                            current_actual_converted = existing_actual.get('actual_converted', 0)
                                            
                                            pipeline_data.append({
                                                'Stage': stage_name,
                                                'TAT': stage_result['tat_days'],
                                                'Conversion %': f"{stage_result['conversion_rate']:.0f}%",
                                                'Profiles in Pipeline': profiles_in_pipeline,
                                                'Needed By Date': needed_by_date.strftime('%Y-%m-%d'),
                                                'Profiles Converted': stage_result['profiles_converted'],
                                                'Pipeline Health': ''  # Will be calculated
                                            })
                                        
                                        # Display editable table
                                        st.markdown("**Editable Pipeline Requirements Table:**")
                                        
                                        # Wrap the editable table in a form to prevent input reset
                                        with st.form(key=f"pipeline_requirements_form_{role}"):
                                            # Create table headers - updated column structure
                                            col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1.5, 2, 2, 2, 2])
                                        with col1:
                                            st.markdown("**Stage**")
                                        with col2:
                                            st.markdown("**TAT**")
                                        with col3:
                                            st.markdown("**Conversion %**")
                                        with col4:
                                            st.markdown("**Profiles in Pipeline**")
                                        with col5:
                                            st.markdown("**Needed By Date**")
                                        with col6:
                                            st.markdown("**Profiles Converted**")
                                        with col7:
                                            st.markdown("**Pipeline Health**")
                                            
                                            # Store updated actual values for saving
                                            updated_actual_data = []
                                            st.info(f"🔍 DEBUG: Processing {len(pipeline_data)} stages for {role}")
                                            
                                            # Display editable rows
                                            for idx, row_data in enumerate(pipeline_data):
                                                col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1.5, 2, 2, 2, 2])
                                                
                                                with col1:
                                                    st.text(row_data['Stage'])
                                                with col2:
                                                    st.text(str(row_data['TAT']))
                                                with col3:
                                                    st.text(row_data['Conversion %'])
                                                with col4:
                                                    st.text(str(row_data['Profiles in Pipeline']))
                                                with col5:
                                                    st.text(row_data['Needed By Date'])
                                                with col6:
                                                    st.text(str(row_data['Profiles Converted']))
                                                with col7:
                                                    # Display Pipeline Health (No actual data available)
                                                    st.markdown("⚪ **Planned**")
                                                
                                                # Store data for saving (simplified without actual data)
                                                stage_save_data = {
                                                    'stage_name': row_data['Stage'],
                                                    'profiles_in_pipeline': row_data['Profiles in Pipeline'],
                                                    'needed_by_date': needed_by
                                                }
                                                updated_actual_data.append(stage_save_data)
                                                
                                                # Debug logging for data collection
                                                import logging
                                                logger = logging.getLogger(__name__)
                                                logger.info(f"FORM DEBUG: Collected data for {row_data['Stage']}")
                                            
                                            # Form submit button (required inside st.form)
                                            if st.form_submit_button(f"💾 Save Pipeline Requirements for {role}"):
                                                st.info(f"🔍 DEBUG: Save button clicked for {role}")
                                                st.info(f"🔍 DEBUG: Current plan ID: {current_plan_id}")
                                                st.info(f"🔍 DEBUG: Number of stages to save: {len(updated_actual_data)}")
                                                
                                                # Debug: Show what data is being sent
                                                for i, stage_data in enumerate(updated_actual_data):
                                                    st.info(f"🔍 DEBUG Stage {i+1}: {stage_data['stage_name']}")
                                                
                                                if current_plan_id:
                                                    st.info(f"🔍 DEBUG: Calling save_pipeline_requirements_actual...")
                                                    success = staffing_manager.save_pipeline_requirements_actual(
                                                        current_plan_id, role, updated_actual_data
                                                    )
                                                    st.info(f"🔍 DEBUG: Save operation returned: {success}")
                                                    
                                                    if success:
                                                        st.success(f"✅ Pipeline requirements saved for {role}!")
                                                        # Force a rerun to show updated data
                                                        st.rerun()
                                                    else:
                                                        st.error(f"❌ Failed to save pipeline requirements for {role}")
                                                        st.error("🔍 DEBUG: Check the server logs for detailed error information")
                                                else:
                                                    st.warning("Please save the staffing plan first before saving pipeline requirements")
                                        
                                        # Add a simple test button outside the form to check save functionality
                                        if st.button(f"🧪 TEST SAVE FUNCTION for {role}", key=f"test_save_{role}"):
                                            st.info("🔍 DEBUG: Test button clicked - creating dummy data")
                                            from datetime import datetime
                                            test_data = [
                                                {
                                                    'stage_name': 'Initial Screening',
                                                    'actual_at_stage': 99,
                                                    'actual_converted': 88,
                                                    'profiles_in_pipeline': 100,
                                                    'needed_by_date': datetime.now().date()
                                                }
                                            ]
                                            if current_plan_id:
                                                st.info("🔍 DEBUG: Calling save function with test data...")
                                                test_result = staffing_manager.save_pipeline_requirements_actual(
                                                    current_plan_id, role, test_data
                                                )
                                                st.info(f"🔍 DEBUG: Test save result: {test_result}")
                                            else:
                                                st.error("🔍 DEBUG: No current_plan_id available")
                                        
                                        # Note: Pipeline Owner is now managed in the Pipeline Planning Table above

                                        # Timeline Visualization using calculated dates
                                        if onboard_by:
                                            from datetime import datetime, timedelta
                                            import plotly.graph_objects as go
                                            
                                            # Get sourcing start date from first stage calculation
                                            sourcing_start = reverse_calculation[0]['needed_by_date']
                                            
                                            if isinstance(onboard_by, str):
                                                onboard_date = datetime.strptime(str(onboard_by), '%Y-%m-%d').date()
                                            else:
                                                onboard_date = onboard_by
                                            
                                            st.markdown("**📅 Supply Timeline:**")
                                            timeline_col1, timeline_col2, timeline_col3 = st.columns(3)
                                            
                                            with timeline_col1:
                                                st.metric("Sourcing Start", sourcing_start.strftime('%Y-%m-%d'))
                                            with timeline_col2:
                                                st.metric("Pipeline Duration", f"{total_tat} days")
                                            with timeline_col3:
                                                st.metric("Target Staff Date", onboard_date.strftime('%Y-%m-%d'))
                                            

                                        

                                    else:
                                        st.error("Error calculating pipeline requirements")
                                else:
                                    st.warning("No active stages found (all stages are 'Reject' or 'On Hold')")
                            else:
                                st.warning("No stages configured for selected pipeline")
                        else:
                            st.warning("Pipeline information not found")
                    except Exception as e:
                        st.error(f"Error loading pipeline data: {str(e)}")
                else:
                    st.warning("No pipeline selected for this role")
                    st.info("💡 Select a pipeline in the Pipeline Planning Table to see supply calculations")
        
        # Action buttons
        st.markdown("---")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("💾 Save Pipeline Plan", key="save_pipeline_plan"):
                # Save pipeline plan with owner details
                try:
                    # Prepare data for saving with current plan context
                    current_plan_name = st.session_state.get('current_plan_name', 'Current Plan')
                    current_from_date = st.session_state.get('current_from_date')
                    current_to_date = st.session_state.get('current_to_date')
                    
                    plan_data = {
                        'plan_name': current_plan_name,
                        'from_date': current_from_date,
                        'to_date': current_to_date,
                        'roles': []
                    }
                    
                    for role_data in pipeline_planning_data:
                        role_info = {
                            'role': role_data.get('role', ''),
                            'skills': role_data.get('skills', ''),
                            'positions': role_data.get('positions', 1),
                            'onboard_by': role_data.get('onboard_by'),
                            'pipeline_id': role_data.get('pipeline_id'),
                            'pipeline_owner': role_data.get('pipeline_owner', '')
                        }
                        plan_data['roles'].append(role_info)
                    
                    # Save to database using staffing manager
                    result = staffing_manager.save_pipeline_plan(plan_data)
                    
                    if result:
                        # Also save all Pipeline Requirements actual data if available
                        pipeline_req_saved = True
                        for role_data in pipeline_planning_data:
                            role = role_data.get('role', '')
                            if f'pipeline_actual_data_{role}' in st.session_state:
                                actual_data = st.session_state[f'pipeline_actual_data_{role}']
                                if actual_data and current_plan_id:
                                    req_success = staffing_manager.save_pipeline_requirements_actual(
                                        current_plan_id, role, actual_data
                                    )
                                    if not req_success:
                                        pipeline_req_saved = False
                        
                        # Clear pipeline generation state immediately and return to main page
                        st.session_state.show_pipeline_generation = False
                        
                        # Clear all pipeline generation related session state
                        pipeline_session_keys = [
                            'supply_plan_auto_loaded', 'current_plan_name', 'current_from_date', 
                            'current_to_date', 'show_staffing_form', 'edit_staffing_plan_id',
                            'pipeline_planning_data'
                        ]
                        
                        for key in pipeline_session_keys:
                            if key in st.session_state:
                                del st.session_state[key]
                        
                        # Clear pipeline requirements data from session state
                        for role_data in pipeline_planning_data:
                            role = role_data.get('role', '')
                            if f'pipeline_actual_data_{role}' in st.session_state:
                                del st.session_state[f'pipeline_actual_data_{role}']
                        
                        # Set success message for display on main page
                        success_msg = "✅ Pipeline plan saved successfully!"
                        if pipeline_req_saved:
                            success_msg += " Pipeline requirements also saved!"
                        else:
                            success_msg += " (Some pipeline requirements failed to save)"
                        st.session_state['last_save_message'] = success_msg
                        
                        # Force refresh to return to main Staffing Plans page
                        st.rerun()
                    else:
                        st.error("❌ Failed to save pipeline plan")
                        
                except Exception as e:
                    st.error(f"❌ Error saving pipeline plan: {str(e)}")
        
        with col2:
            if st.button("💾 Export Supply Plan", key="export_supply_plan"):
                # Create export data
                export_data = []
                for role_data in pipeline_planning_data:
                    export_data.append({
                        'Role': role_data.get('role', ''),
                        'Skills': role_data.get('skills', ''),
                        'Positions': role_data.get('positions', 1),
                        'Onboard_By': str(role_data.get('onboard_by', '')),
                        'Pipeline_ID': role_data.get('pipeline_id', ''),
                        'Pipeline_Owner': role_data.get('pipeline_owner', '')
                    })
                
                export_df = pd.DataFrame(export_data)
                csv = export_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"supply_plan_{plan_name.replace(' ', '_')}.csv",
                    mime="text/csv"
                )
        
        with col3:
            if st.button("🔄 Refresh Analysis", key="refresh_pipeline_analysis"):
                st.rerun()
        
        with col4:
            if st.button("✅ Done", key="close_pipeline_generation"):
                # Clear all pipeline generation state and return to clean landing page
                st.session_state.show_pipeline_generation = False
                if 'supply_plan_auto_loaded' in st.session_state:
                    del st.session_state['supply_plan_auto_loaded']
                if 'current_plan_name' in st.session_state:
                    del st.session_state['current_plan_name']
                if 'current_from_date' in st.session_state:
                    del st.session_state['current_from_date']
                if 'current_to_date' in st.session_state:
                    del st.session_state['current_to_date']
                st.rerun()
        
    except Exception as e:
        st.error(f"Error loading pipeline generation interface: {str(e)}")
        if st.button("✅ Close", key="close_pipeline_generation_error"):
            # Clear all pipeline generation state and return to clean landing page
            st.session_state.show_pipeline_generation = False
            if 'supply_plan_auto_loaded' in st.session_state:
                del st.session_state['supply_plan_auto_loaded']
            if 'current_plan_name' in st.session_state:
                del st.session_state['current_plan_name']
            if 'current_from_date' in st.session_state:
                del st.session_state['current_from_date']
            if 'current_to_date' in st.session_state:
                del st.session_state['current_to_date']
            st.rerun()

def supply_planning_page():
    """Supply Planning page with unified talent management and pipeline configuration"""
    st.header("🏭 Supply Planning")
    
    # Use radio buttons for better state management
    supply_section = st.radio(
        "Select Section:",
        ["👥 Talent Management", "🔧 Pipeline Configuration", "📋 Supply Management"],
        horizontal=True,
        key="supply_planning_section"
    )
    
    st.markdown("---")
    
    if supply_section == "👥 Talent Management":
        talent_management_section()
    elif supply_section == "🔧 Pipeline Configuration":
        pipeline_configuration_section()
    elif supply_section == "📋 Supply Management":
        supply_management_section()

def hiring_talent_management_section():
    """Hiring Talent Management section with candidate management"""
    st.subheader("🎯 Hiring Talent Management")
    
    # Initialize database connection
    env_manager = st.session_state.env_manager
    
    # Add Candidate Button
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("➕ Add Candidate", type="primary"):
            st.session_state.show_add_candidate_form = True
    
    # Show Add Candidate Form if requested
    if st.session_state.get('show_add_candidate_form', False):
        show_add_candidate_form()
    
    # Show Edit Candidate Form if requested
    if st.session_state.get('show_edit_candidate_form', False):
        show_edit_candidate_form()
    
    # Show View Candidate if requested
    if st.session_state.get('show_view_candidate', False):
        show_view_candidate()
    
    st.markdown("---")
    
    # Display existing candidates
    display_candidate_list()

def get_staffing_plans():
    """Get all active staffing plans"""
    import os
    import psycopg2
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    plans = []
    try:
        cursor = conn.cursor()
        staffing_plans_table = env_manager.get_table_name('staffing_plans')
        cursor.execute(f"SELECT id, plan_name FROM {staffing_plans_table} ORDER BY plan_name")
        plans = cursor.fetchall()
    finally:
        conn.close()
    return plans

def get_staffing_plan_owners(plan_id):
    """Get unique owners for a specific staffing plan"""
    import os
    import psycopg2
    
    if not plan_id:
        return []
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    owners = []
    try:
        cursor = conn.cursor()
        # Fix: Query the correct table where owners are actually stored
        staffing_plan_generated_plans_table = env_manager.get_table_name('staffing_plan_generated_plans')
        query = f"SELECT DISTINCT pipeline_owner FROM {staffing_plan_generated_plans_table} WHERE plan_id = %s AND pipeline_owner IS NOT NULL AND pipeline_owner != '' ORDER BY pipeline_owner"
        cursor.execute(query, (plan_id,))
        owners = [row[0] for row in cursor.fetchall() if row[0]]
    finally:
        conn.close()
    return owners

def get_staffing_plan_roles(plan_id, owner):
    """Get roles for a specific staffing plan and owner"""
    import os
    import psycopg2
    
    if not plan_id or not owner:
        return []
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    roles = []
    try:
        cursor = conn.cursor()
        # Fix: Query the correct table where roles are actually stored
        staffing_plan_generated_plans_table = env_manager.get_table_name('staffing_plan_generated_plans')
        query = f"SELECT DISTINCT role FROM {staffing_plan_generated_plans_table} WHERE plan_id = %s AND pipeline_owner = %s AND role IS NOT NULL AND role != '' ORDER BY role"
        cursor.execute(query, (plan_id, owner))
        roles = [row[0] for row in cursor.fetchall() if row[0]]
    finally:
        conn.close()
    return roles

def get_pipelines_for_client(client_id):
    """Get pipelines linked to a specific client"""
    import os
    import psycopg2
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    pipelines = []
    try:
        cursor = conn.cursor()
        talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
        cursor.execute(f"SELECT id, name FROM {talent_pipelines_table} WHERE client_id = %s ORDER BY name", (client_id,))
        pipelines = cursor.fetchall()
    finally:
        conn.close()
    return pipelines

def get_staffing_plans_for_pipeline(pipeline_id):
    """Get staffing plans linked to a specific pipeline"""
    import os
    import psycopg2
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    plans = []
    try:
        cursor = conn.cursor()
        staffing_plans_table = env_manager.get_table_name('staffing_plans')
        pipeline_planning_details_table = env_manager.get_table_name('pipeline_planning_details')
        query = f"""
            SELECT DISTINCT sp.id, sp.plan_name 
            FROM {staffing_plans_table} sp
            JOIN {pipeline_planning_details_table} ppd ON ppd.plan_id = sp.id
            WHERE ppd.pipeline_id = %s
            ORDER BY sp.plan_name
        """
        cursor.execute(query, (pipeline_id,))
        plans = cursor.fetchall()
    finally:
        conn.close()
    return plans

def show_add_candidate_form():
    """Display the Add Candidate form"""
    st.markdown("### ➕ Add New Candidate")
    
    env_manager = st.session_state.env_manager
    
    # STEP 1: Staffing Assignment (outside form for cascading dropdowns)
    st.markdown("#### 🎯 Staffing Assignment")
    
    staffing_col1, staffing_col2 = st.columns(2)
    
    with staffing_col1:
        # 1. Hire for Client
        import os
        import psycopg2
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        clients = []
        try:
            cursor = conn.cursor()
            master_clients_table = env_manager.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id, client_name FROM {master_clients_table} ORDER BY client_name")
            clients = cursor.fetchall()
        finally:
            conn.close()
        
        client_options = [""] + [f"{client[1]}" for client in clients]
        client_names = {f"{client[1]}": client[0] for client in clients}
        hire_for_client = st.selectbox("Hire for Client", client_options, key="staffing_client_new")
        
        # 2. Linked to Pipeline (filtered by selected client)
        pipelines = []
        if hire_for_client and hire_for_client in client_names:
            client_id = client_names[hire_for_client]
            pipelines = get_pipelines_for_client(client_id)
        
        pipeline_options = [""] + [f"{pipeline[1]}" for pipeline in pipelines]
        pipeline_names = {f"{pipeline[1]}": pipeline[0] for pipeline in pipelines}
        linked_pipeline = st.selectbox("Linked to Pipeline", pipeline_options, key="staffing_pipeline_new")
        
        # 3. Staffing Plan (filtered by selected pipeline)
        staffing_plans = []
        if linked_pipeline and linked_pipeline in pipeline_names:
            pipeline_id = pipeline_names[linked_pipeline]
            staffing_plans = get_staffing_plans_for_pipeline(pipeline_id)
        
        plan_options = [""] + [f"{plan[1]}" for plan in staffing_plans]
        plan_names = {f"{plan[1]}": plan[0] for plan in staffing_plans}
        selected_staffing_plan = st.selectbox("Staffing Plan", plan_options, key="staffing_plan_new")
    
    with staffing_col2:
        # 4. Staffing Plan Owner (cascading dropdown)
        staffing_plan_owners = []
        if selected_staffing_plan and selected_staffing_plan in plan_names:
            plan_id = plan_names[selected_staffing_plan]
            staffing_plan_owners = get_staffing_plan_owners(plan_id)
        
        owner_options = [""] + staffing_plan_owners
        selected_staffing_owner = st.selectbox("Staffing Plan Owner", owner_options, key="staffing_owner_new")
        
        # 5. Staffing for Role (cascading dropdown)
        staffing_roles = []
        if (selected_staffing_plan and selected_staffing_plan in plan_names and 
            selected_staffing_owner):
            plan_id = plan_names[selected_staffing_plan]
            staffing_roles = get_staffing_plan_roles(plan_id, selected_staffing_owner)
        
        role_options = [""] + staffing_roles
        selected_staffing_role = st.selectbox("Staffing for Role", role_options, key="staffing_role_new")
    
            # Add new checkbox and Staffing Manager field
        st.markdown("---")
        
        # Not Linked to Staffing Plan checkbox
        not_linked_to_staffing_plan = st.checkbox("Not Linked to Staffing Plan", value=False, key="add_not_linked")
        
        # Staffing Manager field (always visible but enabled only when checkbox is checked)
        staffing_managers = []
        try:
            import os
            import psycopg2
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            cursor = conn.cursor()
            # Get FTE talent from unified talent table
            talent_supply_table = env_manager.get_table_name('talent_supply')
            cursor.execute(f"SELECT DISTINCT name FROM {talent_supply_table} WHERE type = 'FTE' AND name IS NOT NULL AND name != '' ORDER BY name")
            staffing_managers = [row[0] for row in cursor.fetchall()]
            conn.close()
        except Exception as e:
            st.error(f"Error loading staffing managers: {str(e)}")
            staffing_managers = []
        
        staffing_manager_options = [""] + staffing_managers
        selected_staffing_manager = st.selectbox(
            "Staffing Manager", 
            staffing_manager_options, 
            key="add_staffing_manager",
            disabled=not not_linked_to_staffing_plan,  # Enabled only when checkbox is checked
            help="Select staffing manager (enabled only when 'Not Linked to Staffing Plan' is checked)"
        )
    
    st.markdown("---")
    
    # STEP 2: Main Form (all other fields)
    with st.form("add_candidate_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            # 1. Candidate Name
            candidate_name = st.text_input("Candidate Name")
            
            # 2. Role with dynamic addition
            # Get existing roles
            import os
            # Get roles from candidate_data table (cleaned statuses from aggregator transformation)
            import psycopg2
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            existing_roles = []
            try:
                cursor = conn.cursor()
                # Use environment-aware table name
                candidate_data_table = env_manager.get_table_name('candidate_data')
                cursor.execute(f"SELECT DISTINCT staffing_role FROM {candidate_data_table} WHERE staffing_role IS NOT NULL AND staffing_role != '' ORDER BY staffing_role")
                existing_roles = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()
            
            role_col1, role_col2 = st.columns([3, 1])
            with role_col1:
                role = st.selectbox("Role", [""] + existing_roles)
            with role_col2:
                st.markdown("<br>", unsafe_allow_html=True)  # Add space to align with selectbox
                if st.form_submit_button("+ New Role", use_container_width=True):
                    st.session_state.show_new_role_field = True
            
            # New role input field
            if st.session_state.get('show_new_role_field', False):
                new_role = st.text_input("Enter New Role")
                if st.form_submit_button("Add Role") and new_role:
                    # Add new role to candidate_data table instead of non-existent candidate_roles
                    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
                    try:
                        cursor = conn.cursor()
                        # Insert a dummy candidate record with the new role
                        candidate_data_table = env_manager.get_table_name('candidate_data')
                        cursor.execute(f"INSERT INTO {candidate_data_table} (candidate_name, staffing_role, status, created_date) VALUES (%s, %s, %s, %s)", 
                                     (f"New Role Template - {new_role}", new_role, "Screening", datetime.now()))
                        conn.commit()
                        st.success(f"Role '{new_role}' added successfully!")
                        st.session_state.show_new_role_field = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding role: {str(e)}")
                    finally:
                        conn.close()
            
            # 3. Experience
            experience_options = [
                "0-3 yrs", "2-4 yrs", "4-6 yrs", "6-8 yrs", "8-10 yrs", 
                "10-12 yrs", "12-16 yrs", "16-20 yrs", "20-22 yrs", "22+ yrs"
            ]
            experience = st.selectbox("Experience", [""] + experience_options)
            
            # 4. Skills
            skills = st.text_area("Skills", height=100)
            
            # 6. Status - get from pipeline stages
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            statuses = []
            try:
                cursor = conn.cursor()
                pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                cursor.execute(f"SELECT DISTINCT stage_name FROM {pipeline_stages_table} WHERE is_active = TRUE ORDER BY stage_name")
                statuses = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()
            
            if "Added to Pipeline" not in statuses:
                statuses.insert(0, "Added to Pipeline")
            
            status = st.selectbox("Status", statuses, index=0 if statuses else None)
            
            # Status Flag - Radio buttons (moved below Status field)
            status_flag = st.radio("Status Flag", ["Greyamp", "Client"], help="Select if this status is for Greyamp or Client stage")
        
        with col2:
            
            # 9. Source
            source_options = ["Vendor", "Direct", "Referral", "In-bound"]
            source = st.selectbox("Source", [""] + source_options)
            
            # 10. Vendor Partner (always enabled)
            # Get vendor partners data
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            partners = []
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT partner_name FROM vendor_partners ORDER BY partner_name")
                partners = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()
            
            # Always show enabled vendor partner field
            vendor_partner = st.selectbox("Vendor Partner", [""] + partners)
            
            # Resume upload field (moved up as requested)
            st.markdown("#### 📄 Resume/Profile")
            resume_file = st.file_uploader("Resume/Profile", type=['pdf', 'doc', 'docx'])
            
            # Drop Reason (positioned below Resume field as shown in screenshot)
            drop_reason_options = [
                "",
                "Candidate RNR",
                "On Hold",
                "Internal Dropped",
                "Duplicate Profile",
                "Requirement on hold",
                "Salary expectations too high",
                "Notice period too long", 
                "Location mismatch",
                "Skills not matching",
                "Candidate declined offer",
                "Failed technical assessment",
                "Failed interview",
                "Background check failed",
                "Other"
            ]
            
            drop_reason_selection = st.selectbox("Drop Reason", drop_reason_options, help="Select reason or choose 'Other' for custom text")
            
            # Custom drop reason text area (shows when "Other" is selected or for additional details)
            if drop_reason_selection == "Other" or drop_reason_selection == "":
                drop_reason = st.text_area("Drop Reason Details", max_chars=300, height=80,
                                         placeholder="Specify reason for rejection, withdrawal, or hold")
            else:
                drop_reason = drop_reason_selection
                # Optional additional details
                additional_details = st.text_area("Additional Details (Optional)", max_chars=200, height=80,
                                                placeholder="Any additional context or details")
                if additional_details.strip():
                    drop_reason = f"{drop_reason_selection} - {additional_details.strip()}"
        
        # Contact Information & Compensation
        st.markdown("---")
        st.markdown("#### 📞 Contact Information (Optional)")
        
        contact_col1, contact_col2 = st.columns(2)
        with contact_col1:
            email_id = st.text_input("Email ID", placeholder="candidate@email.com")
        with contact_col2:
            contact_number = st.text_input("Contact Number", placeholder="+91-9876543210")
        
        st.markdown("#### 💰 Compensation & Timeline (Optional)")
        
        comp_col1, comp_col2 = st.columns(2)
        with comp_col1:
            expected_ctc = st.text_input("Expected CTC", placeholder="15 LPA, 25-30K USD, etc.")
        with comp_col2:
            position_start_date = st.date_input("Position Start Date", value=None)
        
        st.markdown("#### 🌍 Location & Availability")
        
        location_col1, location_col2 = st.columns(2)
        with location_col1:
            # Location
            location_options = ["", "India", "Indonesia", "MEA", "SEA"]
            location = st.selectbox("Location", location_options)
            
            # Notice Period Details
            notice_period_details = st.text_input("Notice Period Details", placeholder="Additional details from sheets import")
        
        with location_col2:
            # Notice Period (matching Edit form options)
            notice_period_options = ["", "Immediate Joiner", "less than 30 days", "30-60 days", "60-90 days"]
            notice_period = st.selectbox("Notice Period", notice_period_options)
            
            # Notes (moved here to match structure)
            notes = st.text_area("Notes/Screening Notes", max_chars=500, height=100, 
                                placeholder="General notes, screening feedback, etc.")
        
        st.markdown("#### 📋 Process Tracking (Optional)")
        
        process_col1, process_col2 = st.columns(2)
        with process_col1:
            next_steps = st.text_area("Next Steps", height=80, 
                                    placeholder="Schedule interview, send assessment, client review, etc.")
        with process_col2:
            interview_feedback = st.text_area("Interview Feedback", height=80,
                                            placeholder="GA interview notes, client feedback, assessment results, etc.")
        
        # Form submission
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            submitted = st.form_submit_button("💾 Save Details", type="primary")
        with col2:
            if st.form_submit_button("❌ Cancel"):
                st.session_state.show_add_candidate_form = False
                st.rerun()
        
        if submitted:
            # Get staffing assignment values from outside form (session state)
            staffing_client_id = client_names.get(hire_for_client, None) if hire_for_client else None
            staffing_pipeline_id = pipeline_names.get(linked_pipeline, None) if linked_pipeline else None
            staffing_plan_id = plan_names.get(selected_staffing_plan, None) if selected_staffing_plan else None
            
            # Save candidate data with all new fields including staffing assignment
            save_candidate_data(
                candidate_name, role, experience, skills,
                staffing_client_id, status, status_flag,
                staffing_pipeline_id, source, vendor_partner,
                location, notice_period, notice_period_details, resume_file, notes,
                email_id, contact_number, expected_ctc, position_start_date,
                next_steps, interview_feedback, drop_reason,
                # New staffing assignment fields
                staffing_plan_id, selected_staffing_owner, selected_staffing_role,
                # New fields for Not Linked to Staffing Plan
                not_linked_to_staffing_plan, selected_staffing_manager
            )

def save_candidate_data(candidate_name, role, experience, skills, client_id, status, 
                       status_flag, pipeline_id, source, vendor_partner, location, 
                       notice_period, notice_period_details, resume_file, notes,
                       email_id, contact_number, expected_ctc, position_start_date,
                       next_steps, interview_feedback, drop_reason,
                       staffing_plan_id=None, staffing_owner=None, staffing_role=None,
                       not_linked_to_staffing_plan=False, staffing_manager=None):
    """Save candidate data to database"""
    
    if not candidate_name:
        st.error("Candidate Name is required!")
        return
    
    import os
    import psycopg2
    import time
    
    current_user_email = st.session_state.get('user_email', '')
    
    # Handle file upload
    resume_file_path = ""
    if resume_file is not None:
        # Create resume directory if it doesn't exist
        resume_dir = "candidate_resumes"
        if not os.path.exists(resume_dir):
            os.makedirs(resume_dir)
        
        # Save file with unique name
        file_extension = resume_file.name.split('.')[-1]
        safe_name = "".join(c for c in candidate_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        resume_file_path = f"{resume_dir}/{safe_name}_{int(time.time())}.{file_extension}"
        
        with open(resume_file_path, "wb") as f:
            f.write(resume_file.getbuffer())
    
    # Get environment-appropriate table name
    env_manager = st.session_state.get('env_manager')
    if env_manager:
        candidate_table = env_manager.get_table_name('candidate_data')
    else:
        candidate_table = 'candidate_data'  # Fallback for production
    
    # Insert into database
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        # First, add the new staffing columns if they don't exist
        try:
            cursor.execute(f"ALTER TABLE {candidate_table} ADD COLUMN IF NOT EXISTS staffing_plan_id INTEGER")
            cursor.execute(f"ALTER TABLE {candidate_table} ADD COLUMN IF NOT EXISTS staffing_owner VARCHAR(255)")
            cursor.execute(f"ALTER TABLE {candidate_table} ADD COLUMN IF NOT EXISTS staffing_role VARCHAR(255)")
            cursor.execute(f"ALTER TABLE {candidate_table} ADD COLUMN IF NOT EXISTS not_linked_to_staffing_plan BOOLEAN DEFAULT FALSE")
            cursor.execute(f"ALTER TABLE {candidate_table} ADD COLUMN IF NOT EXISTS staffing_manager VARCHAR(255)")
            conn.commit()
        except Exception as e:
            # Columns might already exist
            pass
        
        cursor.execute(f"""
            INSERT INTO {candidate_table} (
                candidate_name, role, experience_level, skills, hire_for_client_id,
                status, status_flag, linked_pipeline_id, source, vendor_partner,
                location, notice_period, notice_period_details, resume_file_path, notes, 
                email_id, contact_number, expected_ctc, position_start_date,
                next_steps, interview_feedback, drop_reason, created_by, data_source, last_manual_edit, created_flag,
                staffing_plan_id, staffing_owner, staffing_role, not_linked_to_staffing_plan, staffing_manager
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            candidate_name, role, experience, skills, client_id, status,
            status_flag, pipeline_id, source, vendor_partner, location,
            notice_period, notice_period_details, resume_file_path, notes,
            email_id, contact_number, expected_ctc, position_start_date,
            next_steps, interview_feedback, drop_reason, current_user_email, 'manual', None, 'Y',
            staffing_plan_id, staffing_owner, staffing_role, not_linked_to_staffing_plan, staffing_manager
        ))
        conn.commit()
        st.success(f"✅ Candidate '{candidate_name}' added successfully!")
        st.session_state.show_add_candidate_form = False
        st.rerun()
    except Exception as e:
        st.error(f"Error saving candidate: {str(e)}")
    finally:
        conn.close()

def show_edit_candidate_form():
    """Display the Edit Candidate form"""
    candidate_id = st.session_state.get('edit_candidate_id')
    if not candidate_id:
        st.error("No candidate selected for editing.")
        return
    
    # Load candidate data
    candidate_data = load_candidate_for_edit(candidate_id)
    if not candidate_data:
        st.error("Could not load candidate data.")
        return
    
    st.markdown("### ✏️ Edit Candidate")
    
    # Extract current values (including new staffing fields and drop_reason)
    (current_name, current_role, current_experience, current_skills, current_client_id,
     current_status, current_status_flag, current_pipeline_id, current_source, current_vendor_partner,
     current_location, current_notice_period, current_notice_period_details, current_resume_path, current_notes,
     current_email, current_contact, current_expected_ctc, current_position_start_date,
     current_next_steps, current_interview_feedback, current_drop_reason, current_staffing_plan_id, current_staffing_owner, current_staffing_role,
     current_not_linked_to_staffing_plan, current_staffing_manager) = candidate_data
    
    env_manager = st.session_state.env_manager
    
    # STEP 1: Staffing Assignment (outside form for cascading dropdowns)
    st.markdown("#### 🎯 Staffing Assignment")
    
    staffing_col1, staffing_col2 = st.columns(2)
    
    with staffing_col1:
        # 1. Hire for Client
        try:
            import os
            import psycopg2
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            cursor = conn.cursor()
            master_clients_table = env_manager.get_table_name('master_clients')
            cursor.execute(f"SELECT master_client_id, client_name FROM {master_clients_table} ORDER BY client_name")
            clients = cursor.fetchall()
            conn.close()
            
            client_options = [("", "Select Client")] + [(str(client[0]), client[1]) for client in clients]
            client_display_options = [option[1] for option in client_options]
            current_client_index = 0
            if current_client_id:
                for i, (client_id_option, _) in enumerate(client_options):
                    if client_id_option == str(current_client_id):
                        current_client_index = i
                        break
            
            selected_client_display = st.selectbox("Hire for Client", client_display_options, index=current_client_index, key=f"edit_staffing_client_{candidate_id}")
            selected_client_index = client_display_options.index(selected_client_display)
            edit_client_id = client_options[selected_client_index][0] if client_options[selected_client_index][0] else None
            
        except Exception as e:
            st.error(f"Error loading clients: {str(e)}")
            edit_client_id = None
        
        # 2. Linked to Pipeline (filtered by selected client)
        pipelines = []
        if edit_client_id:
            pipelines = get_pipelines_for_client(edit_client_id)
        
        pipeline_options = [""] + [f"{pipeline[1]}" for pipeline in pipelines]
        pipeline_names = {f"{pipeline[1]}": pipeline[0] for pipeline in pipelines}
        # Find current pipeline index
        current_pipeline_index = 0
        if current_pipeline_id:
            for i, (pipeline_id, pipeline_name) in enumerate(pipelines):
                if pipeline_id == current_pipeline_id:
                    current_pipeline_index = i + 1  # +1 because of empty string at index 0
                    break
        edit_linked_pipeline = st.selectbox("Linked to Pipeline", pipeline_options, index=current_pipeline_index, key=f"edit_staffing_pipeline_{candidate_id}")
        
        # 3. Staffing Plan (filtered by selected pipeline)
        staffing_plans = []
        if edit_linked_pipeline and edit_linked_pipeline in pipeline_names:
            pipeline_id = pipeline_names[edit_linked_pipeline]
            staffing_plans = get_staffing_plans_for_pipeline(pipeline_id)
        
        plan_options = [""] + [f"{plan[1]}" for plan in staffing_plans]
        plan_names = {f"{plan[1]}": plan[0] for plan in staffing_plans}
        # Find current staffing plan index
        current_staffing_plan_index = 0
        if current_staffing_plan_id:
            for i, (plan_id, plan_name) in enumerate(staffing_plans):
                if plan_id == current_staffing_plan_id:
                    current_staffing_plan_index = i + 1  # +1 because of empty string at index 0
                    break
        edit_selected_staffing_plan = st.selectbox("Staffing Plan", plan_options, index=current_staffing_plan_index, key=f"edit_staffing_plan_{candidate_id}")
    
    with staffing_col2:
        # 4. Staffing Plan Owner (cascading dropdown)
        edit_staffing_plan_owners = []
        if edit_selected_staffing_plan and edit_selected_staffing_plan in plan_names:
            plan_id = plan_names[edit_selected_staffing_plan]
            edit_staffing_plan_owners = get_staffing_plan_owners(plan_id)
        
        owner_options = [""] + edit_staffing_plan_owners
        # Find current owner index
        current_owner_index = 0
        if current_staffing_owner and current_staffing_owner in owner_options:
            current_owner_index = owner_options.index(current_staffing_owner)
        edit_selected_staffing_owner = st.selectbox("Staffing Plan Owner", owner_options, index=current_owner_index, key=f"edit_staffing_owner_{candidate_id}")
        
        # 5. Staffing for Role (cascading dropdown)
        edit_staffing_roles = []
        if (edit_selected_staffing_plan and edit_selected_staffing_plan in plan_names and 
            edit_selected_staffing_owner):
            plan_id = plan_names[edit_selected_staffing_plan]
            edit_staffing_roles = get_staffing_plan_roles(plan_id, edit_selected_staffing_owner)
        
        role_options = [""] + edit_staffing_roles
        # Find current role index
        current_role_index = 0
        if current_staffing_role and current_staffing_role in role_options:
            current_role_index = role_options.index(current_staffing_role)
        edit_selected_staffing_role = st.selectbox("Staffing for Role", role_options, index=current_role_index, key=f"edit_staffing_role_{candidate_id}")
    
    # Add new checkbox and Staffing Manager field
    st.markdown("---")
    
    # Not Linked to Staffing Plan checkbox
    not_linked_to_staffing_plan = st.checkbox("Not Linked to Staffing Plan", value=current_not_linked_to_staffing_plan or False, key=f"edit_not_linked_{candidate_id}")
    
    # Staffing Manager field (always visible but enabled only when checkbox is checked)
    staffing_managers = []
    try:
        import os
        import psycopg2
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        cursor = conn.cursor()
        # Get FTE talent from unified talent table
        talent_supply_table = env_manager.get_table_name('talent_supply')
        cursor.execute(f"SELECT DISTINCT name FROM {talent_supply_table} WHERE type = 'FTE' AND name IS NOT NULL AND name != '' ORDER BY name")
        staffing_managers = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        st.error(f"Error loading staffing managers: {str(e)}")
        staffing_managers = []
    
    staffing_manager_options = [""] + staffing_managers
    current_staffing_manager_index = 0
    if current_staffing_manager and current_staffing_manager in staffing_manager_options:
        current_staffing_manager_index = staffing_manager_options.index(current_staffing_manager)
    
    edit_selected_staffing_manager = st.selectbox(
        "Staffing Manager", 
        staffing_manager_options, 
        index=current_staffing_manager_index, 
        key=f"edit_staffing_manager_{candidate_id}",
        disabled=not not_linked_to_staffing_plan,  # Enabled only when checkbox is checked
        help="Select staffing manager (enabled only when 'Not Linked to Staffing Plan' is checked)"
    )
    
    st.markdown("---")
    
    # STEP 2: Main Form (all other fields)
    with st.form(f"edit_candidate_form_{candidate_id}", clear_on_submit=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📝 Basic Information")
            
            # Candidate Name
            candidate_name = st.text_input("Candidate Name", value=current_name or "")
            
            # Role - get from candidate_data table (cleaned statuses from aggregator transformation)
            import os
            import psycopg2
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            existing_roles = []
            try:
                cursor = conn.cursor()
                # Use environment-aware table name
                candidate_data_table = env_manager.get_table_name('candidate_data')
                cursor.execute(f"SELECT DISTINCT staffing_role FROM {candidate_data_table} WHERE staffing_role IS NOT NULL AND staffing_role != '' ORDER BY staffing_role")
                existing_roles = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()
            
            role_options = [""] + existing_roles
            current_role_index = role_options.index(current_role) if current_role in role_options else 0
            role = st.selectbox("Role", role_options, index=current_role_index, key=f"edit_role_{candidate_id}")
            
            # Experience (updated to match New Candidate form)
            experience_options = [
                "", "0-3 yrs", "2-4 yrs", "4-6 yrs", "6-8 yrs", "8-10 yrs", 
                "10-12 yrs", "12-16 yrs", "16-20 yrs", "20-22 yrs", "22+ yrs"
            ]
            current_exp_index = experience_options.index(current_experience) if current_experience in experience_options else 0
            experience = st.selectbox("Experience", experience_options, index=current_exp_index, key=f"edit_experience_{candidate_id}")
            
            # Skills
            skills = st.text_area("Skills", value=current_skills or "", height=100, key=f"edit_skills_{candidate_id}")
        
        with col2:
            st.markdown("#### 🏢 Work Details")
            
            # Status - get from pipeline stages (to match New Candidate form)
            conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
            statuses = []
            try:
                cursor = conn.cursor()
                pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                cursor.execute(f"SELECT DISTINCT stage_name FROM {pipeline_stages_table} WHERE is_active = TRUE ORDER BY stage_name")
                statuses = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()
            
            if "Added to Pipeline" not in statuses:
                statuses.insert(0, "Added to Pipeline")
            
            # Include current status even if not in pipeline stages
            if current_status and current_status not in statuses:
                statuses.append(current_status)
            
            current_status_index = statuses.index(current_status) if current_status in statuses else 0
            status = st.selectbox("Status", statuses, index=current_status_index, key=f"edit_status_{candidate_id}")
            
            # Status Flag - Radio buttons
            status_flag_options = ["Greyamp", "Client"]
            current_status_flag_index = status_flag_options.index(current_status_flag) if current_status_flag in status_flag_options else 0
            status_flag = st.radio("Status Flag", status_flag_options, index=current_status_flag_index, help="Select if this status is for Greyamp or Client stage", key=f"edit_status_flag_{candidate_id}")
            
            # Source
            source_options = ["", "Vendor", "Direct", "Referral", "In-bound"]
            current_source_index = source_options.index(current_source) if current_source in source_options else 0
            source = st.selectbox("Source", source_options, index=current_source_index, key=f"edit_source_{candidate_id}")
            
            # Vendor Partner (always enabled to match New Candidate form)
            conn_vendor = psycopg2.connect(os.environ.get('DATABASE_URL'))
            partners = []
            try:
                cursor_vendor = conn_vendor.cursor()
                cursor_vendor.execute("SELECT partner_name FROM vendor_partners ORDER BY partner_name")
                partners = [row[0] for row in cursor_vendor.fetchall()]
            finally:
                conn_vendor.close()
            
            # Always show enabled vendor partner field
            vendor_options = [""] + partners
            # Ensure current_vendor_partner is properly handled
            if current_vendor_partner and current_vendor_partner in vendor_options:
                current_vendor_index = vendor_options.index(current_vendor_partner)
            else:
                current_vendor_index = 0
            vendor_partner = st.selectbox("Vendor Partner", vendor_options, index=current_vendor_index, key=f"vendor_partner_edit_{candidate_data[0]}")
            
            # Resume upload field (moved up as requested)
            st.markdown("#### 📄 Resume/Profile")
            resume_file = st.file_uploader("Resume/Profile", type=['pdf', 'doc', 'docx'], key=f"edit_resume_{candidate_id}")
            
            # Drop Reason (positioned below Resume field as shown in screenshot)
            drop_reason_options = [
                "",
                "Candidate RNR",
                "On Hold",
                "Internal Dropped",
                "Duplicate Profile",
                "Requirement on hold",
                "Salary expectations too high",
                "Notice period too long", 
                "Location mismatch",
                "Skills not matching",
                "Candidate declined offer",
                "Failed technical assessment",
                "Failed interview",
                "Background check failed",
                "Other"
            ]
            
            current_drop_reason = candidate_data[21] if len(candidate_data) > 21 else ""  # drop_reason is at index 21
            
            # Try to match current drop reason to dropdown options
            current_drop_reason_selection = ""
            current_additional_details = ""
            
            if current_drop_reason:
                # Check if current drop reason matches any of the predefined options
                matched_option = None
                for option in drop_reason_options[1:-1]:  # Skip empty and "Other"
                    if current_drop_reason.startswith(option):
                        matched_option = option
                        if " - " in current_drop_reason:
                            current_additional_details = current_drop_reason.split(" - ", 1)[1]
                        break
                
                if matched_option:
                    current_drop_reason_selection = matched_option
                else:
                    current_drop_reason_selection = "Other"
                    current_additional_details = current_drop_reason
            
            current_selection_index = drop_reason_options.index(current_drop_reason_selection) if current_drop_reason_selection in drop_reason_options else 0
            drop_reason_selection = st.selectbox("Drop Reason", drop_reason_options, index=current_selection_index, 
                                               help="Select reason or choose 'Other' for custom text", key=f"edit_drop_reason_selection_{candidate_id}")
            
            # Custom drop reason text area (shows when "Other" is selected or for additional details)
            if drop_reason_selection == "Other" or drop_reason_selection == "":
                drop_reason = st.text_area("Drop Reason Details", value=current_additional_details, max_chars=300, height=80,
                                         placeholder="Specify reason for rejection, withdrawal, or hold", key=f"edit_drop_reason_details_{candidate_id}")
            else:
                drop_reason = drop_reason_selection
                # Optional additional details
                additional_details = st.text_area("Additional Details (Optional)", value=current_additional_details, max_chars=200, height=80,
                                                placeholder="Any additional context or details", key=f"edit_additional_details_{candidate_id}")
                if additional_details.strip():
                    drop_reason = f"{drop_reason_selection} - {additional_details.strip()}"
        
        # Contact Information & Compensation
        st.markdown("---")
        st.markdown("#### 📞 Contact Information")
        
        contact_col1, contact_col2 = st.columns(2)
        with contact_col1:
            email_id = st.text_input("Email Address", value=current_email or "", key=f"edit_email_{candidate_id}")
            contact_number = st.text_input("Contact Number", value=current_contact or "", key=f"edit_contact_{candidate_id}")
        
        with contact_col2:
            expected_ctc = st.text_input("Expected CTC", value=current_expected_ctc or "", key=f"edit_ctc_{candidate_id}")
            position_start_date = st.date_input("Position Start Date", value=current_position_start_date, key=f"edit_start_date_{candidate_id}")
        
        st.markdown("#### 🌍 Location & Availability")
        
        location_col1, location_col2 = st.columns(2)
        with location_col1:
            # Location
            location_options = ["", "India", "Indonesia", "MEA", "SEA"]
            current_loc_index = location_options.index(current_location) if current_location in location_options else 0
            location = st.selectbox("Location", location_options, index=current_loc_index, key=f"edit_location_{candidate_id}")
            
            # Notice Period Details
            notice_period_details = st.text_area("Notice Period Details", value=current_notice_period_details or "", height=100, key=f"edit_notice_details_{candidate_id}")
        
        with location_col2:
            # Notice Period (updated to match New Candidate form options)
            notice_period_options = ["", "Immediate Joiner", "less than 30 days", "30-60 days", "60-90 days"]
            # Map old values to new options
            current_notice_mapped = current_notice_period
            if current_notice_period == "Immediate":
                current_notice_mapped = "Immediate Joiner"
            elif current_notice_period in ["15 days", "30 days"]:
                current_notice_mapped = "less than 30 days"
            elif current_notice_period in ["45 days", "60 days"]:
                current_notice_mapped = "30-60 days"
            elif current_notice_period == "90 days":
                current_notice_mapped = "60-90 days"
            
            current_notice_index = notice_period_options.index(current_notice_mapped) if current_notice_mapped in notice_period_options else 0
            notice_period = st.selectbox("Notice Period", notice_period_options, index=current_notice_index, key=f"edit_notice_period_{candidate_id}")
            
            # Notes (moved here to match structure)
            notes = st.text_area("Notes/Screening Notes", value=current_notes or "", height=100, key=f"edit_notes_{candidate_id}")
        
        st.markdown("#### 📋 Process Tracking")
        
        process_col1, process_col2 = st.columns(2)
        with process_col1:
            next_steps = st.text_area("Next Steps", value=current_next_steps or "", height=100, 
                                    placeholder="Schedule interview, send assessment, client review, etc.", key=f"edit_next_steps_{candidate_id}")
        
        with process_col2:
            interview_feedback = st.text_area("Interview Feedback", value=current_interview_feedback or "", height=100,
                                            placeholder="GA interview notes, client feedback, assessment results, etc.", key=f"edit_interview_feedback_{candidate_id}")
        
        # Form buttons
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
        
        with col_btn1:
            submit_button = st.form_submit_button("💾 Update Candidate", type="primary")
        
        with col_btn2:
            cancel_button = st.form_submit_button("❌ Cancel")
        
        if cancel_button:
            st.session_state.show_edit_candidate_form = False
            st.session_state.edit_candidate_id = None
            # Force clear form state
            for key in list(st.session_state.keys()):
                if 'vendor_partner_edit' in key or 'edit_candidate_form' in key:
                    del st.session_state[key]
            st.rerun()
        
        if submit_button:
            if candidate_name.strip():
                # Update candidate
                current_user_email = st.session_state.get('user_email', 'system')
                
                # Get staffing assignment values from outside-form dropdowns
                edit_staffing_plan_id = plan_names.get(edit_selected_staffing_plan, None) if edit_selected_staffing_plan else None
                edit_pipeline_id = pipeline_names.get(edit_linked_pipeline, None) if edit_linked_pipeline else None
                
                candidate_update_data = (
                    candidate_name, role, experience, skills, int(edit_client_id) if edit_client_id else None,
                    status, status_flag, edit_pipeline_id, source, vendor_partner,
                    location, notice_period, notice_period_details, current_resume_path, notes,
                    email_id, contact_number, expected_ctc, position_start_date,
                    next_steps, interview_feedback, drop_reason,
                    # New staffing assignment fields
                    edit_staffing_plan_id, edit_selected_staffing_owner, edit_selected_staffing_role,
                    # New fields for Not Linked to Staffing Plan
                    not_linked_to_staffing_plan, edit_selected_staffing_manager
                )
                
                if update_candidate(candidate_id, candidate_update_data):
                    st.session_state.show_edit_candidate_form = False
                    st.session_state.edit_candidate_id = None
                    st.rerun()
            else:
                st.error("Please provide a candidate name.")

def show_view_candidate():
    """Display candidate details in view-only mode with clean professional layout"""
    candidate_id = st.session_state.get('view_candidate_id')
    if not candidate_id:
        st.error("No candidate selected for viewing.")
        return
    
    # Load full candidate data including all fields
    import os
    import psycopg2
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        candidate_data_table = env_manager.get_table_name('candidate_data')
        master_clients_table = env_manager.get_table_name('master_clients')
        talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
        
        query = f"""
            SELECT 
                cd.candidate_name, cd.role, cd.experience_level, cd.skills, 
                mc.client_name, cd.status, cd.status_flag, tp.name as pipeline_name,
                cd.source, cd.vendor_partner, cd.location, cd.notice_period, 
                cd.notice_period_details, cd.resume_file_path, cd.notes,
                cd.email_id, cd.contact_number, cd.expected_ctc, cd.position_start_date,
                cd.next_steps, cd.interview_feedback, cd.created_date, cd.created_flag,
                cd.data_source, cd.created_by
            FROM {candidate_data_table} cd
            LEFT JOIN {master_clients_table} mc ON cd.hire_for_client_id = mc.master_client_id
            LEFT JOIN {talent_pipelines_table} tp ON cd.linked_pipeline_id = tp.id
            WHERE cd.id = %s
        """
        cursor.execute(query, (candidate_id,))
        candidate = cursor.fetchone()
    finally:
        conn.close()
    
    if not candidate:
        st.error("Candidate not found.")
        return
    
    # Action buttons at the top
    st.markdown("---")
    col_search, col_edit, col_delete, col_close = st.columns([1, 1, 1, 3])
    
    with col_search:
        st.button("🔍 Search", disabled=True, help="Search functionality")
    
    with col_edit:
        if st.button("✏️ Edit"):
            st.session_state.edit_candidate_id = candidate_id
            st.session_state.show_edit_candidate_form = True
            st.session_state.show_view_candidate = False
            st.rerun()
    
    with col_delete:
        if st.button("🗑️ Delete"):
            st.session_state.delete_candidate_id = candidate_id
            st.session_state.confirm_delete = True
            st.session_state.show_view_candidate = False
            st.rerun()
    
    with col_close:
        if st.button("❌ Close View"):
            st.session_state.show_view_candidate = False
            st.session_state.view_candidate_id = None
            st.rerun()
    
    st.markdown("---")
    
    # Clean header layout showing main details
    st.markdown("### Candidate Record")
    
    # Main information table
    main_info = f"""
    | **Field** | **Value** |
    |-----------|-----------|
    | **Candidate Name** | {candidate[0] or 'Not specified'} |
    | **Client** | {candidate[4] or 'Not specified'} |
    | **Role** | {candidate[1] or 'Not specified'} |
    | **Location** | {candidate[10] or 'Not specified'} |
    | **Status** | {candidate[5] or 'Not specified'} |
    """
    st.markdown(main_info)
    
    st.markdown("---")
    
    # Detailed sections in organized layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Professional Details**")
        prof_info = f"""
        | Field | Value |
        |-------|-------|
        | Experience Level | {candidate[2] or 'Not specified'} |
        | Skills | {candidate[3] or 'Not specified'} |
        | Employment Type | {candidate[6] or 'Not specified'} |
        | Source | {candidate[8] or 'Not specified'} |
        | Vendor Partner | {candidate[9] or 'Not specified'} |
        | Pipeline | {candidate[7] or 'Not specified'} |
        """
        st.markdown(prof_info)
        
        st.markdown("**Timing & Availability**")
        timing_info = f"""
        | Field | Value |
        |-------|-------|
        | Notice Period | {candidate[11] or 'Not specified'} |
        | Position Start Date | {candidate[18] if candidate[18] else 'Not specified'} |
        | Expected CTC | {candidate[17] or 'Not specified'} |
        """
        st.markdown(timing_info)
    
    with col2:
        st.markdown("**Contact Information**")
        contact_info = f"""
        | Field | Value |
        |-------|-------|
        | Email | {candidate[15] or 'Not specified'} |
        | Contact Number | {candidate[16] or 'Not specified'} |
        """
        st.markdown(contact_info)
        
        st.markdown("**Process Information**")
        process_info = f"""
        | Field | Value |
        |-------|-------|
        | Created Method | {'Form Entry' if candidate[22] == 'Y' else 'Data Import'} |
        | Data Source | {candidate[23] or 'Manual'} |
        | Created By | {candidate[24] or 'System'} |
        | Added Date | {candidate[21] if candidate[21] else 'Not specified'} |
        """
        st.markdown(process_info)
    
    # Additional details section
    if any([candidate[12], candidate[14], candidate[19], candidate[20]]):
        st.markdown("---")
        st.markdown("**Additional Details**")
        
        if candidate[12]:
            st.markdown(f"**Notice Period Details:** {candidate[12]}")
        
        if candidate[14]:
            st.markdown(f"**Notes:** {candidate[14]}")
        
        if candidate[19]:
            st.markdown(f"**Next Steps:** {candidate[19]}")
        
        if candidate[20]:
            st.markdown(f"**Interview Feedback:** {candidate[20]}")
    
    # Resume section
    if candidate[13]:
        st.markdown("---")
        st.markdown("**Resume Information**")
        st.markdown(f"File Path: `{candidate[13]}`")

def delete_candidate(candidate_id):
    """Delete a candidate from the database"""
    import os
    import psycopg2
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        candidate_data_table = env_manager.get_table_name('candidate_data')
        cursor.execute(f"DELETE FROM {candidate_data_table} WHERE id = %s", (candidate_id,))
        conn.commit()
        st.success("✅ Candidate deleted successfully!")
    except Exception as e:
        st.error(f"Error deleting candidate: {str(e)}")
    finally:
        conn.close()

def load_candidate_for_edit(candidate_id):
    """Load candidate data for editing"""
    import os
    import psycopg2
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        # First, add the new staffing columns if they don't exist
        try:
            candidate_data_table = env_manager.get_table_name('candidate_data')
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_plan_id INTEGER")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_owner VARCHAR(255)")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_role VARCHAR(255)")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS not_linked_to_staffing_plan BOOLEAN DEFAULT FALSE")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_manager VARCHAR(255)")
            conn.commit()
        except Exception as e:
            # Columns might already exist
            pass
        
        query = f"""
            SELECT 
                candidate_name, role, experience_level, skills, hire_for_client_id,
                status, status_flag, linked_pipeline_id, source, vendor_partner,
                location, notice_period, notice_period_details, resume_file_path, notes,
                email_id, contact_number, expected_ctc, position_start_date,
                next_steps, interview_feedback, drop_reason, staffing_plan_id, staffing_owner, staffing_role,
                not_linked_to_staffing_plan, staffing_manager
            FROM {candidate_data_table} 
            WHERE id = %s
        """
        cursor.execute(query, (candidate_id,))
        return cursor.fetchone()
    finally:
        conn.close()

def update_candidate(candidate_id, candidate_data):
    """Update candidate information"""
    import os
    import psycopg2
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        # First, add the new staffing columns if they don't exist
        try:
            candidate_data_table = env_manager.get_table_name('candidate_data')
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_plan_id INTEGER")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_owner VARCHAR(255)")
            cursor.execute(f"ALTER TABLE {candidate_data_table} ADD COLUMN IF NOT EXISTS staffing_role VARCHAR(255)")
            conn.commit()
        except Exception as e:
            # Columns might already exist
            pass
        
        cursor.execute(f"""
            UPDATE {candidate_data_table} SET
                candidate_name = %s, role = %s, experience_level = %s, skills = %s,
                hire_for_client_id = %s, status = %s, status_flag = %s,
                linked_pipeline_id = %s, source = %s, vendor_partner = %s,
                location = %s, notice_period = %s, notice_period_details = %s,
                resume_file_path = %s, notes = %s, email_id = %s, contact_number = %s,
                expected_ctc = %s, position_start_date = %s, next_steps = %s,
                interview_feedback = %s, drop_reason = %s, staffing_plan_id = %s, staffing_owner = %s, staffing_role = %s,
                not_linked_to_staffing_plan = %s, staffing_manager = %s,
                updated_date = CURRENT_TIMESTAMP, last_manual_edit = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (*candidate_data, candidate_id))
        conn.commit()
        st.success("✅ Candidate updated successfully!")
        return True
    except Exception as e:
        st.error(f"Error updating candidate: {str(e)}")
        return False
    finally:
        conn.close()

def display_candidate_list():
    """Display list of existing candidates"""
    st.markdown("### 📋 Candidate List")
    
    import os
    import psycopg2
    
    # Initialize default filter states
    if 'candidate_activity_filter' not in st.session_state:
        st.session_state.candidate_activity_filter = "Active Only"
    
    # Enhanced Filters Section
    st.markdown("#### 🔍 Filters")
    
    # Create filter columns - adding activity status filter and drop reason filter
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5, filter_col6, filter_col7 = st.columns(7)
    
    with filter_col1:
        # Name search
        search_term = st.text_input("Search by Name", placeholder="Enter candidate name...")
    
    with filter_col2:
        # Activity Status filter (Active/Inactive)
        activity_status_options = ["Active Only", "Inactive Only", "All"]
        selected_activity_status = st.selectbox("Activity Status", activity_status_options, key="candidate_activity_filter")
    
    with filter_col3:
        # Status filter - force refresh by clearing cache
        if 'status_options_cache' not in st.session_state:
            st.session_state.status_options_cache = None
        
        # Force refresh of status options
        conn_temp = psycopg2.connect(env_manager.get_database_url())
        cursor_temp = conn_temp.cursor()
        candidate_table_temp = env_manager.get_table_name('candidate_data')
        cursor_temp.execute(f"SELECT DISTINCT status FROM {candidate_table_temp} WHERE status IS NOT NULL AND status != '' ORDER BY status")
        status_options = ["All"] + [row[0] for row in cursor_temp.fetchall()]
        conn_temp.close()
        
        # Store in session state for debugging
        st.session_state.status_options_cache = status_options
        
        selected_status = st.selectbox("Filter by Status", status_options, key="candidate_status_filter")
    
    with filter_col4:
        # Client filter
        conn_temp = psycopg2.connect(env_manager.get_database_url())
        cursor_temp = conn_temp.cursor()
        candidate_table_temp = env_manager.get_table_name('candidate_data')
        clients_table_temp = env_manager.get_table_name('master_clients')
        cursor_temp.execute(f"""
            SELECT DISTINCT mc.client_name 
            FROM {candidate_table_temp} cd
            LEFT JOIN {clients_table_temp} mc ON cd.hire_for_client_id = mc.master_client_id
            WHERE mc.client_name IS NOT NULL
            ORDER BY mc.client_name
        """)
        client_options = ["All"] + [row[0] for row in cursor_temp.fetchall()]
        conn_temp.close()
        
        selected_client = st.selectbox("Filter by Client", client_options, key="candidate_client_filter")
    
    with filter_col5:
        # Role filter
        conn_temp = psycopg2.connect(env_manager.get_database_url())
        cursor_temp = conn_temp.cursor()
        candidate_table_temp = env_manager.get_table_name('candidate_data')
        cursor_temp.execute(f"SELECT DISTINCT role FROM {candidate_table_temp} WHERE role IS NOT NULL AND role != '' ORDER BY role")
        role_options = ["All"] + [row[0] for row in cursor_temp.fetchall()]
        conn_temp.close()
        
        selected_role = st.selectbox("Filter by Role", role_options, key="candidate_role_filter")
    
    with filter_col6:
        # Supply Plan filter
        conn_temp = psycopg2.connect(env_manager.get_database_url())
        cursor_temp = conn_temp.cursor()
        candidate_table_temp = env_manager.get_table_name('candidate_data')
        staffing_table_temp = env_manager.get_table_name('staffing_plans')
        cursor_temp.execute(f"""
            SELECT DISTINCT sp.plan_name 
            FROM {candidate_table_temp} cd
            LEFT JOIN {staffing_table_temp} sp ON cd.staffing_plan_id = sp.id
            WHERE sp.plan_name IS NOT NULL
            ORDER BY sp.plan_name
        """)
        supply_plan_options = ["All"] + [row[0] for row in cursor_temp.fetchall()]
        conn_temp.close()
        
        selected_supply_plan = st.selectbox("Filter by Supply Plan", supply_plan_options, key="candidate_supply_plan_filter")
    
    with filter_col7:
        # Drop Reason filter
        conn_temp = psycopg2.connect(env_manager.get_database_url())
        cursor_temp = conn_temp.cursor()
        candidate_table_temp = env_manager.get_table_name('candidate_data')
        cursor_temp.execute(f"SELECT DISTINCT drop_reason FROM {candidate_table_temp} WHERE drop_reason IS NOT NULL AND drop_reason != '' ORDER BY drop_reason")
        drop_reason_options = ["All"] + [row[0] for row in cursor_temp.fetchall()]
        conn_temp.close()
        
        selected_drop_reason = st.selectbox("Filter by Drop Reason", drop_reason_options, key="candidate_drop_reason_filter")
    
    # Clear filters button and active filters display
    filter_info_col, clear_col = st.columns([3, 1])
    
    with filter_info_col:
        # Show active filters
        active_filters = []
        if search_term:
            active_filters.append(f"Name: '{search_term}'")
        if selected_activity_status != "All":
            active_filters.append(f"Activity: {selected_activity_status}")
        if selected_status != "All":
            active_filters.append(f"Status: {selected_status}")
        if selected_client != "All":
            active_filters.append(f"Client: {selected_client}")
        if selected_role != "All":
            active_filters.append(f"Role: {selected_role}")
        if selected_supply_plan != "All":
            active_filters.append(f"Supply Plan: {selected_supply_plan}")
        if selected_drop_reason != "All":
            active_filters.append(f"Drop Reason: {selected_drop_reason}")
        
        if active_filters:
            st.info(f"🔍 Active filters: {' | '.join(active_filters)}")
    
    with clear_col:
        if st.button("🗑️ Clear All Filters", key="clear_candidate_filters"):
            st.session_state.candidate_activity_filter = "Active Only"
            st.session_state.candidate_status_filter = "All"
            st.session_state.candidate_client_filter = "All" 
            st.session_state.candidate_role_filter = "All"
            st.session_state.candidate_supply_plan_filter = "All"
            st.session_state.candidate_drop_reason_filter = "All"
            st.session_state.candidate_page = 0
            st.rerun()
    
    # Reset pagination when filters change
    current_filters = (search_term, selected_activity_status, selected_status, selected_client, selected_role, selected_supply_plan, selected_drop_reason)
    if 'last_candidate_filters' not in st.session_state:
        st.session_state.last_candidate_filters = current_filters
    elif st.session_state.last_candidate_filters != current_filters:
        st.session_state.candidate_page = 0
        st.session_state.last_candidate_filters = current_filters
    
    # Pagination controls
    if 'candidate_page' not in st.session_state:
        st.session_state.candidate_page = 0
    
    records_per_page = 30
    
    # Build WHERE clause based on filters
    where_conditions = []
    params = []
    
    # Handle Activity Status filter (Active/Inactive)
    if selected_activity_status == "Active Only":
        # Active = NOT (Rejected, On Hold, On-Hold, RNR, Dropped, Internal Dropped)
        where_conditions.append("(cd.status NOT ILIKE %s AND cd.status NOT ILIKE %s AND cd.status NOT ILIKE %s AND cd.status NOT ILIKE %s AND cd.status != %s AND cd.status NOT ILIKE %s)")
        params.extend(['%Rejected%', '%On Hold%', '%On-Hold%', '%RNR%', 'Dropped', '%Internal Dropped%'])
    elif selected_activity_status == "Inactive Only":
        # Inactive = Rejected, On Hold, On-Hold, RNR, Dropped, Internal Dropped
        where_conditions.append("(cd.status ILIKE %s OR cd.status ILIKE %s OR cd.status ILIKE %s OR cd.status ILIKE %s OR cd.status = %s OR cd.status ILIKE %s)")
        params.extend(['%Rejected%', '%On Hold%', '%On-Hold%', '%RNR%', 'Dropped', '%Internal Dropped%'])
    # If "All" is selected, no additional filter is applied
    
    if search_term:
        where_conditions.append("cd.candidate_name ILIKE %s")
        params.append(f'%{search_term}%')
    
    if selected_status != "All":
        where_conditions.append("cd.status = %s")
        params.append(selected_status)
    
    if selected_client != "All":
        where_conditions.append("mc.client_name = %s")
        params.append(selected_client)
    
    if selected_role != "All":
        where_conditions.append("cd.role = %s")
        params.append(selected_role)
    
    if selected_supply_plan != "All":
        where_conditions.append("sp.plan_name = %s")
        params.append(selected_supply_plan)
    
    if selected_drop_reason != "All":
        where_conditions.append("cd.drop_reason = %s")
        params.append(selected_drop_reason)
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Get total count with filters
    conn = psycopg2.connect(env_manager.get_database_url())
    cursor = conn.cursor()
    
    # Get environment-specific table names
    candidate_table = env_manager.get_table_name('candidate_data')
    clients_table = env_manager.get_table_name('master_clients')
    staffing_table = env_manager.get_table_name('staffing_plans')
    
    count_query = f"""
        SELECT COUNT(*) 
        FROM {candidate_table} cd
        LEFT JOIN {clients_table} mc ON cd.hire_for_client_id = mc.master_client_id
        LEFT JOIN {staffing_table} sp ON cd.staffing_plan_id = sp.id
        {where_clause}
    """
    cursor.execute(count_query, params)
    
    total_records = cursor.fetchone()[0]
    total_pages = (total_records + records_per_page - 1) // records_per_page
    
    # Pagination controls
    col_prev, col_page_info, col_next = st.columns([1, 2, 1])
    
    with col_prev:
        if st.button("⬅️ Previous", disabled=st.session_state.candidate_page == 0):
            st.session_state.candidate_page -= 1
            st.rerun()
    
    with col_page_info:
        start_record = st.session_state.candidate_page * records_per_page + 1
        end_record = min((st.session_state.candidate_page + 1) * records_per_page, total_records)
        st.markdown(f"**Showing {start_record}-{end_record} of {total_records} candidates | Page {st.session_state.candidate_page + 1} of {total_pages}**")
    
    with col_next:
        if st.button("Next ➡️", disabled=st.session_state.candidate_page >= total_pages - 1):
            st.session_state.candidate_page += 1
            st.rerun()
    
    # Get paginated candidates with filters
    try:
        offset = st.session_state.candidate_page * records_per_page
        
        pipelines_table = env_manager.get_table_name('talent_pipelines')
        
        main_query = f"""
            SELECT 
                cd.id, cd.candidate_name, cd.role, cd.experience_level, 
                cd.status, mc.client_name, tp.name as pipeline_name,
                cd.source, cd.location, cd.notice_period, cd.created_date,
                cd.email_id, cd.contact_number, cd.expected_ctc, cd.data_source,
                cd.vendor_partner, cd.created_flag, sp.plan_name as supply_plan_name
            FROM {candidate_table} cd
            LEFT JOIN {clients_table} mc ON cd.hire_for_client_id = mc.master_client_id
            LEFT JOIN {pipelines_table} tp ON cd.linked_pipeline_id = tp.id
            LEFT JOIN {staffing_table} sp ON cd.staffing_plan_id = sp.id
            {where_clause}
            ORDER BY cd.created_date DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(main_query, params + [records_per_page, offset])
        
        candidates = cursor.fetchall()
        
        # Get comprehensive metrics from database - always show total database counts (not filtered)
        # Total count (always all candidates)
        cursor.execute(f"SELECT COUNT(*) FROM {candidate_table}")
        total_count = cursor.fetchone()[0]
        
        # Active count
        cursor.execute(f"""
            SELECT COUNT(*) FROM {candidate_table} 
            WHERE (status NOT ILIKE '%Rejected%' AND status NOT ILIKE '%On Hold%' 
                   AND status NOT ILIKE '%On-Hold%' AND status NOT ILIKE '%RNR%' 
                   AND status != 'Dropped' AND status NOT ILIKE '%Internal Dropped%'
                   AND status NOT ILIKE '%Candidate RNR/Dropped%' AND status NOT ILIKE '%Requirement on hold%'
                   AND status NOT ILIKE '%Duplicate Profile%')
        """)
        active_count = cursor.fetchone()[0]
        
        # Inactive count
        cursor.execute(f"""
            SELECT COUNT(*) FROM {candidate_table} 
            WHERE (status ILIKE '%Rejected%' OR status ILIKE '%On Hold%' 
                   OR status ILIKE '%On-Hold%' OR status ILIKE '%RNR%' 
                   OR status = 'Dropped' OR status ILIKE '%Internal Dropped%'
                   OR status ILIKE '%Candidate RNR/Dropped%' OR status ILIKE '%Requirement on hold%'
                   OR status ILIKE '%Duplicate Profile%')
        """)
        inactive_count = cursor.fetchone()[0]
        
        # Hired count - using actual production status values
        cursor.execute(f"""
            SELECT COUNT(*) FROM {candidate_table} 
            WHERE status = 'Staffed'
        """)
        hired_count = cursor.fetchone()[0]
        
        # Vendor count - all candidates are from vendors in production
        cursor.execute(f"""
            SELECT COUNT(*) FROM {candidate_table} 
            WHERE (source = 'Vendor' OR vendor_partner IS NOT NULL OR source IS NOT NULL)
        """)
        vendor_count = cursor.fetchone()[0]
        
        # Verify we got the data
        if not candidates:
            st.warning("No candidate records found. Please check the data consolidation.")
    finally:
        conn.close()
    
    if candidates:
        # Create dataframe for display
        import pandas as pd
        df_data = []
        for candidate in candidates:
            # Add data source and creation indicators
            source_indicator = ""
            if candidate[14]:  # data_source field (index 14)
                if candidate[14] == 'import':
                    source_indicator = " 📊"
                elif candidate[14] == 'hybrid':
                    source_indicator = " 🔄"
                # manual gets no indicator (default)
            
            # Add created flag indicator
            created_indicator = ""
            if candidate[16]:  # created_flag field (index 16)
                if candidate[16] == 'N':
                    created_indicator = " (Import)"
                # 'Y' gets no indicator (form-created is default)
            
            # Determine activity status based on status
            status = candidate[4] or ''
            is_inactive = any(inactive_keyword.lower() in status.lower() for inactive_keyword in ['rejected', 'on hold', 'on-hold', 'rnr'])
            activity_status = "🔴 Inactive" if is_inactive else "🟢 Active"
            
            df_data.append({
                'ID': candidate[0],
                'Candidate Name': (candidate[1] or '') + source_indicator + created_indicator,
                'Role': candidate[2] or '',
                'Experience': candidate[3] or '',
                'Activity': activity_status,
                'Status': candidate[4] or '',
                'Client': candidate[5] or '',
                'Pipeline': candidate[6] or '',
                'Source': candidate[7] or '',
                'Vendor Partner': candidate[15] or '',
                'Linked to Supply Plan': candidate[17] if len(candidate) > 17 and candidate[17] else 'Not Linked',
                'Location': candidate[8] or '',
                'Notice Period': candidate[9] or '',
                'Email': candidate[11] or '',
                'Contact': candidate[12] or '',
                'Created': 'Form' if candidate[16] == 'Y' else 'Import',
                'Added Date': candidate[10] if candidate[10] else ''
            })
        
        # Display metrics using actual database totals (not filtered counts)
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Candidates", total_count)
        with col2:
            st.metric("Active Candidates", active_count)
        with col3:
            st.metric("Inactive Candidates", inactive_count)
        with col4:
            st.metric("Hired", hired_count)
        with col5:
            st.metric("From Vendors", vendor_count)
        
        st.markdown("---")
        
        # Create table header
        header_col1, header_col2, header_col3, header_col4, header_col5, header_col6, header_col7, header_col8, header_col9 = st.columns([2, 1.3, 1.3, 1, 1, 1.2, 0.8, 0.8, 0.8])
        
        with header_col1:
            st.markdown("**Candidate Name**")
        with header_col2:
            st.markdown("**Client**")
        with header_col3:
            st.markdown("**Role**")
        with header_col4:
            st.markdown("**Location**")
        with header_col5:
            st.markdown("**Status**")
        with header_col6:
            st.markdown("**Linked to Supply Plan**")
        with header_col7:
            st.markdown("**View**")
        with header_col8:
            st.markdown("**Edit**")
        with header_col9:
            st.markdown("**Delete**")
        
        st.markdown("---")
        
        # Display candidates as table rows
        for candidate in candidates:
            row_col1, row_col2, row_col3, row_col4, row_col5, row_col6, row_col7, row_col8, row_col9 = st.columns([2, 1.3, 1.3, 1, 1, 1.2, 0.8, 0.8, 0.8])
            
            with row_col1:
                candidate_name = candidate[1] or 'Unnamed'
                # Add source indicator for imported candidates
                source_indicator = ""
                if candidate[14] == 'import':  # data_source = 'import' means imported
                    source_indicator = " 📊"
                st.write(f"{candidate_name}{source_indicator}")
            
            with row_col2:
                st.write(candidate[5] or 'Not specified')
            
            with row_col3:
                st.write(candidate[2] or 'Not specified')
            
            with row_col4:
                st.write(candidate[8] or 'Not specified')
            
            with row_col5:
                st.write(candidate[4] or 'Not specified')
            
            with row_col6:
                # Display linked supply plan name or "Not Linked"
                supply_plan = candidate[17] if len(candidate) > 17 and candidate[17] else None
                if supply_plan:
                    st.write(f"🔗 {supply_plan}")
                else:
                    st.write("❌ Not Linked")
            
            with row_col7:
                if st.button("👁️", key=f"view_{candidate[0]}", help="View details"):
                    st.session_state.view_candidate_id = candidate[0]
                    st.session_state.show_view_candidate = True
                    st.rerun()
            
            with row_col8:
                if st.button("✏️", key=f"edit_{candidate[0]}", help="Edit candidate"):
                    st.session_state.edit_candidate_id = candidate[0]
                    st.session_state.show_edit_candidate_form = True
                    st.rerun()
            
            with row_col9:
                if st.button("🗑️", key=f"delete_{candidate[0]}", help="Delete candidate"):
                    st.session_state.delete_candidate_id = candidate[0]
                    st.session_state.confirm_delete = True
                    st.rerun()
            
            st.markdown("---")
        
        # Handle delete confirmation
        if st.session_state.get('confirm_delete', False):
            candidate_id = st.session_state.get('delete_candidate_id')
            candidate_name = next((c[1] for c in candidates if c[0] == candidate_id), "Unknown")
            
            st.warning(f"⚠️ Are you sure you want to delete candidate '{candidate_name}'? This action cannot be undone.")
            
            col_confirm1, col_confirm2, col_confirm3 = st.columns([1, 1, 4])
            with col_confirm1:
                if st.button("✅ Yes, Delete", type="primary"):
                    delete_candidate(candidate_id)
                    st.session_state.confirm_delete = False
                    st.session_state.delete_candidate_id = None
                    st.rerun()
            
            with col_confirm2:
                if st.button("❌ Cancel"):
                    st.session_state.confirm_delete = False
                    st.session_state.delete_candidate_id = None
                    st.rerun()
    else:
        st.info("No candidates found. Click 'Add Candidate' to get started!")
        st.markdown("""
        **This system will help you:**
        - Track candidate information and progress
        - Link candidates to specific pipelines and clients
        - Manage vendor partnerships
        - Store resumes and notes
        - Monitor hiring pipeline performance
        """)

def talent_management_section():
    """Talent Management section with permission enforcement"""
    
    # Check permissions for talent management
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    # Check if user can view talent management
    if not permission_manager.has_permission(current_user_email, "Supply Planning", "Talent Management", "view"):
        permission_manager.show_access_denied_message("Supply Planning", "Talent Management")
        return
    
    # Import supply data manager
    from utils.supply_data_manager import SupplyDataManager
    
    # Get env_manager from session state
    env_manager = st.session_state.get('env_manager')
    if not env_manager:
        st.error("Environment manager not found in session state")
        return
    
    # Initialize supply manager
    supply_manager = SupplyDataManager(env_manager)
    
    # Get supply statistics
    stats = supply_manager.get_supply_statistics()
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Talent", stats.get('total_talent', 0))
    
    with col2:
        st.metric("FTE", stats.get('total_fte', 0))
    
    with col3:
        st.metric("Non-FTE", stats.get('total_nfte', 0))
    
    with col4:
        st.metric("Avg Availability", f"{stats.get('avg_availability', 0)}%")
    
    st.markdown("---")
    
    # Initialize tab state
    if 'active_talent_tab' not in st.session_state:
        st.session_state.active_talent_tab = 0
    
    # Create sub-tabs within Talent Management with state preservation
    tab_names = ["📊 Unified Talent Management", "🎯 Hiring Talent Management", "🔄 Supply Pipeline Management"]
    
    # Use radio buttons for better state management instead of tabs
    selected_tab = st.radio(
        "Select Talent Management Section:",
        tab_names,
        index=st.session_state.active_talent_tab,
        horizontal=True,
        key="talent_management_tab_selector"
    )
    
    # Update session state when tab changes
    st.session_state.active_talent_tab = tab_names.index(selected_tab)
    
    st.markdown("---")
    
    if selected_tab == "📊 Unified Talent Management":
        unified_talent_management_section(supply_manager, permission_manager, current_user_email)
    elif selected_tab == "🎯 Hiring Talent Management":
        hiring_talent_management_section()
    elif selected_tab == "🔄 Supply Pipeline Management":
        supply_pipeline_management_section()

def unified_talent_management_section(supply_manager, permission_manager, current_user_email):
    """Unified Talent Management sub-section"""
    
    # Get all talent data
    talent_data = supply_manager.get_all_talent_data()
    
    # Debug: Show what data we're getting
    st.write(f"🔍 DEBUG: Loaded {len(talent_data)} talent records")
    if not talent_data.empty:
        st.write(f"🔍 DEBUG: Columns: {list(talent_data.columns)}")
        st.write(f"🔍 DEBUG: First few talent_ids: {talent_data['talent_id'].head().tolist()}")
    
    if not talent_data.empty:
        # Filtering options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            type_filter = st.selectbox(
                "Filter by Type",
                options=["All", "FTE", "Non-FTE"],
                index=0
            )
        
        with col2:
            status_filter = st.selectbox(
                "Filter by Assignment Status",
                options=["All"] + sorted(talent_data['assignment_status'].dropna().unique().tolist()),
                index=0
            )
        
        with col3:
            region_filter = st.selectbox(
                "Filter by Region",
                options=["All"] + sorted(talent_data['region'].dropna().unique().tolist()),
                index=0
            )
        
        # Apply filters
        filtered_data = talent_data.copy()
        
        if type_filter != "All":
            filtered_data = filtered_data[filtered_data['type'] == type_filter]
        
        if status_filter != "All":
            filtered_data = filtered_data[filtered_data['assignment_status'] == status_filter]
        
        if region_filter != "All":
            filtered_data = filtered_data[filtered_data['region'] == region_filter]
        
        st.info(f"Showing {len(filtered_data)} of {len(talent_data)} talent records")
        
        # Prepare data for editing
        if not filtered_data.empty:
            # Select columns for display/editing (removed assigned_to and billable)
            display_columns = [
                'talent_id', 'name', 'role', 'grade', 'doj', 'assignment_status', 
                'type', 'assignment_percentage', 'availability_percentage',
                'employment_status', 'email_id', 'years_of_exp', 'skills', 
                'region', 'partner'
            ]
            
            # Create editable dataframe
            edit_df = filtered_data[display_columns].copy()
            
            # Rename columns for better display (removed assigned_to, billable, client, and track)
            edit_df = edit_df.rename(columns={
                'talent_id': 'Talent ID',
                'name': 'Name',
                'role': 'Role',
                'grade': 'Grade',
                'doj': 'Date of Joining',
                'assignment_status': 'Assignment Status',
                'type': 'Type',
                'assignment_percentage': 'Assigned %',
                'availability_percentage': 'Availability %',
                'employment_status': 'Employment Status',
                'email_id': 'Email ID',
                'years_of_exp': 'Years of Experience',
                'skills': 'Skills',
                'region': 'Region',
                'partner': 'Partner'
            })
            
            # Editable data table
            st.subheader("✏️ Editable Talent Data")
            
            # Show validation warnings for NFTE records
            nfte_data = filtered_data[filtered_data['type'] == 'Non-FTE']
            if not nfte_data.empty:
                validation_issues = []
                for _, row in nfte_data.iterrows():
                    missing = supply_manager.validate_nfte_mandatory_fields(row)
                    if missing:
                        validation_issues.append(f"{row['name']}: Missing {', '.join(missing)}")
                
                if validation_issues:
                    st.warning(f"⚠️ NFTE Validation Issues:\n" + "\n".join(validation_issues))
            
            # Store original data for change detection
            if 'original_talent_data' not in st.session_state:
                st.session_state.original_talent_data = edit_df.copy()
            
            edited_df = st.data_editor(
                edit_df,
                num_rows="dynamic",
                use_container_width=True,
                key="supply_data_editor"
            )
            
            # Detect changes by comparing dataframes
            changes_detected = False
            try:
                if not edited_df.equals(edit_df):
                    changes_detected = True
                    st.info(f"📝 Changes detected in talent data")
                else:
                    st.info("📋 No changes detected")
            except:
                # If comparison fails, assume changes exist
                changes_detected = True
                st.info(f"📝 Data modified - ready to save")
            
            # Save changes button
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col2:
                if permission_manager.permission_protected_button(
                    current_user_email, "Supply Planning", "Talent Management", "edit", 
                    "💾 Save Changes", type="primary", use_container_width=True, key="save_talent_changes"
                ):
                    import psycopg2
                    import pandas as pd
                    
                    st.write("🔄 Starting save operation...")
                    
                    try:
                        # Get database connection using the centralized utility
                        from utils.database_connection import get_database_connection
                        conn = get_database_connection()
                        cursor = conn.cursor()
                        
                        # Get env_manager from session state
                        env_manager = st.session_state.get('env_manager')
                        if not env_manager:
                            st.error("Environment manager not found in session state")
                            return
                        
                        updates_count = 0
                        total_records = len(edited_df)
                        
                        st.write(f"📊 Processing {total_records} records...")
                        
                        # Process each row in the edited dataframe
                        for idx in range(total_records):
                            try:
                                # Get the original record ID from filtered data and convert numpy types
                                record_id = int(filtered_data.iloc[idx]['id']) if pd.notna(filtered_data.iloc[idx]['id']) else None
                                
                                # Skip if no valid record ID
                                if record_id is None:
                                    st.warning(f"⚠️ Skipping record {idx+1}: No valid ID found")
                                    continue
                                
                                # Get all edited values - handle None/NaN values
                                def safe_str(val):
                                    if pd.isna(val) or val is None:
                                        return ''
                                    return str(val).strip()
                                
                                # Extract all field values from the edited dataframe
                                talent_id = safe_str(edited_df.iloc[idx]['Talent ID'])
                                name = safe_str(edited_df.iloc[idx]['Name'])
                                role = safe_str(edited_df.iloc[idx]['Role'])
                                grade = safe_str(edited_df.iloc[idx]['Grade'])
                                doj = safe_str(edited_df.iloc[idx]['Date of Joining'])
                                assignment_status = safe_str(edited_df.iloc[idx]['Assignment Status'])
                                type_val = safe_str(edited_df.iloc[idx]['Type'])
                                
                                # Handle numeric fields properly - convert numpy types
                                try:
                                    assignment_percentage = float(edited_df.iloc[idx]['Assigned %']) if not pd.isna(edited_df.iloc[idx]['Assigned %']) else 0.0
                                except (ValueError, TypeError):
                                    assignment_percentage = 0.0
                                    
                                try:
                                    availability_percentage = float(edited_df.iloc[idx]['Availability %']) if not pd.isna(edited_df.iloc[idx]['Availability %']) else 0.0
                                except (ValueError, TypeError):
                                    availability_percentage = 0.0
                                
                                employment_status = safe_str(edited_df.iloc[idx]['Employment Status'])
                                email_id = safe_str(edited_df.iloc[idx]['Email ID'])
                                years_of_exp = safe_str(edited_df.iloc[idx]['Years of Experience'])
                                skills = safe_str(edited_df.iloc[idx]['Skills'])
                                region = safe_str(edited_df.iloc[idx]['Region'])
                                partner = safe_str(edited_df.iloc[idx]['Partner'])
                                
                                # Debug: Show what we're updating for the first few records
                                if idx < 3:
                                    st.write(f"Record {idx+1} (ID: {record_id}): Assignment Status = '{assignment_status}', Type = '{type_val}'")
                                
                                # Force update by using UPDATE without WHERE conditions checking for changes
                                talent_supply_table = env_manager.get_table_name('talent_supply')
                                
                                # Build the SQL query string first to avoid f-string issues
                                sql_query = f"""
                                    UPDATE {talent_supply_table}
                                    SET talent_id = %s, name = %s, role = %s, grade = %s, doj = %s, 
                                        assignment_status = %s, type = %s, 
                                        assignment_percentage = %s, availability_percentage = %s,
                                        employment_status = %s, email_id = %s, years_of_exp = %s,
                                        skills = %s, region = %s, partner = %s, updated_at = CURRENT_TIMESTAMP
                                    WHERE id = %s
                                """
                                
                                cursor.execute(sql_query, (talent_id, name, role, grade, doj, assignment_status, type_val, 
                                      assignment_percentage, availability_percentage,
                                      employment_status, email_id, years_of_exp, skills, region,
                                      partner, record_id))
                                
                                # Since we're using WHERE id = %s, rowcount should always be 1 if record exists
                                updates_count += 1
                                    
                            except Exception as row_error:
                                st.error(f"❌ Error updating record {idx+1}: {str(row_error)}")
                                continue
                        
                        # Commit all changes
                        conn.commit()
                        conn.close()
                        
                        st.write(f"✅ Successfully processed {total_records} records and attempted {updates_count} updates")
                        
                        if updates_count > 0:
                            st.success(f"🎉 Successfully saved {updates_count} out of {total_records} records!")
                            
                            # Force refresh by clearing session state
                            keys_to_clear = [k for k in st.session_state.keys() if 'talent' in k.lower() or 'supply' in k.lower()]
                            for key in keys_to_clear:
                                del st.session_state[key]
                            
                            st.info("🔄 Refreshing data... Please wait.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("⚠️ No records were updated")
                            
                    except Exception as e:
                        st.error(f"❌ Error during save operation: {str(e)}")
                        import traceback
                        st.error(f"Full error: {traceback.format_exc()}")
                        if 'conn' in locals():
                            conn.close()
            
            with col3:
                if st.button("🔄 Refresh Data", use_container_width=True):
                    st.rerun()
        
        # Export functionality
        st.markdown("---")
        st.subheader("📤 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Export All Talent Data", use_container_width=True):
                csv_data = talent_data.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv_data,
                    file_name=f"talent_supply_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col2:
            if st.button("📋 Export Filtered Data", use_container_width=True):
                csv_data = filtered_data.to_csv(index=False)
                st.download_button(
                    label="Download Filtered CSV",
                    data=csv_data,
                    file_name=f"filtered_talent_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
    
    else:
        st.warning("No talent data found. Please load supply data first.")
        if st.button("🔄 Load Sample Data"):
            from utils.supply_data_manager import SupplyDataManager
            import subprocess
            
            try:
                # Run the data loading script
                result = subprocess.run(["python", "load_supply_data.py"], 
                                      capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("✅ Sample data loaded successfully!")
                    st.rerun()
                else:
                    st.error(f"❌ Error loading data: {result.stderr}")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    logger.info("Supply Planning page displayed")

def pipeline_configuration_section():
    """Pipeline Configuration section with complete management and permission enforcement"""
    
    # Check permissions for pipeline configuration
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    # Check if user can view pipeline configuration
    if not permission_manager.has_permission(current_user_email, "Supply Planning", "Pipeline Configuration", "view"):
        permission_manager.show_access_denied_message("Supply Planning", "Pipeline Configuration")
        return
    
    st.subheader("🔧 Pipeline Configuration")
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    # Initialize Pipeline Manager
    from utils.pipeline_manager import PipelineManager
    from utils.staffing_plans_manager import StaffingPlansManager
    
    if 'pipeline_manager' not in st.session_state:
        st.session_state.pipeline_manager = PipelineManager(env_manager)
    if 'staffing_plans_manager' not in st.session_state:
        st.session_state.staffing_plans_manager = StaffingPlansManager(env_manager)
    
    pipeline_manager = st.session_state.pipeline_manager
    staffing_manager = st.session_state.staffing_plans_manager
    
    # Sub-tabs for Pipeline Configuration
    pipeline_tab1, pipeline_tab2, pipeline_tab3 = st.tabs([
        "📊 Staffing Plans", 
        "⚙️ Pipeline Management", 
        "📈 Pipeline Analytics"
    ])
    
    # Staffing Plans Tab (Tab 1)
    with pipeline_tab1:
        st.subheader("📊 Staffing Plans Management")
        
        # Display success message if available
        if 'last_save_message' in st.session_state:
            st.success(st.session_state['last_save_message'])
            del st.session_state['last_save_message']
        
        # Display existing staffing plans
        plans = staffing_manager.get_all_staffing_plans()
        if not plans.empty:
            st.markdown("**Existing Staffing Plans:**")
            
            plans_df = pd.DataFrame(plans)
            
            # Display staffing plans with compact format
            for idx, plan in plans_df.iterrows():
                with st.container():
                    # Calculate Total Open Positions for display
                    client_name = plan['client_name']
                    try:
                        open_positions_data = staffing_manager.get_total_open_positions(client_name)
                        total_open = open_positions_data['total_open_positions']
                    except:
                        total_open = plan['target_hires']
                    
                    # Use custom CSS for compact display with field names as headers
                    st.markdown(f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px; margin: 6px 0; background-color: #f9f9f9;">
                        <!-- Header row with plan name and field labels -->
                        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 5px;">
                            <div style="flex: 1;">
                                <h3 style="margin: 0; font-size: 16px; color: #333; font-weight: bold;">{plan['plan_name']}</h3>
                            </div>
                            <div style="display: flex; gap: 20px; align-items: center;">
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Planned Positions</div>
                                </div>
                                <div style="text-align: center; min-width: 90px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Target Hires</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;"># of Roles Linked</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Completion</div>
                                </div>
                            </div>
                        </div>
                        <!-- Data row with client name and values -->
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                            <div style="flex: 1;">
                                <p style="margin: 0; font-size: 12px; color: #666;">Client: {plan['client_name']}</p>
                            </div>
                            <div style="display: flex; gap: 20px; align-items: center;">
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-size: 12px; color: #666;">{plan['planned_positions']}</div>
                                </div>
                                <div style="text-align: center; min-width: 90px;">
                                    <div style="font-size: 12px; color: #666;">{plan['target_hires']}</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-size: 12px; color: #666;">{staffing_manager.get_roles_count(plan['id'])}</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-size: 12px; color: #666;">{plan['completion_percentage']:.0f}%</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Action buttons in a single row with smaller buttons
                    col1, col2, col3 = st.columns([6, 1, 1])
                    with col1:
                        st.write("")  # Spacer
                    with col2:
                        if st.button("✏️", key=f"edit_plan_tab1_{plan['id']}", help="Edit this staffing plan"):
                            st.session_state.edit_staffing_plan_id = plan['id']
                            st.session_state.show_staffing_form = True
                            # Clear existing pipeline planning data so it gets reloaded for this plan
                            if 'pipeline_planning_data' in st.session_state:
                                del st.session_state['pipeline_planning_data']
                            st.rerun()
                    
                    with col3:
                        if st.button("🗑️", key=f"delete_plan_tab1_{plan['id']}", help="Delete this staffing plan"):
                            if staffing_manager.delete_staffing_plan(plan['id']):
                                st.success(f"Deleted staffing plan: {plan['plan_name']}")
                                st.rerun()
                            else:
                                st.error("Error deleting staffing plan")
        else:
            st.info("📝 No staffing plans found. Create your first staffing plan below.")
        
        # New Staffing Plan Button
        if st.button("➕ New Staffing Plan", type="primary", key="new_staffing_plan_btn"):
            st.session_state.show_staffing_form = True
            st.session_state.edit_staffing_plan_id = None  # Clear edit mode
            # Clear any existing form data
            keys_to_clear = [
                'selected_client_name', 'planned_positions', 'current_plan_name',
                'current_from_date', 'current_to_date', 'target_hires', 'pipeline_planning_data'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        
        # Show staffing plan form if requested
        if st.session_state.get('show_staffing_form', False):
            staffing_plans_section()

        
        st.markdown("---")
    
    # Pipeline Management Tab (Tab 2)
    with pipeline_tab2:
        st.subheader("⚙️ Pipeline Management")
        
        # Display success message if available
        if 'last_save_message' in st.session_state:
            st.success(st.session_state['last_save_message'])
            del st.session_state['last_save_message']
        
        # Display existing pipeline configurations
        pipelines_df = pipeline_manager.get_all_pipelines()
        if not pipelines_df.empty:
            st.markdown("**Existing Pipeline Configurations:**")
            
            # Display pipelines with edit actions in compact format
            for idx, pipeline in pipelines_df.iterrows():
                with st.container():
                    # Format the created date
                    created_date_str = pipeline['created_date'].strftime('%m/%d/%y') if hasattr(pipeline['created_date'], 'strftime') else str(pipeline['created_date'])[:10]
                    
                    # Use custom CSS for compact display with field names as headers
                    st.markdown(f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px; margin: 6px 0; background-color: #f9f9f9;">
                        <!-- Header row with pipeline name, field labels, and action buttons -->
                        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 5px;">
                            <div style="flex: 1;">
                                <h3 style="margin: 0; font-size: 16px; color: #333; font-weight: bold;">{pipeline['name']}</h3>
                            </div>
                            <div style="display: flex; gap: 20px; align-items: center;">
                                <div style="text-align: center; min-width: 60px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Stages</div>
                                </div>
                                <div style="text-align: center; min-width: 70px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Status</div>
                                </div>
                                <div style="text-align: center; min-width: 70px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Created</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-weight: bold; color: #333; font-size: 14px;">Actions</div>
                                </div>
                            </div>
                        </div>
                        <!-- Data row with client name and values -->
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                            <div style="flex: 1;">
                                <p style="margin: 0; font-size: 12px; color: #666;">Client: {pipeline.get('client_name', '') if not pipeline.get('is_internal', False) else 'Greyamp Pipeline'}</p>
                            </div>
                            <div style="display: flex; gap: 20px; align-items: center;">
                                <div style="text-align: center; min-width: 60px;">
                                    <div style="font-size: 12px; color: #666;">{pipeline.get('stage_count', 0)}</div>
                                </div>
                                <div style="text-align: center; min-width: 70px;">
                                    <div style="font-size: 12px; color: {'#28a745' if pipeline['is_active'] else '#dc3545'};">{'Active' if pipeline['is_active'] else 'Inactive'}</div>
                                </div>
                                <div style="text-align: center; min-width: 70px;">
                                    <div style="font-size: 12px; color: #666;">{created_date_str}</div>
                                </div>
                                <div style="text-align: center; min-width: 80px;">
                                    <div style="font-size: 12px; color: #666;">Edit / Delete</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Hidden action buttons that align with the visual icons
                    col1, col2, col3, col4, col5 = st.columns([3, 1.2, 1.4, 1.4, 1.6])
                    with col1:
                        st.write("")  # Spacer
                    with col2:
                        st.write("")  # Spacer
                    with col3:
                        st.write("")  # Spacer
                    with col4:
                        st.write("")  # Spacer
                    with col5:
                        subcol1, subcol2 = st.columns([1, 1])
                        with subcol1:
                            if st.button("✏️", key=f"edit_pipeline_{pipeline['id']}", help="Edit this pipeline configuration"):
                                st.session_state.edit_pipeline_id = pipeline['id']
                                st.session_state.show_edit_pipeline_form = True
                                # Store pipeline data for editing without conflicting with widget keys
                                st.session_state.pipeline_edit_data = {
                                    'name': pipeline['name'],
                                    'client_name': pipeline['client_name'],
                                    'status': pipeline['is_active'],
                                    'description': pipeline.get('description', '')
                                }
                                st.rerun()
                        
                        with subcol2:
                            if st.button("🗑️", key=f"delete_pipeline_{pipeline['id']}", help="Delete this pipeline configuration"):
                                try:
                                    conn = psycopg2.connect(os.environ['DATABASE_URL'])
                                    cursor = conn.cursor()
                                    talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
                                    cursor.execute(f"DELETE FROM {talent_pipelines_table} WHERE id = %s", (pipeline['id'],))
                                    conn.commit()
                                    conn.close()
                                    st.success("Pipeline deleted successfully!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to delete pipeline: {str(e)}")
        
        st.markdown("---")
        
        # Edit Pipeline Form - Show inline in the pipeline management section
        if st.session_state.get('show_edit_pipeline_form', False) and st.session_state.get('edit_pipeline_id'):
            pipeline_id = st.session_state.edit_pipeline_id
            pipeline_edit_data = st.session_state.get('pipeline_edit_data', {})
            
            st.subheader(f"✏️ Edit Pipeline: {pipeline_edit_data.get('name', 'Unknown Pipeline')}")
            
            # Pipeline Details Panel - Editable
            st.markdown("**Pipeline Details:**")
            col1, col2 = st.columns(2)
            with col1:
                new_pipeline_name = st.text_input(
                    "Pipeline Name", 
                    value=pipeline_edit_data.get('name', ''), 
                    key="edit_form_pipeline_name"
                )
                new_client_name = st.text_input(
                    "Client Name", 
                    value=pipeline_edit_data.get('client_name', ''), 
                    key="edit_form_client_name"
                )
            with col2:
                # Status selection
                status_options = ["Active", "Inactive"]
                current_status = "Active" if pipeline_edit_data.get('status', True) else "Inactive"
                default_index = 0 if current_status == "Active" else 1
                new_status = st.selectbox("Status", status_options, index=default_index, key="edit_form_status")
                
                new_description = st.text_area(
                    "Description", 
                    value=pipeline_edit_data.get('description', ''), 
                    key="edit_form_description"
                )
            
            # Pipeline Stages Section
            st.markdown("---")
            st.markdown("🔗 **Pipeline Stages**")
            
            # Get existing pipeline stages
            try:
                conn = psycopg2.connect(os.environ['DATABASE_URL'])
                cursor = conn.cursor()
                
                # Debug: Check what pipeline_id we're using
                st.write(f"Debug: Looking for stages for pipeline_id: {pipeline_id}")
                
                # Use environment-appropriate table
                table_name = "dev_pipeline_stages" if os.environ.get('ENVIRONMENT', 'development') == 'development' else "pipeline_stages"
                
                cursor.execute(f"""
                    SELECT id, stage_name, conversion_rate, tat_days, stage_description, stage_order
                    FROM {table_name}
                    WHERE pipeline_id = %s
                    ORDER BY CASE WHEN stage_order = -1 THEN 999999 ELSE stage_order END
                """, (pipeline_id,))
                
                existing_stages = cursor.fetchall()
                st.write(f"Debug: Found {len(existing_stages)} stages")  # Debug info
                conn.close()
                
                if existing_stages:
                    for stage in existing_stages:
                        stage_id, stage_name, conversion_rate, tat_days, stage_desc, stage_order = stage
                        
                        # Display stage order - show "Any Stage" for special stages with order -1
                        stage_order_display = "Any Stage" if stage_order == -1 else str(stage_order)
                        stage_icon = "⭐" if stage_order == -1 else "🔴"
                        with st.expander(f"{stage_icon} Stage {stage_order_display}: {stage_name} (Conv: {conversion_rate}% | TAT: {tat_days}d)", expanded=False):
                            col1, col2, col3 = st.columns([2, 1, 1])
                            with col1:
                                st.write(f"**Name:** {stage_name}")
                                st.write(f"**Description:** {stage_desc or 'No description'}")
                            with col2:
                                st.write(f"**Conversion Rate:** {conversion_rate}%")
                                st.write(f"**TAT Days:** {tat_days}")
                            with col3:
                                # Check if this stage is in edit mode first
                                is_editing = st.session_state.get(f'editing_stage_{stage_id}', False)
                                
                                if not is_editing:
                                    if st.button(f"✏️ Edit", key=f"edit_btn_{stage_id}"):
                                        st.session_state[f'editing_stage_{stage_id}'] = True
                                        st.rerun()
                                    if st.button(f"🗑️ Delete", key=f"delete_btn_{stage_id}"):
                                        try:
                                            conn = psycopg2.connect(os.environ['DATABASE_URL'])
                                            cursor = conn.cursor()
                                            pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                                            cursor.execute(f"DELETE FROM {pipeline_stages_table} WHERE id = %s", (stage_id,))
                                            conn.commit()
                                            conn.close()
                                            st.success("Stage deleted successfully!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error deleting stage: {str(e)}")
                                else:
                                    if st.button(f"❌ Cancel", key=f"cancel_btn_{stage_id}"):
                                        del st.session_state[f'editing_stage_{stage_id}']
                                        st.rerun()
                            
                            # Edit form for stages
                            if st.session_state.get(f'editing_stage_{stage_id}', False):
                                st.markdown("---")
                                # Add comprehensive form cache clearing
                                col_clear, col_refresh, col_title = st.columns([1, 1, 2])
                                with col_clear:
                                    if st.button("🔄 Clear Cache", key=f"clear_cache_{stage_id}", help="Clear all form cache"):
                                        # Clear all editing states and form-related session state
                                        keys_to_delete = [k for k in st.session_state.keys() if 
                                                        k.startswith('editing_stage_') or 
                                                        k.startswith('edit_') or 
                                                        k.startswith('form_')]
                                        for key in keys_to_delete:
                                            del st.session_state[key]
                                        st.cache_data.clear()
                                        st.rerun()
                                with col_refresh:
                                    if st.button("↻ Force Reload", key=f"force_reload_{stage_id}", help="Force complete reload"):
                                        # Clear everything and force browser refresh
                                        st.session_state.clear()
                                        st.cache_data.clear()
                                        st.cache_resource.clear()
                                        st.write('<script>window.location.reload();</script>', unsafe_allow_html=True)
                                        st.rerun()
                                with col_title:
                                    st.markdown("**Edit Stage:**")
                                    
                                with st.form(f"edit_stage_form_{stage_id}", clear_on_submit=False):
                                    new_name = st.text_input("Stage Name", value=stage_name)
                                    new_conversion = st.number_input("Conversion Rate (%)", min_value=0.0, max_value=100.0, value=float(conversion_rate))
                                    
                                    # Get current is_special value from database
                                    try:
                                        conn_temp = psycopg2.connect(os.environ['DATABASE_URL'])
                                        cursor_temp = conn_temp.cursor()
                                        pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                                        cursor_temp.execute(f"SELECT is_special FROM {pipeline_stages_table} WHERE id = %s", (stage_id,))
                                        current_is_special = cursor_temp.fetchone()
                                        current_is_special = current_is_special[0] if current_is_special else False
                                        conn_temp.close()
                                    except:
                                        current_is_special = False
                                    
                                    # Special stage checkbox
                                    is_special_stage = st.checkbox(
                                        "Special Stage", 
                                        value=current_is_special,
                                        help="Special stages like 'Rejected', 'On-Hold', 'Dropped', or 'RNR' are alternate final stages with 0 TAT",
                                        key=f"special_stage_{stage_id}"
                                    )
                                    
                                    # Handle TAT days and stage order for special stages
                                    tat_value = int(tat_days) if tat_days >= 0 else 0
                                    current_order = int(stage_order) if stage_order else 1
                                    
                                    if is_special_stage:
                                        new_tat = st.number_input("TAT Days", min_value=0, value=0, 
                                                                help="Special stages automatically have 0 TAT days", 
                                                                disabled=True, key=f"tat_{stage_id}")
                                        
                                        # Special stage order options
                                        st.info("💡 Special stages can be accessed from any point in the pipeline")
                                        any_stage_option = st.checkbox(
                                            "Any Stage Access", 
                                            value=(current_order == -1),
                                            help="Allow candidates to move to this stage from any pipeline stage",
                                            key=f"any_stage_{stage_id}"
                                        )
                                        
                                        if any_stage_option:
                                            new_order = -1  # Use -1 to indicate "Any Stage"
                                            st.caption("Stage Order: Any Stage")
                                        else:
                                            new_order = st.number_input("Stage Order", min_value=1, value=max(1, current_order) if current_order > 0 else 1, key=f"order_special_{stage_id}")
                                    else:
                                        new_tat = st.number_input("TAT Days", min_value=0, value=max(1, tat_value), key=f"tat_normal_{stage_id}")
                                        new_order = st.number_input("Stage Order", min_value=1, value=max(1, current_order) if current_order > 0 else 1, key=f"order_normal_{stage_id}")
                                    
                                    new_desc = st.text_area("Description", value=stage_desc, key=f"desc_{stage_id}")
                                    
                                    # Form submission buttons
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        update_clicked = st.form_submit_button("💾 Update Stage", type="primary")
                                    with col2:
                                        cancel_clicked = st.form_submit_button("❌ Cancel")
                                    
                                    if update_clicked:
                                        try:
                                            conn = psycopg2.connect(os.environ['DATABASE_URL'])
                                            cursor = conn.cursor()
                                            # Get env_manager from session state
                                            env_manager = st.session_state.get('env_manager')
                                            if env_manager:
                                                pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                                                cursor.execute(f"""
                                                    UPDATE {pipeline_stages_table}
                                                    SET stage_name = %s, conversion_rate = %s, tat_days = %s, stage_description = %s, is_special = %s, stage_order = %s
                                                    WHERE id = %s
                                                """, (new_name, new_conversion, new_tat, new_desc, is_special_stage, new_order, stage_id))
                                            else:
                                                st.error("Environment manager not found")
                                            conn.commit()
                                            conn.close()
                                            st.success("Stage updated successfully!")
                                            del st.session_state[f'editing_stage_{stage_id}']
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error updating stage: {str(e)}")
                                    
                                    if cancel_clicked:
                                        del st.session_state[f'editing_stage_{stage_id}']
                                        st.rerun()
                else:
                    st.warning("No pipeline stages configured yet.")
                    st.info("💡 Click 'Add New Stage' below to create workflow stages for this pipeline.")
            
            except Exception as e:
                st.error(f"Error loading pipeline stages: {str(e)}")
            
            # Add New Stage Section
            st.markdown("---")
            if st.button("➕ Add New Stage", type="secondary", key="add_new_stage_btn"):
                st.session_state.show_add_stage_form = True
            
            if st.session_state.get('show_add_stage_form', False):
                st.markdown("**Add New Pipeline Stage:**")
                with st.form("add_new_stage_form", clear_on_submit=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_stage_name = st.text_input("Stage Name", placeholder="e.g., Phone Screening")
                        new_stage_conversion = st.number_input("Conversion Rate (%)", min_value=0.0, max_value=100.0, value=50.0)
                    with col2:
                        new_stage_tat = st.number_input("TAT Days", min_value=0, value=3)
                        new_stage_order = st.number_input("Stage Order", min_value=1, value=1, help="Position in pipeline sequence")
                    
                    # Special stage checkbox
                    is_special_stage = st.checkbox(
                        "Special Stage", 
                        value=False,
                        help="Special stages like 'Rejected', 'On-Hold', 'Dropped', or 'RNR' are alternate final stages with 0 TAT"
                    )
                    
                    # Automatically set TAT to 0 and provide "Any Stage" option for special stages
                    if is_special_stage:
                        new_stage_tat = 0
                        st.info("ℹ️ Special stages automatically have 0 TAT days and serve as alternate final stages")
                        
                        # Override stage order for special stages - allow "Any Stage" option
                        st.info("💡 Special stages can be accessed from any point in the pipeline")
                        any_stage_option = st.checkbox(
                            "Any Stage Access", 
                            value=True,
                            help="Allow candidates to move to this stage from any pipeline stage",
                            disabled=True  # Always enabled for special stages
                        )
                        if any_stage_option:
                            new_stage_order = -1  # Use -1 to indicate "Any Stage"
                    
                    new_stage_desc = st.text_area("Stage Description", placeholder="Describe this stage...")
                    
                    col_add, col_cancel = st.columns(2)
                    with col_add:
                        add_submitted = st.form_submit_button("✅ Add Stage", type="primary")
                    with col_cancel:
                        cancel_submitted = st.form_submit_button("❌ Cancel")
                    
                    if add_submitted and new_stage_name:
                        try:
                            conn = psycopg2.connect(os.environ['DATABASE_URL'])
                            cursor = conn.cursor()
                            # Get env_manager from session state
                            env_manager = st.session_state.get('env_manager')
                            if env_manager:
                                pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                                cursor.execute(f"""
                                    INSERT INTO {pipeline_stages_table}
                                    (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description, is_active, is_special)
                                    VALUES (%s, %s, %s, %s, %s, %s, true, %s)
                                """, (pipeline_id, new_stage_name, new_stage_order, new_stage_conversion, new_stage_tat, new_stage_desc, is_special_stage))
                            else:
                                st.error("Environment manager not found")
                            conn.commit()
                            conn.close()
                            st.success(f"Stage '{new_stage_name}' added successfully!")
                            st.session_state.show_add_stage_form = False
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding stage: {str(e)}")
                    elif add_submitted:
                        st.error("Stage name is required")
                    
                    if cancel_submitted:
                        st.session_state.show_add_stage_form = False
                        st.rerun()
            
            # Action buttons for edit form
            st.markdown("---")
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.write("")  # Spacer
            with col2:
                if st.button("💾 Save Changes", type="primary", key="save_edit_pipeline"):
                    try:
                        conn = psycopg2.connect(os.environ['DATABASE_URL'])
                        cursor = conn.cursor()
                        # Get env_manager from session state
                        env_manager = st.session_state.get('env_manager')
                        if env_manager:
                            talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
                            cursor.execute(f"""
                                UPDATE {talent_pipelines_table}
                                SET name = %s, description = %s, is_active = %s
                                WHERE id = %s
                            """, (new_pipeline_name, new_description, new_status == "Active", pipeline_id))
                        else:
                            st.error("Environment manager not found")
                        conn.commit()
                        conn.close()
                        
                        st.success("Pipeline updated successfully!")
                        # Clear edit form
                        st.session_state.show_edit_pipeline_form = False
                        if 'edit_pipeline_id' in st.session_state:
                            del st.session_state['edit_pipeline_id']
                        if 'pipeline_edit_data' in st.session_state:
                            del st.session_state['pipeline_edit_data']
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to update pipeline: {str(e)}")
            with col3:
                if st.button("❌ Cancel Edit", key="cancel_edit_pipeline"):
                    # Clear edit form
                    st.session_state.show_edit_pipeline_form = False
                    if 'edit_pipeline_id' in st.session_state:
                        del st.session_state['edit_pipeline_id']
                    if 'pipeline_edit_data' in st.session_state:
                        del st.session_state['pipeline_edit_data']
                    st.rerun()
            
            st.markdown("---")
        
        # New Pipeline Configuration
        if st.button("➕ New Pipeline Configuration", type="primary", key="new_pipeline_config_btn"):
            st.session_state.show_new_pipeline_form = True
            # Clear existing edit data for new pipeline
            st.session_state.show_edit_pipeline_form = False
            if 'edit_pipeline_id' in st.session_state:
                del st.session_state['edit_pipeline_id']
            if 'pipeline_edit_data' in st.session_state:
                del st.session_state['pipeline_edit_data']
        
        if st.session_state.get('show_new_pipeline_form', False):
            st.subheader("📝 Create New Pipeline Configuration")
            
            # Initialize temporary session state variables first
            if 'temp_pipeline_name' not in st.session_state:
                st.session_state.temp_pipeline_name = ""
            if 'temp_client_name' not in st.session_state:
                st.session_state.temp_client_name = ""
            if 'temp_pipeline_desc' not in st.session_state:
                st.session_state.temp_pipeline_desc = ""
            if 'temp_status' not in st.session_state:
                st.session_state.temp_status = "Inactive"
            if 'temp_is_internal' not in st.session_state:
                st.session_state.temp_is_internal = False
            
            # Remove form wrapper - just use regular inputs
            col1, col2 = st.columns(2)
            with col1:
                pipeline_name = st.text_input("Pipeline Name", placeholder="e.g., Software Engineering Pipeline", 
                                             value=st.session_state.temp_pipeline_name, key="pipeline_name_input")
                
                # Internal pipeline checkbox
                is_internal = st.checkbox("Internal", value=st.session_state.temp_is_internal, key="internal_pipeline_checkbox", 
                                        help="Check this to tag as 'Greyamp Pipeline' - client selection will be disabled")
                
                # Client selection logic - disabled when Internal is checked
                if is_internal:
                    # Internal pipeline - disable client selection and set to Greyamp
                    client_name = "Greyamp"
                    st.text_input("Client Name", value="Greyamp", disabled=True, key="greyamp_client_display", 
                                help="Internal pipelines are automatically assigned to Greyamp")
                else:
                    # External pipeline - allow client selection
                    try:
                        conn = psycopg2.connect(os.environ['DATABASE_URL'])
                        cursor = conn.cursor()
                        master_clients_table = env_manager.get_table_name('master_clients')
                        cursor.execute(f"SELECT DISTINCT client_name FROM {master_clients_table} ORDER BY client_name")
                        existing_clients = [row[0] for row in cursor.fetchall()]
                        conn.close()
                        
                        if existing_clients:
                            client_name = st.selectbox("Client Name", ["Select existing client..."] + existing_clients + ["+ Add New Client"], key="client_name_select")
                            if client_name == "+ Add New Client":
                                client_name = st.text_input("New Client Name", placeholder="e.g., TechCorp Inc.", key="new_client_input")
                            elif client_name == "Select existing client...":
                                client_name = ""
                        else:
                            client_name = st.text_input("Client Name", placeholder="e.g., TechCorp Inc.", key="client_name_input")
                    except Exception as e:
                        client_name = st.text_input("Client Name", placeholder="e.g., TechCorp Inc.", key="client_name_fallback")
                    
            with col2:
                status_options = ["Draft", "Active", "Inactive"]
                status_idx = status_options.index(st.session_state.temp_status) if st.session_state.temp_status in status_options else 2
                status = st.selectbox("Status", status_options, index=status_idx, key="status_select")
            
            pipeline_description = st.text_area("Pipeline Description", 
                                              placeholder="Describe the purpose and scope of this pipeline...", 
                                              value=st.session_state.temp_pipeline_desc,
                                              key="pipeline_desc_input")
            
            # Workflow States Builder Section
            st.markdown("---")
            st.markdown("**🔄 Workflow States Builder**")
            
            # Initialize workflow states in session state if not exists
            if 'workflow_states' not in st.session_state:
                st.session_state.workflow_states = []
            
            # Session state variables already initialized above
            
            # Display current workflow states
            if st.session_state.workflow_states:
                st.markdown("**Current Workflow States:**")
                
                # Create visual workflow with arrows
                workflow_cols = st.columns(len(st.session_state.workflow_states) * 2 - 1) if len(st.session_state.workflow_states) > 1 else st.columns(1)
                
                for i, state in enumerate(st.session_state.workflow_states):
                    if i < len(workflow_cols):
                        with workflow_cols[i * 2 if i * 2 < len(workflow_cols) else -1]:
                            # Use the user-selected color or default to gray
                            state_color = state.get('color', '#757575')
                            
                            # Add state type indicators
                            state_indicators = ""
                            if state.get('is_initial'):
                                state_indicators += "🏁 "
                            if state.get('is_final'):
                                state_indicators += "🎯 "
                            if state.get('is_special'):
                                state_indicators += "⚠️ "
                            
                            st.markdown(f"""
                                <div style="
                                    background-color: {state_color}; 
                                    color: white; 
                                    padding: 10px; 
                                    border-radius: 5px; 
                                    text-align: center;
                                    margin: 5px 0;
                                    font-weight: bold;
                                    min-height: 70px;
                                    display: flex;
                                    align-items: center;
                                    justify-content: center;
                                    flex-direction: column;
                                ">
                                    <div>{state_indicators}{state['name']}</div>
                                    <small>{state['conversion_rate']}% • {state['tat_days']}d</small>
                                </div>
                            """, unsafe_allow_html=True)
                    
                    # Add arrow between states (except for the last one)
                    if i < len(st.session_state.workflow_states) - 1 and (i * 2 + 1) < len(workflow_cols):
                        with workflow_cols[i * 2 + 1]:
                            st.markdown("""
                                <div style="
                                    text-align: center; 
                                    font-size: 24px; 
                                    margin-top: 20px;
                                    color: #666;
                                ">
                                    →
                                </div>
                            """, unsafe_allow_html=True)
                
                # Show workflow states table with edit functionality
                st.markdown("**Workflow Configuration:**")
                workflow_df = pd.DataFrame(st.session_state.workflow_states)
                if not workflow_df.empty:
                    # Add edit functionality for each row
                    for idx, state in enumerate(st.session_state.workflow_states):
                        with st.expander(f"🔧 {state['name']} - {state.get('status_flag', 'Greyamp')}", expanded=False):
                            col1, col2, col3 = st.columns([2, 2, 1])
                            
                            with col1:
                                new_name = st.text_input("State Name", value=state['name'], key=f"edit_name_{idx}")
                                
                                # Show special state info and disable fields if special
                                is_special = state.get('is_special', False)
                                if is_special:
                                    st.caption("🔸 Special State - Conversion and TAT are set to 0")
                                    new_conversion = st.number_input("Conversion %", value=0, min_value=0, max_value=100, key=f"edit_conv_{idx}", disabled=True)
                                else:
                                    new_conversion = st.number_input("Conversion %", value=state['conversion_rate'], min_value=0, max_value=100, key=f"edit_conv_{idx}")
                                
                            with col2:
                                # Check if this is a special state to set TAT to 0
                                if is_special:
                                    new_tat = st.number_input("TAT Days", value=0, min_value=0, max_value=30, key=f"edit_tat_{idx}", disabled=True)
                                else:
                                    new_tat = st.number_input("TAT Days", value=state['tat_days'], min_value=1, max_value=30, key=f"edit_tat_{idx}")
                                
                                # Get current candidate statuses for dropdown
                                try:
                                    conn = psycopg2.connect(os.environ['DATABASE_URL'])
                                    cursor = conn.cursor()
                                    candidate_data_table = env_manager.get_table_name('candidate_data')
                                    cursor.execute(f"SELECT DISTINCT status FROM {candidate_data_table} WHERE status IS NOT NULL ORDER BY status")
                                    candidate_statuses = [row[0] for row in cursor.fetchall()]
                                    cursor.close()
                                    conn.close()
                                except:
                                    candidate_statuses = ['Screening', 'Interview', 'Assessment', 'Offer', 'Onboarding']
                                
                                status_options = ["None"] + candidate_statuses
                                current_status = state.get('maps_to_status') or "None"
                                status_idx = status_options.index(current_status) if current_status in status_options else 0
                                new_maps_to_status = st.selectbox("Maps to Status", status_options, index=status_idx, key=f"edit_maps_{idx}")
                                
                                current_flag = state.get('status_flag', 'Greyamp')
                                flag_idx = ['Greyamp', 'Client', 'Both'].index(current_flag) if current_flag in ['Greyamp', 'Client', 'Both'] else 0
                                new_status_flag = st.selectbox("Status Flag", ['Greyamp', 'Client', 'Both'], index=flag_idx, key=f"edit_flag_{idx}")
                            
                            with col3:
                                new_initial = st.checkbox("Initial", value=state.get('is_initial', False), key=f"edit_initial_{idx}")
                                new_final = st.checkbox("Final", value=state.get('is_final', False), key=f"edit_final_{idx}")
                                new_special = st.checkbox("Special", value=state.get('is_special', False), key=f"edit_special_{idx}")
                                
                                if st.button(f"💾 Update", key=f"update_{idx}"):
                                    # If special state is checked, force conversion and TAT to 0
                                    final_conversion = 0 if new_special else new_conversion
                                    final_tat = 0 if new_special else new_tat
                                    
                                    st.session_state.workflow_states[idx].update({
                                        'name': new_name,
                                        'conversion_rate': final_conversion,
                                        'tat_days': final_tat,
                                        'maps_to_status': None if new_maps_to_status == "None" else new_maps_to_status,
                                        'status_flag': new_status_flag,
                                        'is_initial': new_initial,
                                        'is_final': new_final,
                                        'is_special': new_special
                                    })
                                    st.success(f"✅ {new_name} updated!")
                                    st.rerun()
                                
                                if st.button(f"🗑️ Delete", key=f"delete_{idx}"):
                                    del st.session_state.workflow_states[idx]
                                    st.success(f"✅ {state['name']} deleted!")
                                    st.rerun()
                    
                    # Display summary table
                    st.markdown("**Summary:**")
                    display_columns = ['name', 'conversion_rate', 'tat_days', 'maps_to_status', 'status_flag', 'is_initial', 'is_final', 'is_special']
                    display_df = workflow_df[display_columns].copy()
                    display_df.columns = ['State Name', 'Conversion %', 'TAT Days', 'Maps to Status', 'Status Flag', 'Initial', 'Final', 'Special']
                    st.dataframe(display_df, use_container_width=True)
                
                # Clear workflow button
                if st.button("🗑️ Clear All States", key="clear_workflow_states"):
                    st.session_state.workflow_states = []
                    st.rerun()
            
            # Show success message if available
            if 'state_added_success' in st.session_state:
                st.success(st.session_state['state_added_success'])
                del st.session_state['state_added_success']
            
            # Add new workflow state with form container
            st.markdown("**Add New Workflow State:**")
            
            # Use form for proper clearing
            with st.form(key="add_workflow_state_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                with col1:
                    state_name = st.text_input("State Name", placeholder="e.g., On Hold")
                    
                    # State Color picker
                    st.markdown("**State Color**")
                    color_options = {
                        "🔴 Red": "#F44336",
                        "🟠 Orange": "#FF9800", 
                        "🟡 Yellow": "#FFEB3B",
                        "🟢 Green": "#4CAF50",
                        "🔵 Blue": "#2196F3",
                        "🟣 Purple": "#9C27B0",
                        "🟤 Brown": "#795548",
                        "⚫ Gray": "#757575"
                    }
                    selected_color = st.selectbox("Color", list(color_options.keys()), label_visibility="collapsed")
                    state_color = color_options[selected_color]
                    
                with col2:
                    # Check if Special State checkbox is checked first to control other fields
                    is_special_temp = st.checkbox("Special State (e.g. On Hold, Rejected)", key="temp_special_check")
                    
                    if is_special_temp:
                        st.info("ℹ️ Special states like 'Rejected', 'Dropped', 'On Hold' are terminal/suspended states that don't require conversion rates or TAT.")
                        conversion_rate = st.number_input("Conversion Rate (%)", min_value=0, max_value=100, value=0, 
                                                        help="Special states typically have 0% conversion as they don't progress", disabled=True)
                        tat_days = st.number_input("TAT (Days)", min_value=0, max_value=30, value=0, 
                                                 help="Special states don't require TAT as they don't progress through workflow", disabled=True)
                    else:
                        conversion_rate = st.number_input("Conversion Rate (%)", min_value=0, max_value=100, value=80)
                        tat_days = st.number_input("TAT (Days)", min_value=1, max_value=30, value=3)
                    
                    # State Type Options
                    st.markdown("**State Type**")
                    col2a, col2b = st.columns(2)
                    with col2a:
                        is_initial = st.checkbox("Initial State")
                    with col2b:
                        is_final = st.checkbox("Final State")
                    
                    # Use the same checkbox value from above
                    is_special = is_special_temp
                    
                    # Maps to Status dropdown
                    st.markdown("**Maps to Status**")
                    # Get current candidate statuses for dropdown
                    try:
                        conn = psycopg2.connect(os.environ['DATABASE_URL'])
                        cursor = conn.cursor()
                        candidate_data_table = env_manager.get_table_name('candidate_data')
                        cursor.execute(f"SELECT DISTINCT status FROM {candidate_data_table} WHERE status IS NOT NULL ORDER BY status")
                        candidate_statuses = [row[0] for row in cursor.fetchall()]
                        cursor.close()
                        conn.close()
                    except:
                        candidate_statuses = ['Screening', 'Interview', 'Assessment', 'Offer', 'Onboarding']
                    
                    status_options = ["None"] + candidate_statuses
                    maps_to_status = st.selectbox("Select Status", status_options, 
                                                help="Select which candidate status this pipeline stage maps to for accurate counting")
                    
                    # Status Flag radio buttons
                    st.markdown("**Status Flag**")
                    status_flag = st.radio("Flag", ["Greyamp", "Client", "Both"], horizontal=True, 
                                         label_visibility="collapsed", index=0,
                                         help="Select whether this stage is managed by Greyamp, Client, or Both (allows manual selection per candidate)")
                
                state_description = st.text_area("State Description (Optional)", 
                                               placeholder="Describe this workflow state...", 
                                               height=100)
                
                submitted = st.form_submit_button("➕ Add State", type="primary")
                
            if submitted:
                if state_name:
                    # Check for duplicate state name and mapping combination
                    existing_states = st.session_state.get('workflow_states', [])
                    final_maps_to_status = None if maps_to_status == "None" else maps_to_status
                    
                    # Check if state with same name and mapping already exists
                    duplicate_found = False
                    for existing_state in existing_states:
                        if (existing_state['name'].lower() == state_name.lower() and 
                            existing_state.get('maps_to_status') == final_maps_to_status):
                            duplicate_found = True
                            break
                    
                    if duplicate_found:
                        st.error(f"❌ A state with name '{state_name}' and the same status mapping already exists!")
                        return
                    
                    new_state = {
                        'name': state_name,
                        'conversion_rate': conversion_rate,
                        'tat_days': tat_days,
                        'description': state_description,
                        'color': state_color,
                        'is_initial': is_initial,
                        'is_final': is_final,
                        'is_special': is_special,
                        'maps_to_status': final_maps_to_status,
                        'status_flag': status_flag,
                        'order': len(st.session_state.workflow_states) + 1
                    }
                    st.session_state.workflow_states.append(new_state)
                    
                    # Save the current form values before rerunning
                    st.session_state.temp_pipeline_name = pipeline_name
                    st.session_state.temp_client_name = client_name
                    st.session_state.temp_pipeline_desc = pipeline_description
                    st.session_state.temp_status = status
                    st.session_state.temp_is_internal = is_internal
                    
                    # Set success message to show after rerun
                    st.session_state['state_added_success'] = f"✅ State '{state_name}' added successfully!"
                    
                    st.rerun()
                else:
                    st.error("Please enter a state name!")
            
            # Save/Cancel buttons at the bottom
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Save Configuration", type="primary", key="save_new_pipeline"):
                    if pipeline_name and client_name:
                        try:
                            conn = psycopg2.connect(os.environ['DATABASE_URL'])
                            cursor = conn.cursor()
                            
                            # Get or create client (handle both selectbox and text input)
                            final_client_name = client_name if client_name not in ["Select existing client...", "+ Add New Client", ""] else None
                            
                            if final_client_name:
                                master_clients_table = env_manager.get_table_name('master_clients')
                                cursor.execute(f"SELECT master_client_id FROM {master_clients_table} WHERE client_name = %s", (final_client_name,))
                                client_result = cursor.fetchone()
                                if client_result:
                                    client_id = client_result[0]
                                else:
                                    cursor.execute(f"INSERT INTO {master_clients_table} (client_name) VALUES (%s) RETURNING master_client_id", (final_client_name,))
                                    client_id = cursor.fetchone()[0]
                                
                                # Create pipeline with internal flag
                                talent_pipelines_table = env_manager.get_table_name('talent_pipelines')
                                cursor.execute(f"""
                                    INSERT INTO {talent_pipelines_table} (name, client_id, description, is_active, created_by, is_internal)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    RETURNING id
                                """, (pipeline_name, client_id, pipeline_description, status == 'Active', 'admin', is_internal))
                                
                                pipeline_id = cursor.fetchone()[0]
                                
                                # Save workflow states as pipeline stages
                                if st.session_state.get('workflow_states'):
                                    for order, state in enumerate(st.session_state.workflow_states, 1):
                                        # Get table name with environment support
                                        pipeline_stages_table = env_manager.get_table_name('pipeline_stages')
                                        cursor.execute(f"""
                                            INSERT INTO {pipeline_stages_table} (pipeline_id, stage_name, conversion_rate, tat_days, stage_description, stage_order, maps_to_status, status_flag)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        """, (pipeline_id, state['name'], state['conversion_rate'], state['tat_days'], 
                                              state['description'], order, state.get('maps_to_status'), state.get('status_flag')))
                                
                                conn.commit()
                                conn.close()
                                
                                st.success(f"✅ Pipeline '{pipeline_name}' created successfully with {len(st.session_state.get('workflow_states', []))} workflow states!")
                                
                                # Clear the form
                                st.session_state.show_new_pipeline_form = False
                                st.session_state.workflow_states = []
                                # Clear form fields including internal checkbox
                                for key in ['pipeline_name_input', 'client_name_select', 'new_client_input', 'pipeline_desc_input', 'status_select', 'internal_pipeline_checkbox', 'greyamp_client_display']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error("Please select or enter a client name")
                        except Exception as e:
                            st.error(f"Error creating pipeline: {str(e)}")
                    # Only show error when user actually tries to save without filling fields
                    # else:
                    #     st.error("Please enter pipeline name and client name")
            
            with col2:
                if st.button("❌ Cancel", key="cancel_new_pipeline"):
                    st.session_state.show_new_pipeline_form = False
                    st.session_state.workflow_states = []
                    # Clear form fields including internal checkbox
                    for key in ['pipeline_name_input', 'client_name_select', 'new_client_input', 'pipeline_desc_input', 'status_select', 'internal_pipeline_checkbox', 'greyamp_client_display']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
    
    # Pipeline Generation Interface
    if st.session_state.get('show_pipeline_generation', False):
        display_pipeline_generation_interface()
    
    # Pipeline Analytics Tab
    with pipeline_tab3:
        st.subheader("📈 Pipeline Analytics")
        
        # Load pipelines for analytics
        try:
            pipelines_df = pipeline_manager.get_all_pipelines()
            if pipelines_df is not None and not pipelines_df.empty:
                pipelines = pipelines_df.to_dict('records')
            else:
                pipelines = []
        except Exception as e:
            st.error(f"Error loading pipelines: {str(e)}")
            pipelines = []
        
        if pipelines is not None and len(pipelines) > 0:
            # Get staffing plans for timeline analysis
            staffing_plans_df = staffing_manager.get_all_staffing_plans()
            staffing_plans = []
            if staffing_plans_df is not None and not staffing_plans_df.empty:
                staffing_plans = staffing_plans_df.to_dict('records')
            
            # Create two main sections
            timeline_tab, velocity_tab, performance_tab = st.tabs(["⏱️ Timeline Performance Dashboard", "🏃 Pipeline Velocity Metrics", "📊 Pipeline Performance"])
            
            # Timeline Performance Dashboard
            with timeline_tab:
                st.subheader("⏱️ Timeline Performance Dashboard")
                st.markdown("Compare planned vs actual timelines across your recruitment pipelines")
                
                # Display current date
                from datetime import datetime
                current_date = datetime.now().strftime("%B %d, %Y at %I:%M %p")
                st.caption(f"📅 Current Date: {current_date}")
                st.markdown("---")
                
                if staffing_plans:
                    # Pipeline selector for timeline analysis
                    pipeline_options = [(p['id'], p['name']) for p in pipelines]
                    selected_pipeline_id = st.selectbox(
                        "Select Pipeline for Timeline Analysis",
                        options=[p[0] for p in pipeline_options],
                        format_func=lambda x: next(p[1] for p in pipeline_options if p[0] == x),
                        key="timeline_pipeline_selector"
                    )
                    
                    if selected_pipeline_id:
                        # Get pipeline stages and staffing plans using this pipeline
                        stages = pipeline_manager.get_pipeline_stages(selected_pipeline_id)
                        pipeline_name = next(p['name'] for p in pipelines if p['id'] == selected_pipeline_id)
                        
                        # Filter staffing plans that use this pipeline
                        relevant_plans = []
                        for plan in staffing_plans:
                            plan_details = staffing_manager.get_pipeline_planning_details(plan['id'])
                            if plan_details:
                                for detail in plan_details:
                                    if detail.get('pipeline_id') == selected_pipeline_id:
                                        relevant_plans.append({
                                            'plan': plan,
                                            'detail': detail
                                        })
                                        break
                        
                        if relevant_plans and stages:
                            # Create timeline visualization
                            import plotly.graph_objects as go
                            from plotly.subplots import make_subplots
                            import plotly.express as px
                            from datetime import datetime, timedelta
                            
                            fig = make_subplots(
                                rows=len(relevant_plans),
                                cols=1,
                                subplot_titles=[f"{p['plan']['plan_name']} - {p['detail']['role']}" for p in relevant_plans],
                                vertical_spacing=0.15,
                                specs=[[{"secondary_y": False}] for _ in range(len(relevant_plans))]
                            )
                            
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
                            
                            for idx, plan_data in enumerate(relevant_plans):
                                plan = plan_data['plan']
                                detail = plan_data['detail']
                                
                                # Calculate planned timeline
                                onboard_date = detail['onboard_by']
                                if isinstance(onboard_date, str):
                                    onboard_date = datetime.strptime(onboard_date, '%Y-%m-%d').date()
                                
                                # Calculate sourcing start date based on total TAT
                                total_tat = sum(stage['tat_days'] for stage in stages)
                                sourcing_start = onboard_date - timedelta(days=total_tat)
                                
                                # Create planned timeline bars
                                current_date = sourcing_start
                                stage_dates = []
                                
                                for stage in stages:
                                    stage_start = current_date
                                    stage_end = current_date + timedelta(days=stage['tat_days'])
                                    stage_dates.append({
                                        'stage': stage['stage_name'],
                                        'start': stage_start,
                                        'end': stage_end,
                                        'tat_days': stage['tat_days']
                                    })
                                    current_date = stage_end
                                
                                # Add planned timeline bars
                                for i, stage_data in enumerate(stage_dates):
                                    fig.add_trace(
                                        go.Scatter(
                                            x=[stage_data['start'], stage_data['end']],
                                            y=[f"Planned - {stage_data['stage']}", f"Planned - {stage_data['stage']}"],
                                            mode='lines',
                                            line=dict(color=colors[i % len(colors)], width=8),
                                            name=f"{stage_data['stage']} (Planned)",
                                            hovertemplate=f"<b>{stage_data['stage']}</b><br>Start: %{{x}}<br>Duration: {stage_data['tat_days']} days<extra></extra>",
                                            showlegend=idx == 0
                                        ),
                                        row=idx+1, col=1
                                    )
                                
                                # Add milestone markers
                                fig.add_trace(
                                    go.Scatter(
                                        x=[sourcing_start, onboard_date],
                                        y=[f"Milestones", f"Milestones"],
                                        mode='markers',
                                        marker=dict(
                                            symbol=['circle', 'star'],
                                            size=[12, 16],
                                            color=['green', 'red']
                                        ),
                                        name="Milestones",
                                        hovertemplate="<b>%{text}</b><br>Date: %{x}<extra></extra>",
                                        text=["Sourcing Start", "Target Onboard"],
                                        showlegend=idx == 0
                                    ),
                                    row=idx+1, col=1
                                )
                                
                                # Add simulated actual data (in a real system, this would come from tracking)
                                import random
                                random.seed(42)  # For consistent demo data
                                
                                actual_current_date = sourcing_start + timedelta(days=random.randint(-2, 5))
                                for i, stage_data in enumerate(stage_dates):
                                    actual_duration = stage_data['tat_days'] + random.randint(-3, 7)
                                    actual_start = actual_current_date
                                    actual_end = actual_current_date + timedelta(days=actual_duration)
                                    
                                    fig.add_trace(
                                        go.Scatter(
                                            x=[actual_start, actual_end],
                                            y=[f"Actual - {stage_data['stage']}", f"Actual - {stage_data['stage']}"],
                                            mode='lines',
                                            line=dict(color=colors[i % len(colors)], width=8, dash='dot'),
                                            name=f"{stage_data['stage']} (Actual)",
                                            hovertemplate=f"<b>{stage_data['stage']} - Actual</b><br>Start: %{{x}}<br>Duration: {actual_duration} days<extra></extra>",
                                            showlegend=idx == 0
                                        ),
                                        row=idx+1, col=1
                                    )
                                    actual_current_date = actual_end
                            
                            fig.update_layout(
                                height=max(500, 400 * len(relevant_plans)),
                                title=dict(
                                    text=f"Timeline Performance Dashboard - {pipeline_name}",
                                    x=0.5,
                                    font=dict(size=16)
                                ),
                                xaxis_title="Date",
                                showlegend=True,
                                legend=dict(
                                    orientation="h",
                                    yanchor="top",
                                    y=-0.35,
                                    xanchor="center",
                                    x=0.5,
                                    bgcolor="rgba(255,255,255,0.9)",
                                    bordercolor="rgba(0,0,0,0.3)",
                                    borderwidth=1
                                ),
                                margin=dict(l=80, r=80, t=100, b=250),
                                font=dict(size=11)
                            )
                            
                            # Update y-axis for each subplot
                            for i in range(len(relevant_plans)):
                                fig.update_yaxes(
                                    title_text="Pipeline Stages",
                                    title_standoff=20,
                                    tickfont=dict(size=10),
                                    automargin=True,
                                    row=i+1, col=1
                                )
                                
                            # Update x-axis formatting
                            for i in range(len(relevant_plans)):
                                fig.update_xaxes(
                                    tickfont=dict(size=10),
                                    tickangle=45,
                                    automargin=True,
                                    row=i+1, col=1
                                )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Timeline Summary Metrics
                            st.markdown("---")
                            st.markdown("### Timeline Performance Summary")
                            st.markdown("")  # Add some spacing
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                total_planned_tat = sum(stage['tat_days'] for stage in stages)
                                st.metric("Total Planned TAT", f"{total_planned_tat} days")
                            
                            with col2:
                                # Simulated actual TAT (would be calculated from real data)
                                actual_tat = total_planned_tat + random.randint(-5, 10)
                                variance = actual_tat - total_planned_tat
                                st.metric(
                                    "Avg Actual TAT", 
                                    f"{actual_tat} days",
                                    delta=f"{variance:+d} days"
                                )
                            
                            with col3:
                                on_time_percentage = random.randint(65, 85)
                                st.metric("On-Time Completion", f"{on_time_percentage}%")
                            
                            with col4:
                                active_roles = len(relevant_plans)
                                st.metric("Active Roles", active_roles)
                        
                        else:
                            st.info(f"No staffing plans found using the {pipeline_name} pipeline")
                else:
                    st.info("No staffing plans available for timeline analysis")
            
            # Pipeline Velocity Metrics
            with velocity_tab:
                st.subheader("🏃 Pipeline Velocity Dashboard")
                st.markdown("Monitor the speed and efficiency of your recruitment stages")
                
                # Display current date
                st.caption(f"📅 Current Date: {current_date}")
                st.markdown("---")
                
                # Filters for velocity analysis (same as performance dashboard)
                if pipelines and staffing_plans:
                    st.markdown("### Filters")
                    
                    # Create filter columns
                    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
                    
                    with filter_col1:
                        # Client filter
                        all_clients = set()
                        for plan in staffing_plans:
                            all_clients.add(plan.get('client_name', 'Unknown'))
                        client_options = ['All Clients'] + sorted(list(all_clients))
                        selected_client = st.selectbox(
                            "Client",
                            options=client_options,
                            key="velocity_client_filter"
                        )
                    
                    with filter_col2:
                        # Owner filter (from pipeline requirements table)
                        all_owners = set()
                        for plan in staffing_plans:
                            plan_details = staffing_manager.get_pipeline_planning_details(plan['id'])
                            if plan_details:
                                for detail in plan_details:
                                    pipeline_owner = detail.get('pipeline_owner', 'Unknown')
                                    if pipeline_owner and pipeline_owner != 'Unknown':
                                        all_owners.add(pipeline_owner)
                        owner_options = ['All Owners'] + sorted(list(all_owners))
                        selected_owner = st.selectbox(
                            "Pipeline Owner",
                            options=owner_options,
                            key="velocity_owner_filter"
                        )
                    
                    with filter_col3:
                        # From Date filter
                        from datetime import timedelta
                        from_date = st.date_input(
                            "From Date",
                            value=datetime.now().date() - timedelta(days=30),
                            key="velocity_from_date"
                        )
                    
                    with filter_col4:
                        # To Date filter
                        to_date = st.date_input(
                            "To Date",
                            value=datetime.now().date() + timedelta(days=90),
                            key="velocity_to_date"
                        )
                    
                    # Pipeline selector with All option (default to All Pipelines)
                    pipeline_options = [('all', 'All Pipelines')] + [(p['id'], f"{p['name']} - {p['client_name']}") for p in pipelines]
                    selected_pipeline_id = st.selectbox(
                        "Select Pipeline for Velocity Analysis",
                        options=[p[0] for p in pipeline_options],
                        format_func=lambda x: next(p[1] for p in pipeline_options if p[0] == x),
                        index=0,  # Default to "All Pipelines"
                        key="velocity_pipeline_selector"
                    )
                    
                    if selected_pipeline_id:
                        # Get stages based on selection
                        if selected_pipeline_id == 'all':
                            # For "All Pipelines", collect all unique stages from all pipelines
                            all_stages = set()
                            stage_details = {}
                            
                            for pipeline in pipelines:
                                pipeline_stages = pipeline_manager.get_pipeline_stages(pipeline['id'])
                                for stage in pipeline_stages:
                                    stage_name = stage['stage_name']
                                    all_stages.add(stage_name)
                                    if stage_name not in stage_details:
                                        stage_details[stage_name] = stage
                            
                            # Convert to list format
                            stages = [stage_details[stage_name] for stage_name in sorted(all_stages)]
                            pipeline_name = "All Pipelines"
                        else:
                            stages = pipeline_manager.get_pipeline_stages(selected_pipeline_id)
                            pipeline_name = next(p['name'] for p in pipelines if p['id'] == selected_pipeline_id)
                        
                        if stages:
                            # Filter and aggregate data from all staffing plans
                            filtered_plans = []
                            for plan in staffing_plans:
                                # Apply filters
                                if selected_client != 'All Clients' and plan.get('client_name') != selected_client:
                                    continue
                                
                                plan_details = staffing_manager.get_pipeline_planning_details(plan['id'])
                                if not plan_details:
                                    continue
                                
                                for detail in plan_details:
                                    # For "All Pipelines", include all pipeline data
                                    if selected_pipeline_id != 'all' and detail.get('pipeline_id') != selected_pipeline_id:
                                        continue
                                    
                                    # Apply owner filter
                                    if selected_owner != 'All Owners' and detail.get('pipeline_owner') != selected_owner:
                                        continue
                                    
                                    # Apply date filter
                                    onboard_date = detail.get('onboard_by')
                                    if isinstance(onboard_date, str):
                                        onboard_date = datetime.strptime(onboard_date, '%Y-%m-%d').date()
                                    
                                    if onboard_date and (onboard_date < from_date or onboard_date > to_date):
                                        continue
                                    
                                    filtered_plans.append({
                                        'plan': plan,
                                        'detail': detail
                                    })
                            
                            # Display filter summary
                            st.markdown("---")
                            filter_summary = f"**Data Summary**: "
                            if selected_client != 'All Clients':
                                filter_summary += f"Client: {selected_client} | "
                            if selected_owner != 'All Owners':
                                filter_summary += f"Owner: {selected_owner} | "
                            filter_summary += f"Period: {from_date} to {to_date} | "
                            filter_summary += f"Plans Found: {len(filtered_plans)}"
                            
                            st.markdown(filter_summary)
                            st.markdown("---")
                            
                            # Create gauge charts for each stage
                            import plotly.graph_objects as go
                            from plotly.subplots import make_subplots
                            
                            # Calculate metrics with better layout
                            num_stages = len(stages)
                            
                            # Use 4 columns for better spacing and smaller gauges
                            cols_per_row = 4
                            rows = (num_stages + cols_per_row - 1) // cols_per_row
                            
                            fig = make_subplots(
                                rows=rows,
                                cols=cols_per_row,
                                subplot_titles=[f"<b style='color:black'>{stage['stage_name']}</b><br><span style='color:black'>Expected: {stage['conversion_percentage']}%</span>" for stage in stages],
                                specs=[[{"type": "indicator"} for _ in range(cols_per_row)] for _ in range(rows)],
                                vertical_spacing=0.25,  # More spacing between rows for subtitle
                                horizontal_spacing=0.1  # Better horizontal spacing
                            )
                            
                            for idx, stage in enumerate(stages):
                                row = (idx // cols_per_row) + 1
                                col = (idx % cols_per_row) + 1
                                
                                # Get actual conversion rate from pipeline requirements data
                                stage_name = stage['stage_name']
                                expected_conversion = stage['conversion_percentage']
                                
                                # Calculate actual conversion from pipeline_requirements_actual table
                                actual_conversion = 0
                                total_actual_records = 0
                                
                                for plan_data in filtered_plans:
                                    try:
                                        # Get actual data for this stage from pipeline_requirements_actual
                                        conn = get_db_connection()
                                        actual_query = """
                                            SELECT actual_at_stage, required_at_stage
                                            FROM pipeline_requirements_actual 
                                            WHERE staffing_plan_id = %s 
                                            AND stage_name = %s
                                        """
                                        cursor = conn.cursor()
                                        cursor.execute(actual_query, (plan_data['plan']['id'], stage_name))
                                        actual_data = cursor.fetchone()
                                        
                                        if actual_data and actual_data[1] > 0:  # required_at_stage > 0
                                            stage_actual_conversion = (actual_data[0] / actual_data[1]) * 100
                                            actual_conversion += stage_actual_conversion
                                            total_actual_records += 1
                                        
                                        cursor.close()
                                        conn.close()
                                    except Exception as e:
                                        pass
                                
                                # Calculate average actual conversion
                                if total_actual_records > 0:
                                    actual_conversion = actual_conversion / total_actual_records
                                else:
                                    actual_conversion = expected_conversion  # Fallback to expected if no actual data
                                
                                # Determine color based on actual vs expected comparison
                                conversion_ratio = actual_conversion / expected_conversion if expected_conversion > 0 else 1
                                
                                if conversion_ratio >= 0.9:  # Actual >= 90% of expected
                                    color = "green"
                                elif conversion_ratio >= 0.7:  # Actual >= 70% of expected
                                    color = "yellow"
                                else:  # Actual < 70% of expected
                                    color = "red"
                                
                                fig.add_trace(
                                    go.Indicator(
                                        mode="gauge+number+delta",
                                        value=actual_conversion,
                                        delta={'reference': expected_conversion, 'relative': False, 'suffix': '%'},
                                        domain={'x': [0.05, 0.95], 'y': [0.05, 0.95]},
                                        number={'font': {'size': 18, 'color': 'black', 'family': 'Arial Black'}, 'suffix': '%'},
                                        gauge={
                                            'axis': {
                                                'range': [None, 100],
                                                'tickwidth': 2,
                                                'tickcolor': "black",
                                                'tickfont': {'size': 8, 'color': 'black', 'family': 'Arial'},
                                                'tick0': 0,
                                                'dtick': 25
                                            },
                                            'bar': {'color': color, 'thickness': 0.8},
                                            'bgcolor': "white",
                                            'borderwidth': 2,
                                            'bordercolor': "black",
                                            'steps': [
                                                {'range': [0, expected_conversion * 0.7], 'color': "#ffebee"},
                                                {'range': [expected_conversion * 0.7, expected_conversion * 0.9], 'color': "#fff3e0"},
                                                {'range': [expected_conversion * 0.9, 100], 'color': "#e8f5e8"}
                                            ],
                                            'threshold': {
                                                'line': {'color': "blue", 'width': 3},
                                                'thickness': 0.7,
                                                'value': expected_conversion
                                            }
                                        }
                                    ),
                                    row=row, col=col
                                )
                            
                            # Create dynamic title based on filters
                            title_parts = ["Expected vs Actual Conversion Dashboard"]
                            if selected_client != 'All Clients':
                                title_parts.append(f"Client: {selected_client}")
                            if selected_owner != 'All Owners':
                                title_parts.append(f"Owner: {selected_owner}")
                            if selected_pipeline_id != 'all':
                                title_parts.append(f"Pipeline: {pipeline_name}")
                            else:
                                title_parts.append("All Pipelines")
                            
                            chart_title = " | ".join(title_parts)
                            
                            fig.update_layout(
                                height=max(300, 200 * rows),
                                title=dict(
                                    text=chart_title,
                                    x=0.5,
                                    font=dict(size=16, color='black', family='Arial Black'),
                                    pad=dict(t=10, b=10)
                                ),
                                font={'size': 10, 'color': 'black', 'family': 'Arial'},
                                margin=dict(l=20, r=20, t=50, b=30),
                                showlegend=False,
                                paper_bgcolor='white',
                                plot_bgcolor='white'
                            )
                            
                            # Update annotations with black text and better contrast
                            fig.update_annotations(
                                font_size=10,
                                font_color="black",
                                font_family="Arial",
                                bgcolor="rgba(255,255,255,0.9)",
                                bordercolor="black",
                                borderwidth=1
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add more space before next section
                            st.markdown("<br>", unsafe_allow_html=True)
                            
                            # Conversion comparison summary table
                            st.markdown("---")
                            st.markdown("### 📋 Expected vs Actual Conversion Breakdown")
                            st.markdown("Detailed comparison of expected vs actual conversion rates by stage")
                            st.markdown("")
                            conversion_data = []
                            
                            for idx, stage in enumerate(stages):
                                stage_name = stage['stage_name']
                                expected_conversion = stage['conversion_percentage']
                                
                                # Recalculate actual conversion for table (same logic as above)
                                actual_conversion = 0
                                total_actual_records = 0
                                
                                for plan_data in filtered_plans:
                                    try:
                                        conn = get_db_connection()
                                        actual_query = """
                                            SELECT actual_at_stage, required_at_stage
                                            FROM pipeline_requirements_actual 
                                            WHERE staffing_plan_id = %s 
                                            AND stage_name = %s
                                        """
                                        cursor = conn.cursor()
                                        cursor.execute(actual_query, (plan_data['plan']['id'], stage_name))
                                        actual_data = cursor.fetchone()
                                        
                                        if actual_data and actual_data[1] > 0:
                                            stage_actual_conversion = (actual_data[0] / actual_data[1]) * 100
                                            actual_conversion += stage_actual_conversion
                                            total_actual_records += 1
                                        
                                        cursor.close()
                                        conn.close()
                                    except Exception as e:
                                        pass
                                
                                if total_actual_records > 0:
                                    actual_conversion = actual_conversion / total_actual_records
                                else:
                                    actual_conversion = expected_conversion
                                
                                # Calculate variance
                                variance = actual_conversion - expected_conversion
                                variance_pct = (variance / expected_conversion * 100) if expected_conversion > 0 else 0
                                
                                # Determine performance status
                                if variance >= 0:
                                    if variance_pct >= 10:
                                        status = "🟢 Exceeding"
                                    else:
                                        status = "🟢 Meeting"
                                elif variance_pct >= -10:
                                    status = "🟡 Near Target"
                                else:
                                    status = "🔴 Below Target"
                                
                                conversion_data.append({
                                    'Stage': stage_name,
                                    'Expected %': f"{expected_conversion:.1f}%",
                                    'Actual %': f"{actual_conversion:.1f}%",
                                    'Variance': f"{variance:+.1f}%",
                                    'Variance %': f"{variance_pct:+.1f}%",
                                    'Status': status,
                                    'Data Points': total_actual_records
                                })
                            
                            conversion_df = pd.DataFrame(conversion_data)
                            st.dataframe(conversion_df, use_container_width=True)
                            
                            # Overall conversion metrics with better spacing
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("---")
                            st.markdown("### 📊 Overall Conversion Performance")
                            st.markdown("Key conversion metrics across all pipeline stages")
                            st.markdown("")
                            col1, col2, col3, col4 = st.columns(4)
                            
                            # Calculate overall metrics from conversion_data
                            total_stages = len(conversion_data)
                            avg_expected = sum(stage['conversion_percentage'] for stage in stages) / total_stages
                            
                            # Calculate average actual conversion from the data we processed
                            total_actual = 0
                            stages_with_data = 0
                            for data in conversion_data:
                                if data['Data Points'] > 0:
                                    total_actual += float(data['Actual %'].replace('%', ''))
                                    stages_with_data += 1
                            
                            avg_actual = total_actual / stages_with_data if stages_with_data > 0 else avg_expected
                            overall_variance = avg_actual - avg_expected
                            
                            # Find best and worst performing stages
                            best_stage = max(conversion_data, key=lambda x: float(x['Variance %'].replace('%', '').replace('+', '')))
                            worst_stage = min(conversion_data, key=lambda x: float(x['Variance %'].replace('%', '').replace('+', '')))
                            
                            with col1:
                                st.metric("Expected Avg", f"{avg_expected:.1f}%")
                            
                            with col2:
                                st.metric("Actual Avg", f"{avg_actual:.1f}%", f"{overall_variance:+.1f}%")
                            
                            with col3:
                                st.metric("Best Stage", best_stage['Stage'])
                            
                            with col4:
                                st.metric("Needs Attention", worst_stage['Stage'])
                            
                            # Explanation and recommendations
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("---")
                            st.markdown("### 📖 How to Read This Dashboard")
                            st.markdown("""
                            **Gauge Charts**: Each gauge shows the **actual conversion rate** for that stage, with:
                            - **Blue line**: Expected conversion rate (target threshold)
                            - **Delta value**: Difference between actual and expected (+ means exceeding, - means below)
                            - **Color coding**: Green (≥90% of expected), Yellow (70-89%), Red (<70%)
                            - **Background zones**: Light colors show performance ranges relative to expected rate
                            
                            **Data Source**: Actual conversion rates are calculated from the Pipeline Requirements table where you enter "Actual at Stage" values.
                            """)
                            
                            st.markdown("### 🎯 Conversion Optimization Recommendations")
                            st.markdown("Automated insights to improve pipeline performance")
                            st.markdown("")
                            
                            # Conversion-based recommendations
                            if overall_variance < -10:
                                st.error("🚨 **Critical**: Actual conversion rates are significantly below expected. Review pipeline processes and criteria.")
                            elif overall_variance < -5:
                                st.warning("⚠️ **Attention**: Actual conversion rates are below expected. Consider process improvements.")
                            elif overall_variance > 5:
                                st.success("✅ **Excellent**: Actual conversion rates are exceeding expectations!")
                            else:
                                st.info("ℹ️ **Good**: Conversion rates are meeting expectations.")
                            
                            # Specific recommendations based on conversion data
                            underperforming_stages = [d for d in conversion_data if float(d['Variance %'].replace('%', '').replace('+', '')) < -10]
                            if underperforming_stages:
                                stage_names = [s['Stage'] for s in underperforming_stages]
                                st.info(f"💡 **Suggestion**: These stages need attention: {', '.join(stage_names)}")
                            
                            high_performing_stages = [d for d in conversion_data if float(d['Variance %'].replace('%', '').replace('+', '')) > 10]
                            if high_performing_stages:
                                stage_names = [s['Stage'] for s in high_performing_stages]
                                st.info(f"🌟 **Great Work**: These stages are exceeding expectations: {', '.join(stage_names)}")
                            
                            # Data quality recommendations
                            stages_no_data = [d for d in conversion_data if d['Data Points'] == 0]
                            if stages_no_data:
                                stage_names = [s['Stage'] for s in stages_no_data]
                                st.info(f"📊 **Data Needed**: Enter actual data for these stages: {', '.join(stage_names)}")
                        
                        else:
                            st.info(f"No stages configured for pipeline: {pipeline_name}")
                    else:
                        st.info("No pipelines available for velocity analysis")
            
            # Pipeline Performance Dashboard
            with performance_tab:
                st.subheader("📊 Pipeline Performance Dashboard")
                st.markdown("Analyze planned vs actual candidate volumes and conversion rates across pipeline stages")
                
                # Display current date
                st.caption(f"📅 Current Date: {current_date}")
                st.markdown("---")
                
                # Filters for performance analysis
                if pipelines and staffing_plans:
                    st.markdown("### Filters")
                    
                    # Create filter columns
                    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
                    
                    with filter_col1:
                        # Client filter
                        all_clients = set()
                        for plan in staffing_plans:
                            all_clients.add(plan.get('client_name', 'Unknown'))
                        client_options = ['All Clients'] + sorted(list(all_clients))
                        selected_client = st.selectbox(
                            "Client",
                            options=client_options,
                            key="performance_client_filter"
                        )
                    
                    with filter_col2:
                        # Owner filter (from pipeline requirements table)
                        all_owners = set()
                        for plan in staffing_plans:
                            plan_details = staffing_manager.get_pipeline_planning_details(plan['id'])
                            if plan_details:
                                for detail in plan_details:
                                    pipeline_owner = detail.get('pipeline_owner', 'Unknown')
                                    if pipeline_owner and pipeline_owner != 'Unknown':
                                        all_owners.add(pipeline_owner)
                        owner_options = ['All Owners'] + sorted(list(all_owners))
                        selected_owner = st.selectbox(
                            "Pipeline Owner",
                            options=owner_options,
                            key="performance_owner_filter"
                        )
                    
                    with filter_col3:
                        # From Date filter
                        from_date = st.date_input(
                            "From Date",
                            value=datetime.now().date() - timedelta(days=30),
                            key="performance_from_date"
                        )
                    
                    with filter_col4:
                        # To Date filter
                        to_date = st.date_input(
                            "To Date",
                            value=datetime.now().date() + timedelta(days=90),
                            key="performance_to_date"
                        )
                    
                    # Pipeline selector with All option
                    pipeline_options = [('all', 'All Pipelines')] + [(p['id'], f"{p['name']} - {p['client_name']}") for p in pipelines]
                    selected_pipeline_id = st.selectbox(
                        "Select Pipeline for Performance Analysis",
                        options=[p[0] for p in pipeline_options],
                        format_func=lambda x: next(p[1] for p in pipeline_options if p[0] == x),
                        key="performance_pipeline_selector"
                    )
                    
                    if selected_pipeline_id:
                        if selected_pipeline_id == 'all':
                            # For "All Pipelines", collect all unique stages from all pipelines
                            all_stages = set()
                            stage_details = {}
                            
                            for pipeline in pipelines:
                                pipeline_stages = pipeline_manager.get_pipeline_stages(pipeline['id'])
                                for stage in pipeline_stages:
                                    stage_name = stage['stage_name']
                                    all_stages.add(stage_name)
                                    if stage_name not in stage_details:
                                        stage_details[stage_name] = stage
                            
                            # Convert to list format
                            stages = [stage_details[stage_name] for stage_name in sorted(all_stages)]
                            pipeline_name = "All Pipelines"
                        else:
                            stages = pipeline_manager.get_pipeline_stages(selected_pipeline_id)
                            pipeline_name = next(p['name'] for p in pipelines if p['id'] == selected_pipeline_id)
                        
                        if stages:
                            # Create combined bar + line chart
                            import plotly.graph_objects as go
                            from plotly.subplots import make_subplots
                            
                            # Get real data from Pipeline Requirements Table
                            stage_names = [stage['stage_name'] for stage in stages]
                            
                            # Initialize data structures for aggregation
                            planned_candidates = [0] * len(stages)
                            actual_candidates = [0] * len(stages)
                            planned_conversion = [stage.get('conversion_percentage', 70) for stage in stages]
                            actual_conversion = [0] * len(stages)
                            
                            # Filter and aggregate data from all staffing plans
                            filtered_plans = []
                            total_actual_counts = [0] * len(stages)
                            plan_counts = [0] * len(stages)
                            
                            for plan in staffing_plans:
                                # Apply filters
                                if selected_client != 'All Clients' and plan.get('client_name') != selected_client:
                                    continue
                                
                                plan_details = staffing_manager.get_pipeline_planning_details(plan['id'])
                                if not plan_details:
                                    continue
                                
                                for detail in plan_details:
                                    # For "All Pipelines", include all pipeline data
                                    if selected_pipeline_id != 'all' and detail.get('pipeline_id') != selected_pipeline_id:
                                        continue
                                    
                                    # Apply owner filter
                                    if selected_owner != 'All Owners' and detail.get('pipeline_owner') != selected_owner:
                                        continue
                                    
                                    # Apply date filter
                                    onboard_date = detail.get('onboard_by')
                                    if isinstance(onboard_date, str):
                                        onboard_date = datetime.strptime(onboard_date, '%Y-%m-%d').date()
                                    
                                    if onboard_date and (onboard_date < from_date or onboard_date > to_date):
                                        continue
                                    
                                    # Get pipeline requirements data
                                    requirements = staffing_manager.get_pipeline_requirements_actual(plan['id'], detail['role'])
                                    if requirements:
                                        for stage_name, req_data in requirements.items():
                                            if stage_name in stage_names:
                                                stage_idx = stage_names.index(stage_name)
                                                
                                                # Aggregate planned and actual pipeline data
                                                profiles_in_pipeline = req_data.get('profiles_in_pipeline', 0)
                                                actual_at_stage = req_data.get('actual_at_stage', 0)
                                                
                                                planned_candidates[stage_idx] += profiles_in_pipeline
                                                actual_candidates[stage_idx] += actual_at_stage
                                                
                                                # Calculate actual conversion rates
                                                if profiles_in_pipeline > 0:
                                                    actual_conv_rate = (actual_at_stage / profiles_in_pipeline) * 100
                                                    total_actual_counts[stage_idx] += actual_conv_rate
                                                    plan_counts[stage_idx] += 1
                                    
                                    filtered_plans.append({
                                        'plan': plan,
                                        'detail': detail
                                    })
                            
                            # Calculate average actual conversion rates
                            for i in range(len(stages)):
                                if plan_counts[i] > 0:
                                    actual_conversion[i] = total_actual_counts[i] / plan_counts[i]
                                else:
                                    actual_conversion[i] = planned_conversion[i]  # Use planned if no actual data
                            
                            # Display filter summary
                            st.markdown("---")
                            filter_summary = f"**Data Summary**: "
                            if selected_client != 'All Clients':
                                filter_summary += f"Client: {selected_client} | "
                            if selected_owner != 'All Owners':
                                filter_summary += f"Owner: {selected_owner} | "
                            filter_summary += f"Period: {from_date} to {to_date} | "
                            filter_summary += f"Plans Found: {len(filtered_plans)}"
                            st.markdown(filter_summary)
                            
                            # Check if we have any data to display
                            if not any(planned_candidates) and not any(actual_candidates):
                                st.warning("No pipeline requirements data found for the selected filters. Please ensure:")
                                st.markdown("- The selected pipeline has staffing plans with pipeline requirements")
                                st.markdown("- The filters match existing data")
                                st.markdown("- The date range includes plans with the selected pipeline")
                                return
                            
                            # Create subplot with secondary y-axis
                            fig = make_subplots(
                                specs=[[{"secondary_y": True}]],
                                subplot_titles=[f"Pipeline Performance Analysis - {pipeline_name}"]
                            )
                            
                            # Add bar charts for candidate volumes
                            fig.add_trace(
                                go.Bar(
                                    name="Planned Candidates",
                                    x=stage_names,
                                    y=planned_candidates,
                                    marker_color='#1f77b4',
                                    hovertemplate="<b>%{x}</b><br>Planned Candidates: %{y}<extra></extra>",
                                    showlegend=True
                                ),
                                secondary_y=False,
                            )
                            
                            fig.add_trace(
                                go.Bar(
                                    name="Actual Candidates",
                                    x=stage_names,
                                    y=actual_candidates,
                                    marker_color='#ff7f0e',
                                    hovertemplate="<b>%{x}</b><br>Actual Candidates: %{y}<extra></extra>",
                                    showlegend=True
                                ),
                                secondary_y=False,
                            )
                            
                            # Add line charts for conversion rates
                            fig.add_trace(
                                go.Scatter(
                                    name="Planned Conversion Rate",
                                    x=stage_names,
                                    y=planned_conversion,
                                    mode='lines+markers',
                                    line=dict(color='#2ca02c', width=3),
                                    marker=dict(size=8),
                                    hovertemplate="<b>%{x}</b><br>Planned Conversion: %{y:.1f}%<extra></extra>",
                                    showlegend=True
                                ),
                                secondary_y=True,
                            )
                            
                            fig.add_trace(
                                go.Scatter(
                                    name="Actual Conversion Rate",
                                    x=stage_names,
                                    y=actual_conversion,
                                    mode='lines+markers',
                                    line=dict(color='#d62728', width=3),
                                    marker=dict(size=8),
                                    hovertemplate="<b>%{x}</b><br>Actual Conversion: %{y:.1f}%<extra></extra>",
                                    showlegend=True
                                ),
                                secondary_y=True,
                            )
                            
                            # Create dynamic title based on filters
                            title_parts = [f"Pipeline Performance: {pipeline_name}"]
                            if selected_client != 'All Clients':
                                title_parts.append(f"Client: {selected_client}")
                            if selected_owner != 'All Owners':
                                title_parts.append(f"Owner: {selected_owner}")
                            dynamic_title = " | ".join(title_parts)
                            
                            # Update layout
                            fig.update_layout(
                                height=600,
                                title=dict(
                                    text=dynamic_title,
                                    x=0.5,
                                    font=dict(size=16)
                                ),
                                barmode='group',
                                legend=dict(
                                    orientation="h",
                                    yanchor="top",
                                    y=-0.2,
                                    xanchor="center",
                                    x=0.5,
                                    bgcolor="rgba(255,255,255,0.9)",
                                    bordercolor="rgba(0,0,0,0.3)",
                                    borderwidth=1,
                                    font=dict(color="black", size=12)
                                ),
                                margin=dict(l=80, r=80, t=100, b=150),
                                font=dict(size=12)
                            )
                            
                            # Set y-axes titles
                            fig.update_yaxes(title_text="Number of Candidates", secondary_y=False)
                            fig.update_yaxes(title_text="Conversion Rate (%)", secondary_y=True)
                            
                            # Update x-axis
                            fig.update_xaxes(title_text="Pipeline Stage")
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Performance Analysis Summary
                            st.markdown("---")
                            st.markdown("### Performance Analysis Summary")
                            st.markdown("")
                            
                            # Create performance insights
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                total_planned = sum(planned_candidates)
                                total_actual = sum(actual_candidates)
                                volume_variance = ((total_actual - total_planned) / total_planned) * 100
                                st.metric(
                                    "Total Volume Performance",
                                    f"{total_actual:,} candidates",
                                    delta=f"{volume_variance:+.1f}%"
                                )
                            
                            with col2:
                                avg_planned_conv = sum(planned_conversion) / len(planned_conversion)
                                avg_actual_conv = sum(actual_conversion) / len(actual_conversion)
                                conv_variance = avg_actual_conv - avg_planned_conv
                                st.metric(
                                    "Avg Conversion Rate",
                                    f"{avg_actual_conv:.1f}%",
                                    delta=f"{conv_variance:+.1f}%"
                                )
                            
                            with col3:
                                # Find biggest volume gap
                                volume_gaps = [abs(a - p) for a, p in zip(actual_candidates, planned_candidates)]
                                max_gap_idx = volume_gaps.index(max(volume_gaps))
                                worst_stage = stage_names[max_gap_idx]
                                st.metric("Biggest Volume Gap", worst_stage)
                            
                            with col4:
                                # Find conversion performance
                                conv_performance = [a - p for a, p in zip(actual_conversion, planned_conversion)]
                                best_conv_idx = conv_performance.index(max(conv_performance))
                                best_stage = stage_names[best_conv_idx]
                                st.metric("Best Conversion Performance", best_stage)
                            
                            # Detailed stage analysis
                            st.markdown("---")
                            st.markdown("### Stage-by-Stage Analysis")
                            st.markdown("")
                            
                            # Create detailed performance table
                            performance_data = []
                            for i, stage in enumerate(stage_names):
                                planned_vol = planned_candidates[i]
                                actual_vol = actual_candidates[i]
                                vol_gap = actual_vol - planned_vol
                                vol_gap_pct = (vol_gap / planned_vol) * 100 if planned_vol > 0 else 0
                                
                                planned_conv = planned_conversion[i]
                                actual_conv = actual_conversion[i]
                                conv_gap = actual_conv - planned_conv
                                
                                # Determine status
                                if vol_gap_pct >= 10 and conv_gap >= 5:
                                    status = "🟢 Excellent"
                                elif vol_gap_pct >= 0 and conv_gap >= 0:
                                    status = "🟡 Good"
                                elif vol_gap_pct >= -10 and conv_gap >= -5:
                                    status = "🟠 Needs Attention"
                                else:
                                    status = "🔴 Critical"
                                
                                performance_data.append({
                                    'Stage': stage,
                                    'Planned Volume': f"{planned_vol:,}",
                                    'Actual Volume': f"{actual_vol:,}",
                                    'Volume Gap': f"{vol_gap:+,} ({vol_gap_pct:+.1f}%)",
                                    'Planned Conversion': f"{planned_conv:.1f}%",
                                    'Actual Conversion': f"{actual_conv:.1f}%",
                                    'Conversion Gap': f"{conv_gap:+.1f}%",
                                    'Status': status
                                })
                            
                            performance_df = pd.DataFrame(performance_data)
                            st.dataframe(performance_df, use_container_width=True)
                            
                            # Recommendations
                            st.markdown("---")
                            st.markdown("### 🎯 Performance Optimization Recommendations")
                            st.markdown("")
                            
                            # Generate intelligent recommendations
                            recommendations = []
                            
                            for i, stage in enumerate(stage_names):
                                vol_gap_pct = ((actual_candidates[i] - planned_candidates[i]) / planned_candidates[i]) * 100 if planned_candidates[i] > 0 else 0
                                conv_gap = actual_conversion[i] - planned_conversion[i]
                                
                                if vol_gap_pct < -15:
                                    recommendations.append(f"**{stage}**: Low candidate volume ({vol_gap_pct:.1f}% gap) - Increase sourcing efforts")
                                elif conv_gap < -10:
                                    recommendations.append(f"**{stage}**: Poor conversion rate ({conv_gap:.1f}% gap) - Review process quality")
                                elif vol_gap_pct < -10 and conv_gap < -5:
                                    recommendations.append(f"**{stage}**: Both volume and conversion issues - Critical bottleneck requiring immediate attention")
                                elif vol_gap_pct > 0 and conv_gap < -5:
                                    recommendations.append(f"**{stage}**: High volume but low conversion - Process improvement needed")
                                elif vol_gap_pct < 0 and conv_gap > 0:
                                    recommendations.append(f"**{stage}**: Low volume but good conversion - Sourcing problem")
                            
                            if recommendations:
                                for rec in recommendations:
                                    st.warning(rec)
                            else:
                                st.success("🎉 **Great Performance!** All stages are performing within acceptable ranges.")
                            
                        else:
                            st.info(f"No stages configured for pipeline: {pipeline_name}")
                else:
                    st.info("No pipelines available for performance analysis")
            
            # Export functionality for analytics
            st.markdown("---")
            if st.button("📥 Export Performance Analytics", type="secondary"):
                analytics_data = []
                for pipeline in pipelines:
                    stages = pipeline_manager.get_pipeline_stages(pipeline['id'])
                    if stages:
                        total_tat = sum(stage['tat_days'] for stage in stages)
                        avg_conversion = sum(stage['conversion_percentage'] for stage in stages) / len(stages)
                        overall_velocity = max(0, 100 - (total_tat * 2))
                        analytics_data.append({
                            'Pipeline': pipeline['name'],
                            'Total TAT (Days)': total_tat,
                            'Avg Conversion %': avg_conversion,
                            'Velocity Score': overall_velocity,
                            'Number of Stages': len(stages),
                            'Bottleneck Stage': max(stages, key=lambda x: x['tat_days'])['stage_name']
                        })
                
                if analytics_data:
                    analytics_df = pd.DataFrame(analytics_data)
                    csv_data = analytics_df.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Download Performance Analytics CSV",
                        data=csv_data,
                        file_name=f"pipeline_performance_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
        else:
            st.info("No pipelines configured yet. Create your first pipeline to see performance analytics.")

def forecast_management_page():
    """Forecast Management page - Coming shortly"""
    st.subheader("📊 Forecast Management")
    
    # Coming soon message
    st.info("🚧 **Coming Shortly**")
    st.markdown("""
    This section will include advanced forecast management features such as:
    - Forecast model configuration
    - Advanced analytics and insights
    - Forecast accuracy tracking
    - Model performance metrics
    """)
    
    # Add some placeholder content to make it look more complete
    st.markdown("---")
    st.markdown("**Feature Development Timeline:**")
    st.progress(0.3)
    st.caption("30% Complete - Expected release in next update")

def demand_pipeline_configuration_page():
    """Demand Pipeline Configuration page with workflow state builder"""
    st.subheader("🔧 Demand Pipeline Configuration")
    
    # Import the demand pipeline manager
    from utils.demand_pipeline_manager import DemandPipelineManager
    
    # Initialize manager
    dm_pipeline_manager = DemandPipelineManager()
    
    # Check if in edit mode
    if 'edit_demand_pipeline_id' in st.session_state and st.session_state.edit_demand_pipeline_id:
        # Edit mode
        edit_demand_pipeline_form(dm_pipeline_manager)
    else:
        # Main view
        col1, col2 = st.columns([3, 1])
        
        with col2:
            if st.button("🆕 New Demand Config", type="primary", use_container_width=True):
                st.session_state.show_new_demand_pipeline = True
                st.rerun()
        
        # Show new pipeline form if requested
        if st.session_state.get('show_new_demand_pipeline', False):
            new_demand_pipeline_form(dm_pipeline_manager)
        else:
            # Show existing pipelines
            show_existing_demand_pipelines(dm_pipeline_manager)

def new_demand_pipeline_form(dm_pipeline_manager):
    """Form for creating new demand pipeline"""
    st.markdown("### 🆕 New Demand Pipeline Configuration")
    
    # Panel 1: Basic Information
    with st.container():
        st.markdown("#### Panel 1: Basic Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            pipeline_name = st.text_input("Pipeline Name *", key="new_demand_pipeline_name")
            region = st.selectbox("Region *", ["India", "SEA", "MEA", "Others"], key="new_demand_region")
        
        with col2:
            prospect_type = st.selectbox("Prospect Type *", ["Hunt", "Grow"], key="new_demand_prospect_type")
            description = st.text_area("Description", key="new_demand_description")
    
    # Panel 2: Workflow State Builder
    with st.container():
        st.markdown("#### Panel 2: Workflow State Builder")
        
        # Initialize workflow states in session state
        if 'demand_workflow_states' not in st.session_state:
            st.session_state.demand_workflow_states = []
        
        # Display current workflow states with edit functionality
        if st.session_state.demand_workflow_states:
            st.markdown("**Current Workflow States:**")
            
            # Track which stages are being edited
            if 'demand_stage_editing' not in st.session_state:
                st.session_state.demand_stage_editing = set()
            
            for i, state in enumerate(st.session_state.demand_workflow_states):
                # Check if this stage is in edit mode
                is_editing = i in st.session_state.demand_stage_editing
                
                if not is_editing:
                    # Display mode
                    with st.container():
                        col1, col2, col3, col4, col5, col6 = st.columns([3, 2, 2, 3, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{state['stage_name']}**")
                        with col2:
                            st.markdown(f"Conv: {state['conversion_rate']}%")
                        with col3:
                            st.markdown(f"TAT: {state['tat_days']} days")
                        with col4:
                            st.markdown(f"{state['stage_description'][:30]}...")
                        with col5:
                            if st.button("✏️", key=f"edit_new_demand_stage_{i}", help="Edit this stage"):
                                st.session_state.demand_stage_editing.add(i)
                                st.rerun()
                        with col6:
                            if st.button("🗑️", key=f"delete_demand_state_{i}", help="Delete this stage"):
                                st.session_state.demand_workflow_states.pop(i)
                                # Adjust editing indices after deletion
                                st.session_state.demand_stage_editing = {
                                    idx - 1 if idx > i else idx 
                                    for idx in st.session_state.demand_stage_editing 
                                    if idx != i
                                }
                                st.rerun()
                else:
                    # Edit mode
                    with st.container():
                        st.markdown(f"**Edit Stage {i + 1}:**")
                        
                        with st.form(f"edit_new_demand_stage_form_{i}", clear_on_submit=False):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                edit_stage_name = st.text_input(
                                    "Stage Name", 
                                    value=state['stage_name'], 
                                    key=f"edit_new_stage_name_{i}"
                                )
                                edit_conversion_rate = st.number_input(
                                    "Conversion Rate (%)", 
                                    min_value=0.0, 
                                    max_value=100.0, 
                                    value=float(state['conversion_rate']),
                                    key=f"edit_new_conversion_rate_{i}"
                                )
                            
                            with col2:
                                edit_tat_days = st.number_input(
                                    "TAT (Days)", 
                                    min_value=1, 
                                    value=int(state['tat_days']),
                                    key=f"edit_new_tat_days_{i}"
                                )
                                edit_stage_description = st.text_input(
                                    "Stage Description", 
                                    value=state['stage_description'],
                                    key=f"edit_new_stage_description_{i}"
                                )
                            
                            col_save, col_cancel = st.columns(2)
                            
                            with col_save:
                                if st.form_submit_button("💾 Save Changes"):
                                    if edit_stage_name:
                                        # Update the stage in session state
                                        st.session_state.demand_workflow_states[i] = {
                                            'stage_name': edit_stage_name,
                                            'conversion_rate': edit_conversion_rate,
                                            'tat_days': edit_tat_days,
                                            'stage_description': edit_stage_description
                                        }
                                        # Exit edit mode
                                        st.session_state.demand_stage_editing.discard(i)
                                        st.success(f"✅ Stage '{edit_stage_name}' updated!")
                                        st.rerun()
                                    else:
                                        st.error("Stage Name is required")
                            
                            with col_cancel:
                                if st.form_submit_button("❌ Cancel"):
                                    # Exit edit mode without saving
                                    st.session_state.demand_stage_editing.discard(i)
                                    st.rerun()
                        
                        st.markdown("---")
            
            if not st.session_state.demand_stage_editing:
                st.markdown("---")
        
        # Add new state form
        st.markdown("**Add New State:**")
        
        # Use form to handle inputs better
        with st.form("add_new_demand_state_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                new_stage_name = st.text_input("Stage Name")
                new_conversion_rate = st.number_input("Conversion Rate (%)", min_value=0.0, max_value=100.0, value=50.0)
            
            with col2:
                new_tat_days = st.number_input("TAT (Days)", min_value=1, value=7)
                new_stage_description = st.text_input("Stage Description")
            
            submitted = st.form_submit_button("➕ Add New State")
            
            if submitted and new_stage_name:
                st.session_state.demand_workflow_states.append({
                    'stage_name': new_stage_name,
                    'conversion_rate': new_conversion_rate,
                    'tat_days': new_tat_days,
                    'stage_description': new_stage_description
                })
                st.rerun()
            elif submitted and not new_stage_name:
                st.error("Stage Name is required")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("💾 Save Pipeline", key="save_demand_pipeline"):
                if pipeline_name and region and prospect_type and st.session_state.demand_workflow_states:
                    try:
                        # Create pipeline
                        pipeline_id = dm_pipeline_manager.create_pipeline(
                            pipeline_name, region, prospect_type, description
                        )
                        
                        # Add stages
                        for i, state in enumerate(st.session_state.demand_workflow_states):
                            dm_pipeline_manager.add_pipeline_stage(
                                pipeline_id,
                                state['stage_name'],
                                state['conversion_rate'],
                                state['tat_days'],
                                state['stage_description'],
                                i + 1
                            )
                        
                        st.success(f"✅ Pipeline '{pipeline_name}' created successfully!")
                        
                        # Clear form and states
                        st.session_state.show_new_demand_pipeline = False
                        st.session_state.demand_workflow_states = []
                        for key in ['new_demand_pipeline_name', 'new_demand_region', 'new_demand_prospect_type', 'new_demand_description']:
                            if key in st.session_state:
                                del st.session_state[key]
                        
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error creating pipeline: {str(e)}")
                else:
                    st.error("Please fill all required fields and add at least one workflow state")
        
        with col2:
            if st.button("❌ Cancel", key="cancel_demand_pipeline"):
                st.session_state.show_new_demand_pipeline = False
                st.session_state.demand_workflow_states = []
                st.rerun()

def show_existing_demand_pipelines(dm_pipeline_manager):
    """Display existing demand pipelines with edit buttons"""
    st.markdown("### 📋 Existing Demand Pipelines")
    
    pipelines = dm_pipeline_manager.get_all_pipelines()
    
    if pipelines:
        for pipeline in pipelines:
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])
                
                with col1:
                    st.markdown(f"**{pipeline['name']}**")
                    st.markdown(f"*{pipeline['description']}*" if pipeline['description'] else "*No description*")
                
                with col2:
                    st.markdown(f"**Region:** {pipeline['region']}")
                    st.markdown(f"**Type:** {pipeline['prospect_type']}")
                
                with col3:
                    st.markdown(f"**Stages:** {pipeline['stage_count']}")
                    try:
                        created_str = pipeline['created_date'].strftime('%Y-%m-%d') if hasattr(pipeline['created_date'], 'strftime') else str(pipeline['created_date'])[:10]
                    except (ValueError, AttributeError):
                        created_str = "N/A"
                    st.markdown(f"**Created:** {created_str}")
                
                with col4:
                    if st.button("⚙️ Edit", key=f"edit_demand_pipeline_{pipeline['id']}"):
                        st.session_state.edit_demand_pipeline_id = pipeline['id']
                        st.rerun()
                
                with col5:
                    if st.button("🗑️ Delete", key=f"delete_demand_pipeline_{pipeline['id']}"):
                        if st.session_state.get(f'confirm_delete_demand_{pipeline["id"]}', False):
                            dm_pipeline_manager.delete_pipeline(pipeline['id'])
                            st.success(f"Pipeline '{pipeline['name']}' deleted successfully!")
                            st.rerun()
                        else:
                            st.session_state[f'confirm_delete_demand_{pipeline["id"]}'] = True
                            st.warning("Click again to confirm deletion")
                            st.rerun()
                
                st.markdown("---")
    else:
        st.info("No demand pipelines configured yet. Click 'New Demand Config' to create your first pipeline.")

def edit_demand_pipeline_form(dm_pipeline_manager):
    """Form for editing existing demand pipeline"""
    pipeline_id = st.session_state.edit_demand_pipeline_id
    pipeline = dm_pipeline_manager.get_pipeline_by_id(pipeline_id)
    
    if not pipeline:
        st.error("Pipeline not found")
        st.session_state.edit_demand_pipeline_id = None
        st.rerun()
        return
    
    st.markdown(f"### ⚙️ Edit Demand Pipeline: {pipeline['name']}")
    
    # Panel 1: Basic Information
    with st.container():
        st.markdown("#### Panel 1: Basic Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            pipeline_name = st.text_input("Pipeline Name *", value=pipeline['name'], key="edit_demand_pipeline_name")
            # Handle region values
            current_region = pipeline.get('region') or 'India'
            region_options = ["India", "SEA", "MEA", "Others"]
            region_index = region_options.index(current_region) if current_region in region_options else 0
            region = st.selectbox("Region *", region_options, 
                                index=region_index, 
                                key="edit_demand_region")
        
        with col2:
            # Handle NULL prospect_type values
            current_prospect_type = pipeline.get('prospect_type') or 'Hunt'
            prospect_type_index = ["Hunt", "Grow"].index(current_prospect_type) if current_prospect_type in ["Hunt", "Grow"] else 0
            prospect_type = st.selectbox("Prospect Type *", ["Hunt", "Grow"], 
                                       index=prospect_type_index, 
                                       key="edit_demand_prospect_type")
            description = st.text_area("Description", value=pipeline['description'] or "", key="edit_demand_description")
    
    # Panel 2: Workflow State Builder
    with st.container():
        st.markdown("#### Panel 2: Workflow State Builder")
        
        # Load existing stages into session state if not already loaded
        if 'edit_demand_workflow_states' not in st.session_state:
            st.session_state.edit_demand_workflow_states = [
                {
                    'stage_name': stage['stage_name'],
                    'conversion_rate': float(stage['conversion_rate']),
                    'tat_days': stage.get('tat_value', stage.get('tat_days', 1)),  # Handle both column names
                    'stage_description': stage.get('stage_description') or ""
                }
                for stage in pipeline.get('stages', [])
            ]
        
        # Display current workflow states with edit functionality
        if st.session_state.edit_demand_workflow_states:
            st.markdown("**Current Workflow States:**")
            
            # Track which stages are being edited
            if 'edit_demand_stage_editing' not in st.session_state:
                st.session_state.edit_demand_stage_editing = set()
            
            for i, state in enumerate(st.session_state.edit_demand_workflow_states):
                # Check if this stage is in edit mode
                is_editing = i in st.session_state.edit_demand_stage_editing
                
                if not is_editing:
                    # Display mode
                    with st.container():
                        col1, col2, col3, col4, col5, col6 = st.columns([3, 2, 2, 3, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{state['stage_name']}**")
                        with col2:
                            st.markdown(f"Conv: {state['conversion_rate']}%")
                        with col3:
                            st.markdown(f"TAT: {state['tat_days']} days")
                        with col4:
                            st.markdown(f"{state['stage_description'][:30]}...")
                        with col5:
                            if st.button("✏️", key=f"edit_demand_stage_{i}", help="Edit this stage"):
                                st.session_state.edit_demand_stage_editing.add(i)
                                st.rerun()
                        with col6:
                            if st.button("🗑️", key=f"delete_edit_demand_state_{i}", help="Delete this stage"):
                                st.session_state.edit_demand_workflow_states.pop(i)
                                # Adjust editing indices after deletion
                                st.session_state.edit_demand_stage_editing = {
                                    idx - 1 if idx > i else idx 
                                    for idx in st.session_state.edit_demand_stage_editing 
                                    if idx != i
                                }
                                st.rerun()
                else:
                    # Edit mode
                    with st.container():
                        st.markdown(f"**Edit Stage {i + 1}:**")
                        
                        with st.form(f"edit_demand_stage_form_{i}", clear_on_submit=False):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                edit_stage_name = st.text_input(
                                    "Stage Name", 
                                    value=state['stage_name'], 
                                    key=f"edit_stage_name_{i}"
                                )
                                edit_conversion_rate = st.number_input(
                                    "Conversion Rate (%)", 
                                    min_value=0.0, 
                                    max_value=100.0, 
                                    value=float(state['conversion_rate']),
                                    key=f"edit_conversion_rate_{i}"
                                )
                            
                            with col2:
                                edit_tat_days = st.number_input(
                                    "TAT (Days)", 
                                    min_value=1, 
                                    value=int(state['tat_days']),
                                    key=f"edit_tat_days_{i}"
                                )
                                edit_stage_description = st.text_input(
                                    "Stage Description", 
                                    value=state['stage_description'],
                                    key=f"edit_stage_description_{i}"
                                )
                            
                            col_save, col_cancel = st.columns(2)
                            
                            with col_save:
                                if st.form_submit_button("💾 Save Changes"):
                                    if edit_stage_name:
                                        # Update the stage in session state
                                        st.session_state.edit_demand_workflow_states[i] = {
                                            'stage_name': edit_stage_name,
                                            'conversion_rate': edit_conversion_rate,
                                            'tat_days': edit_tat_days,
                                            'stage_description': edit_stage_description
                                        }
                                        # Exit edit mode
                                        st.session_state.edit_demand_stage_editing.discard(i)
                                        st.success(f"✅ Stage '{edit_stage_name}' updated!")
                                        st.rerun()
                                    else:
                                        st.error("Stage Name is required")
                            
                            with col_cancel:
                                if st.form_submit_button("❌ Cancel"):
                                    # Exit edit mode without saving
                                    st.session_state.edit_demand_stage_editing.discard(i)
                                    st.rerun()
                        
                        st.markdown("---")
            
            if not st.session_state.edit_demand_stage_editing:
                st.markdown("---")
        
        # Add new state form
        st.markdown("**Add New State:**")
        
        # Use form to handle inputs better
        with st.form("add_edit_demand_state_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                new_stage_name = st.text_input("Stage Name")
                new_conversion_rate = st.number_input("Conversion Rate (%)", min_value=0.0, max_value=100.0, value=50.0)
            
            with col2:
                new_tat_days = st.number_input("TAT (Days)", min_value=1, value=7)
                new_stage_description = st.text_input("Stage Description")
            
            submitted = st.form_submit_button("➕ Add New State")
            
            if submitted and new_stage_name:
                st.session_state.edit_demand_workflow_states.append({
                    'stage_name': new_stage_name,
                    'conversion_rate': new_conversion_rate,
                    'tat_days': new_tat_days,
                    'stage_description': new_stage_description
                })
                st.rerun()
            elif submitted and not new_stage_name:
                st.error("Stage Name is required")
        
        col1, col2, col3 = st.columns([2, 2, 2])
        
        with col1:
            if st.button("💾 Update Pipeline", key="update_demand_pipeline"):
                if pipeline_name and region and prospect_type:
                    try:
                        # Update pipeline
                        dm_pipeline_manager.update_pipeline(
                            pipeline_id, pipeline_name, region, prospect_type, description
                        )
                        
                        # Delete existing stages and add new ones
                        dm_pipeline_manager.delete_pipeline_stages(pipeline_id)
                        
                        for i, state in enumerate(st.session_state.edit_demand_workflow_states):
                            dm_pipeline_manager.add_pipeline_stage(
                                pipeline_id,
                                state['stage_name'],
                                state['conversion_rate'],
                                state['tat_days'],
                                state['stage_description'],
                                i + 1
                            )
                        
                        st.success(f"✅ Pipeline '{pipeline_name}' updated successfully!")
                        
                        # Clear edit mode
                        st.session_state.edit_demand_pipeline_id = None
                        if 'edit_demand_workflow_states' in st.session_state:
                            del st.session_state.edit_demand_workflow_states
                        
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error updating pipeline: {str(e)}")
                else:
                    st.error("Please fill all required fields")
        
        with col3:
            if st.button("❌ Cancel", key="cancel_edit_demand_pipeline"):
                st.session_state.edit_demand_pipeline_id = None
                if 'edit_demand_workflow_states' in st.session_state:
                    del st.session_state.edit_demand_workflow_states
                st.rerun()

def supply_management_section():
    """Supply Management section showing all supply pipelines planned with real-time candidate counts"""
    
    # Check permissions for supply management
    permission_manager = st.session_state.permission_manager
    current_user_email = st.session_state.get('user_email', '')
    
    # Check if user can view supply planning
    if not permission_manager.has_permission(current_user_email, "Supply Planning", "Supply Management", "view"):
        permission_manager.show_access_denied_message("Supply Planning", "Supply Management")
        return
    
    st.subheader("📋 Supply Management Dashboard")
    st.markdown("Real-time candidate tracking integrated with supply pipeline plans")
    
    # Get environment manager from session state
    env_manager = st.session_state.env_manager
    
    # Initialize candidate pipeline mapper
    from utils.candidate_pipeline_mapper import CandidatePipelineMapper
    mapper = CandidatePipelineMapper()
    
    # Controls for counting method
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        use_cumulative = st.checkbox("Use Cumulative Counting", value=True, 
                                   help="Count candidates at current stage + all subsequent stages")
    with col2:
        show_breakdown = st.checkbox("Show Status Breakdown", value=False,
                                   help="Show detailed candidate status breakdown")
    with col3:
        if st.button("🔄 Refresh", help="Refresh candidate counts"):
            st.rerun()
    
    # Data Quality Report section
    st.markdown("---")
    if st.expander("📊 Data Quality Report", expanded=False):
        with st.spinner("Generating data quality report..."):
            quality_report = mapper.get_data_quality_report()
            
            if 'error' not in quality_report:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Unmatched Staffing Plans", quality_report.get('unmatched_staffing_plans', 0))
                    st.caption("Candidates with staffing plans not found in system")
                
                with col2:
                    st.metric("Unrecognized Statuses", quality_report.get('unrecognized_statuses', 0))
                    st.caption("Candidates with statuses not in mapping system")
                
                with col3:
                    st.metric("Missing Client/Role Mapping", quality_report.get('missing_client_role_mapping', 0))
                    st.caption("Candidates missing client or role information")
                

            else:
                st.error(f"Error generating data quality report: {quality_report['error']}")
    
    st.markdown("---")
    
    try:
        # Connect to database and get supply pipeline data
        import psycopg2
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        # Query to get all supply pipeline plans with details
        staffing_plans_table = env_manager.get_table_name('staffing_plans')
        master_clients_table = env_manager.get_table_name('master_clients')
        pipeline_planning_table = env_manager.get_table_name('pipeline_planning_details')
        
        query = f"""
        SELECT DISTINCT
            sp.plan_name,
            mc.client_name,
            sp.target_start_date,
            sp.target_end_date,
            sp.planned_positions,
            COALESCE(ppd.pipeline_owner, sp.created_by) as plan_owner,
            sp.created_date,
            spgp.role,
            spgp.generated_data
        FROM {staffing_plans_table} sp
        JOIN {master_clients_table} mc ON sp.client_id = mc.master_client_id
        LEFT JOIN {env_manager.get_table_name('staffing_plan_generated_plans')} spgp ON sp.id = spgp.plan_id
        LEFT JOIN {pipeline_planning_table} ppd ON sp.id = ppd.plan_id AND spgp.role = ppd.role
        WHERE sp.status IN ('Active', 'Planning')
        ORDER BY sp.target_end_date DESC, sp.plan_name, spgp.role
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        if results:
            # Create DataFrame for display
            supply_data = []
            for row in results:
                plan_name, client_name, start_date, end_date, planned_positions, plan_owner, created_date, role, generated_data = row
                
                # Parse generated data if available (it's a JSONB array of pipeline stages)
                role_positions = None
                role_staffed_date = None
                stage_of_pipeline = None
                profiles_in_stage = None
                last_updated = None
                
                if generated_data and role:
                    try:
                        import json
                        # generated_data is a JSONB array of pipeline stages
                        stages = generated_data if isinstance(generated_data, list) else json.loads(generated_data)
                        
                        if stages and len(stages) > 0:
                            # Get the final stage date as staffed_by_date
                            final_stage = max(stages, key=lambda x: x.get('stage_order', 0))
                            role_staffed_date = final_stage.get('needed_by_date')
                            
                            # Get target positions from the final stage
                            role_positions = final_stage.get('profiles_converted', 2)  # Default to 2 if not found
                            
                            # Find the latest stage in pipeline progression where Actual # is > 0
                            latest_stage = None
                            max_stage_order = -1
                            
                            for stage in stages:
                                actual_profiles = stage.get('actual_profiles', 0)
                                if actual_profiles and actual_profiles > 0:
                                    # Get stage order (higher order = later in pipeline)
                                    stage_order = stage.get('stage_order', 0)
                                    if stage_order > max_stage_order:
                                        max_stage_order = stage_order
                                        latest_stage = stage
                            
                            if latest_stage:
                                stage_of_pipeline = latest_stage.get('stage_name', 'N/A')
                                profiles_in_stage = latest_stage.get('actual_profiles', 0)
                            
                            # If no stage has actual profiles, use the first stage but keep actual as 0
                            if not stage_of_pipeline and stages:
                                first_stage = stages[0]
                                stage_of_pipeline = first_stage.get('stage_name', 'N/A')
                                profiles_in_stage = 0  # Keep actual profiles as 0 since no stage has actuals
                            
                            # Set a default last updated time - in production this would come from a timestamp field
                            last_updated = "2025-07-27"  # Default placeholder
                    except Exception as e:
                        print(f"Error parsing generated_data: {e}")
                
                # Set defaults if no data found
                if not stage_of_pipeline:
                    stage_of_pipeline = "N/A"
                if not profiles_in_stage:
                    profiles_in_stage = 0
                if not last_updated:
                    last_updated = str(created_date)[:10] if created_date else "N/A"
                
                # Get the actual pipeline owner from the generated_data or use plan owner as fallback
                owner_name = plan_owner  # Default to plan owner (created_by)
                
                # Try to extract actual pipeline owner from generated_data
                if generated_data:
                    try:
                        import json
                        if isinstance(generated_data, str):
                            stages = json.loads(generated_data)
                        else:
                            stages = generated_data
                        
                        # Look for pipeline_owner in the first stage data or metadata
                        if stages and len(stages) > 0:
                            first_stage = stages[0]
                            if 'pipeline_owner' in first_stage:
                                actual_owner = first_stage['pipeline_owner']
                                # Convert email to display name if needed
                                if actual_owner == 'anna.pauly@greyamp.com':
                                    owner_name = 'Anna Pauly'
                                elif actual_owner == 'priyanka.r@greyamp.com':
                                    owner_name = 'Priyanka R'
                                else:
                                    owner_name = actual_owner
                    except Exception as e:
                        print(f"Error extracting pipeline owner: {e}")
                        # Fallback to role-based mapping only if extraction fails
                        if role:
                            role_owner_mapping = {
                                'FE Dev': 'Anna Pauly',
                                'Frontend Dev': 'Anna Pauly',
                                'Frontedn Dev': 'Anna Pauly', # Handle typo in database
                                'BE Dev': 'Priyanka R', 
                                'TPM': 'Priyanka R'
                            }
                            owner_name = role_owner_mapping.get(role, plan_owner)
                
                # Use extracted data if available, otherwise fall back to plan-level data
                positions = int(role_positions) if role_positions else planned_positions
                staffed_by_date = role_staffed_date if role_staffed_date else str(end_date)
                role_name = role if role else "General Staffing"
                
                # Check for manually updated actual_profiles first
                manually_updated_actual = None
                if stages and len(stages) > 0:
                    # Find the matching stage based on stage_of_pipeline for manually updated actual count
                    stage_mapping = {
                        'Initial Screening': ['Initial Screening', 'initialscreening'],
                        'Code Test': ['Code Test', 'codetest'],
                        'Code Pairing': ['Code Pairing', 'codepairing'],
                        'Client Interview': ['Client Interview', 'clientinterview'],
                        'Fitment': ['Fitment', 'fitment']
                    }
                    
                    for stage in stages:
                        stage_name = stage.get('stage_name', '')
                        # Check if this stage matches the current pipeline stage
                        for mapped_stage, variations in stage_mapping.items():
                            if stage_name in variations or stage_name.lower().replace(' ', '') in [v.lower().replace(' ', '') for v in variations]:
                                if mapped_stage == stage_of_pipeline and 'actual_profiles' in stage:
                                    manually_updated_actual = stage['actual_profiles']
                                    break
                        if manually_updated_actual is not None:
                            break
                
                # Use manually updated value if available, otherwise calculate real-time count
                if manually_updated_actual is not None:
                    actual_count = manually_updated_actual
                    status_breakdown = {'manual_update': manually_updated_actual}
                    match_level = 'manual'
                else:
                    # Calculate real-time candidate count using pipeline mapper
                    candidate_count_result = mapper.get_candidate_count_for_stage(
                        client=client_name,
                        staffing_plan=plan_name,
                        role=role_name,
                        pipeline_stage=stage_of_pipeline,
                        use_cumulative=use_cumulative
                    )
                    
                    actual_count = candidate_count_result.get('count', 0)
                    status_breakdown = candidate_count_result.get('breakdown', {})
                    match_level = candidate_count_result.get('match_level', 'none')
                
                # Calculate exited process count (Dropped + Rejected + On-Hold) for this supply plan
                exited_count_result = mapper.get_exited_process_count_for_plan(
                    client=client_name,
                    staffing_plan=plan_name,
                    role=role_name
                )
                
                exited_count = exited_count_result.get('count', 0)
                exited_breakdown = exited_count_result.get('breakdown', {})
                
                supply_data.append({
                    'Owner': owner_name,
                    'Role': role_name,
                    'Client': client_name,
                    '# of Positions': positions,
                    'To be Staffed by Date': staffed_by_date,
                    'Stage of Pipeline': stage_of_pipeline,
                    'Actual #': actual_count,
                    'Exited Process #': exited_count,
                    'Match Quality': match_level,
                    'Status Breakdown': status_breakdown,
                    'Exited Breakdown': exited_breakdown,
                    'Last Updated': last_updated,
                    'Plan Name': plan_name,
                    'Start Date': str(start_date),
                    'Created Date': str(created_date)[:10]
                })
            
            # Convert to DataFrame
            df = pd.DataFrame(supply_data)
            
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_plans = len(df['Plan Name'].unique())
                st.metric("Total Plans", total_plans)
            
            with col2:
                total_roles = len(df)
                st.metric("Total Roles", total_roles)
            
            with col3:
                total_positions = df['# of Positions'].sum()
                st.metric("Total Positions", total_positions)
            
            with col4:
                unique_clients = len(df['Client'].unique())
                st.metric("Unique Clients", unique_clients)
            
            # Legend for visual indicators
            with st.expander("ℹ️ Visual Indicators Guide", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Actual # Icons:**")
                    st.markdown("✏️ - Manually updated value")
                    st.markdown("🟢 - Auto-calculated (exact match)")
                    st.markdown("🟡 - Auto-calculated (owner match)")
                    st.markdown("🟠 - Auto-calculated (client+role match)")
                    st.markdown("🔴 - Auto-calculated (no match)")
                with col2:
                    st.markdown("**How to Update:**")
                    st.markdown("1. Click the ⚙️ Edit button")
                    st.markdown("2. Update the Actual # for any stage")  
                    st.markdown("3. Click 💾 Save Changes")
                    st.markdown("4. Manual values override auto-calculations")
            
            st.markdown("---")
            
            # Filter options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                owner_filter = st.selectbox(
                    "Filter by Owner",
                    options=["All"] + sorted(df['Owner'].dropna().unique().tolist()),
                    index=0
                )
            
            with col2:
                client_filter = st.selectbox(
                    "Filter by Client", 
                    options=["All"] + sorted(df['Client'].unique().tolist()),
                    index=0
                )
            
            with col3:
                role_filter = st.selectbox(
                    "Filter by Role",
                    options=["All"] + sorted(df['Role'].unique().tolist()),
                    index=0
                )
            
            # Apply filters
            filtered_df = df.copy()
            
            if owner_filter != "All":
                filtered_df = filtered_df[filtered_df['Owner'] == owner_filter]
            
            if client_filter != "All":
                filtered_df = filtered_df[filtered_df['Client'] == client_filter]
            
            if role_filter != "All":
                filtered_df = filtered_df[filtered_df['Role'] == role_filter]
            
            # Display filtered results
            st.markdown(f"**Showing {len(filtered_df)} records**")
            
            # Show headers first  
            if show_breakdown:
                col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([1.2, 1.2, 1.2, 1, 1.2, 1.2, 0.8, 0.8, 0.8, 1, 1.2, 1])
            else:
                col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([1.5, 1.5, 1.5, 1.5, 1, 1.5, 1.5, 1, 1, 1.2, 0.8, 0.8])
            
            with col1:
                st.markdown("**Owner**")
            with col2:
                st.markdown("**Role**")
            with col3:
                st.markdown("**Client**")
            with col4:
                st.markdown("**Supply Plan Name**")
            with col5:
                st.markdown("**# Positions**")
            with col6:
                st.markdown("**To be Staffed by Date**")
            with col7:
                st.markdown("**Stage of Pipeline**")
            with col8:
                st.markdown("**Actual #**")
            with col9:
                st.markdown("**Exited Process #**")
            with col10:
                st.markdown("**Last Updated**")
            with col11:
                st.markdown("**View**")
            with col12:
                st.markdown("**Edit**")
            
            st.markdown("---")
            
            # Display records with action buttons
            for idx, row in filtered_df.iterrows():
                if show_breakdown:
                    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([1.2, 1.2, 1.2, 1, 1.2, 1.2, 0.8, 0.8, 0.8, 1, 1.2, 1])
                else:
                    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([1.5, 1.5, 1.5, 1.5, 1, 1.5, 1.5, 1, 1, 1.2, 0.8, 0.8])
                
                with col1:
                    st.write(row['Owner'])
                with col2:
                    st.write(row['Role'])
                with col3:
                    st.write(row['Client'])
                with col4:
                    st.write(row['Plan Name'])
                with col5:
                    st.write(str(row['# of Positions']))
                with col6:
                    st.write(row['To be Staffed by Date'])
                with col7:
                    st.write(row['Stage of Pipeline'])
                with col8:
                    # Enhanced Actual # display with quality indicator
                    actual_count = row['Actual #']
                    match_quality = row.get('Match Quality', 'none')
                    
                    # Determine if this is a manual update or automatic calculation
                    is_manual = match_quality == 'manual'
                    
                    # Icons for different types of values
                    if is_manual:
                        value_icon = '✏️'  # Pencil for manually edited
                        tooltip = f"Manually updated actual count: {actual_count}"
                    else:
                        # Quality indicators for automatic calculations
                        quality_icons = {
                            'exact': '🟢',      # Green for exact match
                            'owner': '🟡',     # Yellow for owner match
                            'client_role': '🟠', # Orange for client+role match
                            'none': '🔴'       # Red for no match
                        }
                        value_icon = quality_icons.get(match_quality, '❓')
                        tooltip = f"Auto-calculated from database - Match level: {match_quality}"
                    
                    if show_breakdown and row.get('Status Breakdown'):
                        # Show breakdown details in expander
                        with st.expander(f"{value_icon} {actual_count}", expanded=False):
                            breakdown = row['Status Breakdown']
                            if is_manual:
                                st.write("📝 **Manually Updated Value**")
                                st.write(f"Actual Count: {actual_count}")
                            elif breakdown:
                                st.write("🤖 **Auto-calculated from Database**")
                                for status, count in breakdown.items():
                                    if status != 'manual_update':  # Skip the manual update marker
                                        st.write(f"• {status}: {count}")
                                st.write(f"**Match Level:** {match_quality}")
                            else:
                                st.write("No candidates found")
                    else:
                        # Add tooltip to show the type of value
                        st.write(f"{value_icon} {actual_count}")
                        if is_manual:
                            st.caption("✏️ Manual")
                        
                with col9:
                    # Show rejected count with breakdown if available
                    rejected_count = row['Exited Process #']
                    
                    if show_breakdown and row.get('Exited Breakdown'):
                        # Show exited process breakdown details in expander
                        with st.expander(f"❌ {rejected_count}", expanded=False):
                            rejected_breakdown = row['Exited Breakdown']
                            if rejected_breakdown:
                                for status, count in rejected_breakdown.items():
                                    st.write(f"• {status}: {count}")
                            else:
                                st.write("No rejected candidates")
                    else:
                        st.write(f"❌ {rejected_count}")
                        
                with col10:
                    st.write(row['Last Updated'])
                with col11:
                    # Create unique key using plan name, role, and owner to avoid duplicates
                    unique_view_key = f"view_{row['Plan Name']}_{row['Role']}_{row['Owner']}_{idx}".replace(" ", "_").replace("-", "_")
                    if st.button("👁️", key=unique_view_key, help="View Details"):
                        st.session_state.view_supply_plan = {
                            'plan_name': row['Plan Name'],
                            'owner': row['Owner'],
                            'role': row['Role'],
                            'client': row['Client'],
                            'positions': row['# of Positions'],
                            'staffed_date': row['To be Staffed by Date'],
                            'start_date': row['Start Date'],
                            'created_date': row['Created Date']
                        }
                        st.rerun()
                with col12:
                    # Create unique key using plan name, role, and owner to avoid duplicates
                    unique_edit_key = f"edit_{row['Plan Name']}_{row['Role']}_{row['Owner']}_{idx}".replace(" ", "_").replace("-", "_")
                    if st.button("⚙️", key=unique_edit_key, help="Edit Plan"):
                        st.session_state.edit_supply_plan = {
                            'plan_name': row['Plan Name'],
                            'owner': row['Owner'],
                            'role': row['Role'],
                            'client': row['Client'],
                            'positions': row['# of Positions'],
                            'staffed_date': row['To be Staffed by Date'],
                            'start_date': row['Start Date'],
                            'created_date': row['Created Date']
                        }
                        st.rerun()
            
            # Export functionality
            if st.button("📥 Export Supply Management Data"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"supply_management_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
        else:
            st.info("📋 No supply pipeline plans found in the system.")
            st.markdown("Create staffing plans in the Pipeline Configuration tab to see them here.")
            
    except Exception as e:
        st.error(f"Error loading supply management data: {str(e)}")
        st.info("Please check your database connection and try again.")
    
    # Handle View Details Modal
    if 'view_supply_plan' in st.session_state:
        view_plan_details_modal()
    
    # Handle Edit Plan Modal  
    if 'edit_supply_plan' in st.session_state:
        edit_plan_modal()

def view_plan_details_modal():
    """Modal to show detailed pipeline plan information"""
    plan = st.session_state.view_supply_plan
    
    st.markdown("---")
    st.subheader(f"📋 Pipeline Plan Details: {plan['role']} - {plan['owner']}")
    
    # Basic plan information
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**Plan Name:** {plan['plan_name']}")
        st.markdown(f"**Owner:** {plan['owner']}")
        st.markdown(f"**Role:** {plan['role']}")
        st.markdown(f"**Client:** {plan['client']}")
    
    with col2:
        st.markdown(f"**# of Positions:** {plan['positions']}")
        st.markdown(f"**To be Staffed by:** {plan['staffed_date']}")
        st.markdown(f"**Start Date:** {plan['start_date']}")
        st.markdown(f"**Created Date:** {plan['created_date']}")
    
    # Get detailed pipeline data from database
    try:
        import psycopg2
        import pandas as pd
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        # Query for detailed pipeline stages
        staffing_plans_table = env_manager.get_table_name('staffing_plans')
        query = f"""
        SELECT generated_data 
        FROM {env_manager.get_table_name('staffing_plan_generated_plans')} spgp
        JOIN {staffing_plans_table} sp ON spgp.plan_id = sp.id
        WHERE sp.plan_name = %s AND spgp.role = %s
        """
        
        cursor.execute(query, (plan['plan_name'], plan['role']))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            import json
            stages = result[0] if isinstance(result[0], list) else json.loads(result[0])
            
            st.markdown("---")
            st.markdown("**Pipeline Stages:**")
            
            # Get actual candidate data for this plan/role/owner combination from both data sources
            def get_stage_actual_candidates(stage_name, plan_name, role, owner):
                """Get actual candidates in a specific stage from both unified and candidate data"""
                total_candidates = 0
                
                try:
                    conn2 = psycopg2.connect(os.environ['DATABASE_URL'])
                    cursor2 = conn2.cursor()
                    
                    # Query 1: Get candidates from candidate_data table
                    candidate_data_table = env_manager.get_table_name('candidate_data')
                    cursor2.execute(f"""
                        SELECT COUNT(DISTINCT cd.id)
                        FROM {candidate_data_table} cd
                        JOIN {env_manager.get_table_name('staffing_plans')} sp ON cd.staffing_plan_id = sp.id
                        WHERE sp.plan_name = %s 
                        AND cd.staffing_role = %s 
                        AND cd.staffing_owner = %s
                        AND cd.status = %s
                    """, (plan_name, role, owner, stage_name))
                    
                    candidate_count = cursor2.fetchone()[0] or 0
                    total_candidates += candidate_count
                    
                    # Query 2: Get candidates from talent_supply table (check if has matching fields)
                    talent_supply_table = env_manager.get_table_name('talent_supply')
                    cursor2.execute(f"""
                        SELECT COUNT(*) as candidate_count
                        FROM {talent_supply_table} ts
                        WHERE ts.role = %s 
                        AND ts.assigned_to = %s
                        AND COALESCE(ts.sub_status, ts.assignment_status) = %s
                    """, (role, owner, stage_name))
                    
                    unified_count = cursor2.fetchone()[0] or 0
                    total_candidates += unified_count
                    
                    # Note: demand_supply_assignments table doesn't contain role/owner/status fields needed for pipeline tracking
                    # This table is used for assignment tracking, not candidate pipeline status
                    # Pipeline status is tracked in candidate_data and talent_supply tables
                    
                    conn2.close()
                    return total_candidates
                    
                except Exception as e:
                    print(f"Error getting stage candidates for {stage_name}: {e}")
                    return 0
            
            # Create a DataFrame for pipeline stages
            stage_data = []
            for stage in stages:
                stage_name = stage.get('stage_name', '')
                
                stage_data.append({
                    'Stage': stage_name,
                    'Profiles Planned': stage.get('profiles_converted', 0),
                    'Planned Conversion Rate': f"{stage.get('conversion_rate', 0)}%",
                    'Planned TAT': stage.get('tat_days', 0),
                    'Needed by Date': stage.get('needed_by_date', '')
                })
            
            # Add Special Stages to the display
            special_stages = ["Dropped", "On Hold", "Rejected"]
            for special_stage in special_stages:
                # Add special stage with 0 values for planned data
                stage_data.append({
                    'Stage': special_stage,
                    'Profiles Planned': 0,
                    'Planned Conversion Rate': "0%",
                    'Planned TAT': 0,
                    'Needed by Date': ""
                })
            
            if stage_data:
                stage_df = pd.DataFrame(stage_data)
                st.dataframe(stage_df, use_container_width=True, hide_index=True)
        
    except Exception as e:
        st.error(f"Error loading pipeline details: {str(e)}")
    
    # Close button
    if st.button("❌ Close Details", key="close_view_details"):
        del st.session_state.view_supply_plan
        st.rerun()

def edit_plan_modal():
    """Modal to edit pipeline plan information with full pipeline table"""
    plan = st.session_state.edit_supply_plan
    
    st.markdown("---")
    st.subheader(f"⚙️ Edit Pipeline Plan: {plan['role']} - {plan['owner']}")
    
    # Get the full pipeline data from database - filter by plan, role AND owner
    try:
        import psycopg2
        import pandas as pd
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        # Query for detailed pipeline stages - more specific filtering
        query = """
        SELECT spgp.generated_data, sp.id as plan_id, sp.plan_name, spgp.role, spgp.pipeline_owner
        FROM staffing_plan_generated_plans spgp
        JOIN f"{env_manager.get_table_name('staffing_plans')}" sp ON spgp.plan_id = sp.id
        WHERE sp.plan_name = %s AND spgp.role = %s AND spgp.pipeline_owner = %s
        ORDER BY spgp.id DESC
        LIMIT 1
        """
        
        cursor.execute(query, (plan['plan_name'], plan['role'], plan['owner']))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            import json
            # result[0] is generated_data, result[1] is plan_id, etc.
            stages = result[0] if isinstance(result[0], list) else json.loads(result[0])
            plan_info = {
                'plan_id': result[1],
                'plan_name': result[2], 
                'role': result[3],
                'pipeline_owner': result[4]
            }
            
            # Display the full pipeline table like in Generated Pipeline Plans
            st.markdown(f"### 📋 Pipeline Plan for {plan['role']} - {plan['owner']}")
            
            # Add verification info to confirm correct data is loaded
            st.info(f"📋 **Plan Details**: {plan_info['plan_name']} | **Role**: {plan_info['role']} | **Owner**: {plan_info['pipeline_owner']}")
            
            # Create the pipeline table with editable fields
            if stages and len(stages) > 0:
                # Initialize session state for edits if not exists
                if 'pipeline_edits' not in st.session_state:
                    st.session_state.pipeline_edits = {}
                
                # Table headers
                col1, col2, col3, col4, col5 = st.columns([2.5, 1.5, 1.5, 2, 2])
                with col1:
                    st.markdown("**Stage**")
                with col2:
                    st.markdown("**# in Pipeline**")
                with col3:
                    st.markdown("**# Planned**")
                with col4:
                    st.markdown("**Planned Conversion Rate**")
                with col5:
                    st.markdown("**Needed By Date**")
                
                st.markdown("---")
                
                # Display each stage with editable fields
                for i, stage in enumerate(stages):
                    col1, col2, col3, col4, col5 = st.columns([2.5, 1.5, 1.5, 2, 2])
                    
                    stage_key = f"{plan['plan_name']}_{plan['role']}_{i}"
                    
                    with col1:
                        st.write(stage.get('stage_name', ''))
                    
                    with col2:
                        # Read-only display for # in Pipeline
                        pipeline_count = int(stage.get('profiles_in_pipeline', 0))
                        st.write(str(pipeline_count))
                    
                    with col3:
                        # Read-only display for # Planned
                        planned_count = int(stage.get('profiles_converted', 0))
                        st.write(str(planned_count))
                    
                    with col4:
                        # Read-only display for Planned Conversion Rate
                        conversion_rate = float(stage.get('conversion_rate', 0))
                        st.write(f"{conversion_rate:.1f}%")
                    
                    with col5:
                        # Read-only display for Needed By Date
                        needed_date = pd.to_datetime(stage.get('needed_by_date', plan['staffed_date'])).date()
                        st.write(needed_date.strftime('%Y-%m-%d'))
                

                
                # Summary row showing plan details
                st.markdown("---")
                col1, col2, col3, col4 = st.columns([2, 2, 2, 4])
                with col1:
                    st.markdown(f"**Role:** {plan['role']}")
                with col2:
                    # Get actual skills from the role, not from pipeline stage name
                    actual_skills = 'React'  # Default fallback
                    if plan['role']:
                        # Map roles to their primary skills
                        role_skills_mapping = {
                            'Frontend Dev': 'React, JavaScript, HTML, CSS',
                            'Frontedn Dev': 'React, JavaScript, HTML, CSS',  # Handle typo
                            'FE Dev': 'React, JavaScript, HTML, CSS',
                            'Backend Dev': 'Node.js, Python, SQL',
                            'BE Dev': 'Node.js, Python, SQL',
                            'Full Stack Dev': 'React, Node.js, JavaScript',
                            'TPM': 'Project Management, Agile, Technical Leadership',
                            'Data Scientist': 'Python, Machine Learning, SQL',
                            'DevOps': 'AWS, Docker, Kubernetes, CI/CD'
                        }
                        actual_skills = role_skills_mapping.get(plan['role'], actual_skills)
                    st.markdown(f"**Skills:** {actual_skills}")
                with col3:
                    st.markdown(f"**Owner:** {plan['owner']}")
                with col4:
                    final_stage = stages[-1] if stages else {}
                    target_hires = final_stage.get('profiles_converted', plan['positions'])
                    final_date = final_stage.get('needed_by_date', plan['staffed_date'])
                    st.markdown(f"**Target:** {target_hires} hires by {final_date}")
            
        else:
            st.error("No pipeline data found for this plan")
            st.warning(f"🔍 Searched for: Plan='{plan['plan_name']}', Role='{plan['role']}', Owner='{plan['owner']}'")
            
            # Debug: Show available pipeline data for troubleshooting
            with st.expander("🔧 Debug: Show Available Pipeline Data"):
                try:
                    import psycopg2
                    debug_conn = psycopg2.connect(os.environ['DATABASE_URL'])
                    debug_cursor = debug_conn.cursor()
                    debug_query = """
                    SELECT sp.plan_name, spgp.role, spgp.pipeline_owner, 
                           CASE WHEN spgp.generated_data IS NOT NULL THEN 'Yes' ELSE 'No' END as has_data
                    FROM staffing_plan_generated_plans spgp
                    JOIN f"{env_manager.get_table_name('staffing_plans')}" sp ON spgp.plan_id = sp.id
                    WHERE sp.plan_name LIKE %s OR spgp.role LIKE %s OR spgp.pipeline_owner LIKE %s
                    ORDER BY sp.plan_name, spgp.role
                    """
                    debug_cursor.execute(debug_query, (f"%{plan['plan_name']}%", f"%{plan['role']}%", f"%{plan['owner']}%"))
                    debug_results = debug_cursor.fetchall()
                    debug_conn.close()
                    
                    if debug_results:
                        import pandas as pd
                        debug_df = pd.DataFrame(debug_results, columns=['Plan Name', 'Role', 'Pipeline Owner', 'Has Data'])
                        st.dataframe(debug_df, use_container_width=True)
                    else:
                        st.write("No similar pipeline data found in database")
                        
                except Exception as debug_e:
                    st.write(f"Debug query failed: {str(debug_e)}")
            
    except Exception as e:
        st.error(f"Error loading pipeline data: {str(e)}")
    
    # Save and Cancel buttons at the bottom
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("💾 Save Changes", key="save_pipeline_edits", type="primary"):
            try:
                # Get the current pipeline data and update with edits
                import psycopg2
                conn = psycopg2.connect(os.environ['DATABASE_URL'])
                cursor = conn.cursor()
                
                # Get current generated_data with specific filtering
                query = """
                SELECT generated_data 
                FROM staffing_plan_generated_plans spgp
                JOIN f"{env_manager.get_table_name('staffing_plans')}" sp ON spgp.plan_id = sp.id
                WHERE sp.plan_name = %s AND spgp.role = %s AND spgp.pipeline_owner = %s
                """
                
                cursor.execute(query, (plan['plan_name'], plan['role'], plan['owner']))
                result = cursor.fetchone()
                
                if result and result[0]:
                    import json
                    stages = result[0] if isinstance(result[0], list) else json.loads(result[0])
                    
                    # Update stages with only actual profile values from session state
                    updated_stages = []
                    for i, stage in enumerate(stages):
                        stage_key = f"{plan['plan_name']}_{plan['role']}_{i}"
                        updated_stage = stage.copy()
                        
                        # Only update actual profiles since other fields are read-only
                        if 'pipeline_edits' in st.session_state and f"actual_{stage_key}" in st.session_state.pipeline_edits:
                            updated_stage['actual_profiles'] = st.session_state.pipeline_edits[f"actual_{stage_key}"]
                        
                        updated_stages.append(updated_stage)
                    
                    # Update the database with new generated_data using specific filtering
                    update_query = """
                    UPDATE staffing_plan_generated_plans 
                    SET generated_data = %s
                    FROM f"{env_manager.get_table_name('staffing_plans')}" sp 
                    WHERE staffing_plan_generated_plans.plan_id = sp.id 
                    AND sp.plan_name = %s 
                    AND staffing_plan_generated_plans.role = %s
                    AND staffing_plan_generated_plans.pipeline_owner = %s
                    """
                    
                    cursor.execute(update_query, (
                        json.dumps(updated_stages),
                        plan['plan_name'],
                        plan['role'],
                        plan['owner']
                    ))
                    
                    conn.commit()
                    conn.close()
                    
                    # Clear session state
                    if 'pipeline_edits' in st.session_state:
                        del st.session_state.pipeline_edits
                    
                    st.success("✅ Pipeline plan updated successfully!")
                    del st.session_state.edit_supply_plan
                    st.rerun()
                else:
                    st.error("No pipeline data found to update")
                    
            except Exception as e:
                st.error(f"Error updating plan: {str(e)}")
    
    with col2:
        if st.button("❌ Cancel", key="cancel_pipeline_edits"):
            # Clear any unsaved changes
            if 'pipeline_edits' in st.session_state:
                del st.session_state.pipeline_edits
            del st.session_state.edit_supply_plan
            st.rerun()

def supply_pipeline_management_section():
    """Supply Pipeline Management section with Candidate Pipeline Analytics Dashboard"""
    
    st.subheader("📊 Candidate Pipeline Analytics Dashboard")
    st.markdown("Real-time funnel visualization and performance metrics from original Google Sheets data")
    
    try:
        # Use environment-aware table routing for raw data
        env_manager = st.session_state.get('env_manager')
        if env_manager:
            dataaggregator_table = env_manager.get_table_name('dataaggregator')
            master_clients_table = env_manager.get_table_name('master_clients')
        else:
            # Fallback to production table names
            dataaggregator_table = 'dataaggregator'
            master_clients_table = 'master_clients'
        
        # Get candidate data from environment-appropriate table
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        
        # Get candidate count and last import sync info from raw dataaggregator table
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*), MAX(created_at) 
            FROM {dataaggregator_table}
            WHERE data LIKE '%Candidate name%'
            AND data NOT LIKE '%Candidate name%None%'
        """)
        total_count, last_sync = cursor.fetchone()
        
        # Get all data for processing in Python
        cursor.execute(f"SELECT data FROM {dataaggregator_table} WHERE data LIKE '%Candidate name%'")
        all_data = cursor.fetchall()
        
        # Extract unique values using Python
        unique_clients = set()
        unique_roles = set()
        unique_statuses = set()
        unique_sources = set()
        for row in all_data:
            data_str = row[0]
            # Extract client
            if "'Potential Client': '" in data_str:
                start = data_str.find("'Potential Client': '") + 21
                end = data_str.find("'", start)
                if end > start:
                    client = data_str[start:end]
                    if client and client != 'None':
                        unique_clients.add(client)
            
            # Extract role
            if "'Role': '" in data_str:
                start = data_str.find("'Role': '") + 9
                end = data_str.find("'", start)
                if end > start:
                    role = data_str[start:end]
                    if role and role != 'None':
                        unique_roles.add(role)
            
            # Extract status
            if "'Status': '" in data_str:
                start = data_str.find("'Status': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    status = data_str[start:end]
                    if status and status != 'None':
                        unique_statuses.add(status)
            
            # Extract source
            if "'Source': '" in data_str:
                start = data_str.find("'Source': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    source = data_str[start:end]
                    if source and source != 'None':
                        unique_sources.add(source)
        
        # Display header metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Candidates", total_count or 0)
        with col2:
            if last_sync:
                sync_date = last_sync.strftime("%Y-%m-%d %H:%M")
                st.markdown(f"""
                <div style="background-color: #262730; padding: 10px; border-radius: 6px;">
                    <div style="color: #FAFAFA; font-size: 0.75rem; margin-bottom: 4px;">Last Sync</div>
                    <div style="color: #FAFAFA; font-size: 0.9rem; font-weight: 600;">{sync_date}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.metric("Last Sync", "No data")
        with col3:
            # Use Python-extracted unique clients count
            client_count = len(unique_clients)
            st.metric("Active Clients", client_count or 0)
        with col4:
            # Use Python-extracted unique roles count
            role_count = len(unique_roles)
            st.metric("Active Roles", role_count or 0)
        
        st.markdown("---")
        
        # Filters section
        st.markdown("#### 🎯 Filters")
        filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(5)
        
        with filter_col1:
            # Use Python-extracted unique clients
            clients = ["All"] + sorted(list(unique_clients))
            selected_client = st.selectbox("Client", clients, key="pipeline_client_filter")
        
        with filter_col2:
            # Use Python-extracted unique roles
            roles = ["All"] + sorted(list(unique_roles))
            selected_role = st.selectbox("Role", roles, key="pipeline_role_filter")
        
        with filter_col3:
            # Use Python-extracted unique statuses
            statuses = ["All"] + sorted(list(unique_statuses))
            selected_status = st.selectbox("Status", statuses, key="pipeline_status_filter")
        
        with filter_col4:
            # Extract candidate names using Python
            candidate_names = set()
            for row in all_data:
                data_str = row[0]
                if "'Candidate name': '" in data_str:
                    start = data_str.find("'Candidate name': '") + 19
                    end = data_str.find("'", start)
                    if end > start:
                        name = data_str[start:end]
                        if name and name != 'None':
                            candidate_names.add(name)
            
            all_candidate_names = sorted(list(candidate_names))
            
            # Import and use streamlit-searchbox for autocomplete
            try:
                from streamlit_searchbox import st_searchbox
                
                def search_candidates(searchterm: str):
                    """Search function for candidates"""
                    if not searchterm:
                        return ["All"] + all_candidate_names[:20]  # Show first 20 when no search
                    
                    # Filter candidates that contain the search term (case-insensitive) - more flexible than startswith
                    filtered = [name for name in all_candidate_names 
                              if searchterm.lower() in name.lower()]
                    return ["All"] + filtered[:20]  # Limit to 20 results for performance
                
                selected_candidate_name = st_searchbox(
                    search_candidates,
                    placeholder="Type to search candidates...",
                    label="Candidate Name",
                    key="pipeline_candidate_name_filter",
                    default="All"
                )
                
                # Ensure we have a valid value
                if selected_candidate_name is None:
                    selected_candidate_name = "All"
                
            except ImportError:
                # Fallback to regular selectbox if streamlit-searchbox is not available
                candidate_options = ["All"] + all_candidate_names
                selected_candidate_name = st.selectbox(
                    "Candidate Name",
                    candidate_options,
                    key="pipeline_candidate_name_filter_fallback",
                    help="Select a candidate from the list"
                )
        
        with filter_col5:
            # Use Python-extracted unique sources
            sources = ["All"] + sorted(list(unique_sources))
            selected_source = st.selectbox("Source", sources, key="pipeline_source_filter")
        
        # Filter data using Python instead of complex SQL
        filtered_data = []
        for row in all_data:
            data_str = row[0]
            include_record = True
            
            # Extract values for filtering
            candidate_name = ""
            role = ""
            client = ""
            status = ""
            source = ""
            
            if "'Candidate name': '" in data_str:
                start = data_str.find("'Candidate name': '") + 19
                end = data_str.find("'", start)
                if end > start:
                    candidate_name = data_str[start:end]
            
            if "'Role': '" in data_str:
                start = data_str.find("'Role': '") + 9
                end = data_str.find("'", start)
                if end > start:
                    role = data_str[start:end]
            
            if "'Potential Client': '" in data_str:
                start = data_str.find("'Potential Client': '") + 21
                end = data_str.find("'", start)
                if end > start:
                    client = data_str[start:end]
            
            if "'Status': '" in data_str:
                start = data_str.find("'Status': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    status = data_str[start:end]
            
            if "'Source': '" in data_str:
                start = data_str.find("'Source': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    source = data_str[start:end]
            
            # Apply filters
            if selected_client != "All" and client != selected_client:
                include_record = False
            if selected_role != "All" and role != selected_role:
                include_record = False
            if selected_status != "All" and status != selected_status:
                include_record = False
            if selected_candidate_name != "All" and candidate_name != selected_candidate_name:
                include_record = False
            if selected_source != "All" and source != selected_source:
                include_record = False
            
            if include_record:
                filtered_data.append((candidate_name, role, client, status, source, data_str))
        
        # Create a simple where clause for compatibility (will be ignored by Python processing)
        where_clause = ""
        params = []
        
        # Debug: Show filtered data count
        st.info(f"📊 Showing {len(filtered_data)} candidates from {len(all_data)} total records")
        st.write(f"**Filters Applied:** Client: {selected_client}, Role: {selected_role}, Status: {selected_status}, Candidate: {selected_candidate_name}, Source: {selected_source}")
        
        st.markdown("---")
        
        # Editable candidate data view
        candidate_editable_data_view_aggregator(conn, where_clause, params)
        
        st.markdown("---")
        
        # Main analytics sections
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Pipeline Funnel", "🎯 Performance Metrics", "⏱️ Cycle Time Analysis", "🔍 Current Wait Times"])
        
        with tab1:
            candidate_pipeline_funnel_visualization(conn, where_clause, params)
        
        with tab2:
            candidate_performance_metrics(conn, where_clause, params)
        
        with tab3:
            candidate_cycle_time_analysis(conn, where_clause, params)
        
        with tab4:
            candidate_current_wait_times(conn, where_clause, params)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error loading candidate analytics: {str(e)}")
        st.info("Please ensure Google Sheets data is synced in Settings > Google Sheets")

def candidate_editable_data_view_aggregator(conn, where_clause, params):
    """Display view for candidate pipeline data - allows switching between raw and cleaned data"""
    
    cursor = conn.cursor()
    
    # Data source selector
    st.markdown("#### 📝 Candidate Data Viewer")
    
    data_source = st.selectbox(
        "Select Data Source:",
        ["Raw DataAggregator (Excel Import)", "Cleaned Candidate Data (Processed)"],
        key="candidate_data_source_selector",
        help="Choose between raw Google Sheets data or processed candidate data",
        index=0  # Default to Raw DataAggregator
    )
    
    if data_source == "Raw DataAggregator (Excel Import)":
        st.markdown("##### 📋 Raw Google Sheets Data (Exactly as imported from Google Sheets)")
        
        # Get environment-aware table name
        env_manager = st.session_state.get('env_manager')
        dataaggregator_table = env_manager.get_table_name('dataaggregator') if env_manager else 'dataaggregator'
        
        # Get all data and process with Python instead of SQL
        cursor.execute(f"SELECT id, data, created_at FROM {dataaggregator_table} WHERE data LIKE '%Candidate name%' ORDER BY created_at DESC")
        all_data = cursor.fetchall()
        
        if all_data:
            import pandas as pd
            
            # Process data with Python
            processed_data = []
            for row in all_data:
                record_id, data_str, created_at = row
                
                # Extract values using Python string parsing
                candidate_name = ""
                role = ""
                client = ""
                status = ""
                source = ""
                experience = ""
                location = ""
                email = ""
                contact = ""
                
                if "'Candidate name': '" in data_str:
                    start = data_str.find("'Candidate name': '") + 19
                    end = data_str.find("'", start)
                    if end > start:
                        candidate_name = data_str[start:end]
                
                if "'Role': '" in data_str:
                    start = data_str.find("'Role': '") + 9
                    end = data_str.find("'", start)
                    if end > start:
                        role = data_str[start:end]
                
                if "'Potential Client': '" in data_str:
                    start = data_str.find("'Potential Client': '") + 21
                    end = data_str.find("'", start)
                    if end > start:
                        client = data_str[start:end]
                
                if "'Status': '" in data_str:
                    start = data_str.find("'Status': '") + 11
                    end = data_str.find("'", start)
                    if end > start:
                        status = data_str[start:end]
                
                if "'Source': '" in data_str:
                    start = data_str.find("'Source': '") + 11
                    end = data_str.find("'", start)
                    if end > start:
                        source = data_str[start:end]
                
                if "'Experience': '" in data_str:
                    start = data_str.find("'Experience': '") + 14
                    end = data_str.find("'", start)
                    if end > start:
                        experience = data_str[start:end]
                
                if "'Location': '" in data_str:
                    start = data_str.find("'Location': '") + 13
                    end = data_str.find("'", start)
                    if end > start:
                        location = data_str[start:end]
                
                if "'Email ID': '" in data_str:
                    start = data_str.find("'Email ID': '") + 12
                    end = data_str.find("'", start)
                    if end > start:
                        email = data_str[start:end]
                
                if "'Contact number': '" in data_str:
                    start = data_str.find("'Contact number': '") + 19
                    end = data_str.find("'", start)
                    if end > start:
                        contact = data_str[start:end]
                
                processed_data.append([
                    record_id, candidate_name, role, client, status, source, 
                    experience, location, email, contact, created_at
                ])
            
            # Create DataFrame
            df = pd.DataFrame(processed_data, columns=[
                'ID', 'Candidate Name', 'Role', 'Potential Client', 'Status (Raw)', 'Source (Raw)', 
                'Experience', 'Location', 'Email', 'Contact', 'Import Date'
            ])
            
            # Fill empty values
            df = df.fillna('')
            df['Candidate Name'] = df['Candidate Name'].replace('', '(Name not provided)')
            
            # Format date
            df['Import Date'] = pd.to_datetime(df['Import Date']).dt.strftime('%Y-%m-%d %H:%M')
            
            st.info(f"Showing {len(df)} raw records exactly as imported from Google Sheets. No transformations applied.")
            
            st.dataframe(
                df,
                use_container_width=True,
                height=400,
                column_config={
                    "Status (Raw)": st.column_config.TextColumn(
                        "Status (Raw)",
                        help="Original status from Google Sheets (e.g., '09 - Staffed', '10 - GA - Screen Rejected')",
                        width="large"
                    ),
                    "Source (Raw)": st.column_config.TextColumn(
                        "Source (Raw)", 
                        help="Original vendor name from Google Sheets (e.g., 'CoffeeBeans', 'Gemberg')",
                        width="medium"
                    )
                }
            )
        else:
            st.warning("No raw data found in DataAggregator table. Please sync from Google Sheets.")
        
    else:  # Raw DataAggregator Data  
        st.markdown("##### 📋 Raw DataAggregator Data (Exactly as imported from Excel/Google Sheets)")
        
        # Get all data and process with Python instead of SQL
        cursor.execute(f"SELECT id, data, created_at FROM {dataaggregator_table} WHERE data LIKE '%Candidate name%' ORDER BY created_at DESC")
        all_data = cursor.fetchall()
        
        # Process data with Python
        processed_data = []
        for row in all_data:
            record_id, data_str, created_at = row
            
            # Extract values using Python string parsing
            candidate_name = ""
            role = ""
            client = ""
            status = ""
            source = ""
            experience = ""
            location = ""
            notes = ""
            notice_period = ""
            
            if "'Candidate name': '" in data_str:
                start = data_str.find("'Candidate name': '") + 19
                end = data_str.find("'", start)
                if end > start:
                    candidate_name = data_str[start:end]
            
            if "'Role': '" in data_str:
                start = data_str.find("'Role': '") + 9
                end = data_str.find("'", start)
                if end > start:
                    role = data_str[start:end]
            
            if "'Potential Client': '" in data_str:
                start = data_str.find("'Potential Client': '") + 21
                end = data_str.find("'", start)
                if end > start:
                    client = data_str[start:end]
            
            if "'Status': '" in data_str:
                start = data_str.find("'Status': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    status = data_str[start:end]
            
            if "'Source': '" in data_str:
                start = data_str.find("'Source': '") + 11
                end = data_str.find("'", start)
                if end > start:
                    source = data_str[start:end]
            
            if "'Experience': '" in data_str:
                start = data_str.find("'Experience': '") + 14
                end = data_str.find("'", start)
                if end > start:
                    experience = data_str[start:end]
            
            if "'Location': '" in data_str:
                start = data_str.find("'Location': '") + 13
                end = data_str.find("'", start)
                if end > start:
                    location = data_str[start:end]
            
            if "'Screening Notes': '" in data_str:
                start = data_str.find("'Screening Notes': '") + 20
                end = data_str.find("'", start)
                if end > start:
                    notes = data_str[start:end]
            
            if "'Notice period': '" in data_str:
                start = data_str.find("'Notice period': '") + 18
                end = data_str.find("'", start)
                if end > start:
                    notice_period = data_str[start:end]
            
            processed_data.append([
                record_id, candidate_name, role, client, status, source, 
                experience, location, notes, notice_period, created_at
            ])
        
        results = processed_data
        
        if results:
            import pandas as pd
            
            # Create DataFrame with display columns
            df = pd.DataFrame(results, columns=[
                'ID', 'Candidate Name', 'Role', 'Client', 'Status', 'Source', 
                'Experience', 'Location', 'Notes', 'Notice Period', 'Last Synced'
            ])
            
            # Fill empty candidate names with a placeholder for better UX
            df['Candidate Name'] = df['Candidate Name'].fillna('(Name not provided)')
            df['Candidate Name'] = df['Candidate Name'].replace('', '(Name not provided)')
            
            # Fill other empty fields
            df = df.fillna('')
            
            # Format the Last Synced column
            df['Last Synced'] = pd.to_datetime(df['Last Synced']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Display record count
            st.info(f"Showing {len(df)} raw candidate records from original Excel data (exactly as imported from Google Sheets).")
            
            # Create read-only data display
            st.dataframe(
                df,
                use_container_width=True,
                height=400,
                column_config={
                    "ID": st.column_config.NumberColumn(
                        "ID",
                        help="Unique record identifier",
                        width="small"
                    ),
                    "Candidate Name": st.column_config.TextColumn(
                        "Candidate Name",
                        help="Name of the candidate",
                        width="medium"
                    ),
                    "Role": st.column_config.TextColumn(
                        "Role",
                        help="Position/Role for the candidate",
                        width="medium"
                    ),
                    "Client": st.column_config.TextColumn(
                        "Client",
                        help="Target client organization",
                        width="medium"
                    ),
                    "Status": st.column_config.TextColumn(
                        "Status",
                        help="Current pipeline status",
                        width="large"
                    ),
                    "Source": st.column_config.TextColumn(
                        "Source",
                        help="Candidate source/channel",
                        width="medium"
                    ),
                    "Experience": st.column_config.TextColumn(
                        "Experience",
                        help="Years of experience",
                        width="small"
                    ),
                    "Location": st.column_config.TextColumn(
                        "Location",
                        help="Candidate location",
                        width="medium"
                    ),
                    "Notes": st.column_config.TextColumn(
                        "Notes",
                        help="Screening notes and comments",
                        width="large"
                    ),
                    "Notice Period": st.column_config.TextColumn(
                        "Notice Period",
                        help="Notice period duration",
                        width="small"
                    ),
                    "Last Synced": st.column_config.TextColumn(
                        "Last Synced",
                        help="Last sync timestamp",
                        width="medium"
                    )
                }
            )
            
            # Refresh data functionality
            if st.button("🔄 Refresh Data", key="refresh_aggregator_data"):
                st.rerun()
        
        else:
            st.info("No candidate data available for the selected filters")

def save_candidate_changes(conn, original_df, edited_df):
    """Save changes to candidate data"""
    
    try:
        cursor = conn.cursor()
        changes_saved = 0
        
        # Get env_manager from session state
        env_manager = st.session_state.get('env_manager')
        if not env_manager:
            st.error("Environment manager not found in session state")
            return
        
        # Compare and update changed records
        for idx, edited_row in edited_df.iterrows():
            original_row = original_df.iloc[idx]
            record_id = edited_row['ID']
            
            # Build updated data object
            updated_data = {
                'Candidate Name': edited_row['Candidate Name'],
                'Role': edited_row['Role'],
                'Potential Client': edited_row['Client'],
                'Status': edited_row['Status'],
                'Source': edited_row['Source'],
                'Experience': edited_row['Experience'],
                'Location': edited_row['Location'],
                'Screening Notes': edited_row['Notes'],
                'Notice Period': edited_row['Notice Period']
            }
            
            # Check if any field changed
            changed = False
            for field_key, new_value in updated_data.items():
                # Map display column names to data keys
                display_field = field_key
                if field_key == 'Potential Client':
                    display_field = 'Client'
                elif field_key == 'Screening Notes':
                    display_field = 'Notes'
                
                if str(new_value) != str(original_row[display_field]):
                    changed = True
                    break
            
            if changed:
                # Update candidate_data table directly for imported records
                update_fields = []
                update_values = []
                
                # Map fields to database columns
                field_mapping = {
                    'Candidate Name': 'candidate_name',
                    'Role': 'role', 
                    'Status': 'status',
                    'Source': 'source',
                    'Experience': 'experience_level',
                    'Location': 'location',
                    'Screening Notes': 'interview_feedback',
                    'Notice Period': 'notice_period_details'
                }
                
                for field_key, new_value in updated_data.items():
                    if field_key in field_mapping:
                        update_fields.append(f"{field_mapping[field_key]} = %s")
                        update_values.append(str(new_value).strip() if pd.notna(new_value) else None)
                
                if update_fields:
                    update_values.append(record_id)
                    candidate_data_table = env_manager.get_table_name('candidate_data')
                    
                    # Build the SQL query string first to avoid f-string issues
                    sql_query = f"UPDATE {candidate_data_table} SET {', '.join(update_fields)}, last_manual_edit = NOW() WHERE id = %s AND data_source = 'import'"
                    
                    cursor.execute(sql_query, update_values)
                    changes_saved += 1
        
        conn.commit()
        
        if changes_saved > 0:
            st.success(f"✅ Successfully saved {changes_saved} candidate record(s)")
            st.rerun()
        else:
            st.info("No changes detected to save")
            
    except Exception as e:
        conn.rollback()
        st.error(f"Error saving changes: {str(e)}")

def candidate_pipeline_funnel_visualization(conn, where_clause, params):
    """Funnel visualization showing conversion rates by role/client from original Google Sheets data"""
    
    st.markdown("#### 📊 Candidate Pipeline Funnel")
    
    cursor = conn.cursor()
    
    # Get pipeline stage data with counts from raw dataaggregator
    env_manager = st.session_state.get('env_manager')
    dataaggregator_table = env_manager.get_table_name('dataaggregator') if env_manager else 'dataaggregator'
    
    # Get all data and process with Python instead of SQL
    cursor.execute(f"SELECT data FROM {dataaggregator_table} WHERE data LIKE '%Candidate name%'")
    all_data = cursor.fetchall()
    
    # Process data with Python
    status_counts = {}
    for row in all_data:
        data_str = row[0]
        
        # Extract values using Python string parsing
        status = ""
        role = ""
        client = ""
        
        if "'Status': '" in data_str:
            start = data_str.find("'Status': '") + 11
            end = data_str.find("'", start)
            if end > start:
                status = data_str[start:end]
        
        if "'Role': '" in data_str:
            start = data_str.find("'Role': '") + 9
            end = data_str.find("'", start)
            if end > start:
                role = data_str[start:end]
        
        if "'Potential Client': '" in data_str:
            start = data_str.find("'Potential Client': '") + 21
            end = data_str.find("'", start)
            if end > start:
                client = data_str[start:end]
        
        # Count combinations
        key = (status, role, client)
        status_counts[key] = status_counts.get(key, 0) + 1
    
    # Convert to list format for DataFrame
    results = [(status, role, client, count) for (status, role, client), count in status_counts.items()]
    
    if results:
        # Create DataFrame for analysis
        import pandas as pd
        df = pd.DataFrame(results, columns=['Status', 'Role', 'Client', 'Count'])
        
        # Define pipeline stage mapping for funnel
        stage_mapping = {
            '01 - Profile received': 'Received',
            '02 - Profile Screening by GA': 'GA Screening',
            '03 - Profile Screening by Vendor': 'Vendor Screening',
            '09 - Staffed': 'Staffed',
            '10 - GA - Screen Rejected': 'GA Rejected',
            '12 - GA - Interview Rejected': 'Interview Rejected',
            '16 - Requirement on hold': 'On Hold',
            '19 - Candidate RNR/Dropped': 'Dropped',
            '20 - Internal Dropped': 'Internal Dropped'
        }
        
        # Map statuses to simplified stages
        df['Stage'] = df['Status'].map(stage_mapping).fillna(df['Status'])
        
        # Create funnel visualization
        stage_totals = df.groupby('Stage')['Count'].sum().sort_values(ascending=False)
        
        # Plotly funnel chart
        import plotly.graph_objects as go
        
        fig = go.Figure(go.Funnel(
            y = stage_totals.index,
            x = stage_totals.values,
            textposition = "inside",
            textinfo = "value+percent initial",
            opacity = 0.65,
            marker = {"color": ["deepskyblue", "lightsalmon", "tan", "teal", "silver"],
                     "line": {"width": [4, 2, 2, 3, 1, 1]}},
            connector = {"line": {"color": "royalblue", "dash": "dot", "width": 3}}
        ))
        
        fig.update_layout(
            title="Candidate Pipeline Funnel",
            font_size=12,
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Conversion rates table
        st.markdown("#### 📊 Stage Conversion Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performing combinations
            st.markdown("**Top Role-Client Combinations:**")
            top_combinations = df.groupby(['Role', 'Client'])['Count'].sum().sort_values(ascending=False).head(10)
            for (role, client), count in top_combinations.items():
                st.write(f"• **{role}** at **{client}**: {count} candidates")
        
        with col2:
            # Success rate analysis
            st.markdown("**Success Rates by Role:**")
            staffed_df = df[df['Stage'] == 'Staffed'].groupby('Role')['Count'].sum()
            total_df = df.groupby('Role')['Count'].sum()
            
            success_rates = ((staffed_df / total_df) * 100).sort_values(ascending=False).head(10)
            for role, rate in success_rates.items():
                if pd.notna(rate):
                    st.write(f"• **{role}**: {rate:.1f}% success rate")
    
    else:
        st.info("No data available for the selected filters")

def candidate_cycle_time_analysis(conn, where_clause, params):
    """Cycle time analysis showing stage transition times"""
    
    st.markdown("#### ⏱️ Cycle Time Analysis")
    
    try:
        analyzer = CycleTimeAnalyzer(conn)
        
        # Get performance summary metrics
        summary = analyzer.get_performance_summary()
        
        if summary:
            st.markdown("#### 📊 Pipeline Performance Overview")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Candidates", summary['total_candidates'])
            with col2:
                st.metric("Avg Current Wait", f"{summary['avg_current_wait_days']} days")
            with col3:
                st.metric("Avg Pipeline Time", f"{summary['avg_pipeline_days']} days")
            with col4:
                st.metric("Success Rate", f"{summary['success_rate']}%")
        
        # Get stage transition data
        transition_data = analyzer.get_stage_transition_data(where_clause, params)
        
        if not transition_data.empty:
            # Calculate cycle times
            cycle_times = analyzer.calculate_average_cycle_times(transition_data)
            
            if not cycle_times.empty:
                st.markdown("#### 🔄 Stage Transition Times")
                
                # Show cycle time visualization
                cycle_chart = analyzer.create_cycle_time_visualization(cycle_times)
                if cycle_chart:
                    st.plotly_chart(cycle_chart, use_container_width=True)
                
                # Show detailed cycle time table
                st.markdown("**Detailed Cycle Time Analysis:**")
                
                # Format the cycle times table for better display
                display_cycle = cycle_times[cycle_times['total_transitions'] >= 2].copy()
                display_cycle['transition'] = display_cycle['previous_stage'] + ' → ' + display_cycle['new_stage']
                display_cycle = display_cycle[['transition', 'avg_days', 'median_days', 'total_transitions', 'unique_candidates']]
                display_cycle.columns = ['Stage Transition', 'Avg Days', 'Median Days', 'Total Transitions', 'Candidates']
                
                st.dataframe(
                    display_cycle.head(15),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Insufficient data for cycle time analysis")
        else:
            st.info("No transition data available for the selected filters")
            
    except Exception as e:
        st.error(f"Error in cycle time analysis: {str(e)}")

def candidate_current_wait_times(conn, where_clause, params):
    """Current wait times and bottleneck analysis"""
    
    st.markdown("#### 🔍 Current Wait Times & Bottlenecks")
    
    try:
        analyzer = CycleTimeAnalyzer(conn)
        
        # Get current wait times
        wait_data = analyzer.get_current_stage_wait_times(where_clause, params)
        
        if not wait_data.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Candidates with Longest Wait Times:**")
                
                # Show top candidates with longest wait times
                longest_wait = wait_data.nlargest(15, 'days_in_current_stage')
                
                # Format for display
                display_wait = longest_wait[['candidate_name', 'role', 'client_name', 'current_status', 'days_in_current_stage']].copy()
                display_wait.columns = ['Candidate', 'Role', 'Client', 'Current Status', 'Days Waiting']
                
                st.dataframe(
                    display_wait,
                    use_container_width=True,
                    hide_index=True
                )
            
            with col2:
                st.markdown("**Wait Time Statistics:**")
                
                # Calculate wait time stats
                avg_wait = wait_data['days_in_current_stage'].mean()
                max_wait = wait_data['days_in_current_stage'].max()
                candidates_over_7_days = len(wait_data[wait_data['days_in_current_stage'] > 7])
                candidates_over_14_days = len(wait_data[wait_data['days_in_current_stage'] > 14])
                
                st.metric("Average Wait Time", f"{avg_wait:.1f} days")
                st.metric("Maximum Wait Time", f"{max_wait:.0f} days")
                st.metric("Over 7 Days", f"{candidates_over_7_days} candidates")
                st.metric("Over 14 Days", f"{candidates_over_14_days} candidates")
        
        # Get bottleneck analysis
        bottlenecks = analyzer.get_bottleneck_analysis()
        
        if not bottlenecks.empty:
            st.markdown("#### 🚧 Stage Bottleneck Analysis")
            
            # Create wait time heatmap if we have role data
            if not wait_data.empty:
                heatmap = analyzer.create_wait_time_heatmap(wait_data)
                if heatmap:
                    st.plotly_chart(heatmap, use_container_width=True)
            
            # Show bottleneck table
            st.markdown("**Stages with Highest Average Wait Times:**")
            
            # Format bottleneck data
            display_bottlenecks = bottlenecks.copy()
            display_bottlenecks.columns = ['Status', 'Active Candidates', 'Avg Wait Days', 'Max Wait Days', 'Min Wait Days']
            
            st.dataframe(
                display_bottlenecks.head(10),
                use_container_width=True,
                hide_index=True
            )
            
            # Highlight critical bottlenecks
            critical_bottlenecks = bottlenecks[bottlenecks['avg_wait_days'] > 10]
            if not critical_bottlenecks.empty:
                st.warning(f"⚠️ **Critical Bottlenecks Found**: {len(critical_bottlenecks)} stages have average wait times over 10 days")
        else:
            st.info("No active candidates found for bottleneck analysis")
            
    except Exception as e:
        st.error(f"Error in wait time analysis: {str(e)}")

def candidate_performance_metrics(conn, where_clause, params):
    """Performance metrics with actionable insights from original Google Sheets data"""
    
    st.markdown("#### 🎯 Performance Metrics")
    
    cursor = conn.cursor()
    
    # Role-wise performance analysis from raw dataaggregator  
    env_manager = st.session_state.get('env_manager')
    dataaggregator_table = env_manager.get_table_name('dataaggregator') if env_manager else 'dataaggregator'
    
    # Get all data and process with Python instead of SQL
    cursor.execute(f"SELECT data FROM {dataaggregator_table} WHERE data LIKE '%Candidate name%'")
    all_data = cursor.fetchall()
    
    # Process data with Python
    role_counts = {}
    for row in all_data:
        data_str = row[0]
        
        # Extract values using Python string parsing
        role = ""
        status = ""
        client = ""
        source = ""
        
        if "'Role': '" in data_str:
            start = data_str.find("'Role': '") + 9
            end = data_str.find("'", start)
            if end > start:
                role = data_str[start:end]
        
        if "'Status': '" in data_str:
            start = data_str.find("'Status': '") + 11
            end = data_str.find("'", start)
            if end > start:
                status = data_str[start:end]
        
        if "'Potential Client': '" in data_str:
            start = data_str.find("'Potential Client': '") + 21
            end = data_str.find("'", start)
            if end > start:
                client = data_str[start:end]
        
        if "'Source': '" in data_str:
            start = data_str.find("'Source': '") + 11
            end = data_str.find("'", start)
            if end > start:
                source = data_str[start:end]
        
        # Count combinations
        key = (role, status, client, source)
        role_counts[key] = role_counts.get(key, 0) + 1
    
    # Convert to list format for DataFrame
    results = [(role, status, client, source, count) for (role, status, client, source), count in role_counts.items()]
    
    if results:
        import pandas as pd
        df = pd.DataFrame(results, columns=['Role', 'Status', 'Client', 'Source', 'Count'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Role Performance Overview:**")
            
            # Calculate metrics by role
            role_metrics = []
            for role in df['Role'].unique():
                if pd.notna(role):
                    role_data = df[df['Role'] == role]
                    total = role_data['Count'].sum()
                    staffed = role_data[role_data['Status'] == '09 - Staffed']['Count'].sum()
                    rejected = role_data[role_data['Status'].str.contains('Rejected', na=False)]['Count'].sum()
                    
                    success_rate = (staffed / total * 100) if total > 0 else 0
                    rejection_rate = (rejected / total * 100) if total > 0 else 0
                    
                    role_metrics.append({
                        'Role': role,
                        'Total': total,
                        'Staffed': staffed,
                        'Success Rate': f"{success_rate:.1f}%",
                        'Rejection Rate': f"{rejection_rate:.1f}%"
                    })
            
            metrics_df = pd.DataFrame(role_metrics).sort_values('Total', ascending=False)
            st.dataframe(metrics_df.head(10), use_container_width=True)
        
        with col2:
            st.markdown("**Source Performance:**")
            
            # Source effectiveness
            source_metrics = []
            for source in df['Source'].unique():
                if pd.notna(source) and source.strip():
                    source_data = df[df['Source'] == source]
                    total = source_data['Count'].sum()
                    staffed = source_data[source_data['Status'] == '09 - Staffed']['Count'].sum()
                    
                    success_rate = (staffed / total * 100) if total > 0 else 0
                    
                    source_metrics.append({
                        'Source': source,
                        'Total Candidates': total,
                        'Staffed': staffed,
                        'Success Rate': f"{success_rate:.1f}%"
                    })
            
            source_df = pd.DataFrame(source_metrics).sort_values('Total Candidates', ascending=False)
            st.dataframe(source_df.head(8), use_container_width=True)
        
        # Client performance chart
        st.markdown("**Client Performance Comparison:**")
        
        client_performance = df.groupby(['Client', 'Status'])['Count'].sum().reset_index()
        client_totals = df.groupby('Client')['Count'].sum().sort_values(ascending=False)
        
        import plotly.express as px
        
        fig = px.bar(
            client_performance,
            x='Client',
            y='Count',
            color='Status',
            title="Candidate Volume by Client and Status",
            category_orders={'Client': client_totals.index.tolist()}
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("No performance data available for the selected filters")

def candidate_bottleneck_analysis(conn, where_clause, params):
    """Identify bottlenecks and provide actionable insights from original Google Sheets data"""
    
    st.markdown("#### 🔍 Bottleneck Analysis & Insights")
    
    cursor = conn.cursor()
    
    # Get all data and process with Python instead of SQL
    cursor.execute(f"SELECT data FROM dataaggregator WHERE data LIKE '%Candidate name%'")
    all_data = cursor.fetchall()
    
    # Process data with Python
    bottleneck_counts = {}
    for row in all_data:
        data_str = row[0]
        
        # Extract values using Python string parsing
        role = ""
        status = ""
        client = ""
        notes = ""
        
        if "'Role': '" in data_str:
            start = data_str.find("'Role': '") + 9
            end = data_str.find("'", start)
            if end > start:
                role = data_str[start:end]
        
        if "'Status': '" in data_str:
            start = data_str.find("'Status': '") + 11
            end = data_str.find("'", start)
            if end > start:
                status = data_str[start:end]
        
        if "'Potential Client': '" in data_str:
            start = data_str.find("'Potential Client': '") + 21
            end = data_str.find("'", start)
            if end > start:
                client = data_str[start:end]
        
        if "'Screening Notes': '" in data_str:
            start = data_str.find("'Screening Notes': '") + 20
            end = data_str.find("'", start)
            if end > start:
                notes = data_str[start:end]
        
        # Count combinations
        key = (role, status, client, notes)
        bottleneck_counts[key] = bottleneck_counts.get(key, 0) + 1
    
    # Filter for bottlenecks with count >= 2 and convert to list format
    results = [(role, status, client, notes, count) for (role, status, client, notes), count in bottleneck_counts.items() if count >= 2]
    results.sort(key=lambda x: x[4], reverse=True)  # Sort by count descending
    results = results[:20]  # Limit to 20
    
    if results:
        import pandas as pd
        df = pd.DataFrame(results, columns=['Role', 'Status', 'Client', 'Notes', 'Count'])
        
        # Identify major bottlenecks
        st.markdown("#### 🚨 Major Bottlenecks Identified:")
        
        # High rejection categories
        rejection_bottlenecks = df[df['Status'].str.contains('Rejected', na=False)].sort_values('Count', ascending=False)
        
        if not rejection_bottlenecks.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Rejection Bottlenecks:**")
                for _, row in rejection_bottlenecks.head(5).iterrows():
                    st.error(f"🔴 **{row['Role']}** at **{row['Client']}**: {row['Count']} rejections")
                    if pd.notna(row['Notes']) and row['Notes'].strip():
                        st.caption(f"Common reason: {row['Notes'][:100]}...")
            
            with col2:
                # Generate actionable insights
                st.markdown("**🎯 Actionable Insights:**")
                
                top_bottleneck = rejection_bottlenecks.iloc[0]
                
                insights = [
                    f"**Priority Action**: Focus on {top_bottleneck['Role']} quality for {top_bottleneck['Client']}",
                    f"**Screen Enhancement**: Review screening criteria - {top_bottleneck['Count']} similar rejections",
                    "**Source Optimization**: Consider alternative talent sources",
                    "**Process Review**: Analyze rejection patterns for process improvements"
                ]
                
                for insight in insights:
                    st.info(insight)
        
        # Recommendations based on data patterns
        st.markdown("#### 💡 Strategic Recommendations:")
        
        # Calculate overall success rates by role-client combination
        success_analysis = []
        
        for (role, client), group in df.groupby(['Role', 'Client']):
            if pd.notna(role) and pd.notna(client):
                total = group['Count'].sum()
                staffed = group[group['Status'] == '09 - Staffed']['Count'].sum()
                rejected = group[group['Status'].str.contains('Rejected', na=False)]['Count'].sum()
                
                if total >= 5:  # Only analyze combinations with sufficient data
                    success_rate = (staffed / total * 100) if total > 0 else 0
                    
                    success_analysis.append({
                        'role': role,
                        'client': client,
                        'total': total,
                        'success_rate': success_rate,
                        'rejected': rejected
                    })
        
        if success_analysis:
            success_df = pd.DataFrame(success_analysis)
            
            # Best performing combinations
            best_performers = success_df.sort_values('success_rate', ascending=False).head(3)
            worst_performers = success_df.sort_values('success_rate', ascending=True).head(3)
            
            rec_col1, rec_col2 = st.columns(2)
            
            with rec_col1:
                st.success("**🌟 Best Performing Combinations:**")
                for _, row in best_performers.iterrows():
                    st.write(f"✅ **{row['role']}** at **{row['client']}**: {row['success_rate']:.1f}% success")
                    st.caption("Recommendation: Scale this successful pattern")
            
            with rec_col2:
                st.warning("**⚠️ Needs Improvement:**")
                for _, row in worst_performers.iterrows():
                    st.write(f"❌ **{row['role']}** at **{row['client']}**: {row['success_rate']:.1f}% success")
                    st.caption("Recommendation: Review process and requirements")
        
        # Overall pipeline health
        st.markdown("#### 📊 Pipeline Health Summary:")
        
        total_candidates = df['Count'].sum()
        total_rejections = df[df['Status'].str.contains('Rejected', na=False)]['Count'].sum()
        total_staffed = df[df['Status'] == '09 - Staffed']['Count'].sum()
        
        health_col1, health_col2, health_col3 = st.columns(3)
        
        with health_col1:
            rejection_rate = (total_rejections / total_candidates * 100) if total_candidates > 0 else 0
            if rejection_rate > 60:
                st.error(f"High Rejection Rate: {rejection_rate:.1f}%")
            elif rejection_rate > 40:
                st.warning(f"Moderate Rejection Rate: {rejection_rate:.1f}%")
            else:
                st.success(f"Healthy Rejection Rate: {rejection_rate:.1f}%")
        
        with health_col2:
            success_rate = (total_staffed / total_candidates * 100) if total_candidates > 0 else 0
            if success_rate > 20:
                st.success(f"Good Success Rate: {success_rate:.1f}%")
            elif success_rate > 10:
                st.warning(f"Average Success Rate: {success_rate:.1f}%")
            else:
                st.error(f"Low Success Rate: {success_rate:.1f}%")
        
        with health_col3:
            pipeline_efficiency = 100 - rejection_rate
            if pipeline_efficiency > 60:
                st.success(f"Pipeline Efficiency: {pipeline_efficiency:.1f}%")
            elif pipeline_efficiency > 40:
                st.warning(f"Pipeline Efficiency: {pipeline_efficiency:.1f}%")
            else:
                st.error(f"Pipeline Efficiency: {pipeline_efficiency:.1f}%")
    
    else:
        st.info("No bottleneck data available for the selected filters")

if __name__ == "__main__":
    main()
