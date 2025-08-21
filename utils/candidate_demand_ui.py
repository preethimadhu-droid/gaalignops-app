"""
UI Components for Candidate-Demand Mapping System
Provides Streamlit interfaces for managing automatic candidate mapping.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils.candidate_demand_mapper import CandidateDemandMapper, auto_map_on_status_change
import os

def show_candidate_demand_mapping_interface():
    """Show the main interface for candidate-demand mapping management"""
    
    st.markdown("### 🔗 Candidate-Demand Mapping System")
    st.markdown("Automatically map hired candidates to demand records and update financial tracking.")
    
    # Initialize mapper
    mapper = CandidateDemandMapper(os.environ.get('DATABASE_URL'))
    
    tab1, tab2, tab3 = st.tabs(["One-time Setup", "Client Analysis", "Manual Mapping"])
    
    with tab1:
        show_batch_processing_interface(mapper)
    
    with tab2:
        show_client_demand_analysis(mapper)
    
    with tab3:
        show_manual_mapping_interface(mapper)

def show_batch_processing_interface(mapper):
    """Interface for one-time batch processing of hired candidates"""
    
    st.markdown("#### 🚀 One-time Setup: Map All Hired Candidates")
    st.info("This will process all candidates with 'On Boarded' status and create demand-supply assignments.")
    
    # Client selection
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mc.master_client_id, mc.client_name, COUNT(c.id) as hired_count
            FROM master_clients mc
            LEFT JOIN dev_candidate_data c ON mc.master_client_id = c.hire_for_client_id 
                AND c.status = 'On Boarded'
            GROUP BY mc.master_client_id, mc.client_name
            HAVING COUNT(c.id) > 0
            ORDER BY hired_count DESC
        """)
        clients_with_hired = cursor.fetchall()
    finally:
        conn.close()
    
    if not clients_with_hired:
        st.warning("No clients found with hired candidates.")
        return
    
    col1, col2 = st.columns([2, 1])
    with col1:
        client_options = ["All Clients"] + [f"{row[1]} ({row[2]} hired)" for row in clients_with_hired]
        selected_client = st.selectbox("Select Client for Processing", client_options)
    
    with col2:
        if st.button("🔄 Process Candidates", type="primary"):
            # Determine client ID
            if selected_client == "All Clients":
                client_id = None
                st.info("Processing all clients...")
            else:
                # Extract client ID from selection
                selected_client_name = selected_client.split(" (")[0]
                client_id = next(row[0] for row in clients_with_hired if row[1] == selected_client_name)
                st.info(f"Processing {selected_client_name}...")
            
            # Process candidates
            with st.spinner("Processing hired candidates..."):
                results = mapper.batch_process_hired_candidates(client_id)
            
            # Show results
            st.markdown("#### 📊 Processing Results")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Processed", results['processed'])
            with col2:
                st.metric("Successful", results['successful'])
            with col3:
                st.metric("Failed", results['failed'])
            
            if results['details']:
                st.markdown("#### 📋 Detailed Results")
                df_results = pd.DataFrame(results['details'])
                if 'error' not in df_results.columns:
                    # Color code by success
                    def highlight_success(row):
                        if row['success']:
                            return ['background-color: #d4edda'] * len(row)
                        else:
                            return ['background-color: #f8d7da'] * len(row)
                    
                    styled_df = df_results.style.apply(highlight_success, axis=1)
                    st.dataframe(styled_df, use_container_width=True)
                else:
                    st.error(f"Processing error: {df_results.iloc[0]['error']}")

def show_client_demand_analysis(mapper):
    """Show analysis of client demand vs hired candidates"""
    
    st.markdown("#### 📈 Client Demand Analysis")
    st.markdown("Compare demand records with hired candidates to identify mapping opportunities.")
    
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        
        # Get demand vs hired analysis
        cursor.execute("""
            WITH demand_summary AS (
                SELECT 
                    client_id,
                    SUM(CASE WHEN metric_type = 'Booked' THEN value ELSE 0 END) as total_booked,
                    SUM(CASE WHEN metric_type = 'Billed' THEN value ELSE 0 END) as total_billed
                FROM unified_sales_data 
                GROUP BY client_id
            ),
            hired_summary AS (
                SELECT 
                    hire_for_client_id as client_id,
                    COUNT(*) as hired_count
                FROM dev_candidate_data 
                WHERE status = 'On Boarded'
                GROUP BY hire_for_client_id
            ),
            assignment_summary AS (
                SELECT 
                    client_id,
                    COUNT(*) as assigned_count
                FROM demand_supply_assignments
                WHERE status = 'Allocated'
                GROUP BY client_id
            )
            SELECT 
                mc.client_name,
                COALESCE(d.total_booked, 0) as booked,
                COALESCE(d.total_billed, 0) as billed,
                (COALESCE(d.total_booked, 0) - COALESCE(d.total_billed, 0)) as unfulfilled_demand,
                COALESCE(h.hired_count, 0) as hired_candidates,
                COALESCE(a.assigned_count, 0) as assigned_candidates,
                (COALESCE(h.hired_count, 0) - COALESCE(a.assigned_count, 0)) as unassigned_hired
            FROM master_clients mc
            LEFT JOIN demand_summary d ON mc.master_client_id = d.client_id
            LEFT JOIN hired_summary h ON mc.master_client_id = h.client_id
            LEFT JOIN assignment_summary a ON mc.master_client_id = a.client_id
            WHERE COALESCE(d.total_booked, 0) > 0 OR COALESCE(h.hired_count, 0) > 0
            ORDER BY unfulfilled_demand DESC, hired_candidates DESC
        """)
        
        analysis_data = cursor.fetchall()
        
    finally:
        conn.close()
    
    if analysis_data:
        df_analysis = pd.DataFrame(analysis_data, columns=[
            'Client', 'Booked', 'Billed', 'Unfulfilled Demand', 
            'Hired Candidates', 'Assigned Candidates', 'Unassigned Hired'
        ])
        
        # Highlight clients with mapping opportunities
        def highlight_opportunities(row):
            colors = [''] * len(row)
            if row['Unassigned Hired'] > 0 and row['Unfulfilled Demand'] > 0:
                colors = ['background-color: #fff3cd'] * len(row)  # Yellow for opportunities
            elif row['Unassigned Hired'] > 0:
                colors = ['background-color: #d1ecf1'] * len(row)  # Blue for unassigned
            return colors
        
        styled_df = df_analysis.style.apply(highlight_opportunities, axis=1)
        st.dataframe(styled_df, use_container_width=True)
        
        # Summary metrics
        st.markdown("#### 🎯 Summary Metrics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_unfulfilled = df_analysis['Unfulfilled Demand'].sum()
            st.metric("Total Unfulfilled Demand", int(total_unfulfilled))
        
        with col2:
            total_hired = df_analysis['Hired Candidates'].sum()
            st.metric("Total Hired Candidates", int(total_hired))
        
        with col3:
            total_assigned = df_analysis['Assigned Candidates'].sum()
            st.metric("Total Assigned", int(total_assigned))
        
        with col4:
            total_unassigned = df_analysis['Unassigned Hired'].sum()
            st.metric("Unassigned Hired", int(total_unassigned))
    
    else:
        st.info("No demand or hired candidate data found.")

def show_manual_mapping_interface(mapper):
    """Interface for manual candidate mapping"""
    
    st.markdown("#### ✋ Manual Mapping")
    st.markdown("Manually map specific hired candidates to demand records.")
    
    # Get hired candidates not yet assigned
    import psycopg2
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.id, c.candidate_name, c.role, mc.client_name, c.hire_for_client_id
            FROM dev_candidate_data c
            JOIN master_clients mc ON c.hire_for_client_id = mc.master_client_id
            LEFT JOIN demand_supply_assignments dsa ON c.id = dsa.talent_id 
                AND dsa.status = 'Allocated'
            WHERE c.status = 'On Boarded' AND dsa.id IS NULL
            ORDER BY c.created_date DESC
        """)
        unassigned_candidates = cursor.fetchall()
    finally:
        conn.close()
    
    if not unassigned_candidates:
        st.success("All hired candidates are already assigned to demand records!")
        return
    
    # Select candidate for manual mapping
    candidate_options = [
        f"{row[1]} ({row[3]}) - {row[2] or 'No Role'}" 
        for row in unassigned_candidates
    ]
    
    selected_candidate = st.selectbox("Select Candidate to Map", [""] + candidate_options)
    
    if selected_candidate:
        # Extract candidate info
        candidate_index = candidate_options.index(selected_candidate)
        candidate_data = unassigned_candidates[candidate_index]
        candidate_id, candidate_name, role, client_name, client_id = candidate_data
        
        st.info(f"Mapping: **{candidate_name}** ({role or 'No Role'}) → **{client_name}**")
        
        # Show available demand for this client
        demand_records = mapper.find_matching_demand_records(client_id, role)
        
        if demand_records:
            st.markdown("**Available Demand Records:**")
            for i, record in enumerate(demand_records):
                st.write(f"• {record['account_name']} - {record['offering']} "
                        f"(Available: {record['available_positions']} positions)")
        else:
            st.warning(f"No unfulfilled demand found for {client_name}")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("🔗 Create Mapping", type="primary"):
                with st.spinner("Creating mapping..."):
                    result = mapper.process_hired_candidate(
                        candidate_id, client_id, candidate_name, role or ""
                    )
                
                if result['success']:
                    st.success(f"Successfully mapped {candidate_name}!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error(f"Mapping failed: {result['message']}")
        
        with col2:
            st.markdown("**Mapping Details:**")
            st.write("• Assignment: 100% allocated")
            st.write("• Status: Allocated")
            st.write("• Available: 0%")
            st.write("• Financial: Booked -1, Billed +1")