"""
Onboarding Management UI

Streamlit interface for managing candidate onboarding automation
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import logging

from .candidate_onboarding_automation import CandidateOnboardingAutomation, auto_onboard_candidate, process_all_pending_onboarded
from .status_change_monitor import StatusChangeMonitor, setup_status_monitoring, process_pending_onboarding, get_recent_status_changes

logger = logging.getLogger(__name__)

def onboarding_automation_page():
    """Main page for onboarding automation management"""
    st.header("🔄 Candidate Onboarding Automation")
    
    # Initialize components
    automation = CandidateOnboardingAutomation()
    monitor = StatusChangeMonitor()
    
    # Create tabs for different functions
    tab1, tab2, tab3, tab4 = st.tabs([
        "🚀 Setup & Control", 
        "📋 Process Pending", 
        "📊 Status Changes", 
        "🔍 Manual Processing"
    ])
    
    with tab1:
        setup_and_control_section(monitor)
    
    with tab2:
        process_pending_section()
    
    with tab3:
        status_changes_section()
    
    with tab4:
        manual_processing_section(automation)

def setup_and_control_section(monitor):
    """Setup and control section"""
    st.subheader("🛠️ System Setup & Control")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Infrastructure Setup")
        if st.button("🔧 Setup Status Monitoring", type="primary"):
            with st.spinner("Setting up monitoring infrastructure..."):
                try:
                    success = setup_status_monitoring()
                    if success:
                        st.success("✅ Status monitoring infrastructure set up successfully!")
                        st.info("This creates:\n- Status change tracking table\n- Database triggers\n- Audit trail system")
                    else:
                        st.error("❌ Failed to set up monitoring infrastructure")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        st.markdown("#### System Information")
        st.info("""
        **Automation Workflow:**
        1. Candidate marked as "On Boarded"
        2. Auto-create talent record in Unified Talent Management
        3. Set appropriate FTE/NFTE type based on source
        4. Create demand-supply assignment
        5. Update availability and assignment status
        """)
    
    with col2:
        st.markdown("#### Processing Control")
        
        if st.button("▶️ Process All Pending"):
            with st.spinner("Processing all pending onboarded candidates..."):
                try:
                    results = process_pending_onboarding()
                    if results:
                        successful = sum(1 for r in results if r['success'])
                        total = len(results)
                        st.success(f"✅ Processed {successful}/{total} candidates successfully")
                        
                        if successful < total:
                            st.warning("Some candidates failed processing. Check details below.")
                            
                        # Show results
                        df_results = pd.DataFrame(results)
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info("No pending candidates to process")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        st.markdown("#### Batch Processing")
        if st.button("🔄 Process All Historical On-Boarded"):
            with st.spinner("Processing all historical on-boarded candidates..."):
                try:
                    results = process_all_pending_onboarded()
                    if results:
                        successful = sum(1 for r in results if r['success'])
                        total = len(results)
                        st.success(f"✅ Processed {successful}/{total} historical candidates")
                        
                        # Show results
                        df_results = pd.DataFrame(results)
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info("No historical candidates to process")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

def process_pending_section():
    """Process pending candidates section"""
    st.subheader("📋 Pending Candidates")
    
    try:
        # Get recent status changes
        recent_changes = get_recent_status_changes(hours=48)
        
        if recent_changes:
            # Filter for "On Boarded" status changes
            onboarded_changes = [c for c in recent_changes if c['new_status'] == 'On Boarded']
            
            if onboarded_changes:
                st.markdown(f"**Found {len(onboarded_changes)} recent 'On Boarded' status changes:**")
                
                # Create DataFrame for display
                df_changes = pd.DataFrame(onboarded_changes)
                df_changes['changed_at'] = pd.to_datetime(df_changes['changed_at']).dt.strftime('%Y-%m-%d %H:%M')
                
                # Color code based on processing status
                def color_processed(val):
                    return 'background-color: lightgreen' if val else 'background-color: lightcoral'
                
                styled_df = df_changes.style.applymap(color_processed, subset=['processed'])
                st.dataframe(styled_df, use_container_width=True)
                
                # Process unprocessed ones
                unprocessed = [c for c in onboarded_changes if not c['processed']]
                if unprocessed:
                    st.warning(f"⚠️ {len(unprocessed)} candidates need processing")
                    
                    if st.button("🚀 Process Unprocessed Candidates"):
                        with st.spinner("Processing unprocessed candidates..."):
                            results = process_pending_onboarding()
                            if results:
                                successful = sum(1 for r in results if r['success'])
                                st.success(f"✅ Processed {successful}/{len(results)} candidates")
                                st.rerun()
                else:
                    st.success("✅ All recent 'On Boarded' candidates have been processed")
            else:
                st.info("No recent 'On Boarded' status changes found")
        else:
            st.info("No recent status changes found")
            
    except Exception as e:
        st.error(f"Error loading pending candidates: {str(e)}")

def status_changes_section():
    """Status changes monitoring section"""
    st.subheader("📊 Status Change Activity")
    
    # Time range selector
    col1, col2 = st.columns(2)
    with col1:
        hours_back = st.selectbox("Time Range", [6, 12, 24, 48, 72, 168], index=2)
    with col2:
        if st.button("🔄 Refresh Data"):
            st.rerun()
    
    try:
        changes = get_recent_status_changes(hours=hours_back)
        
        if changes:
            st.markdown(f"**Status changes in the last {hours_back} hours:**")
            
            # Create summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_changes = len(changes)
                st.metric("Total Changes", total_changes)
            
            with col2:
                onboarded_changes = len([c for c in changes if c['new_status'] == 'On Boarded'])
                st.metric("On Boarded", onboarded_changes)
            
            with col3:
                processed_changes = len([c for c in changes if c['processed']])
                st.metric("Processed", processed_changes)
            
            with col4:
                success_rate = (processed_changes / total_changes * 100) if total_changes > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
            
            # Show detailed table
            df_changes = pd.DataFrame(changes)
            df_changes['changed_at'] = pd.to_datetime(df_changes['changed_at']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Filter options
            status_filter = st.selectbox(
                "Filter by New Status",
                options=["All"] + sorted(df_changes['new_status'].unique().tolist()),
                index=0
            )
            
            if status_filter != "All":
                df_changes = df_changes[df_changes['new_status'] == status_filter]
            
            st.dataframe(df_changes, use_container_width=True, height=400)
            
        else:
            st.info(f"No status changes found in the last {hours_back} hours")
            
    except Exception as e:
        st.error(f"Error loading status changes: {str(e)}")

def manual_processing_section(automation):
    """Manual processing section"""
    st.subheader("🔍 Manual Processing")
    
    st.markdown("#### Process Individual Candidate")
    
    # Candidate ID input
    candidate_id = st.number_input(
        "Candidate ID",
        min_value=1,
        value=1,
        help="Enter the ID of the candidate to process manually"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🚀 Process This Candidate", type="primary"):
            with st.spinner(f"Processing candidate {candidate_id}..."):
                try:
                    success = auto_onboard_candidate(candidate_id)
                    if success:
                        st.success(f"✅ Successfully processed candidate {candidate_id}")
                    else:
                        st.error(f"❌ Failed to process candidate {candidate_id}")
                        st.info("Possible reasons:\n- Candidate not found\n- Status is not 'On Boarded'\n- Already processed\n- Database error")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
    
    with col2:
        if st.button("🔍 Check Candidate Status"):
            try:
                conn = automation.get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        id, candidate_name, status, hire_for_client_id,
                        linked_talent_id, status_last_changed
                    FROM candidate_data 
                    WHERE id = %s
                """, (candidate_id,))
                
                result = cursor.fetchone()
                
                if result:
                    st.info(f"""
                    **Candidate Details:**
                    - Name: {result[1]}
                    - Status: {result[2]}
                    - Client ID: {result[3]}
                    - Linked Talent ID: {result[4] or 'Not linked'}
                    - Status Last Changed: {result[5] or 'Unknown'}
                    """)
                else:
                    st.warning(f"Candidate {candidate_id} not found")
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                st.error(f"Error checking candidate: {str(e)}")
    
    st.markdown("---")
    st.markdown("#### Bulk Processing Tools")
    
    if st.button("📊 Show Talent Management Statistics"):
        try:
            conn = automation.get_db_connection()
            cursor = conn.cursor()
            
            # Get statistics
            cursor.execute("SELECT COUNT(*) FROM talent_supply")
            total_talent = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM talent_supply WHERE assignment_status = 'Allocated'")
            allocated_talent = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM candidate_data WHERE status = 'On Boarded'")
            onboarded_candidates = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM candidate_data WHERE status = 'On Boarded' AND linked_talent_id IS NOT NULL")
            linked_candidates = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Talent", total_talent)
            with col2:
                st.metric("Allocated Talent", allocated_talent)
            with col3:
                st.metric("On-Boarded Candidates", onboarded_candidates)
            with col4:
                st.metric("Linked to Talent", linked_candidates)
            
            # Show unlinked candidates
            unlinked = onboarded_candidates - linked_candidates
            if unlinked > 0:
                st.warning(f"⚠️ {unlinked} on-boarded candidates are not yet linked to talent management")
                
        except Exception as e:
            st.error(f"Error getting statistics: {str(e)}")