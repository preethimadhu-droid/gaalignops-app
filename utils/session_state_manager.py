
import streamlit as st
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SessionStateManager:
    """Manages session state to prevent data loss during auto-refresh"""

    @staticmethod
    def preserve_form_data(form_id: str, form_data: Dict[str, Any]):
        """Preserve form data in session state"""
        if 'preserved_forms' not in st.session_state:
            st.session_state.preserved_forms = {}

        st.session_state.preserved_forms[form_id] = {
            'data': form_data,
            'timestamp': datetime.now().isoformat(),
            'page': st.session_state.get('current_page', 'unknown')
        }

        logger.info(f"Form data preserved for {form_id}")

    @staticmethod
    def restore_form_data(form_id: str) -> Optional[Dict[str, Any]]:
        """Restore form data from session state"""
        preserved_forms = st.session_state.get('preserved_forms', {})

        if form_id in preserved_forms:
            form_data = preserved_forms[form_id]['data']
            logger.info(f"Form data restored for {form_id}")
            return form_data

        return None

    @staticmethod
    def clear_form_data(form_id: str):
        """Clear preserved form data after successful submission"""
        if 'preserved_forms' in st.session_state and form_id in st.session_state.preserved_forms:
            del st.session_state.preserved_forms[form_id]
            logger.info(f"Form data cleared for {form_id}")

    @staticmethod
    def auto_preserve_inputs():
        """Auto-preserve all current input values with enhanced tracking"""
        preserved_inputs = {}
        form_specific_data = {}

        # Preserve common input types
        input_keys = [key for key in st.session_state.keys() 
                     if not key.startswith('_') and 
                     not key.startswith('preserved_') and
                     not key.startswith('FormSubmitter:')]

        for key in input_keys:
            try:
                value = st.session_state[key]
                if value is not None and value != "":
                    preserved_inputs[key] = value
                    
                    # Capture specific form data patterns
                    if any(pattern in key.lower() for pattern in ['plan', 'client', 'target', 'role', 'pipeline']):
                        form_specific_data[key] = {
                            'value': value,
                            'type': type(value).__name__,
                            'timestamp': datetime.now().isoformat()
                        }
            except:
                continue

        if preserved_inputs:
            st.session_state.preserved_inputs = preserved_inputs
            st.session_state.preservation_timestamp = datetime.now().isoformat()
            
        if form_specific_data:
            st.session_state.preserved_form_specific = form_specific_data
            logger.info(f"Preserved {len(form_specific_data)} form-specific values")

    @staticmethod
    def restore_inputs():
        """Restore previously preserved inputs"""
        preserved_inputs = st.session_state.get('preserved_inputs', {})

        for key, value in preserved_inputs.items():
            if key not in st.session_state:
                try:
                    st.session_state[key] = value
                except:
                    continue

    @staticmethod
    def show_recovery_notice():
        """Show data recovery notice if unsaved data exists"""
        preserved_forms = st.session_state.get('preserved_forms', {})
        preserved_inputs = st.session_state.get('preserved_inputs', {})

        if preserved_forms or preserved_inputs:
            with st.expander("🔄 **Data Recovery Available**", expanded=False):
                if preserved_forms:
                    st.info(f"📝 Found {len(preserved_forms)} preserved forms:")
                    for form_id, form_info in preserved_forms.items():
                        st.write(f"- **{form_id}** (saved at {form_info['timestamp'][:16]})")

                if preserved_inputs:
                    st.info(f"💾 Found {len(preserved_inputs)} preserved inputs from {st.session_state.get('preservation_timestamp', 'unknown time')[:16]}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🔄 Restore All Data"):
                        SessionStateManager.restore_inputs()
                        st.success("✅ Data restored successfully!")
                        st.rerun()

                with col2:
                    if st.button("🗑️ Clear Recovery Data"):
                        for key in ['preserved_forms', 'preserved_inputs', 'preservation_timestamp']:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.success("🧹 Recovery data cleared!")
                        st.rerun()

    @staticmethod
    def prevent_refresh_during_edit():
        """Add JavaScript to prevent accidental refresh during form editing"""
        st.markdown("""
        <script>
        let formEditing = false;
        let autoSaveInterval;
        
        // Track form editing state
        document.addEventListener('DOMContentLoaded', function() {
            const inputs = document.querySelectorAll('input, select, textarea');
            inputs.forEach(input => {
                input.addEventListener('focus', function() {
                    formEditing = true;
                    startAutoSave();
                });
                
                input.addEventListener('blur', function() {
                    setTimeout(() => {
                        const activeElement = document.activeElement;
                        if (!activeElement || !['INPUT', 'SELECT', 'TEXTAREA'].includes(activeElement.tagName)) {
                            formEditing = false;
                            stopAutoSave();
                        }
                    }, 100);
                });
            });
        });
        
        // Auto-save mechanism
        function startAutoSave() {
            if (autoSaveInterval) return;
            
            autoSaveInterval = setInterval(() => {
                if (formEditing) {
                    saveFormData();
                }
            }, 3000); // Auto-save every 3 seconds
        }
        
        function stopAutoSave() {
            if (autoSaveInterval) {
                clearInterval(autoSaveInterval);
                autoSaveInterval = null;
            }
        }
        
        function saveFormData() {
            const formData = {};
            const inputs = document.querySelectorAll('input, select, textarea');
            inputs.forEach(input => {
                if (input.value && (input.name || input.id)) {
                    const key = input.name || input.id;
                    formData[key] = input.value;
                }
            });
            
            if (Object.keys(formData).length > 0) {
                sessionStorage.setItem('auto_saved_form_data', JSON.stringify({
                    data: formData,
                    timestamp: new Date().toISOString()
                }));
            }
        }
        
        // Prevent refresh during editing
        window.addEventListener('beforeunload', function(e) {
            if (formEditing) {
                saveFormData();
                e.preventDefault();
                e.returnValue = 'You have unsaved changes. Are you sure you want to leave?';
                return e.returnValue;
            }
        });
        
        // Restore data on page load
        window.addEventListener('load', function() {
            const savedData = sessionStorage.getItem('auto_saved_form_data');
            if (savedData) {
                try {
                    const parsed = JSON.parse(savedData);
                    const formData = parsed.data;
                    
                    // Wait a bit for Streamlit to render
                    setTimeout(() => {
                        Object.keys(formData).forEach(key => {
                            const input = document.querySelector(`[name="${key}"], #${key}`);
                            if (input && !input.value) {
                                input.value = formData[key];
                                input.dispatchEvent(new Event('change', { bubbles: true }));
                            }
                        });
                    }, 1000);
                } catch (e) {
                    console.error('Error restoring form data:', e);
                }
            }
        });
        </script>
        """, unsafe_allow_html=True)

    @staticmethod
    def clear_form_state(keys_to_clear: list, preserve_keys: list = None):
        """Clear form state while preserving specified keys"""
        preserve_keys = preserve_keys or []
        
        # Backup data we want to preserve
        backup_data = {}
        for key in preserve_keys:
            if key in st.session_state:
                backup_data[key] = st.session_state[key]
        
        # Clear specified keys
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        # Restore preserved data
        for key, value in backup_data.items():
            st.session_state[key] = value
    
    @staticmethod
    def create_stable_form(form_key: str, submit_text: str = "Submit"):
        """Create a form that preserves data across refreshes"""
        
        # Check for recovered data
        recovered_data = SessionStateManager.restore_form_data(form_key)
        if recovered_data:
            with st.expander("🔄 Data Recovery Available", expanded=True):
                st.info("Found unsaved data from previous session")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("📋 Restore Data", key=f"restore_{form_key}"):
                        for key, value in recovered_data.items():
                            st.session_state[key] = value
                        st.success("✅ Data restored!")
                        st.rerun()
                with col2:
                    if st.button("🗑️ Discard", key=f"discard_{form_key}"):
                        SessionStateManager.clear_form_data(form_key)
                        st.success("🧹 Recovery data cleared!")
                        st.rerun()
            
        # Add refresh prevention
        SessionStateManager.prevent_refresh_during_edit()
        
        return st.form(form_key, clear_on_submit=False)
