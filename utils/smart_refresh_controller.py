import streamlit as st
from datetime import datetime, timedelta
from typing import Callable
import time
import logging

logger = logging.getLogger(__name__)

class SmartRefreshController:
    """Controls when page refreshes are necessary vs when they can be avoided"""

    def __init__(self):
        self.refresh_triggers = set()
        self.last_refresh = None
        self.pending_operations = []

    def register_refresh_trigger(self, trigger_name: str, condition: Callable = None):
        """Register a condition that requires a refresh"""
        self.refresh_triggers.add(trigger_name)
        if condition:
            st.session_state[f"refresh_condition_{trigger_name}"] = condition

    def needs_refresh(self, trigger_name: str = None) -> bool:
        """Check if a refresh is actually needed"""

        # Check if enough time has passed since last refresh
        if self.last_refresh:
            time_since_refresh = datetime.now() - self.last_refresh
            if time_since_refresh < timedelta(seconds=2):
                return False  # Too soon to refresh again

        # Check specific trigger conditions
        if trigger_name:
            condition_key = f"refresh_condition_{trigger_name}"
            if condition_key in st.session_state:
                condition = st.session_state[condition_key]
                if callable(condition):
                    return condition()

        # Check for pending operations that require refresh
        return len(self.pending_operations) > 0

    def controlled_rerun(self, reason: str = "User requested", force: bool = False):
        """Execute a controlled refresh only when necessary"""
        # Check if form is being edited
        if st.session_state.get('form_editing', False) and not force:
            logger.info(f"Refresh blocked - form editing in progress: {reason}")
            return False
            
        if force or self.needs_refresh():
            logger.info(f"Controlled refresh triggered: {reason}")
            self.last_refresh = datetime.now()
            st.rerun()
            return True
        else:
            logger.info(f"Refresh skipped - not needed: {reason}")
            return False

    def add_pending_operation(self, operation_name: str, data: dict = None):
        """Add a pending operation that requires refresh"""
        self.pending_operations.append({
            'name': operation_name,
            'data': data,
            'timestamp': datetime.now()
        })

    def clear_pending_operations(self):
        """Clear all pending operations"""
        self.pending_operations = []

    def show_refresh_control(self):
        """Show refresh control UI"""
        if self.pending_operations:
            with st.expander("🔄 Pending Updates", expanded=True):
                st.info(f"You have {len(self.pending_operations)} pending updates")
                for op in self.pending_operations:
                    st.write(f"• {op['name']} ({op['timestamp'].strftime('%H:%M:%S')})")

                if st.button("Apply Updates", type="primary"):
                    self.clear_pending_operations()
                    st.rerun()

# Global instance
smart_refresh = SmartRefreshController()

def controlled_rerun(reason: str = "User requested", force: bool = False):
    """Global function for controlled refresh"""
    smart_refresh.controlled_rerun(reason, force)

def add_pending_update(operation_name: str, data: dict = None):
    """Global function to add pending updates"""
    smart_refresh.add_pending_operation(operation_name, data)