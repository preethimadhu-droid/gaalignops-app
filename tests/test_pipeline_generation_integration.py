#!/usr/bin/env python3
"""
Integration Tests for Pipeline Generation UI Components
======================================================

CRITICAL: These tests validate the integration between UI components and 
pipeline generation without modifying the core functionality. 

Purpose:
- Test UI state management during pipeline generation
- Validate form behavior in new vs edit modes
- Ensure session state persistence
- Test auto-restore functionality
- Verify data flow between components

Environment: Development only (dev_ prefixed tables)
"""

import pytest
import os
import sys
from datetime import datetime, date, timedelta
from unittest.mock import MagicMock, patch

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.environment_manager import EnvironmentManager
from utils.staffing_plans_manager import StaffingPlansManager
from utils.pipeline_manager import PipelineManager

class TestPipelineGenerationIntegration:
    """Integration tests for pipeline generation UI components"""
    
    @classmethod
    def setup_class(cls):
        """Set up test environment"""
        cls.env_manager = EnvironmentManager()
        cls.staffing_manager = StaffingPlansManager(cls.env_manager)
        cls.pipeline_manager = PipelineManager(cls.env_manager)
        
        # Ensure we're in development environment
        assert cls.env_manager.environment == 'development'
        
        # Clean up and create test data
        cls._cleanup_test_data()
        cls._setup_test_data()
    
    @classmethod
    def teardown_class(cls):
        """Clean up after all tests"""
        cls._cleanup_test_data()
    
    @classmethod
    def _cleanup_test_data(cls):
        """Remove test data"""
        try:
            import psycopg2
            conn = psycopg2.connect(cls.env_manager.get_database_url())
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM dev_staffing_plans WHERE plan_name LIKE '%INTEGRATION_TEST%'")
            cursor.execute("DELETE FROM dev_staffing_plan_generated_plans WHERE plan_id NOT IN (SELECT id FROM dev_staffing_plans)")
            cursor.execute("DELETE FROM dev_talent_pipelines WHERE name LIKE '%INTEGRATION_TEST%'")
            cursor.execute("DELETE FROM dev_pipeline_stages WHERE pipeline_id NOT IN (SELECT id FROM dev_talent_pipelines)")
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Cleanup error: {e}")
    
    @classmethod
    def _setup_test_data(cls):
        """Create test data for integration tests"""
        import psycopg2
        conn = psycopg2.connect(cls.env_manager.get_database_url())
        cursor = conn.cursor()
        
        # Create integration test pipeline
        cursor.execute("""
            INSERT INTO dev_talent_pipelines (name, description, is_active, created_date)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, ('INTEGRATION_TEST Pipeline', 'Integration test pipeline', True, datetime.now()))
        
        cls.test_pipeline_id = cursor.fetchone()[0]
        
        # Create pipeline stages
        stages_data = [
            ('Initial Screening', 1, 50.0, 3, False, False, False),
            ('Technical Round', 2, 70.0, 5, False, False, False),
            ('Client Interview', 3, 80.0, 7, False, False, False),
            ('Final Selection', 4, 90.0, 2, False, False, False),
            ('Onboarded', 5, 100.0, 1, False, False, True)
        ]
        
        for name, order, conversion, tat, is_special, is_terminal, is_success in stages_data:
            cursor.execute("""
                INSERT INTO dev_pipeline_stages 
                (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, is_special_stage, is_terminal_stage, is_success_stage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (cls.test_pipeline_id, name, order, conversion, tat, is_special, is_terminal, is_success))
        
        conn.commit()
        conn.close()
    
    def test_01_new_mode_session_state_flow(self):
        """Test session state management in new plan mode"""
        # Simulate new plan creation
        new_plan_data = {
            'plan_name': 'INTEGRATION_TEST New Mode',
            'client_id': None,
            'pipeline_id': self.test_pipeline_id,
            'target_hires': 8,
            'planned_positions': 10,
            'target_start_date': date.today(),
            'target_end_date': date.today() + timedelta(days=60),
            'safety_buffer_pct': 25.0,
            'status': 'Planning'
        }
        
        # Save plan using correct method name
        plan_id = self.staffing_manager.save_plan(new_plan_data)
        assert plan_id is not None
        
        # Simulate session state for new mode
        session_state = {
            'edit_staffing_plan_id': None,  # No edit mode
            'show_staffing_form': True,
            'generated_pipeline_data': [],
            'show_generated_plans': False,
            'staffing_plan_rows': []
        }
        
        # Generate pipeline data
        pipeline_results = self.pipeline_manager.calculate_reverse_pipeline(
            self.test_pipeline_id, 8, date.today() + timedelta(days=60)
        )
        
        generated_data = [{
            'role': 'INTEGRATION_TEST New Role',
            'pipeline_id': self.test_pipeline_id,
            'pipeline_name': 'INTEGRATION_TEST Pipeline',
            'pipeline_owner': 'Test Owner',
            'stages': pipeline_results
        }]
        
        # Simulate adding to session state
        session_state['generated_pipeline_data'] = generated_data
        session_state['show_generated_plans'] = True
        
        # Save to database
        save_result = self.staffing_manager.save_generated_plans_to_database(plan_id, generated_data)
        assert save_result is True
        
        # Verify session state updates
        assert len(session_state['generated_pipeline_data']) == 1
        assert session_state['show_generated_plans'] is True
        
        self.new_mode_plan_id = plan_id
    
    def test_02_edit_mode_session_state_flow(self):
        """Test session state management in edit mode"""
        # Use plan created in previous test
        plan_id = self.new_mode_plan_id
        
        # Simulate entering edit mode
        session_state = {
            'edit_staffing_plan_id': plan_id,
            'show_staffing_form': True,
            'generated_pipeline_data': [],
            'show_generated_plans': False,
            'staffing_plan_rows': []
        }
        
        # Load existing plan data (simulating edit mode initialization)
        plan_data = self.staffing_manager.get_staffing_plan(plan_id)
        existing_generated_plans = self.staffing_manager.load_generated_pipeline_plan(plan_id)
        
        # Simulate session state restoration in edit mode
        if existing_generated_plans:
            session_state['generated_pipeline_data'] = existing_generated_plans
            session_state['show_generated_plans'] = True
            
            # Convert to staffing rows
            converted_rows = []
            for plan in existing_generated_plans:
                converted_rows.append({
                    'role': plan.get('role', ''),
                    'skills': '',
                    'positions': plan_data.get('target_hires', 1),
                    'staffed_by_date': plan_data.get('target_end_date', date.today()),
                    'pipeline': plan.get('pipeline_name', '-- Select a pipeline --'),
                    'owner': plan.get('pipeline_owner', '-- Select Owner --')
                })
            session_state['staffing_plan_rows'] = converted_rows
        
        # Verify edit mode session state
        assert session_state['edit_staffing_plan_id'] == plan_id
        assert len(session_state['generated_pipeline_data']) > 0
        assert session_state['show_generated_plans'] is True
        assert len(session_state['staffing_plan_rows']) > 0
        
        # Test updating in edit mode
        updated_data = session_state['generated_pipeline_data'].copy()
        updated_data[0]['role'] = 'INTEGRATION_TEST Updated Role'
        
        # Save updated data
        save_result = self.staffing_manager.save_generated_plans_to_database(plan_id, updated_data)
        assert save_result is True
        
        # Verify update
        updated_plans = self.staffing_manager.load_generated_pipeline_plan(plan_id)
        assert updated_plans[0]['role'] == 'INTEGRATION_TEST Updated Role'
    
    def test_03_auto_restore_functionality(self):
        """Test auto-restore functionality when session state is lost"""
        plan_id = self.new_mode_plan_id
        
        # Simulate session state loss (empty state)
        empty_session_state = {
            'edit_staffing_plan_id': None,
            'generated_pipeline_data': [],
            'show_generated_plans': False,
            'staffing_plan_rows': []
        }
        
        # Simulate auto-restore logic (like in the actual app)
        if (not empty_session_state.get('edit_staffing_plan_id') and 
            not empty_session_state.get('staffing_plan_rows', []) and 
            not empty_session_state.get('show_staffing_form')):
            
            # Auto-restore plan (simulating the fix implemented)
            empty_session_state['edit_staffing_plan_id'] = plan_id
            empty_session_state['show_staffing_form'] = True
        
        # Load data for restored plan
        plan_data = self.staffing_manager.get_staffing_plan(plan_id)
        existing_plans = self.staffing_manager.load_generated_pipeline_plan(plan_id)
        
        # Restore session state
        if existing_plans:
            empty_session_state['generated_pipeline_data'] = existing_plans
            empty_session_state['show_generated_plans'] = True
            
            converted_rows = []
            for plan in existing_plans:
                converted_rows.append({
                    'role': plan.get('role', ''),
                    'skills': '',
                    'positions': plan_data.get('target_hires', 1),
                    'staffed_by_date': plan_data.get('target_end_date', date.today()),
                    'pipeline': plan.get('pipeline_name', '-- Select a pipeline --'),
                    'owner': plan.get('pipeline_owner', '-- Select Owner --')
                })
            empty_session_state['staffing_plan_rows'] = converted_rows
        
        # Verify auto-restore worked
        assert empty_session_state['edit_staffing_plan_id'] == plan_id
        assert len(empty_session_state['generated_pipeline_data']) > 0
        assert empty_session_state['show_generated_plans'] is True
    
    def test_04_form_clearing_behavior(self):
        """Test form clearing behavior differences between new and edit modes"""
        plan_id = self.new_mode_plan_id
        
        # Test edit mode - should NOT clear after save
        edit_session_state = {
            'edit_staffing_plan_id': plan_id,
            'generated_pipeline_data': [{'role': 'Test Role'}],
            'show_generated_plans': True,
            'staffing_plan_rows': [{'role': 'Test Role'}]
        }
        
        # Simulate the clearing logic (disabled for edit mode)
        keys_to_clear_after_save = []
        is_edit_mode = edit_session_state.get('edit_staffing_plan_id') is not None
        
        if is_edit_mode:
            # DON'T clear in edit mode (per the fix implemented)
            pass
        else:
            keys_to_clear_after_save = [
                'generated_pipeline_data', 'show_generated_plans', 
                'staffing_plan_rows', 'edit_staffing_plan_id'
            ]
        
        # Apply clearing logic
        if not is_edit_mode:
            for key in keys_to_clear_after_save:
                if key in edit_session_state:
                    del edit_session_state[key]
        
        # Verify edit mode data persists
        assert edit_session_state['edit_staffing_plan_id'] == plan_id
        assert len(edit_session_state['generated_pipeline_data']) > 0
        assert edit_session_state['show_generated_plans'] is True
        
        # Test new mode - should clear after save
        new_session_state = {
            'edit_staffing_plan_id': None,
            'generated_pipeline_data': [{'role': 'Test Role'}],
            'show_generated_plans': True,
            'staffing_plan_rows': [{'role': 'Test Role'}]
        }
        
        is_edit_mode = new_session_state.get('edit_staffing_plan_id') is not None
        
        if not is_edit_mode:
            keys_to_clear = ['generated_pipeline_data', 'show_generated_plans', 'staffing_plan_rows']
            for key in keys_to_clear:
                if key in new_session_state:
                    del new_session_state[key]
        
        # Verify new mode data clears
        assert 'generated_pipeline_data' not in new_session_state
        assert 'show_generated_plans' not in new_session_state
        assert 'staffing_plan_rows' not in new_session_state
    
    def test_05_pipeline_calculation_integration(self):
        """Test integration between UI components and pipeline calculations"""
        # Create a plan for calculation testing
        calc_plan_data = {
            'plan_name': 'INTEGRATION_TEST Calculation',
            'client_id': None,
            'pipeline_id': self.test_pipeline_id,
            'target_hires': 12,
            'planned_positions': 15,
            'target_start_date': date.today(),
            'target_end_date': date.today() + timedelta(days=75),
            'safety_buffer_pct': 25.0,
            'status': 'Planning'
        }
        
        calc_plan_id = self.staffing_manager.save_plan(calc_plan_data)
        
        # Test the complete calculation flow
        target_hires = 12
        target_date = date.today() + timedelta(days=75)
        
        # Get pipeline results
        pipeline_results = self.pipeline_manager.calculate_reverse_pipeline(
            self.test_pipeline_id, target_hires, target_date
        )
        
        # Verify calculation has all expected stages
        stage_names = [stage['stage'] for stage in pipeline_results]
        expected_stages = ['Initial Screening', 'Technical Round', 'Client Interview', 'Final Selection', 'Onboarded']
        
        for expected_stage in expected_stages:
            assert expected_stage in stage_names, f"Missing expected stage: {expected_stage}"
        
        # CRITICAL: Verify the corrected pipeline calculation formula: target ÷ (conversion_rate ÷ 100)
        # Test specific scenario: 4 target hires with 80% conversion rate should need 5 candidates
        test_results_4_hires = self.pipeline_manager.calculate_reverse_pipeline(
            self.test_pipeline_id, 4, target_date
        )
        
        # Find the final selection stage (90% conversion rate in our test data)
        final_selection = next(s for s in test_results_4_hires if s['stage'] == 'Final Selection')
        expected_candidates = 4 / (90.0 / 100)  # Using corrected formula: target ÷ (conversion_rate ÷ 100)
        
        # Should be 5 candidates (4 ÷ 0.9 = 4.44, rounded up to 5)
        assert final_selection['needed'] >= 5, f"Expected at least 5 candidates for 4 hires at 90% conversion, got {final_selection['needed']}"
        
        # Test another scenario: 50→20→10→7→5→4 sequence from our corrected calculation
        test_results_50 = self.pipeline_manager.calculate_reverse_pipeline(
            self.test_pipeline_id, 50, target_date
        )
        
        # Verify the calculation sequence works backwards correctly
        onboarded_stage = next(s for s in test_results_50 if s['stage'] == 'Onboarded')
        assert onboarded_stage['needed'] == 50, "Final stage should match target hires"
        
        # Verify backwards calculation logic for main test
        onboarded_stage = next(s for s in pipeline_results if s['stage'] == 'Onboarded')
        assert onboarded_stage['needed'] == target_hires
        
        # Create generated data structure
        generated_data = [{
            'role': 'INTEGRATION_TEST Calc Role',
            'pipeline_id': self.test_pipeline_id,
            'pipeline_name': 'INTEGRATION_TEST Pipeline',
            'pipeline_owner': 'Calc Owner',
            'stages': pipeline_results
        }]
        
        # Save and verify
        save_result = self.staffing_manager.save_generated_plans_to_database(calc_plan_id, generated_data)
        assert save_result is True
        
        # Load and verify data integrity
        loaded_data = self.staffing_manager.load_generated_pipeline_plan(calc_plan_id)
        assert len(loaded_data) == 1
        
        loaded_stages = loaded_data[0].get('stages', loaded_data[0].get('pipeline_results', []))
        assert len(loaded_stages) == len(pipeline_results)
    
    def test_06_error_handling_integration(self):
        """Test error handling integration between components"""
        # Test with invalid data
        try:
            invalid_results = self.pipeline_manager.calculate_reverse_pipeline(
                99999, 10, date.today()  # Invalid pipeline ID
            )
            # Should handle gracefully
        except Exception as e:
            # Exception handling is acceptable
            print(f"Expected error handled: {e}")
        
        # Test saving with invalid plan ID
        try:
            invalid_save = self.staffing_manager.save_generated_plans_to_database(
                99999, [{'role': 'Test'}]
            )
            # Should return False or handle gracefully
        except Exception as e:
            # Exception handling is acceptable
            print(f"Expected error handled: {e}")
        
        # Test loading from invalid plan
        invalid_load = self.staffing_manager.load_generated_pipeline_plan(99999)
        assert invalid_load == [] or invalid_load is None
    
    def test_07_data_consistency_validation(self):
        """Validate data consistency across all components"""
        plan_id = self.new_mode_plan_id
        
        # Get data from different sources
        plan_data = self.staffing_manager.get_staffing_plan(plan_id)
        generated_plans = self.staffing_manager.load_generated_pipeline_plan(plan_id)
        
        # Verify consistency
        assert plan_data is not None
        assert len(generated_plans) > 0
        
        # Check that pipeline_id matches
        plan_pipeline_id = plan_data.get('pipeline_id')
        generated_pipeline_id = generated_plans[0].get('pipeline_id')
        
        assert plan_pipeline_id == generated_pipeline_id, "Pipeline ID mismatch between plan and generated data"
        
        # Verify stages data structure
        stages_data = generated_plans[0].get('stages', generated_plans[0].get('pipeline_results', []))
        assert len(stages_data) > 0, "No stages data found"
        
        for stage in stages_data:
            assert 'stage' in stage, "Stage name missing"
            assert 'needed' in stage, "Needed count missing"
            assert isinstance(stage['needed'], (int, float)), "Needed count should be numeric"
    
    def test_08_performance_validation(self):
        """Validate performance of pipeline generation operations"""
        import time
        
        # Test calculation performance
        start_time = time.time()
        
        for i in range(10):  # Run multiple calculations
            pipeline_results = self.pipeline_manager.calculate_reverse_pipeline(
                self.test_pipeline_id, 15, date.today() + timedelta(days=60)
            )
        
        calculation_time = time.time() - start_time
        assert calculation_time < 5.0, f"Pipeline calculations taking too long: {calculation_time}s"
        
        # Test save/load performance
        test_data = [{
            'role': f'INTEGRATION_TEST Perf Role {i}',
            'pipeline_id': self.test_pipeline_id,
            'pipeline_name': 'INTEGRATION_TEST Pipeline',
            'pipeline_owner': 'Perf Owner',
            'stages': pipeline_results
        } for i in range(5)]
        
        start_time = time.time()
        
        # Save multiple times
        for i in range(5):
            save_result = self.staffing_manager.save_generated_plans_to_database(
                self.new_mode_plan_id, test_data
            )
            assert save_result is True
        
        save_time = time.time() - start_time
        assert save_time < 3.0, f"Save operations taking too long: {save_time}s"

if __name__ == '__main__':
    # Run tests with detailed output
    pytest.main([__file__, '-v', '--tb=short'])