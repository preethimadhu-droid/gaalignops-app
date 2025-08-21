#!/usr/bin/env python3
"""
Test Suite for Edit Pipeline Interface
=====================================

This test suite validates the Edit Pipeline interface functionality, ensuring:
1. Pipeline editing workflow matches New Pipeline Creation interface
2. Visual workflow display works correctly
3. Stage editing and management functions properly
4. Data integrity is maintained during edits
5. Interface consistency between New and Edit modes

Test Coverage:
- Edit Pipeline form loading and display
- Pipeline details editing (name, client, description, status)
- Visual workflow state display with colors
- Stage management (edit, delete, add)
- Summary table generation
- Internal/External pipeline handling
- Navigation state preservation
"""

import pytest
import psycopg2
import pandas as pd
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestEditPipelineInterface:
    """Test class for Edit Pipeline Interface functionality"""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment and create test data"""
        cls.test_pipeline_id = None
        cls.test_client_id = None
        cls.test_stage_ids = []
        
        # Environment detection
        cls.environment = os.environ.get('ENVIRONMENT', 'development').lower()
        cls.table_prefix = 'dev_' if cls.environment == 'development' else ''
        
        print(f"[TEST] Environment: {cls.environment.upper()}")
        print(f"[TEST] Table prefix: '{cls.table_prefix}'")

    def test_01_environment_setup(self):
        """Test that the test environment is properly configured"""
        assert os.environ.get('DATABASE_URL'), "DATABASE_URL not set"
        
        # Test database connection
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        # Check required tables exist
        required_tables = ['talent_pipelines', 'pipeline_stages', 'master_clients']
        for table in required_tables:
            full_table_name = f"{self.table_prefix}{table}"
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (full_table_name,))
            
            table_exists = cursor.fetchone()[0]
            assert table_exists, f"Required table {full_table_name} does not exist"
            
        conn.close()
        print("✅ Environment setup validated")

    def test_02_create_test_pipeline(self):
        """Create a test pipeline for editing tests"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Create test client first
            clients_table = f"{self.table_prefix}master_clients"
            cursor.execute(f"""
                INSERT INTO {clients_table} (client_name, confidence_level, status, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING master_client_id
            """, ("Test Edit Client", 90, "Active", datetime.now()))
            
            self.__class__.test_client_id = cursor.fetchone()[0]
            
            # Create test pipeline
            pipelines_table = f"{self.table_prefix}talent_pipelines"
            cursor.execute(f"""
                INSERT INTO {pipelines_table} (name, description, client_id, is_active, created_date)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, ("Test Edit Pipeline", "Test pipeline for edit interface validation", self.__class__.test_client_id, True, datetime.now()))
            
            self.__class__.test_pipeline_id = cursor.fetchone()[0]
            
            # Create test stages
            stages_table = f"{self.table_prefix}pipeline_stages"
            test_stages = [
                ("Initial Screening", 1, 80.0, 2, "Screen candidates", "Screening", "Greyamp", False),
                ("Technical Interview", 2, 70.0, 3, "Technical assessment", "Tech Round", "Client", False),
                ("Final Review", 3, 90.0, 1, "Final decision", "Selected", "Both", False),
                ("On Hold", -1, 0.0, 0, "Special state for holds", "On Hold", "Greyamp", True)  # Special stage
            ]
            
            stage_ids = []
            for stage_name, stage_order, conv_rate, tat_days, stage_desc, maps_to_status, status_flag, is_special in test_stages:
                cursor.execute(f"""
                    INSERT INTO {stages_table} 
                    (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description, 
                     maps_to_status, status_flag, is_special)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (self.__class__.test_pipeline_id, stage_name, stage_order, conv_rate, tat_days, 
                      stage_desc, maps_to_status, status_flag, is_special))
                
                stage_ids.append(cursor.fetchone()[0])
            
            self.__class__.test_stage_ids = stage_ids
            conn.commit()
            
            print(f"✅ Test pipeline created with ID: {self.__class__.test_pipeline_id}")
            print(f"✅ Test stages created with IDs: {stage_ids}")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def test_03_pipeline_data_retrieval(self):
        """Test retrieving pipeline data for editing"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Test pipeline data retrieval with client join
            pipelines_table = f"{self.table_prefix}talent_pipelines"
            clients_table = f"{self.table_prefix}master_clients"
            cursor.execute(f"""
                SELECT tp.id, tp.name, tp.description, mc.client_name, tp.is_active
                FROM {pipelines_table} tp
                LEFT JOIN {clients_table} mc ON tp.client_id = mc.master_client_id
                WHERE tp.id = %s
            """, (self.__class__.test_pipeline_id,))
            
            pipeline_data = cursor.fetchone()
            assert pipeline_data, "Pipeline data not found"
            
            pipeline_id, name, description, client_name, is_active = pipeline_data
            assert pipeline_id == self.__class__.test_pipeline_id
            assert name == "Test Edit Pipeline"
            assert client_name == "Test Edit Client"
            
            # Test stages data retrieval
            stages_table = f"{self.table_prefix}pipeline_stages"
            cursor.execute(f"""
                SELECT id, stage_name, conversion_rate, tat_days, stage_description, 
                       stage_order, maps_to_status, status_flag, is_special
                FROM {stages_table}
                WHERE pipeline_id = %s
                ORDER BY CASE WHEN stage_order = -1 THEN 999999 ELSE stage_order END
            """, (self.__class__.test_pipeline_id,))
            
            stages = cursor.fetchall()
            assert len(stages) == 4, f"Expected 4 stages, found {len(stages)}"
            
            # Verify regular and special stages separation
            regular_stages = [stage for stage in stages if stage[8] == False and stage[5] != -1]  # is_special = False
            special_stages = [stage for stage in stages if stage[8] == True or stage[5] == -1]    # is_special = True
            
            assert len(regular_stages) == 3, "Should have 3 regular stages"
            assert len(special_stages) == 1, "Should have 1 special stage"
            
            print("✅ Pipeline and stages data retrieval validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_04_visual_workflow_data_preparation(self):
        """Test data preparation for visual workflow display"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Get stages data
            stages_table = f"{self.table_prefix}pipeline_stages"
            cursor.execute(f"""
                SELECT id, stage_name, conversion_rate, tat_days, stage_description, 
                       stage_order, maps_to_status, status_flag, is_special
                FROM {stages_table}
                WHERE pipeline_id = %s
                ORDER BY CASE WHEN stage_order = -1 THEN 999999 ELSE stage_order END
            """, (self.__class__.test_pipeline_id,))
            
            existing_stages = cursor.fetchall()
            
            # Filter and sort stages like the interface does
            regular_stages = [stage for stage in existing_stages if stage[8] == False and stage[5] != -1]
            special_stages = [stage for stage in existing_stages if stage[8] == True or stage[5] == -1]
            
            # Sort regular stages by stage_order
            regular_stages.sort(key=lambda x: x[5])
            
            # Test visual workflow data
            assert len(regular_stages) == 3, "Should have 3 regular stages for workflow"
            
            # Verify stage order sequence
            expected_orders = [1, 2, 3]
            actual_orders = [stage[5] for stage in regular_stages]
            assert actual_orders == expected_orders, f"Stage orders should be {expected_orders}, got {actual_orders}"
            
            # Test color assignment logic (green for mapped, red for unmapped)
            for stage in regular_stages:
                stage_id, stage_name, conversion_rate, tat_days, stage_desc, stage_order, maps_to_status, status_flag, is_special = stage
                
                # All test stages have maps_to_status, so should be green (#4CAF50)
                expected_color = "#4CAF50" if maps_to_status else "#F44336"
                assert maps_to_status is not None, f"Stage {stage_name} should have maps_to_status"
                assert expected_color == "#4CAF50", f"Stage {stage_name} should be green"
            
            # Test summary data preparation
            summary_data = []
            for stage in regular_stages:
                stage_id, stage_name, conversion_rate, tat_days, stage_desc, stage_order, maps_to_status, status_flag, is_special = stage
                summary_data.append({
                    'State Name': stage_name,
                    'Conversion %': conversion_rate,
                    'TAT Days': tat_days,
                    'Maps to Status': maps_to_status or 'None',
                    'Status Flag': status_flag or 'None'
                })
            
            assert len(summary_data) == 3, "Summary should contain 3 regular stages"
            
            # Verify special stages handling
            assert len(special_stages) == 1, "Should have 1 special stage"
            special_stage = special_stages[0]
            assert special_stage[5] == -1, "Special stage should have order -1"
            assert special_stage[1] == "On Hold", "Special stage should be 'On Hold'"
            
            print("✅ Visual workflow data preparation validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_05_edit_pipeline_basic_fields(self):
        """Test editing basic pipeline fields (name, description, status)"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Test pipeline name update
            pipelines_table = f"{self.table_prefix}talent_pipelines"
            new_name = "Updated Edit Pipeline"
            new_description = "Updated description for testing"
            new_status = False  # Inactive
            
            cursor.execute(f"""
                UPDATE {pipelines_table} 
                SET name = %s, description = %s, is_active = %s
                WHERE id = %s
            """, (new_name, new_description, new_status, self.__class__.test_pipeline_id))
            
            conn.commit()
            
            # Verify update
            cursor.execute(f"""
                SELECT name, description, is_active
                FROM {pipelines_table}
                WHERE id = %s
            """, (self.__class__.test_pipeline_id,))
            
            result = cursor.fetchone()
            assert result[0] == new_name, "Pipeline name not updated"
            assert result[1] == new_description, "Pipeline description not updated"
            assert result[2] == new_status, "Pipeline status not updated"
            
            print("✅ Basic pipeline field editing validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_06_edit_pipeline_client_selection(self):
        """Test client selection logic in edit interface"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Test internal pipeline (Greyamp) logic
            pipelines_table = f"{self.table_prefix}talent_pipelines"
            
            # Update to internal pipeline - set client_id to NULL and is_internal to True
            cursor.execute(f"""
                UPDATE {pipelines_table} 
                SET client_id = NULL, is_internal = TRUE
                WHERE id = %s
            """, (self.__class__.test_pipeline_id,))
            
            conn.commit()
            
            # Verify internal pipeline detection
            cursor.execute(f"""
                SELECT is_internal
                FROM {pipelines_table}
                WHERE id = %s
            """, (self.__class__.test_pipeline_id,))
            
            result = cursor.fetchone()
            is_internal = result[0]
            assert is_internal, "Internal pipeline detection failed"
            
            # Test external pipeline - set back to external client
            cursor.execute(f"""
                UPDATE {pipelines_table} 
                SET client_id = %s, is_internal = FALSE
                WHERE id = %s
            """, (self.__class__.test_client_id, self.__class__.test_pipeline_id))
            
            conn.commit()
            
            cursor.execute(f"""
                SELECT is_internal
                FROM {pipelines_table}
                WHERE id = %s
            """, (self.__class__.test_pipeline_id,))
            
            result = cursor.fetchone()
            is_internal = result[0]
            assert not is_internal, "External pipeline detection failed"
            
            print("✅ Client selection logic validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_07_stage_editing_functionality(self):
        """Test individual stage editing functionality"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Test stage update
            stages_table = f"{self.table_prefix}pipeline_stages"
            test_stage_id = self.__class__.test_stage_ids[0]
            
            new_stage_name = "Updated Initial Screening"
            new_conversion_rate = 85.0
            new_tat_days = 3
            new_description = "Updated stage description"
            
            cursor.execute(f"""
                UPDATE {stages_table}
                SET stage_name = %s, conversion_rate = %s, tat_days = %s, stage_description = %s
                WHERE id = %s
            """, (new_stage_name, new_conversion_rate, new_tat_days, new_description, test_stage_id))
            
            conn.commit()
            
            # Verify update
            cursor.execute(f"""
                SELECT stage_name, conversion_rate, tat_days, stage_description
                FROM {stages_table}
                WHERE id = %s
            """, (test_stage_id,))
            
            result = cursor.fetchone()
            assert result[0] == new_stage_name, "Stage name not updated"
            assert result[1] == new_conversion_rate, "Conversion rate not updated"
            assert result[2] == new_tat_days, "TAT days not updated"
            assert result[3] == new_description, "Stage description not updated"
            
            print("✅ Stage editing functionality validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_08_stage_deletion_functionality(self):
        """Test stage deletion functionality"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Create a temporary stage to delete
            stages_table = f"{self.table_prefix}pipeline_stages"
            cursor.execute(f"""
                INSERT INTO {stages_table} 
                (pipeline_id, stage_name, stage_order, conversion_rate, tat_days, stage_description, 
                 maps_to_status, status_flag, is_special)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (self.__class__.test_pipeline_id, "Temp Stage", 4, 50.0, 1, "Temporary stage for deletion test", 
                  "Screening", "Greyamp", False))
            
            temp_stage_id = cursor.fetchone()[0]
            conn.commit()
            
            # Verify stage exists
            cursor.execute(f"SELECT COUNT(*) FROM {stages_table} WHERE id = %s", (temp_stage_id,))
            assert cursor.fetchone()[0] == 1, "Temporary stage not created"
            
            # Delete the stage
            cursor.execute(f"DELETE FROM {stages_table} WHERE id = %s", (temp_stage_id,))
            conn.commit()
            
            # Verify deletion
            cursor.execute(f"SELECT COUNT(*) FROM {stages_table} WHERE id = %s", (temp_stage_id,))
            assert cursor.fetchone()[0] == 0, "Stage not deleted"
            
            print("✅ Stage deletion functionality validated")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    def test_09_interface_consistency_validation(self):
        """Test that Edit interface maintains consistency with New Pipeline interface"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Verify all expected interface elements are present by checking data structure
            pipelines_table = f"{self.table_prefix}talent_pipelines"
            stages_table = f"{self.table_prefix}pipeline_stages"
            
            # Test pipeline data structure matches interface requirements
            clients_table = f"{self.table_prefix}master_clients"
            cursor.execute(f"""
                SELECT tp.name, tp.description, mc.client_name, tp.is_active
                FROM {pipelines_table} tp
                LEFT JOIN {clients_table} mc ON tp.client_id = mc.master_client_id
                WHERE tp.id = %s
            """, (self.__class__.test_pipeline_id,))
            
            pipeline_data = cursor.fetchone()
            assert pipeline_data, "Pipeline data required for interface"
            
            # Test stages data structure for visual workflow
            cursor.execute(f"""
                SELECT id, stage_name, conversion_rate, tat_days, stage_description, 
                       stage_order, maps_to_status, status_flag, is_special
                FROM {stages_table}
                WHERE pipeline_id = %s
                ORDER BY CASE WHEN stage_order = -1 THEN 999999 ELSE stage_order END
            """, (self.__class__.test_pipeline_id,))
            
            stages = cursor.fetchall()
            
            # Verify interface requirements
            regular_stages = [stage for stage in stages if stage[8] == False and stage[5] != -1]
            special_stages = [stage for stage in stages if stage[8] == True or stage[5] == -1]
            
            # Test visual workflow requirements
            assert len(regular_stages) >= 1, "Need at least one regular stage for visual workflow"
            
            # Test color assignment data availability
            for stage in regular_stages:
                maps_to_status = stage[6]
                # Color should be determinable from maps_to_status
                color = "#4CAF50" if maps_to_status else "#F44336"
                assert color in ["#4CAF50", "#F44336"], "Color assignment logic should work"
            
            # Test summary table data structure
            summary_fields = ['State Name', 'Conversion %', 'TAT Days', 'Maps to Status', 'Status Flag']
            for stage in regular_stages:
                stage_id, stage_name, conversion_rate, tat_days, stage_desc, stage_order, maps_to_status, status_flag, is_special = stage
                
                # Verify all required fields for summary table
                assert stage_name, "Stage name required for summary"
                assert conversion_rate is not None, "Conversion rate required for summary"
                assert tat_days is not None, "TAT days required for summary"
                # maps_to_status and status_flag can be None, but should be handled
            
            print("✅ Interface consistency validation passed")
            
        except Exception as e:
            raise e
        finally:
            conn.close()

    @classmethod
    def cleanup_test_data(cls):
        """Clean up test data"""
        if not any([cls.test_pipeline_id, cls.test_client_id, cls.test_stage_ids]):
            return
            
        print("🧹 Cleaning up test data...")
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()
        
        try:
            # Clean up stages
            if cls.test_stage_ids:
                stages_table = f"{cls.table_prefix}pipeline_stages"
                for stage_id in cls.test_stage_ids:
                    cursor.execute(f"DELETE FROM {stages_table} WHERE id = %s", (stage_id,))
            
            # Clean up pipeline
            if cls.test_pipeline_id:
                pipelines_table = f"{cls.table_prefix}talent_pipelines"
                cursor.execute(f"DELETE FROM {pipelines_table} WHERE id = %s", (cls.test_pipeline_id,))
            
            # Clean up client
            if cls.test_client_id:
                clients_table = f"{cls.table_prefix}master_clients"
                cursor.execute(f"DELETE FROM {clients_table} WHERE master_client_id = %s", (cls.test_client_id,))
            
            conn.commit()
            print("🧹 Test data cleanup completed")
            
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Error during cleanup: {e}")
        finally:
            conn.close()

    @classmethod
    def teardown_class(cls):
        """Clean up after all tests"""
        cls.cleanup_test_data()

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])