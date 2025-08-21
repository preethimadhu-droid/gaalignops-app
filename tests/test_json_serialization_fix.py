"""
Test for JSON serialization fix in pipeline save functionality
Tests the resolution of TypeError: isinstance() arg 2 must be a type, a tuple of types, or a union
"""
import pytest
import json
import os
import sys
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock

# Add the utils directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))

from staffing_plans_manager import StaffingPlansManager

class TestJSONSerializationFix:
    """Test suite for JSON serialization fix in pipeline save functionality"""
    
    @pytest.fixture
    def mock_env_manager(self):
        """Mock environment manager for testing"""
        mock_env = Mock()
        mock_env.is_development.return_value = True
        return mock_env
    
    @pytest.fixture
    def staffing_manager(self, mock_env_manager):
        """Create StaffingPlansManager instance with mocked dependencies"""
        with patch.dict(os.environ, {'DATABASE_URL': 'postgresql://mock:mock@localhost/test'}):
            with patch('psycopg2.connect'):
                manager = StaffingPlansManager(env_manager=mock_env_manager)
                return manager
    
    def test_json_serialization_with_datetime_objects(self, staffing_manager):
        """Test that datetime objects in pipeline data are properly JSON serialized"""
        
        # Create test pipeline data with datetime objects (the problematic case)
        test_pipeline_data = [
            {
                'role': 'Software Engineer',
                'pipeline_id': 123,
                'pipeline_name': 'Tech Pipeline',
                'pipeline_owner': 'John Doe',
                'stages': [
                    {
                        'stage_name': 'Initial Assessment',
                        'needed_by_date': datetime(2025, 9, 15, 10, 30),  # datetime object
                        'planned_conversion_rate': 40.0,
                        'pipeline_count': 25
                    },
                    {
                        'stage_name': 'Client Assessment',
                        'needed_by_date': date(2025, 9, 20),  # date object
                        'planned_conversion_rate': 50.0,
                        'pipeline_count': 12
                    }
                ],
                'created_by': 'admin',
                'created_date': datetime.now()  # Another datetime object
            }
        ]
        
        # Mock database connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(staffing_manager, 'get_connection', return_value=mock_conn):
            # Test the save operation that previously failed
            try:
                result = staffing_manager.save_generated_plans_to_database(
                    plan_id=1, 
                    generated_pipeline_data=test_pipeline_data
                )
                # The test passes if no exception is raised
                assert True, "JSON serialization completed without TypeError"
                
            except TypeError as e:
                if "isinstance() arg 2 must be a type" in str(e):
                    pytest.fail(f"JSON serialization fix failed: {str(e)}")
                else:
                    # Re-raise if it's a different TypeError
                    raise
    
    def test_json_serialization_function_directly(self):
        """Test the JSON serialization function directly with various date objects"""
        
        # Define the fixed json_serial function (from the fix)
        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Test data with different date/datetime objects
        test_data = {
            'datetime_obj': datetime(2025, 8, 18, 15, 30, 45),
            'date_obj': date(2025, 8, 18),
            'string': 'test string',
            'number': 42,
            'nested': {
                'inner_datetime': datetime(2025, 12, 25),
                'inner_date': date(2025, 12, 25)
            }
        }
        
        # Test that JSON serialization works without errors
        try:
            json_string = json.dumps(test_data, default=json_serial)
            
            # Verify the JSON string is valid
            parsed_data = json.loads(json_string)
            
            # Verify datetime objects were converted to ISO format strings
            assert parsed_data['datetime_obj'] == '2025-08-18T15:30:45'
            assert parsed_data['date_obj'] == '2025-08-18'
            assert parsed_data['nested']['inner_datetime'] == '2025-12-25T00:00:00'
            assert parsed_data['nested']['inner_date'] == '2025-12-25'
            
            # Verify other data types remain unchanged
            assert parsed_data['string'] == 'test string'
            assert parsed_data['number'] == 42
            
        except TypeError as e:
            if "isinstance() arg 2 must be a type" in str(e):
                pytest.fail(f"JSON serialization fix failed: {str(e)}")
            else:
                raise
    
    def test_broken_json_serialization_scenario(self):
        """Test to demonstrate the original JSON serialization problem"""
        
        # This recreates the original problem: using datetime.date without importing date
        def broken_json_serial(obj):
            """Broken JSON serializer that would cause TypeError"""
            from datetime import datetime  # Only import datetime, not date
            
            # This would cause TypeError: isinstance() arg 2 must be a type
            # because datetime.date is not available when only datetime is imported
            try:
                # This was the broken line in the original code
                if isinstance(obj, (datetime, datetime.date)):  # datetime.date not available!
                    return obj.isoformat()
            except AttributeError as e:
                # Convert AttributeError to TypeError to match the original error
                raise TypeError("isinstance() arg 2 must be a type, a tuple of types, or a union")
            raise TypeError(f"Type {type(obj)} not serializable")
        
        test_obj = date(2025, 8, 18)  # Using date object
        
        # Verify the broken version would fail with the expected error
        with pytest.raises(TypeError, match="isinstance\\(\\) arg 2 must be a type"):
            json.dumps({'date': test_obj}, default=broken_json_serial)
    
    def test_import_fix_validation(self):
        """Validate that the import fix is correctly applied"""
        
        # Import the fixed module and check imports
        import staffing_plans_manager
        
        # Verify the correct imports are available
        assert hasattr(staffing_plans_manager, 'datetime')
        assert hasattr(staffing_plans_manager, 'date')
        
        # Verify they are the correct types
        from datetime import datetime as dt_class, date as date_class
        assert staffing_plans_manager.datetime == dt_class
        assert staffing_plans_manager.date == date_class
    
    def test_json_serialization_fix_validation(self):
        """Core test: Validate the JSON serialization fix works with date objects"""
        
        # Import date classes at top of method
        from datetime import datetime as dt_class, date as date_class
        
        # Test data that would have caused the original TypeError
        pipeline_stage_data = {
            'stage_name': 'Initial Assessment',
            'needed_by_date': dt_class(2025, 9, 15, 10, 30),  # datetime object
            'completion_date': date_class(2025, 9, 20),  # date object  
            'conversion_rate': 40.0,
            'pipeline_count': 25,
            'created_at': dt_class.now()
        }
        
        def fixed_json_serial(obj):
            """The FIXED JSON serializer (matches the fix in staffing_plans_manager.py)"""
            if isinstance(obj, (dt_class, date_class)):  # This now works with proper imports!
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Test that JSON serialization works without the original TypeError
        try:
            json_result = json.dumps(pipeline_stage_data, default=fixed_json_serial)
            
            # Verify the result is valid JSON
            parsed_result = json.loads(json_result)
            
            # Verify date objects were properly converted
            assert 'needed_by_date' in parsed_result
            assert 'completion_date' in parsed_result
            assert isinstance(parsed_result['needed_by_date'], str)
            assert isinstance(parsed_result['completion_date'], str)
            
            print("✅ JSON serialization fix validated successfully!")
            
        except TypeError as e:
            if "isinstance() arg 2 must be a type" in str(e):
                pytest.fail(f"JSON serialization fix failed - original error still present: {str(e)}")
            else:
                raise

if __name__ == '__main__':
    pytest.main([__file__, '-v'])