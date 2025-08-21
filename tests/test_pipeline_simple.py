#!/usr/bin/env python3
"""
Simple Pipeline Configuration Test
Tests basic pipeline functionality to ensure system stability
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.environment_manager import EnvironmentManager
from utils.pipeline_manager import PipelineManager

class TestPipelineSimple:
    """Simple pipeline tests"""
    
    def setup_method(self):
        """Setup test environment"""
        self.env_manager = EnvironmentManager()
        self.pipeline_manager = PipelineManager()
        
    def test_environment_detection(self):
        """Test environment is properly detected"""
        assert self.env_manager.environment == "development"
        
    def test_pipeline_manager_initialization(self):
        """Test pipeline manager can initialize"""
        assert self.pipeline_manager is not None
        
    def test_table_name_mapping(self):
        """Test table name mapping works"""
        table_name = self.env_manager.get_table_name('talent_pipelines')
        assert 'dev_talent_pipelines' in table_name or 'talent_pipelines' in table_name
        
    def test_basic_pipeline_query(self):
        """Test basic pipeline query works"""
        try:
            # Just test we can call the method without errors
            pipelines = self.pipeline_manager.get_client_pipelines(125)  # Piramal client_id
            assert isinstance(pipelines, list)
        except Exception as e:
            pytest.fail(f"Basic pipeline query failed: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])