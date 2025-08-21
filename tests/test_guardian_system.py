#!/usr/bin/env python3
"""
Guardian System Tests
Tests the guardian systems that prevent SQL template variable issues
"""

import pytest
import sys
import os
import subprocess

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

class TestGuardianSystem:
    """Guardian system tests"""
    
    def test_sql_template_guardian_exists(self):
        """Test SQL template guardian script exists"""
        assert os.path.exists('test_sql_template_guard.py')
        
    def test_sql_template_guardian_runs(self):
        """Test SQL template guardian can run"""
        result = subprocess.run([
            sys.executable, 'test_sql_template_guard.py'
        ], capture_output=True, text=True)
        
        # Guardian should pass (exit code 0) since we fixed the issues
        assert result.returncode == 0
        assert "Guardian: All SQL template variables resolved!" in result.stdout
        
    def test_build_pipeline_exists(self):
        """Test build pipeline exists and is executable"""
        assert os.path.exists('build_pipeline.sh')
        assert os.access('build_pipeline.sh', os.X_OK)
        
    def test_app_no_sql_template_variables(self):
        """Test app.py has no unresolved SQL template variables"""
        with open('app.py', 'r') as f:
            content = f.read()
            
        # Should not contain any {env_table_*} patterns
        import re
        template_pattern = r'\{env_table_[a-zA-Z_]+\}'
        matches = re.findall(template_pattern, content)
        
        assert len(matches) == 0, f"Found unresolved template variables: {matches}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])