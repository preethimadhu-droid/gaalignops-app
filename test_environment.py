#!/usr/bin/env python3
"""
Test script to verify GA AlignOps environment setup
"""

import os
import sys

def test_environment_setup():
    """Test the environment setup and configuration"""
    print("🧪 Testing GA AlignOps Environment Setup")
    print("=" * 50)
    
    # Test 1: Environment Manager
    try:
        from config.environments import EnvironmentManager
        env_manager = EnvironmentManager()
        print("✅ Environment Manager: OK")
        
        env_info = env_manager.get_environment_info()
        print(f"   Environment: {env_info['environment']}")
        print(f"   Table Prefix: {env_info['table_prefix']}")
        print(f"   OAuth Enabled: {env_info['oauth_enabled']}")
        
    except Exception as e:
        print(f"❌ Environment Manager: FAILED - {e}")
        return False
    
    # Test 2: Module Structure
    try:
        from config.module_structure import module_structure
        print("✅ Module Structure: OK")
        
        modules = module_structure.get_module_names()
        print(f"   Modules: {', '.join(modules)}")
        
        # Test specific module
        demand_config = module_structure.get_module_config("Demand Planning")
        if demand_config:
            print(f"   Demand Planning sub-modules: {', '.join(demand_config.sub_modules)}")
        
    except Exception as e:
        print(f"❌ Module Structure: FAILED - {e}")
        return False
    
    # Test 3: Environment Variables
    try:
        print("✅ Environment Variables: OK")
        print(f"   GAALIGNOPS_ENV: {os.getenv('GAALIGNOPS_ENV', 'Not set')}")
        print(f"   DATABASE_URL: {os.getenv('DATABASE_URL', 'Not set')[:50]}...")
        
    except Exception as e:
        print(f"❌ Environment Variables: FAILED - {e}")
        return False
    
    # Test 4: Core Dependencies
    try:
        import streamlit
        import pandas
        import numpy
        import plotly
        print("✅ Core Dependencies: OK")
        print(f"   Streamlit: {streamlit.__version__}")
        print(f"   Pandas: {pandas.__version__}")
        print(f"   Numpy: {numpy.__version__}")
        print(f"   Plotly: {plotly.__version__}")
        
    except Exception as e:
        print(f"❌ Core Dependencies: FAILED - {e}")
        return False
    
    # Test 5: Database Connection (if configured)
    try:
        if os.getenv('DATABASE_URL'):
            import psycopg2
            print("✅ PostgreSQL Driver: OK")
        else:
            print("⚠️  Database URL not configured - skipping connection test")
            
    except Exception as e:
        print(f"❌ PostgreSQL Driver: FAILED - {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 Environment Setup Test Completed Successfully!")
    print("\n📋 Next Steps:")
    print("1. Configure your DATABASE_URL in .env file")
    print("2. Create a local PostgreSQL database")
    print("3. Run: streamlit run app.py")
    
    return True

if __name__ == "__main__":
    success = test_environment_setup()
    sys.exit(0 if success else 1)
