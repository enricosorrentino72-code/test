#!/usr/bin/env python3
"""
Verification script to test MCP server functionality.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_mcp_imports():
    """Test that MCP server can be imported successfully."""
    try:
        import mcp_server
        print("✅ MCP server imports successfully")
        return True
    except ImportError as e:
        print(f"❌ MCP server import failed: {e}")
        return False

def test_function_availability():
    """Test that all expected functions are available."""
    try:
        from mcp_server import (
            get_demographics, get_weather_info, add_numbers, 
            multiply_numbers, download_ml_model
        )
        print("✅ All MCP tool functions are available")
        return True
    except ImportError as e:
        print(f"❌ Function import failed: {e}")
        return False

def test_function_execution():
    """Test that MCP tool functions execute correctly."""
    try:
        from mcp_server import (
            get_demographics, get_weather_info, add_numbers, 
            multiply_numbers
        )
        
        result = get_demographics("Paris")
        assert isinstance(result, dict)
        assert "demographics" in result
        assert "location" in result
        print("✅ Demographics function works")
        
        result = get_weather_info("Tokyo")
        assert isinstance(result, dict)
        assert "temperature" in result
        assert "location" in result
        print("✅ Weather function works")
        
        assert add_numbers(5, 3) == 8
        assert multiply_numbers(4, 3) == 12
        print("✅ Math functions work")
        
        return True
    except Exception as e:
        print(f"❌ Function execution failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing MCP Server Functionality")
    print("=" * 40)
    
    success = True
    success &= test_mcp_imports()
    success &= test_function_availability()
    success &= test_function_execution()
    
    print("=" * 40)
    if success:
        print("🎉 All MCP server tests passed!")
        sys.exit(0)
    else:
        print("💥 Some MCP server tests failed!")
        sys.exit(1)
