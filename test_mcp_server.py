import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def mock_tool():
    """Mock @tool decorator that preserves the original function."""
    def decorator(func):
        func.name = func.__name__
        return func
    return decorator

mock_fastmcp = MagicMock()
mock_fastmcp.FastMCP = MagicMock()
mock_fastmcp_instance = MagicMock()
mock_fastmcp_instance.tool = mock_tool
mock_fastmcp.FastMCP.return_value = mock_fastmcp_instance

sys.modules['mcp.server.fastmcp'] = mock_fastmcp
sys.modules['mcp'] = MagicMock()

from mcp_server import (
    get_demographics, get_weather_info, add_numbers, 
    multiply_numbers, download_ml_model
)


class TestMCPServer(unittest.TestCase):
    """Unit tests for MCP server tool functions."""

    def test_get_demographics_tool(self):
        """Test demographics MCP tool wrapper."""
        result = get_demographics("Paris")
        self.assertIsInstance(result, dict)
        self.assertIn("demographics", result)
        self.assertIn("location", result)
        self.assertEqual(result["location"], "Paris")
        self.assertEqual(result["demographics"], "250000000 Persons")

    def test_get_demographics_different_locations(self):
        """Test demographics tool with various locations."""
        test_locations = ["Tokyo", "Berlin", "New York", "London"]
        
        for location in test_locations:
            result = get_demographics(location)
            self.assertEqual(result["location"], location)
            self.assertEqual(result["demographics"], "250000000 Persons")

    def test_get_weather_info_tool(self):
        """Test weather MCP tool wrapper."""
        result = get_weather_info("Tokyo")
        self.assertIsInstance(result, dict)
        self.assertIn("temperature", result)
        self.assertIn("location", result)
        self.assertEqual(result["location"], "Tokyo")
        self.assertEqual(result["temperature"], "23°C")

    def test_get_weather_info_different_locations(self):
        """Test weather tool with various locations."""
        test_locations = ["Paris", "Berlin", "Sydney", "Mumbai"]
        
        for location in test_locations:
            result = get_weather_info(location)
            self.assertEqual(result["location"], location)
            self.assertEqual(result["temperature"], "23°C")

    def test_add_numbers_tool(self):
        """Test addition MCP tool wrapper."""
        self.assertEqual(add_numbers(5, 3), 8)
        self.assertEqual(add_numbers(10, 20), 30)
        self.assertEqual(add_numbers(0, 5), 5)
        self.assertEqual(add_numbers(5, 0), 5)
        self.assertEqual(add_numbers(0, 0), 0)
        self.assertEqual(add_numbers(-5, 3), -2)
        self.assertEqual(add_numbers(-5, -3), -8)
        self.assertEqual(add_numbers(5, -3), 2)

    def test_add_numbers_edge_cases(self):
        """Test addition tool with edge cases."""
        self.assertEqual(add_numbers(1000000, 2000000), 3000000)
        self.assertEqual(add_numbers(-1000, -2000), -3000)

    def test_multiply_numbers_tool(self):
        """Test multiplication MCP tool wrapper."""
        self.assertEqual(multiply_numbers(4, 3), 12)
        self.assertEqual(multiply_numbers(10, 4), 40)
        self.assertEqual(multiply_numbers(0, 5), 0)
        self.assertEqual(multiply_numbers(5, 0), 0)
        self.assertEqual(multiply_numbers(1, 5), 5)
        self.assertEqual(multiply_numbers(5, 1), 5)
        self.assertEqual(multiply_numbers(-2, 5), -10)
        self.assertEqual(multiply_numbers(-5, -3), 15)
        self.assertEqual(multiply_numbers(5, -3), -15)

    def test_multiply_numbers_edge_cases(self):
        """Test multiplication tool with edge cases."""
        self.assertEqual(multiply_numbers(1000, 2000), 2000000)
        self.assertEqual(multiply_numbers(-1, -1), 1)

    @patch('mcp_server.download_model')
    def test_download_ml_model_tool_success(self, mock_download):
        """Test model download MCP tool wrapper success case."""
        mock_download.return_value = True
        result = download_ml_model("http://example.com/model.pt", "model.pt")
        self.assertTrue(result)
        mock_download.assert_called_once_with("http://example.com/model.pt", "model.pt", 1)

    @patch('mcp_server.download_model')
    def test_download_ml_model_tool_failure(self, mock_download):
        """Test model download MCP tool wrapper failure case."""
        mock_download.return_value = False
        result = download_ml_model("http://invalid.com/model.pt", "model.pt")
        self.assertFalse(result)
        mock_download.assert_called_once_with("http://invalid.com/model.pt", "model.pt", 1)

    @patch('mcp_server.download_model')
    def test_download_ml_model_tool_custom_size(self, mock_download):
        """Test model download tool with custom minimum size."""
        mock_download.return_value = True
        result = download_ml_model("http://example.com/large_model.pt", "large_model.pt", 10)
        self.assertTrue(result)
        mock_download.assert_called_once_with("http://example.com/large_model.pt", "large_model.pt", 10)

    def test_mcp_server_initialization(self):
        """Test that MCP server is properly initialized."""
        from mcp_server import mcp
        self.assertIsNotNone(mcp)

    def test_function_type_hints(self):
        """Test that MCP tool functions have proper type hints."""
        self.assertEqual(get_demographics.__annotations__.get('location'), str)
        self.assertEqual(get_demographics.__annotations__.get('return'), dict)
        
        self.assertEqual(get_weather_info.__annotations__.get('location'), str)
        self.assertEqual(get_weather_info.__annotations__.get('return'), dict)
        
        self.assertEqual(add_numbers.__annotations__.get('a'), int)
        self.assertEqual(add_numbers.__annotations__.get('b'), int)
        self.assertEqual(add_numbers.__annotations__.get('return'), int)
        
        self.assertEqual(multiply_numbers.__annotations__.get('a'), int)
        self.assertEqual(multiply_numbers.__annotations__.get('b'), int)
        self.assertEqual(multiply_numbers.__annotations__.get('return'), int)
        
        self.assertEqual(download_ml_model.__annotations__.get('url'), str)
        self.assertEqual(download_ml_model.__annotations__.get('output_path'), str)
        self.assertEqual(download_ml_model.__annotations__.get('min_size_mb'), int)
        self.assertEqual(download_ml_model.__annotations__.get('return'), bool)


if __name__ == '__main__':
    unittest.main(verbosity=2)
