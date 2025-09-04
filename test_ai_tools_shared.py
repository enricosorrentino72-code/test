import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def mock_tool(func):
    """Mock @tool decorator that preserves the original function."""
    func.name = func.__name__
    return func

mock_langchain_ollama = MagicMock()
mock_langchain_core_tools = MagicMock()
mock_langchain_core_tools.tool = mock_tool

sys.modules['langchain_ollama'] = mock_langchain_ollama
sys.modules['langchain_core.tools'] = mock_langchain_core_tools

from ai_tools_shared import demographics, get_weather, add, multiply, create_llm, system_prompt, tools


class TestAIToolsShared(unittest.TestCase):
    """Unit tests for ai_tools_shared module functions."""

    def test_demographics_function(self):
        """Test demographics function returns correct format and data."""
        location = "Paris"
        result = demographics(location)
        
        self.assertIsInstance(result, dict)
        
        self.assertIn("demographics", result)
        self.assertIn("location", result)
        
        self.assertEqual(result["location"], location)
        self.assertEqual(result["demographics"], "250000000 Persons")

    def test_demographics_with_different_locations(self):
        """Test demographics function with various location inputs."""
        test_locations = ["Tokyo", "Berlin", "New York", "London"]
        
        for location in test_locations:
            result = demographics(location)
            self.assertEqual(result["location"], location)
            self.assertEqual(result["demographics"], "250000000 Persons")

    def test_get_weather_function(self):
        """Test get_weather function returns correct format and data."""
        location = "Tokyo"
        result = get_weather(location)
        
        self.assertIsInstance(result, dict)
        
        self.assertIn("temperature", result)
        self.assertIn("location", result)
        
        self.assertEqual(result["location"], location)
        self.assertEqual(result["temperature"], "23°C")

    def test_get_weather_with_different_locations(self):
        """Test get_weather function with various location inputs."""
        test_locations = ["Paris", "Berlin", "Sydney", "Mumbai"]
        
        for location in test_locations:
            result = get_weather(location)
            self.assertEqual(result["location"], location)
            self.assertEqual(result["temperature"], "23°C")

    def test_add_function(self):
        """Test add function performs correct arithmetic."""
        self.assertEqual(add(5, 3), 8)
        self.assertEqual(add(10, 20), 30)
        
        self.assertEqual(add(0, 5), 5)
        self.assertEqual(add(5, 0), 5)
        self.assertEqual(add(0, 0), 0)
        
        self.assertEqual(add(-5, 3), -2)
        self.assertEqual(add(-5, -3), -8)
        self.assertEqual(add(5, -3), 2)

    def test_add_function_edge_cases(self):
        """Test add function with edge cases."""
        self.assertEqual(add(1000000, 2000000), 3000000)
        
        self.assertEqual(add(-1000, -2000), -3000)

    def test_multiply_function(self):
        """Test multiply function performs correct arithmetic."""
        self.assertEqual(multiply(5, 3), 15)
        self.assertEqual(multiply(10, 4), 40)
        
        self.assertEqual(multiply(0, 5), 0)
        self.assertEqual(multiply(5, 0), 0)
        
        self.assertEqual(multiply(1, 5), 5)
        self.assertEqual(multiply(5, 1), 5)
        
        self.assertEqual(multiply(-5, 3), -15)
        self.assertEqual(multiply(-5, -3), 15)
        self.assertEqual(multiply(5, -3), -15)

    def test_multiply_function_edge_cases(self):
        """Test multiply function with edge cases."""
        self.assertEqual(multiply(1000, 2000), 2000000)
        
        self.assertEqual(multiply(-1, -1), 1)

    @patch('ai_tools_shared.ChatOllama')
    def test_create_llm_default_parameters(self, mock_chat_ollama):
        """Test create_llm function with default parameters."""
        mock_instance = MagicMock()
        mock_chat_ollama.return_value = mock_instance
        
        result = create_llm()
        
        mock_chat_ollama.assert_called_once_with(
            model="llama3.2",
            temperature=0,
            verbose=True
        )
        
        self.assertEqual(result, mock_instance)

    @patch('ai_tools_shared.ChatOllama')
    def test_create_llm_custom_parameters(self, mock_chat_ollama):
        """Test create_llm function with custom parameters."""
        mock_instance = MagicMock()
        mock_chat_ollama.return_value = mock_instance
        
        result = create_llm(
            model="custom-model",
            temperature=0.5,
            verbose=False,
            max_tokens=1000
        )
        
        mock_chat_ollama.assert_called_once_with(
            model="custom-model",
            temperature=0.5,
            verbose=False,
            max_tokens=1000
        )
        
        self.assertEqual(result, mock_instance)

    def test_system_prompt_exists(self):
        """Test that system_prompt is properly defined."""
        self.assertIsInstance(system_prompt, str)
        self.assertGreater(len(system_prompt), 100)  # Should be a substantial prompt
        
        self.assertIn("smart assistant", system_prompt.lower())
        self.assertIn("tools", system_prompt.lower())
        self.assertIn("weather", system_prompt.lower())
        self.assertIn("demographics", system_prompt.lower())

    def test_tools_list_exists(self):
        """Test that tools list is properly defined."""
        self.assertIsInstance(tools, list)
        self.assertEqual(len(tools), 4)
        
        tool_names = [tool.name for tool in tools]
        expected_tools = ["multiply", "add", "get_weather", "demographics"]
        
        for expected_tool in expected_tools:
            self.assertIn(expected_tool, tool_names)

    def test_function_type_hints(self):
        """Test that functions have proper type hints."""
        self.assertEqual(demographics.__annotations__.get('location'), str)
        self.assertEqual(demographics.__annotations__.get('return'), dict)
        
        self.assertEqual(get_weather.__annotations__.get('location'), str)
        self.assertEqual(get_weather.__annotations__.get('return'), dict)
        
        self.assertEqual(add.__annotations__.get('a'), int)
        self.assertEqual(add.__annotations__.get('b'), int)
        self.assertEqual(add.__annotations__.get('return'), int)
        
        self.assertEqual(multiply.__annotations__.get('a'), int)
        self.assertEqual(multiply.__annotations__.get('b'), int)
        self.assertEqual(multiply.__annotations__.get('return'), int)


if __name__ == '__main__':
    unittest.main(verbosity=2)
