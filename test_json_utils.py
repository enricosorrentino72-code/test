import unittest
import json
from unittest.mock import MagicMock

def parse_streaming_json_response(response_lines):
    """
    Parse streaming JSON response lines and concatenate response content.
    Extracted from Test-AI-Modal.py for testing.
    """
    output = ""
    for line in response_lines:
        if line:
            try:
                json_line = json.loads(line.decode('utf-8'))
                output += json_line.get("response", "")
            except json.JSONDecodeError as e:
                print("Errore nel parsing JSON:", e)
                print("Linea ricevuta:", line.decode('utf-8'))
    return output

class TestJSONUtils(unittest.TestCase):
    """Unit tests for JSON utility functions."""
    
    def test_parse_streaming_json_response_success(self):
        """Test successful JSON parsing."""
        mock_lines = [
            b'{"response": "Hello "}',
            b'{"response": "World"}',
            b'{"response": "!"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "Hello World!")
    
    def test_parse_streaming_json_response_with_empty_lines(self):
        """Test JSON parsing with empty lines."""
        mock_lines = [
            b'{"response": "Hello"}',
            b'',
            b'{"response": " World"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "Hello World")
    
    def test_parse_streaming_json_response_with_invalid_json(self):
        """Test JSON parsing with invalid JSON lines."""
        mock_lines = [
            b'{"response": "Valid"}',
            b'invalid json',
            b'{"response": " Response"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "Valid Response")
    
    def test_parse_streaming_json_response_empty_response_field(self):
        """Test JSON parsing with empty response fields."""
        mock_lines = [
            b'{"response": "Start"}',
            b'{"response": ""}',
            b'{"response": "End"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "StartEnd")
    
    def test_parse_streaming_json_response_missing_response_field(self):
        """Test JSON parsing with missing response fields."""
        mock_lines = [
            b'{"response": "Valid"}',
            b'{"other_field": "ignored"}',
            b'{"response": " Content"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "Valid Content")
    
    def test_parse_streaming_json_response_empty_input(self):
        """Test JSON parsing with empty input."""
        mock_lines = []
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "")
    
    def test_parse_streaming_json_response_none_lines(self):
        """Test JSON parsing with None values in lines."""
        mock_lines = [
            b'{"response": "Start"}',
            None,
            b'{"response": "End"}'
        ]
        
        result = parse_streaming_json_response(mock_lines)
        self.assertEqual(result, "StartEnd")

if __name__ == '__main__':
    unittest.main(verbosity=2)
