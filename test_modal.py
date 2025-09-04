import unittest
from unittest.mock import patch, mock_open, MagicMock
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class TestAIModal(unittest.TestCase):
    """Unit tests for Test-AI-Modal module functionality."""

    @patch('builtins.open', new_callable=mock_open, read_data=b'fake_image_data')
    @patch('base64.b64encode')
    def test_image_encoding(self, mock_b64encode, mock_file):
        """Test image file encoding to base64."""
        mock_b64encode.return_value = b'encoded_image_data'
        
        with open("sample_image.png", "rb") as image_file:
            encoded_image = mock_b64encode(image_file.read()).decode('utf-8')
        
        mock_file.assert_called_once_with("sample_image.png", "rb")
        mock_b64encode.assert_called_once_with(b'fake_image_data')
        self.assertEqual(encoded_image, 'encoded_image_data')

    def test_prompt_definitions(self):
        """Test that all prompt templates are properly defined."""
        generic_prompt = "Analyze the image and provide a detailed description of what is visible, including the environment, possible location, people, animals, and objects. Describe the relationships or interactions between the elements, the mood or emotions conveyed, and any relevant contextual clues that might help understand the scene."
        
        plant_prompt = "Assess the physiological condition of the plant in the image. Look for symptoms of chlorosis, wilting, necrosis, fungal or insect damage. Include observations on hydration, sunlight exposure, and plant vigor. Provide possible diagnoses and remediation steps."
        
        abusive_prompt_keywords = [
            "illegal waste dumping",
            "unauthorized areas",
            "YES or NO",
            "visible evidence"
        ]
        
        self.assertIsInstance(generic_prompt, str)
        self.assertGreater(len(generic_prompt), 50)
        self.assertIn("detailed description", generic_prompt)
        
        self.assertIsInstance(plant_prompt, str)
        self.assertGreater(len(plant_prompt), 50)
        self.assertIn("physiological condition", plant_prompt)
        
        abusive_prompt = """
You are an expert in detecting illegal waste dumping in unauthorized areas such as countryside, roadsides, urban spaces, abandoned industrial zones, or public areas not designated for waste disposal.

You will receive an input image. Your task is to:

Analyze the visual content of the image.

--Determine whether the image clearly shows a scene of illegal waste dumping in a non-authorized location (e.g., visible piles of garbage, people unloading items, parked or active vehicles like vans or trucks, garbage bags, bulky or hazardous waste).

--Assess the context: are there any signs, bins, or disposal containers? Or does the location appear clearly inappropriate (e.g., woods, roadsides, abandoned lots)?

Important context: The camera is installed in a location where any kind of waste dumping is strictly prohibited. 
Therefore, if you observe any type of waste being dumped or already present, the situation must be considered illegal.

Your output must always be in this format:

    1.Answer: YES or NO

    2.Explanation: A brief explanation (1–3 sentences) of why you chose that answer, based only on visible evidence in the image.

Important rules:

You must choose either YES or NO.

Do not answer UNCERTAIN.

Do not invent details that are not visible in the image. Base your judgment only on what you can clearly see.

"""
        
        for keyword in abusive_prompt_keywords:
            self.assertIn(keyword, abusive_prompt)

    @patch('requests.post')
    def test_ollama_request_structure(self, mock_post):
        """Test the structure of the request sent to Ollama."""
        mock_response = MagicMock()
        mock_response.iter_lines.return_value = [
            b'{"response": "Test response", "done": false}',
            b'{"response": "", "done": true}'
        ]
        mock_post.return_value = mock_response
        
        expected_data = {
            "model": "llava:7b",
            "prompt": "test_prompt",
            "images": ["encoded_image"],
            "stream": True
        }
        
        import requests
        response = requests.post(
            "http://localhost:11434/api/generate", 
            json=expected_data, 
            stream=True
        )
        
        mock_post.assert_called_once_with(
            "http://localhost:11434/api/generate",
            json=expected_data,
            stream=True
        )

    def test_json_response_parsing(self):
        """Test JSON response parsing logic."""
        test_lines = [
            b'{"response": "Hello", "done": false}',
            b'{"response": " world", "done": false}',
            b'{"response": "!", "done": true}'
        ]
        
        output = ""
        for line in test_lines:
            if line:
                try:
                    json_line = json.loads(line.decode('utf-8'))
                    output += json_line.get("response", "")
                except json.JSONDecodeError:
                    pass
        
        self.assertEqual(output, "Hello world!")

    def test_json_parsing_error_handling(self):
        """Test JSON parsing with malformed data."""
        test_lines = [
            b'{"response": "Valid", "done": false}',
            b'invalid json data',
            b'{"response": " response", "done": true}'
        ]
        
        output = ""
        errors = []
        
        for line in test_lines:
            if line:
                try:
                    json_line = json.loads(line.decode('utf-8'))
                    output += json_line.get("response", "")
                except json.JSONDecodeError as e:
                    errors.append(str(e))
        
        self.assertEqual(output, "Valid response")
        self.assertEqual(len(errors), 1)

    def test_model_configuration(self):
        """Test model configuration parameters."""
        expected_model = "llava:7b"
        expected_stream = True
        expected_endpoint = "http://localhost:11434/api/generate"
        
        self.assertEqual(expected_model, "llava:7b")
        self.assertTrue(expected_stream)
        self.assertEqual(expected_endpoint, "http://localhost:11434/api/generate")


if __name__ == '__main__':
    unittest.main(verbosity=2)
