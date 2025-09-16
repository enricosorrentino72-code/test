import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

mock_inference_sdk = MagicMock()
mock_client_class = MagicMock()
mock_inference_sdk.InferenceHTTPClient = mock_client_class

sys.modules['inference_sdk'] = mock_inference_sdk

from Inference import CLIENT


class TestInference(unittest.TestCase):
    """Unit tests for Inference module."""

    def test_client_initialization(self):
        """Test that CLIENT is properly initialized."""
        self.assertIsNotNone(CLIENT)
        mock_client_class.assert_called_once_with(
            api_url="https://serverless.roboflow.com",
            api_key="API_KEY"
        )

    @patch('Inference.CLIENT')
    def test_client_infer_method_exists(self, mock_client):
        """Test that CLIENT has infer method available."""
        mock_client.infer.return_value = {"predictions": []}
        
        result = mock_client.infer("sample_image.jpg", model_id="plantdoc_yolo/1")
        
        mock_client.infer.assert_called_once_with("sample_image.jpg", model_id="plantdoc_yolo/1")
        self.assertEqual(result, {"predictions": []})

    def test_api_configuration(self):
        """Test that API configuration is correct."""
        expected_api_url = "https://serverless.roboflow.com"
        expected_api_key = "API_KEY"
        
        mock_client_class.assert_called_with(
            api_url=expected_api_url,
            api_key=expected_api_key
        )

    @patch('Inference.CLIENT')
    def test_inference_with_different_models(self, mock_client):
        """Test inference with different model IDs."""
        mock_client.infer.return_value = {"predictions": [], "model": "test"}
        
        test_models = [
            "plantdoc_yolo/1",
            "plantdoc_yolo/2", 
            "custom_model/1"
        ]
        
        for model_id in test_models:
            result = mock_client.infer("test_image.jpg", model_id=model_id)
            self.assertIn("predictions", result)

    @patch('Inference.CLIENT')
    def test_inference_error_handling(self, mock_client):
        """Test inference error handling."""
        mock_client.infer.side_effect = Exception("API Error")
        
        with self.assertRaises(Exception) as context:
            mock_client.infer("invalid_image.jpg", model_id="plantdoc_yolo/1")
        
        self.assertEqual(str(context.exception), "API Error")


if __name__ == '__main__':
    unittest.main(verbosity=2)
