import unittest
from unittest.mock import patch, mock_open, MagicMock, call
import sys
import os
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class MockRequestException(Exception):
    pass

mock_requests = MagicMock()
mock_requests.exceptions.RequestException = MockRequestException
sys.modules['requests'] = mock_requests
sys.modules['requests.exceptions'] = mock_requests.exceptions

from Download import download_model


class TestDownloadModel(unittest.TestCase):
    """Unit tests for download_model function."""

    @patch('Download.requests.get')
    @patch('Download.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_model_success(self, mock_file, mock_getsize, mock_get):
        """Test successful model download."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2', b'chunk3']
        mock_get.return_value = mock_response
        mock_getsize.return_value = 2 * 1024 * 1024  # 2MB file
        
        result = download_model("http://example.com/model.pt", "model.pt", min_size_mb=1)
        
        self.assertTrue(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()
        mock_file.assert_called_once_with("model.pt", "wb")
        mock_getsize.assert_called_once_with("model.pt")

    @patch('Download.requests.get')
    @patch('Download.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_model_file_too_small(self, mock_file, mock_getsize, mock_get):
        """Test download with file smaller than minimum size."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'small']
        mock_get.return_value = mock_response
        mock_getsize.return_value = 0.5 * 1024 * 1024  # 0.5MB file (too small)
        
        result = download_model("http://example.com/model.pt", "model.pt", min_size_mb=1)
        
        self.assertFalse(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()
        mock_file.assert_called_once_with("model.pt", "wb")
        mock_getsize.assert_called_once_with("model.pt")

    @patch('Download.requests.get')
    def test_download_model_request_exception(self, mock_get):
        """Test download with network request exception."""
        mock_get.side_effect = MockRequestException("Network error")
        
        result = download_model("http://example.com/model.pt", "model.pt")
        
        self.assertFalse(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)

    @patch('Download.requests.get')
    @patch('Download.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_model_custom_min_size(self, mock_file, mock_getsize, mock_get):
        """Test download with custom minimum size requirement."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2']
        mock_get.return_value = mock_response
        mock_getsize.return_value = 3 * 1024 * 1024  # 3MB file
        
        result = download_model("http://example.com/model.pt", "model.pt", min_size_mb=5)
        
        self.assertFalse(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()
        mock_file.assert_called_once_with("model.pt", "wb")
        mock_getsize.assert_called_once_with("model.pt")

    @patch('Download.requests.get')
    @patch('Download.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_model_empty_chunks(self, mock_file, mock_getsize, mock_get):
        """Test download with empty chunks in response."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'chunk1', b'', b'chunk2', None, b'chunk3']
        mock_get.return_value = mock_response
        mock_getsize.return_value = 2 * 1024 * 1024  # 2MB file
        
        result = download_model("http://example.com/model.pt", "model.pt", min_size_mb=1)
        
        self.assertTrue(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()
        
        handle = mock_file.return_value
        expected_calls = [call(b'chunk1'), call(b'chunk2'), call(b'chunk3')]
        handle.write.assert_has_calls(expected_calls)

    @patch('Download.requests.get')
    @patch('Download.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_model_default_min_size(self, mock_file, mock_getsize, mock_get):
        """Test download with default minimum size (1MB)."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'chunk1']
        mock_get.return_value = mock_response
        mock_getsize.return_value = 1.5 * 1024 * 1024  # 1.5MB file
        
        result = download_model("http://example.com/model.pt", "model.pt")
        
        self.assertTrue(result)
        mock_get.assert_called_once_with("http://example.com/model.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()
        mock_file.assert_called_once_with("model.pt", "wb")
        mock_getsize.assert_called_once_with("model.pt")

    @patch('Download.requests.get')
    def test_download_model_http_error(self, mock_get):
        """Test download with HTTP error response."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = MockRequestException("HTTP 404 Error")
        mock_get.return_value = mock_response
        
        result = download_model("http://example.com/nonexistent.pt", "model.pt")
        
        self.assertFalse(result)
        mock_get.assert_called_once_with("http://example.com/nonexistent.pt", stream=True)
        mock_response.raise_for_status.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
