#!/usr/bin/env python3
"""
Unit tests for the run_tests.py module.
"""

import unittest
import sys
import os
from unittest.mock import patch, MagicMock
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_tests import run_all_tests


class TestRunTests(unittest.TestCase):
    """Unit tests for run_tests module functionality."""

    @patch('run_tests.unittest.TextTestRunner')
    @patch('run_tests.unittest.TestLoader')
    def test_run_all_tests_success(self, mock_loader, mock_runner):
        """Test run_all_tests with successful test execution."""
        mock_result = MagicMock()
        mock_result.testsRun = 10
        mock_result.failures = []
        mock_result.errors = []
        mock_result.skipped = []
        
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = mock_result
        mock_runner.return_value = mock_runner_instance
        
        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = run_all_tests()
        
        self.assertTrue(result)
        mock_loader.assert_called_once()
        mock_runner.assert_called_once_with(verbosity=2)
        mock_runner_instance.run.assert_called_once()
        
        output = mock_stdout.getvalue()
        self.assertIn("TEST SUMMARY", output)
        self.assertIn("Tests run: 10", output)
        self.assertIn("Failures: 0", output)
        self.assertIn("Errors: 0", output)

    @patch('run_tests.unittest.TextTestRunner')
    @patch('run_tests.unittest.TestLoader')
    def test_run_all_tests_with_failures(self, mock_loader, mock_runner):
        """Test run_all_tests with test failures."""
        mock_result = MagicMock()
        mock_result.testsRun = 5
        mock_result.failures = [("test_example", "traceback")]
        mock_result.errors = []
        mock_result.skipped = []
        
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = mock_result
        mock_runner.return_value = mock_runner_instance
        
        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = run_all_tests()
        
        self.assertFalse(result)
        
        output = mock_stdout.getvalue()
        self.assertIn("FAILURES (1):", output)
        self.assertIn("test_example", output)

    @patch('run_tests.unittest.TextTestRunner')
    @patch('run_tests.unittest.TestLoader')
    def test_run_all_tests_with_errors(self, mock_loader, mock_runner):
        """Test run_all_tests with test errors."""
        mock_result = MagicMock()
        mock_result.testsRun = 3
        mock_result.failures = []
        mock_result.errors = [("test_error", "error traceback")]
        mock_result.skipped = []
        
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = mock_result
        mock_runner.return_value = mock_runner_instance
        
        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = run_all_tests()
        
        self.assertFalse(result)
        
        output = mock_stdout.getvalue()
        self.assertIn("ERRORS (1):", output)
        self.assertIn("test_error", output)

    @patch('run_tests.unittest.TextTestRunner')
    @patch('run_tests.unittest.TestLoader')
    def test_run_all_tests_with_skipped(self, mock_loader, mock_runner):
        """Test run_all_tests with skipped tests."""
        mock_result = MagicMock()
        mock_result.testsRun = 8
        mock_result.failures = []
        mock_result.errors = []
        mock_result.skipped = ["test_skipped"]
        
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = mock_result
        mock_runner.return_value = mock_runner_instance
        
        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = run_all_tests()
        
        self.assertTrue(result)
        
        output = mock_stdout.getvalue()
        self.assertIn("Skipped: 1", output)

    @patch('run_tests.unittest.TextTestRunner')
    @patch('run_tests.unittest.TestLoader')
    def test_run_all_tests_no_skipped_attribute(self, mock_loader, mock_runner):
        """Test run_all_tests when result has no skipped attribute."""
        mock_result = MagicMock()
        mock_result.testsRun = 5
        mock_result.failures = []
        mock_result.errors = []
        del mock_result.skipped
        
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = mock_result
        mock_runner.return_value = mock_runner_instance
        
        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = run_all_tests()
        
        self.assertTrue(result)
        
        output = mock_stdout.getvalue()
        self.assertIn("Skipped: 0", output)


if __name__ == '__main__':
    unittest.main(verbosity=2)
