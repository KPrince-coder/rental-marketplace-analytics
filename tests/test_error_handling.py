"""
Unit tests for ETL error handling.
"""

import unittest
from unittest.mock import Mock, patch
import time
from datetime import datetime

from src.etl.error_handling import (
    with_retry,
    handle_etl_error,
    ETLError,
    DataValidationError,
    DataTransformationError,
    DataLoadError,
    QualityCheckError,
)


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling utilities."""

    def test_retry_decorator_success(self):
        """Test retry decorator with successful function."""
        mock_fn = Mock(return_value="success")
        decorated = with_retry()(mock_fn)

        result = decorated()
        self.assertEqual(result, "success")
        self.assertEqual(mock_fn.call_count, 1)

    def test_retry_decorator_eventual_success(self):
        """Test retry decorator with function that succeeds after failures."""
        mock_fn = Mock(side_effect=[ValueError(), ValueError(), "success"])
        decorated = with_retry(max_attempts=3, delay_seconds=0)(mock_fn)

        result = decorated()
        self.assertEqual(result, "success")
        self.assertEqual(mock_fn.call_count, 3)

    def test_retry_decorator_failure(self):
        """Test retry decorator with function that always fails."""
        mock_fn = Mock(side_effect=ValueError("test error"))
        decorated = with_retry(max_attempts=3, delay_seconds=0)(mock_fn)

        with self.assertRaises(ValueError):
            decorated()
        self.assertEqual(mock_fn.call_count, 3)

    def test_retry_decorator_exponential_backoff(self):
        """Test retry decorator's exponential backoff."""
        start_time = time.time()
        mock_fn = Mock(side_effect=[ValueError(), ValueError(), "success"])
        decorated = with_retry(
            max_attempts=3, delay_seconds=1, exponential_backoff=True
        )(mock_fn)

        result = decorated()
        duration = time.time() - start_time

        self.assertEqual(result, "success")
        # Should wait 1s then 2s (3s total minimum)
        self.assertGreaterEqual(duration, 3)

    def test_etl_error_to_dict(self):
        """Test ETL error conversion to dictionary."""
        error = ETLError("Test error", details={"key": "value"})
        error_dict = error.to_dict()

        self.assertEqual(error_dict["error_type"], "ETLError")
        self.assertEqual(error_dict["message"], "Test error")
        self.assertEqual(error_dict["details"], {"key": "value"})
        self.assertIsInstance(datetime.fromisoformat(error_dict["timestamp"]), datetime)

    def test_specific_error_types(self):
        """Test specific ETL error types."""
        validation_error = DataValidationError("Invalid data")
        self.assertIsInstance(validation_error, ETLError)

        transform_error = DataTransformationError("Transform failed")
        self.assertIsInstance(transform_error, ETLError)

        load_error = DataLoadError("Load failed")
        self.assertIsInstance(load_error, ETLError)

        quality_error = QualityCheckError("Quality check failed")
        self.assertIsInstance(quality_error, ETLError)

    @patch("src.etl.error_handling.ETLMonitoring")
    def test_handle_etl_error(self, mock_monitoring):
        """Test error handling function."""
        error = DataValidationError("Test validation error", details={"field": "test"})
        context = {"component": "test_component"}

        error_info = handle_etl_error(error, context)

        self.assertEqual(error_info["error_type"], "DataValidationError")
        self.assertEqual(error_info["message"], "Test validation error")
        self.assertEqual(error_info["details"], {"field": "test"})
        self.assertEqual(error_info["context"], context)
        self.assertIn("timestamp", error_info)
        self.assertIn("handled_at", error_info)

        # Verify monitoring metric was recorded
        mock_monitoring.return_value.put_metric.assert_called_once_with(
            "ETLError",
            1,
            {"ErrorType": "DataValidationError", "Component": "test_component"},
        )

    def test_handle_non_etl_error(self):
        """Test handling of non-ETL errors."""
        error = ValueError("Regular error")
        context = {"component": "test"}

        error_info = handle_etl_error(error, context)

        self.assertEqual(error_info["error_type"], "ValueError")
        self.assertEqual(error_info["message"], "Regular error")
        self.assertEqual(error_info["details"], {})
        self.assertEqual(error_info["context"], context)


if __name__ == "__main__":
    unittest.main()
