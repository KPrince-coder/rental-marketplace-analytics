"""
Unit tests for ETL pipeline orchestration.
"""

import unittest
import argparse
import logging
import os
import tempfile
import shutil
from unittest.mock import patch, Mock, ANY
from datetime import datetime

import main
from src.etl.pipeline import ETLPipeline
from src.monitoring.log_management import LogManager
from src.monitoring.monitoring import ETLMonitoring


class TestMain(unittest.TestCase):
    """Test cases for main ETL script."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.log_dir = os.path.join(self.test_dir, "logs")
        os.makedirs(self.log_dir)

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_setup_logging(self):
        """Test logging setup."""
        with (
            patch("logging.FileHandler") as mock_fh,
            patch("logging.StreamHandler") as mock_sh,
        ):
            main.setup_logging()

            # Should create both handlers
            mock_fh.assert_called_once()
            mock_sh.assert_called_once()

            # Verify log filename format
            log_file = mock_fh.call_args[0][0]
            self.assertTrue(log_file.startswith("logs/etl_"))
            self.assertTrue(log_file.endswith(".log"))

    def test_parse_args_defaults(self):
        """Test argument parsing with defaults."""
        with patch("sys.argv", ["main.py"]):
            args = main.parse_args()

            self.assertEqual(
                args.listings_days, main.ETL_CONFIG["default_listings_days"]
            )
            self.assertEqual(
                args.activity_days, main.ETL_CONFIG["default_activity_days"]
            )
            self.assertEqual(args.metrics_days, main.ETL_CONFIG["default_metrics_days"])
            self.assertFalse(args.rotate_logs)

    def test_parse_args_custom(self):
        """Test argument parsing with custom values."""
        test_args = [
            "main.py",
            "--listings-days",
            "14",
            "--activity-days",
            "3",
            "--metrics-days",
            "60",
            "--rotate-logs",
        ]

        with patch("sys.argv", test_args):
            args = main.parse_args()

            self.assertEqual(args.listings_days, 14)
            self.assertEqual(args.activity_days, 3)
            self.assertEqual(args.metrics_days, 60)
            self.assertTrue(args.rotate_logs)

    @patch("src.etl.pipeline.ETLPipeline")
    @patch("src.monitoring.monitoring.ETLMonitoring")
    def test_run_pipeline_success(self, mock_monitoring, mock_pipeline):
        """Test successful pipeline execution."""
        # Setup mocks
        mock_pipeline_instance = Mock()
        mock_pipeline_instance.run.return_value = True
        mock_pipeline.return_value = mock_pipeline_instance

        args = argparse.Namespace(listings_days=7, activity_days=7, metrics_days=30)

        success = main.run_pipeline(args)

        self.assertTrue(success)
        mock_pipeline_instance.run.assert_called_once_with(
            listings_days=7, activity_days=7, metrics_days=30
        )
        mock_monitoring.return_value.record_pipeline_metrics.assert_called()

    @patch("src.etl.pipeline.ETLPipeline")
    @patch("src.monitoring.monitoring.ETLMonitoring")
    def test_run_pipeline_failure(self, mock_monitoring, mock_pipeline):
        """Test pipeline execution failure."""
        # Setup mocks
        mock_pipeline_instance = Mock()
        mock_pipeline_instance.run.return_value = False
        mock_pipeline.return_value = mock_pipeline_instance

        args = argparse.Namespace(listings_days=7, activity_days=7, metrics_days=30)

        success = main.run_pipeline(args)

        self.assertFalse(success)
        mock_monitoring.return_value.record_pipeline_metrics.assert_called()

    @patch("src.etl.pipeline.ETLPipeline")
    @patch("src.monitoring.monitoring.ETLMonitoring")
    def test_run_pipeline_exception(self, mock_monitoring, mock_pipeline):
        """Test pipeline execution with exception."""
        # Setup mocks
        mock_pipeline_instance = Mock()
        mock_pipeline_instance.run.side_effect = Exception("Test error")
        mock_pipeline.return_value = mock_pipeline_instance

        args = argparse.Namespace(listings_days=7, activity_days=7, metrics_days=30)

        success = main.run_pipeline(args)

        self.assertFalse(success)
        mock_monitoring.return_value.record_pipeline_metrics.assert_called()

    @patch("src.monitoring.log_management.LogManager")
    def test_manage_logs(self, mock_log_manager):
        """Test log management."""
        main.manage_logs()

        mock_log_manager.return_value.rotate_logs.assert_called_once()
        mock_log_manager.return_value.cleanup_archives.assert_called_once()

    @patch("main.run_pipeline")
    @patch("main.manage_logs")
    @patch("main.LogManager")
    def test_main_success(self, mock_log_manager, mock_manage_logs, mock_run):
        """Test main function with successful execution."""
        mock_run.return_value = True

        with patch("sys.argv", ["main.py"]), self.assertRaises(SystemExit) as cm:
            main.main()

        self.assertEqual(cm.exception.code, 0)
        mock_run.assert_called_once()
        mock_manage_logs.assert_not_called()

    @patch("main.run_pipeline")
    @patch("main.manage_logs")
    @patch("main.LogManager")
    def test_main_failure(self, mock_log_manager, mock_manage_logs, mock_run):
        """Test main function with pipeline failure."""
        mock_run.return_value = False
        mock_log_manager.return_value.get_recent_errors.return_value = [
            "Test error 1",
            "Test error 2",
        ]

        with patch("sys.argv", ["main.py"]), self.assertRaises(SystemExit) as cm:
            main.main()

        self.assertEqual(cm.exception.code, 1)
        mock_run.assert_called_once()
        mock_log_manager.return_value.get_recent_errors.assert_called_once()

    @patch("main.run_pipeline")
    @patch("main.manage_logs")
    def test_main_with_log_rotation(self, mock_manage_logs, mock_run):
        """Test main function with log rotation enabled."""
        mock_run.return_value = True

        with (
            patch("sys.argv", ["main.py", "--rotate-logs"]),
            self.assertRaises(SystemExit) as cm,
        ):
            main.main()

        self.assertEqual(cm.exception.code, 0)
        mock_manage_logs.assert_called_once()
        mock_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
