"""
Unit tests for log management.
"""

import unittest
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from unittest.mock import patch, Mock
import time

from src.monitoring.log_management import LogManager


class TestLogManager(unittest.TestCase):
    """Test cases for log management utilities."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for test logs
        self.test_dir = tempfile.mkdtemp()
        self.log_manager = LogManager(
            log_dir=self.test_dir, max_age_days=1, max_size_mb=1, backup_to_s3=False
        )

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def create_test_log(self, name: str, age_days: float = 0, size_mb: float = 0.1):
        """Create a test log file."""
        path = os.path.join(self.test_dir, f"{name}.log")

        # Create file with specified size
        with open(path, "w") as f:
            f.write("x" * int(size_mb * 1024 * 1024))

        # Set file modification time
        mtime = datetime.now() - timedelta(days=age_days)
        os.utime(path, (mtime.timestamp(), mtime.timestamp()))

        return path

    def test_get_log_files(self):
        """Test retrieving log files."""
        # Create test logs
        self.create_test_log("test1")
        self.create_test_log("test2")
        self.create_test_log("notlog", age_days=0)  # Should be ignored

        files = self.log_manager._get_log_files()
        self.assertEqual(len(files), 2)
        self.assertTrue(all(f.endswith(".log") for f in files))

    def test_get_file_age_days(self):
        """Test file age calculation."""
        age_days = 2.5
        path = self.create_test_log("old", age_days=age_days)

        calculated_age = self.log_manager._get_file_age_days(path)
        self.assertAlmostEqual(calculated_age, age_days, places=1)

    def test_get_dir_size(self):
        """Test directory size calculation."""
        # Create test logs of specific sizes
        self.create_test_log("small", size_mb=0.5)
        self.create_test_log("large", size_mb=1.5)

        size_bytes = self.log_manager._get_dir_size()
        expected_size = int(2 * 1024 * 1024)  # 2 MB total
        self.assertAlmostEqual(
            size_bytes, expected_size, delta=1024
        )  # Allow 1KB variance

    @patch("boto3.client")
    def test_backup_to_s3(self, mock_boto):
        """Test S3 backup functionality."""
        # Create log manager with S3 backup enabled
        manager = LogManager(log_dir=self.test_dir, backup_to_s3=True)

        # Create test log
        path = self.create_test_log("test")

        # Mock successful S3 upload
        mock_s3 = Mock()
        mock_boto.return_value = mock_s3

        key = manager._backup_to_s3(path)

        self.assertIsNotNone(key)
        mock_s3.upload_file.assert_called_once()

    def test_rotate_logs_by_age(self):
        """Test log rotation based on age."""
        # Create old and new logs
        old_log = self.create_test_log("old", age_days=2)
        new_log = self.create_test_log("new", age_days=0)

        self.log_manager.rotate_logs()

        # Old log should be archived
        self.assertFalse(os.path.exists(old_log))
        # New log should remain
        self.assertTrue(os.path.exists(new_log))

    def test_rotate_logs_by_size(self):
        """Test log rotation based on size."""
        # Create logs that exceed max size
        self.create_test_log("big1", size_mb=0.6)
        self.create_test_log("big2", size_mb=0.6)

        initial_count = len(self.log_manager._get_log_files())
        self.log_manager.rotate_logs()
        final_count = len(self.log_manager._get_log_files())

        # Should have archived at least one log
        self.assertLess(final_count, initial_count)

    def test_cleanup_archives(self):
        """Test archive cleanup."""
        archive_dir = os.path.join(self.test_dir, "archived")
        os.makedirs(archive_dir)

        # Create old and new archives
        old_archive = os.path.join(archive_dir, "old.log.20200101")
        new_archive = os.path.join(archive_dir, "new.log.20991231")

        with open(old_archive, "w") as f:
            f.write("old")
        with open(new_archive, "w") as f:
            f.write("new")

        # Set modification times
        os.utime(
            old_archive, (time.time() - 100 * 24 * 3600, time.time() - 100 * 24 * 3600)
        )

        self.log_manager.cleanup_archives(max_archive_age_days=30)

        # Old archive should be removed
        self.assertFalse(os.path.exists(old_archive))
        # New archive should remain
        self.assertTrue(os.path.exists(new_archive))

    def test_get_recent_errors(self):
        """Test retrieving recent error messages."""
        log_path = os.path.join(self.test_dir, "test.log")

        # Create log with error messages
        with open(log_path, "w") as f:
            f.write(f"{datetime.now().isoformat()} - ERROR - Test error 1\n")
            f.write(f"{datetime.now().isoformat()} - INFO - Test info\n")
            f.write(
                f"{(datetime.now() - timedelta(days=2)).isoformat()} - ERROR - Old error\n"
            )
            f.write(f"{datetime.now().isoformat()} - ERROR - Test error 2\n")

        errors = self.log_manager.get_recent_errors(hours=24)

        self.assertEqual(len(errors), 2)  # Should only get recent errors
        self.assertTrue(all("ERROR" in err for err in errors))


if __name__ == "__main__":
    unittest.main()
