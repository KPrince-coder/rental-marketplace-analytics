"""
Integration tests for the rental marketplace ETL pipeline.
"""

import unittest
from datetime import datetime, timedelta
import logging
import os
import boto3
from moto import mock_s3, mock_glue, mock_cloudwatch, mock_sns
from src.etl.pipeline import ETLPipeline
from src.config.settings import AWS_CONFIG

logging.basicConfig(level=logging.INFO)


@mock_s3
@mock_glue
@mock_cloudwatch
@mock_sns
class TestETLPipeline(unittest.TestCase):
    """Test cases for ETL pipeline."""

    def setUp(self):
        """Set up test environment."""
        # Set up mock AWS services
        self.s3 = boto3.client("s3", region_name=AWS_CONFIG["region"])
        self.glue = boto3.client("glue", region_name=AWS_CONFIG["region"])
        self.cloudwatch = boto3.client("cloudwatch", region_name=AWS_CONFIG["region"])
        self.sns = boto3.client("sns", region_name=AWS_CONFIG["region"])

        # Create mock S3 bucket
        self.s3.create_bucket(Bucket=AWS_CONFIG["s3_bucket"])

        # Create mock SNS topic
        self.sns.create_topic(Name="rental-marketplace-alerts")

        # Initialize pipeline
        self.pipeline = ETLPipeline()

    def test_process_listings(self):
        """Test processing of apartment listings."""
        # Process listings for last 7 days
        result = self.pipeline.process_listings(days=7)

        # Verify processing succeeded
        self.assertIsNotNone(result)

        # Check S3 for output file
        response = self.s3.list_objects_v2(
            Bucket=AWS_CONFIG["s3_bucket"], Prefix="processed/listings/"
        )
        self.assertGreater(len(response.get("Contents", [])), 0)

    def test_process_user_activity(self):
        """Test processing of user activity data."""
        # Process activity for last 7 days
        result = self.pipeline.process_user_activity(days=7)

        # Verify processing succeeded
        self.assertIsNotNone(result)

        # Check S3 for output file
        response = self.s3.list_objects_v2(
            Bucket=AWS_CONFIG["s3_bucket"], Prefix="processed/activity/"
        )
        self.assertGreater(len(response.get("Contents", [])), 0)

    def test_process_metrics(self):
        """Test processing of marketplace metrics."""
        # Process metrics for last 30 days
        result = self.pipeline.process_metrics(days=30)

        # Verify processing succeeded
        self.assertIsNotNone(result)

        # Check S3 for output file
        response = self.s3.list_objects_v2(
            Bucket=AWS_CONFIG["s3_bucket"], Prefix="processed/metrics/"
        )
        self.assertGreater(len(response.get("Contents", [])), 0)

    def test_cloudwatch_metrics(self):
        """Test CloudWatch metrics are recorded."""
        # Run pipeline
        self.pipeline.run()

        # Check CloudWatch metrics
        response = self.cloudwatch.list_metrics(Namespace="RentalMarketplace/ETL")
        self.assertGreater(len(response["Metrics"]), 0)

    def test_retry_mechanism(self):
        """Test pipeline retry mechanism."""
        # Process with simulated failure
        with self.assertLogs(level="ERROR"):
            self.pipeline.process_listings(days=-1)  # Invalid input to trigger error

        # Check CloudWatch for error metrics
        response = self.cloudwatch.get_metric_statistics(
            Namespace="RentalMarketplace/ETL",
            MetricName="PipelineStatus",
            Dimensions=[{"Name": "DataType", "Value": "listings"}],
            StartTime=datetime.utcnow() - timedelta(minutes=5),
            EndTime=datetime.utcnow(),
            Period=300,
            Statistics=["Sum"],
        )
        self.assertGreater(len(response["Datapoints"]), 0)


if __name__ == "__main__":
    unittest.main()
