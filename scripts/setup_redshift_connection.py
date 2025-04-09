#!/usr/bin/env python3
"""
Script to set up the Redshift connection in AWS Glue.
"""

import os
import boto3
import logging
import argparse
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def setup_redshift_connection():
    """Set up the Redshift connection in AWS Glue."""
    # Load environment variables
    load_dotenv()

    # Get Redshift connection details from environment variables
    redshift_host = os.getenv("REDSHIFT_HOST")
    redshift_port = os.getenv("REDSHIFT_PORT", "5439")
    redshift_database = os.getenv("REDSHIFT_DATABASE", "rental_marketplace")
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_password = os.getenv("REDSHIFT_PASSWORD")
    aws_region = os.getenv("AWS_REGION", "eu-west-1")

    # Check if we have all the required connection details
    if not redshift_host or not redshift_user or not redshift_password:
        logger.error(
            "Missing Redshift connection details. "
            "Please set REDSHIFT_HOST, REDSHIFT_USER, and REDSHIFT_PASSWORD environment variables."
        )
        return False

    # Initialize Glue client
    glue = boto3.client("glue", region_name=aws_region)

    # Connection name
    connection_name = "rental-marketplace-redshift"

    # Check if connection already exists
    try:
        response = glue.get_connection(Name=connection_name)
        logger.info(f"Connection {connection_name} already exists.")

        # Update the connection
        logger.info(f"Updating connection {connection_name}...")
        try:
            response = glue.update_connection(
                Name=connection_name,
                ConnectionInput={
                    "Name": connection_name,
                    "Description": "Connection to Redshift for rental marketplace ETL",
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_database}",
                        "USERNAME": redshift_user,
                        "PASSWORD": redshift_password,
                    },
                    # Remove PhysicalConnectionRequirements for simplicity
                    # AWS will use default VPC settings
                },
            )
            logger.info(
                f"Connection {connection_name} updated successfully: {response}"
            )
        except Exception as e:
            logger.error(f"Failed to update connection {connection_name}: {str(e)}")
            return False

    except glue.exceptions.EntityNotFoundException:
        # Create the connection
        logger.info(f"Creating connection {connection_name}...")
        try:
            response = glue.create_connection(
                ConnectionInput={
                    "Name": connection_name,
                    "Description": "Connection to Redshift for rental marketplace ETL",
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_database}",
                        "USERNAME": redshift_user,
                        "PASSWORD": redshift_password,
                    },
                    # Remove PhysicalConnectionRequirements for simplicity
                    # AWS will use default VPC settings
                }
            )
            logger.info(
                f"Connection {connection_name} created successfully: {response}"
            )
        except Exception as e:
            logger.error(f"Failed to create connection {connection_name}: {str(e)}")
            return False

    except Exception as e:
        logger.error(f"Failed to set up Redshift connection: {str(e)}")
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set up Redshift connection in AWS Glue."
    )
    args = parser.parse_args()

    if setup_redshift_connection():
        logger.info("Redshift connection setup completed successfully.")
    else:
        logger.error("Redshift connection setup failed.")
