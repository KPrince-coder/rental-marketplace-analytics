"""
Deploy AWS Glue jobs and Step Function for the ETL pipeline.

This script:
1. Creates AWS Glue jobs for each part of the ETL pipeline
2. Creates a Step Function to orchestrate the Glue jobs

Usage:
    python scripts/deploy_glue_jobs.py
"""

import os
import sys
import json
import logging
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(dotenv_path=project_root / ".env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
S3_BUCKET = os.getenv("S3_BUCKET", "rental-marketplace-data")
ROLE_ARN = os.getenv("GLUE_ROLE_ARN")
STEP_FUNCTION_ROLE_ARN = os.getenv("STEP_FUNCTION_ROLE_ARN")

# Glue job definitions
glue_jobs = [
    {
        "name": "mysql-to-s3-extraction",
        "description": "Extract data from MySQL and load it to S3",
        "script_path": "aws/mysql_to_s3_job.py",
        "max_capacity": 2.0,
        "timeout": 60,  # minutes
        "max_retries": 3,
        "glue_version": "3.0",
        "python_version": "3",
        "default_arguments": {
            "--job-language": "python",
            "--mysql_host": os.getenv("DB_HOST", "localhost"),
            "--mysql_port": os.getenv("DB_PORT", "3306"),
            "--mysql_database": os.getenv("DB_NAME", "rental_marketplace"),
            "--mysql_user": os.getenv("DB_USER", "root"),
            "--mysql_password": os.getenv("DB_PASSWORD", "password"),
            "--s3_bucket": S3_BUCKET,
            "--s3_prefix": "raw/",
            "--region": AWS_REGION,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }
    },
    {
        "name": "transform-data",
        "description": "Transform data in S3",
        "script_path": "aws/transform_data_job.py",
        "max_capacity": 2.0,
        "timeout": 60,  # minutes
        "max_retries": 3,
        "glue_version": "3.0",
        "python_version": "3",
        "default_arguments": {
            "--job-language": "python",
            "--s3_bucket": S3_BUCKET,
            "--raw_prefix": "raw/",
            "--processed_prefix": "processed/",
            "--region": AWS_REGION,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }
    },
    {
        "name": "s3-to-redshift-loading",
        "description": "Load raw data from S3 to Redshift",
        "script_path": "aws/s3_to_redshift_job.py",
        "max_capacity": 2.0,
        "timeout": 60,  # minutes
        "max_retries": 3,
        "glue_version": "3.0",
        "python_version": "3",
        "default_arguments": {
            "--job-language": "python",
            "--s3_bucket": S3_BUCKET,
            "--s3_prefix": "raw/",
            "--redshift_connection": os.getenv("REDSHIFT_CONNECTION", "rental-marketplace-redshift"),
            "--redshift_database": os.getenv("REDSHIFT_DATABASE", "rental_marketplace"),
            "--redshift_schema": "public",
            "--region": AWS_REGION,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }
    },
    {
        "name": "processed-to-redshift",
        "description": "Load processed data from S3 to Redshift",
        "script_path": "aws/processed_to_redshift_job.py",
        "max_capacity": 2.0,
        "timeout": 60,  # minutes
        "max_retries": 3,
        "glue_version": "3.0",
        "python_version": "3",
        "default_arguments": {
            "--job-language": "python",
            "--s3_bucket": S3_BUCKET,
            "--processed_prefix": "processed/",
            "--redshift_connection": os.getenv("REDSHIFT_CONNECTION", "rental-marketplace-redshift"),
            "--redshift_database": os.getenv("REDSHIFT_DATABASE", "rental_marketplace"),
            "--redshift_schema": "curated",
            "--region": AWS_REGION,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }
    },
    {
        "name": "generate-metrics",
        "description": "Generate metrics and load them to Redshift",
        "script_path": "aws/generate_metrics_job.py",
        "max_capacity": 2.0,
        "timeout": 60,  # minutes
        "max_retries": 3,
        "glue_version": "3.0",
        "python_version": "3",
        "default_arguments": {
            "--job-language": "python",
            "--s3_bucket": S3_BUCKET,
            "--processed_prefix": "processed/",
            "--metrics_prefix": "metrics/",
            "--redshift_connection": os.getenv("REDSHIFT_CONNECTION", "rental-marketplace-redshift"),
            "--redshift_database": os.getenv("REDSHIFT_DATABASE", "rental_marketplace"),
            "--redshift_schema": "presentation",
            "--region": AWS_REGION,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }
    }
]

def upload_script_to_s3(local_path, s3_key):
    """Upload a script to S3."""
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(str(local_path), S3_BUCKET, s3_key)
        logger.info(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")
        return f"s3://{S3_BUCKET}/{s3_key}"
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to S3: {str(e)}")
        return None

def create_or_update_glue_job(job_def):
    """Create or update an AWS Glue job."""
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)

        # Upload script to S3
        local_script_path = project_root / job_def["script_path"]
        s3_script_key = f"scripts/{os.path.basename(job_def['script_path'])}"
        s3_script_path = upload_script_to_s3(local_script_path, s3_script_key)

        if not s3_script_path:
            logger.error(f"Failed to upload script for job {job_def['name']}")
            return False

        # Check if job already exists
        try:
            glue.get_job(JobName=job_def["name"])
            job_exists = True
        except glue.exceptions.EntityNotFoundException:
            job_exists = False

        # Create or update job
        if job_exists:
            # For update, we need to use JobUpdate parameter without the Name
            job_update_params = {
                "Description": job_def["description"],
                "Role": ROLE_ARN,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": s3_script_path,
                    "PythonVersion": job_def["python_version"]
                },
                "DefaultArguments": job_def["default_arguments"],
                "MaxRetries": job_def["max_retries"],
                "Timeout": job_def["timeout"],
                "MaxCapacity": job_def["max_capacity"],
                "GlueVersion": job_def["glue_version"]
            }
            glue.update_job(JobName=job_def["name"], JobUpdate=job_update_params)
            logger.info(f"Updated Glue job: {job_def['name']}")
        else:
            # For create, we need to include the Name
            job_create_params = {
                "Name": job_def["name"],
                "Description": job_def["description"],
                "Role": ROLE_ARN,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": s3_script_path,
                    "PythonVersion": job_def["python_version"]
                },
                "DefaultArguments": job_def["default_arguments"],
                "MaxRetries": job_def["max_retries"],
                "Timeout": job_def["timeout"],
                "MaxCapacity": job_def["max_capacity"],
                "GlueVersion": job_def["glue_version"]
            }
            glue.create_job(**job_create_params)
            logger.info(f"Created Glue job: {job_def['name']}")

        return True

    except Exception as e:
        logger.error(f"Failed to create/update Glue job {job_def['name']}: {str(e)}")
        return False

def create_or_update_step_function():
    """Create or update the Step Function."""
    try:
        sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

        # Load Step Function definition
        with open(project_root / "aws" / "step_function_definition.json", "r") as f:
            definition = f.read()

        # Check if Step Function already exists
        step_function_name = "rental-marketplace-etl"
        step_function_arn = None

        try:
            response = sfn.list_state_machines()
            for state_machine in response["stateMachines"]:
                if state_machine["name"] == step_function_name:
                    step_function_arn = state_machine["stateMachineArn"]
                    break
        except Exception as e:
            logger.warning(f"Error checking for existing Step Function: {str(e)}")

        # Create or update Step Function
        if step_function_arn:
            sfn.update_state_machine(
                stateMachineArn=step_function_arn,
                definition=definition,
                roleArn=STEP_FUNCTION_ROLE_ARN
            )
            logger.info(f"Updated Step Function: {step_function_name}")
        else:
            response = sfn.create_state_machine(
                name=step_function_name,
                definition=definition,
                roleArn=STEP_FUNCTION_ROLE_ARN,
                type="STANDARD"
            )
            step_function_arn = response["stateMachineArn"]
            logger.info(f"Created Step Function: {step_function_name}")

        # Save the Step Function ARN to .env file
        with open(project_root / ".env", "a") as f:
            f.write(f"\nSTEP_FUNCTION_ARN={step_function_arn}\n")

        return True

    except Exception as e:
        logger.error(f"Failed to create/update Step Function: {str(e)}")
        return False

def main():
    """Main function."""
    try:
        # Check if required environment variables are set
        if not ROLE_ARN:
            logger.error("GLUE_ROLE_ARN environment variable is not set")
            return False

        if not STEP_FUNCTION_ROLE_ARN:
            logger.error("STEP_FUNCTION_ROLE_ARN environment variable is not set")
            return False

        # Create or update Glue jobs
        for job_def in glue_jobs:
            if not create_or_update_glue_job(job_def):
                logger.error(f"Failed to create/update Glue job: {job_def['name']}")
                return False

        # Create or update Step Function
        if not create_or_update_step_function():
            logger.error("Failed to create/update Step Function")
            return False

        logger.info("Successfully deployed all Glue jobs and Step Function")
        return True

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        return False

if __name__ == "__main__":
    main()
