"""
Main script for the Rental Marketplace ETL Pipeline.

This script provides commands to:
1. Set up the RDS MySQL database
2. Set up the S3 bucket
3. Set up the Redshift schema
4. Deploy AWS Glue jobs and Step Function
5. Run the ETL pipeline

Run with:
    python main.py --setup-all
    python main.py --trigger-step-function
"""

import argparse
import logging
import os
import sys
import json
import boto3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple
from dotenv import load_dotenv

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Load environment variables from .env file
dotenv_path = project_root / ".env"
load_dotenv(dotenv_path=dotenv_path)

# Import project modules
from src.config.settings import DB_CONFIG, AWS_CONFIG, REDSHIFT_CONFIG


# Configure logging
def setup_logging() -> None:
    """Configure logging for the application."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = f"{log_dir}/rental_marketplace_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Rental Marketplace Analytics Pipeline"
    )

    # Setup options
    setup_group = parser.add_argument_group("Setup Options")
    setup_group.add_argument(
        "--setup-all",
        action="store_true",
        help="Run all setup steps (RDS, S3, Redshift, AWS deployment)",
    )
    setup_group.add_argument(
        "--setup-rds",
        action="store_true",
        help="Set up the RDS MySQL database",
    )
    setup_group.add_argument(
        "--setup-s3",
        action="store_true",
        help="Set up the S3 bucket",
    )
    setup_group.add_argument(
        "--setup-redshift",
        action="store_true",
        help="Set up the Redshift schema",
    )
    setup_group.add_argument(
        "--setup-iam-roles",
        action="store_true",
        help="Set up IAM roles for AWS Glue and Step Functions",
    )
    setup_group.add_argument(
        "--deploy-aws",
        action="store_true",
        help="Deploy AWS Glue jobs and Step Function",
    )
    setup_group.add_argument(
        "--trigger-step-function",
        action="store_true",
        help="Trigger the Step Function",
    )

    return parser.parse_args()


def setup_rds() -> Tuple[str, int, bool]:
    """
    Set up the RDS MySQL database.

    Returns:
        Tuple containing (endpoint, port, success)
    """
    logger = logging.getLogger(__name__)
    logger.info("Setting up RDS MySQL database...")

    try:
        # Initialize RDS client
        rds = boto3.client("rds", region_name=AWS_CONFIG["region"])

        # Check if the DB instance already exists
        try:
            response = rds.describe_db_instances(
                DBInstanceIdentifier="rental-marketplace-db"
            )

            # Get the endpoint and port
            endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
            port = response["DBInstances"][0]["Endpoint"]["Port"]

            logger.info(f"RDS instance already exists: {endpoint}:{port}")
            return endpoint, port, True

        except rds.exceptions.DBInstanceNotFoundFault:
            # Create the DB instance
            logger.info("Creating new RDS instance...")

            response = rds.create_db_instance(
                DBInstanceIdentifier="rental-marketplace-db",
                AllocatedStorage=20,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername=DB_CONFIG["user"],
                MasterUserPassword=DB_CONFIG["password"],
                DBName=DB_CONFIG["database"],
                Port=DB_CONFIG["port"],
                PubliclyAccessible=True,
                BackupRetentionPeriod=7,
                MultiAZ=False,
                EngineVersion="8.0.28",
                AutoMinorVersionUpgrade=True,
                Tags=[
                    {"Key": "Project", "Value": "RentalMarketplace"},
                ],
            )

            # Wait for the DB instance to be available
            logger.info("Waiting for RDS instance to be available...")
            waiter = rds.get_waiter("db_instance_available")
            waiter.wait(DBInstanceIdentifier="rental-marketplace-db")

            # Get the endpoint and port
            response = rds.describe_db_instances(
                DBInstanceIdentifier="rental-marketplace-db"
            )
            endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
            port = response["DBInstances"][0]["Endpoint"]["Port"]

            logger.info(f"RDS instance created: {endpoint}:{port}")
            return endpoint, port, True

    except Exception as e:
        logger.error(f"Failed to set up RDS: {str(e)}")
        return DB_CONFIG["host"], DB_CONFIG["port"], False


def setup_s3() -> bool:
    """
    Set up the S3 bucket.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Setting up S3 bucket: {AWS_CONFIG['s3_bucket']}")

    try:
        # Initialize S3 client
        s3 = boto3.client("s3", region_name=AWS_CONFIG["region"])

        # Check if the bucket already exists
        try:
            s3.head_bucket(Bucket=AWS_CONFIG["s3_bucket"])
            logger.info(f"S3 bucket already exists: {AWS_CONFIG['s3_bucket']}")
        except Exception:
            # Create bucket
            s3.create_bucket(
                Bucket=AWS_CONFIG["s3_bucket"],
                CreateBucketConfiguration={
                    "LocationConstraint": AWS_CONFIG["region"]
                },
            )
            logger.info(f"Created S3 bucket: {AWS_CONFIG['s3_bucket']}")

        # Create directories
        directories = [
            "raw/",
            "processed/",
            "metrics/",
            "scripts/",
            "temp/",
        ]

        for directory in directories:
            s3.put_object(
                Bucket=AWS_CONFIG["s3_bucket"],
                Key=directory,
            )
            logger.info(f"Created directory: s3://{AWS_CONFIG['s3_bucket']}/{directory}")

        logger.info("S3 setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to set up S3: {str(e)}")
        return False


def setup_redshift() -> bool:
    """
    Set up the Redshift schema.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info("Setting up Redshift schema...")

    try:
        # Import the setup script
        from scripts.setup_redshift import main as setup_redshift_main

        # Run the setup script
        if not setup_redshift_main():
            logger.error("Failed to set up Redshift schema")
            return False

        logger.info("Redshift setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to set up Redshift: {str(e)}")
        return False


def setup_iam_roles() -> bool:
    """
    Set up IAM roles for AWS Glue and Step Functions.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info("Setting up IAM roles...")

    try:
        # Import the setup script
        from scripts.setup_iam_roles import main as setup_iam_roles_main

        # Run the setup script
        if not setup_iam_roles_main():
            logger.error("Failed to set up IAM roles")
            return False

        logger.info("IAM roles setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to set up IAM roles: {str(e)}")
        return False


def deploy_aws() -> bool:
    """
    Deploy AWS Glue jobs and Step Function.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info("Deploying AWS resources...")

    try:
        # First, ensure IAM roles are set up
        if not setup_iam_roles():
            logger.error("Cannot deploy AWS resources without IAM roles")
            return False

        # Import the deployment script
        from scripts.deploy_glue_jobs import main as deploy_main

        # Deploy AWS resources
        if not deploy_main():
            logger.error("Failed to deploy AWS resources")
            return False

        logger.info("AWS resources deployed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to deploy AWS resources: {str(e)}")
        return False


def stop_running_glue_jobs() -> bool:
    """
    Stop running Glue jobs to free up concurrency slots.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info("Stopping running Glue jobs...")

    try:
        # Initialize Glue client
        glue = boto3.client("glue", region_name=AWS_CONFIG["region"])

        # Get all job runs for the specified jobs
        job_names = [
            "mysql-to-s3-extraction",
            "transform-data",
            "s3-to-redshift-loading",
            "processed-to-redshift",
            "generate-metrics"
        ]

        # First, get all running jobs across all job names
        all_running_jobs = []

        for job_name in job_names:
            try:
                # Get all job runs for the specified job
                response = glue.get_job_runs(JobName=job_name)

                # Filter for running jobs
                running_job_runs = [
                    {"JobName": job_name, "JobRunId": job_run.get("Id"), "State": job_run.get("JobRunState")}
                    for job_run in response.get("JobRuns", [])
                    if job_run.get("JobRunState") in ["STARTING", "RUNNING", "STOPPING"]
                ]

                all_running_jobs.extend(running_job_runs)

                if not running_job_runs:
                    logger.info(f"No running job runs found for job: {job_name}")
                    continue

                # Stop running jobs
                job_run_ids = [job_run["JobRunId"] for job_run in running_job_runs]
                logger.info(f"Found {len(job_run_ids)} running job runs to stop for {job_name}: {job_run_ids}")

                if job_run_ids:
                    response = glue.batch_stop_job_run(
                        JobName=job_name, JobRunIds=job_run_ids
                    )

                    successful_runs = response.get("SuccessfulSubmissions", [])
                    logger.info(f"Successfully stopped {len(successful_runs)} job runs for {job_name}")

            except Exception as e:
                logger.warning(f"Error stopping job runs for {job_name}: {str(e)}")

        # If we found any running jobs, wait for them to actually stop
        if all_running_jobs:
            logger.info(f"Waiting for {len(all_running_jobs)} jobs to fully stop...")

            # Wait for up to 2 minutes for jobs to stop
            max_wait_time = 120  # seconds
            wait_interval = 10  # seconds
            elapsed_time = 0

            while elapsed_time < max_wait_time:
                # Check if any jobs are still running
                still_running = False

                for job in all_running_jobs:
                    try:
                        response = glue.get_job_run(
                            JobName=job["JobName"],
                            RunId=job["JobRunId"]
                        )

                        current_state = response["JobRun"]["JobRunState"]

                        if current_state in ["STARTING", "RUNNING", "STOPPING"]:
                            logger.info(f"Job {job['JobName']} (ID: {job['JobRunId']}) is still in state: {current_state}")
                            still_running = True
                    except Exception as e:
                        logger.warning(f"Error checking job state: {str(e)}")

                if not still_running:
                    logger.info("All jobs have stopped successfully")
                    break

                # Wait before checking again
                import time
                time.sleep(wait_interval)
                elapsed_time += wait_interval

            if elapsed_time >= max_wait_time:
                logger.warning("Timed out waiting for jobs to stop. Some jobs may still be running.")

        # Also check for any other running jobs in the account
        try:
            # List all jobs
            all_jobs_response = glue.list_jobs()
            all_job_names = [job["Name"] for job in all_jobs_response.get("Jobs", [])]

            # Check each job for running instances
            for job_name in all_job_names:
                if job_name not in job_names:  # Only check jobs not in our main list
                    try:
                        response = glue.get_job_runs(JobName=job_name)

                        # Filter for running jobs
                        running_job_runs = [
                            job_run.get("Id")
                            for job_run in response.get("JobRuns", [])
                            if job_run.get("JobRunState") in ["STARTING", "RUNNING", "STOPPING"]
                        ]

                        if running_job_runs:
                            logger.info(f"Found {len(running_job_runs)} running job runs for other job: {job_name}")

                            # Stop these jobs too
                            response = glue.batch_stop_job_run(
                                JobName=job_name, JobRunIds=running_job_runs
                            )

                            successful_runs = response.get("SuccessfulSubmissions", [])
                            logger.info(f"Successfully stopped {len(successful_runs)} job runs for other job: {job_name}")
                    except Exception as e:
                        logger.warning(f"Error checking/stopping other job {job_name}: {str(e)}")
        except Exception as e:
            logger.warning(f"Error checking for other running jobs: {str(e)}")

        return True

    except Exception as e:
        logger.error(f"Failed to stop Glue jobs: {str(e)}")
        return False


def trigger_step_function() -> bool:
    """
    Trigger the Step Function.

    Returns:
        bool indicating success
    """
    logger = logging.getLogger(__name__)
    logger.info("Triggering Step Function...")

    try:
        # First, stop any running Glue jobs
        stop_running_glue_jobs()

        # Initialize Step Functions client
        sfn = boto3.client("stepfunctions", region_name=AWS_CONFIG["region"])

        # Get Step Function ARN
        step_function_arn = AWS_CONFIG["step_function_arn"]

        # Check if the ARN is valid
        if not step_function_arn or step_function_arn == "":
            logger.error("Step Function ARN is not set")
            return False

        # Prepare input parameters
        input_params = {
            "mysql_host": DB_CONFIG["host"],
            "mysql_port": str(DB_CONFIG["port"]),
            "mysql_database": DB_CONFIG["database"],
            "mysql_user": DB_CONFIG["user"],
            "mysql_password": DB_CONFIG["password"],
            "s3_bucket": AWS_CONFIG["s3_bucket"],
            "redshift_connection": REDSHIFT_CONFIG["connection"],
            "redshift_database": REDSHIFT_CONFIG["database"],
            "region": AWS_CONFIG["region"]
        }

        # Start execution
        response = sfn.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(input_params)
        )

        execution_arn = response["executionArn"]
        logger.info(f"Step Function execution started: {execution_arn}")

        # Wait for execution to complete
        logger.info("Waiting for execution to complete...")

        while True:
            # Get execution status
            execution = sfn.describe_execution(executionArn=execution_arn)
            status = execution["status"]

            if status == "RUNNING":
                logger.info("Execution still running, waiting...")
                # Wait for 30 seconds before checking again
                import time
                time.sleep(30)
            else:
                if status == "SUCCEEDED":
                    logger.info("Execution completed successfully!")
                    return True
                else:
                    logger.error(f"Execution failed with status: {status}")
                    if "output" in execution:
                        try:
                            output = json.loads(execution["output"])
                            if "error" in output:
                                logger.error(f"Error details: {output['error']}")
                        except Exception as e:
                            logger.error(f"Error parsing output: {str(e)}")
                    return False

    except Exception as e:
        logger.error(f"Failed to trigger Step Function: {str(e)}")
        return False


def main() -> None:
    """Main entry point for the Rental Marketplace Analytics Pipeline."""
    # Parse command line arguments
    args = parse_args()

    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Determine which steps to run
        run_setup_rds = args.setup_all or args.setup_rds
        run_setup_s3 = args.setup_all or args.setup_s3
        run_setup_redshift = args.setup_all or args.setup_redshift
        run_setup_iam_roles = args.setup_all or args.setup_iam_roles
        run_deploy_aws = args.setup_all or args.deploy_aws

        # Run setup steps
        if run_setup_rds:
            endpoint, port, _ = setup_rds()
            # Update DB_CONFIG with actual endpoint
            DB_CONFIG["host"] = endpoint
            DB_CONFIG["port"] = port

        if run_setup_s3 and not setup_s3():
            logger.error("Failed to set up S3. Exiting.")
            exit(1)

        if run_setup_redshift and not setup_redshift():
            logger.error("Failed to set up Redshift. Exiting.")
            exit(1)

        if run_setup_iam_roles and not setup_iam_roles():
            logger.error("Failed to set up IAM roles. Exiting.")
            exit(1)

        if run_deploy_aws and not deploy_aws():
            logger.error("Failed to deploy AWS resources. Exiting.")
            exit(1)

        # Trigger Step Function
        if args.trigger_step_function and not trigger_step_function():
            logger.error("Step Function execution failed. Exiting.")
            exit(1)

        logger.info("All operations completed successfully")
        exit(0)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
