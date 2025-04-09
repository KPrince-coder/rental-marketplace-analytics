"""
Trigger the ETL pipeline by starting the Step Function execution.

This script:
1. Starts the Step Function execution with the necessary parameters
2. Waits for the execution to complete
3. Reports the execution status

Usage:
    python scripts/run_etl_pipeline.py
"""

import os
import sys
import json
import logging
import boto3
import time
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

# Get the latest Step Function ARN
try:
    # Try to get the ARN from AWS
    sfn = boto3.client("stepfunctions", region_name=AWS_REGION)
    response = sfn.list_state_machines()
    for state_machine in response.get("stateMachines", []):
        if state_machine["name"] == "rental-marketplace-etl":
            STEP_FUNCTION_ARN = state_machine["stateMachineArn"]
            break
    else:
        # If not found, use the environment variable
        STEP_FUNCTION_ARN = os.getenv(
            "STEP_FUNCTION_ARN",
            "arn:aws:states:eu-west-1:529088286633:stateMachine:rental-marketplace-etl",
        )
except Exception as e:
    logger.warning(f"Failed to get Step Function ARN from AWS: {str(e)}")
    # Use the environment variable as fallback
    STEP_FUNCTION_ARN = os.getenv(
        "STEP_FUNCTION_ARN",
        "arn:aws:states:eu-west-1:529088286633:stateMachine:rental-marketplace-etl",
    )


def stop_running_glue_jobs():
    """Stop running Glue jobs to free up concurrency slots."""
    logger.info("Stopping running Glue jobs...")

    try:
        # Initialize Glue client
        glue = boto3.client("glue", region_name=AWS_REGION)

        # Get all job runs for the specified jobs
        job_names = [
            "mysql-to-s3-extraction",
            "transform-data",
            "s3-to-redshift-loading",
            "processed-to-redshift",
            "generate-metrics",
        ]

        # First, get all running jobs across all job names
        all_running_jobs = []

        for job_name in job_names:
            try:
                # Get all job runs for the specified job
                response = glue.get_job_runs(JobName=job_name)

                # Filter for running jobs
                running_job_runs = [
                    {
                        "JobName": job_name,
                        "JobRunId": job_run.get("Id"),
                        "State": job_run.get("JobRunState"),
                    }
                    for job_run in response.get("JobRuns", [])
                    if job_run.get("JobRunState") in ["STARTING", "RUNNING", "STOPPING"]
                ]

                all_running_jobs.extend(running_job_runs)

                if not running_job_runs:
                    logger.info(f"No running job runs found for job: {job_name}")
                    continue

                # Stop running jobs
                job_run_ids = [job_run["JobRunId"] for job_run in running_job_runs]
                logger.info(
                    f"Found {len(job_run_ids)} running job runs to stop for {job_name}: {job_run_ids}"
                )

                if job_run_ids:
                    response = glue.batch_stop_job_run(
                        JobName=job_name, JobRunIds=job_run_ids
                    )

                    successful_runs = response.get("SuccessfulSubmissions", [])
                    logger.info(
                        f"Successfully stopped {len(successful_runs)} job runs for {job_name}"
                    )

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
                            JobName=job["JobName"], RunId=job["JobRunId"]
                        )

                        current_state = response["JobRun"]["JobRunState"]

                        if current_state in ["STARTING", "RUNNING", "STOPPING"]:
                            logger.info(
                                f"Job {job['JobName']} (ID: {job['JobRunId']}) is still in state: {current_state}"
                            )
                            still_running = True
                    except Exception as e:
                        logger.warning(f"Error checking job state: {str(e)}")

                if not still_running:
                    logger.info("All jobs have stopped successfully")
                    break

                # Wait before checking again
                time.sleep(wait_interval)
                elapsed_time += wait_interval

            if elapsed_time >= max_wait_time:
                logger.warning(
                    "Timed out waiting for jobs to stop. Some jobs may still be running."
                )

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
                            if job_run.get("JobRunState")
                            in ["STARTING", "RUNNING", "STOPPING"]
                        ]

                        if running_job_runs:
                            logger.info(
                                f"Found {len(running_job_runs)} running job runs for other job: {job_name}"
                            )

                            # Stop these jobs too
                            response = glue.batch_stop_job_run(
                                JobName=job_name, JobRunIds=running_job_runs
                            )

                            successful_runs = response.get("SuccessfulSubmissions", [])
                            logger.info(
                                f"Successfully stopped {len(successful_runs)} job runs for other job: {job_name}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Error checking/stopping other job {job_name}: {str(e)}"
                        )
        except Exception as e:
            logger.warning(f"Error checking for other running jobs: {str(e)}")

        return True

    except Exception as e:
        logger.error(f"Failed to stop Glue jobs: {str(e)}")
        return False


def trigger_step_function():
    """Trigger the Step Function and wait for completion."""
    try:
        if not STEP_FUNCTION_ARN:
            logger.error("STEP_FUNCTION_ARN environment variable is not set")
            return False

        # First, stop any running Glue jobs
        stop_running_glue_jobs()

        # Initialize Step Functions client
        sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

        # Prepare input parameters
        input_params = {
            "mysql_host": os.getenv("DB_HOST", "localhost"),
            "mysql_port": os.getenv("DB_PORT", "3306"),
            "mysql_database": os.getenv("DB_NAME", "rental_marketplace"),
            "mysql_user": os.getenv("DB_USER", "root"),
            "mysql_password": os.getenv("DB_PASSWORD", "password"),
            "s3_bucket": S3_BUCKET,
            "redshift_connection": os.getenv(
                "REDSHIFT_CONNECTION", "rental-marketplace-redshift"
            ),
            "redshift_database": os.getenv("REDSHIFT_DATABASE", "rental_marketplace"),
            "redshift_host": os.getenv(
                "REDSHIFT_HOST",
                "rental-marketplace-redshift.cacze0tfwgno.eu-west-1.redshift.amazonaws.com",
            ),
            "redshift_port": os.getenv("REDSHIFT_PORT", "5439"),
            "redshift_user": os.getenv("REDSHIFT_USER", "admin"),
            "redshift_password": os.getenv(
                "REDSHIFT_PASSWORD", "MyRedshiftPassword123"
            ),
            "region": AWS_REGION,
        }

        # Start execution
        response = sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN, input=json.dumps(input_params)
        )

        execution_arn = response["executionArn"]
        logger.info(f"Started Step Function execution: {execution_arn}")

        # Wait for execution to complete
        logger.info("Waiting for execution to complete...")

        while True:
            # Get execution status
            execution = sfn.describe_execution(executionArn=execution_arn)
            status = execution["status"]

            if status == "RUNNING":
                logger.info("Execution still running, waiting...")
                time.sleep(30)  # Wait for 30 seconds before checking again
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


def main():
    """Main function."""
    try:
        # Trigger the Step Function
        if not trigger_step_function():
            logger.error("Failed to run ETL pipeline")
            return False

        logger.info("ETL pipeline completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to run ETL pipeline: {str(e)}")
        return False


if __name__ == "__main__":
    main()
