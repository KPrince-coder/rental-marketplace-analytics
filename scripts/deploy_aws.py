"""
Deploy AWS resources for the rental marketplace ETL pipeline.
"""

import json
import boto3
import logging
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Now import from src
from src.config.settings import AWS_CONFIG

logger = logging.getLogger(__name__)


def create_glue_job(glue_client, job_name: str, script_location: str) -> None:
    """Create or update AWS Glue job."""
    try:
        glue_client.create_job(
            Name=job_name,
            Role=AWS_CONFIG["glue_role_arn"],
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3.9",
            },
            DefaultArguments={
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-language": "python",
            },
            MaxRetries=3,
            Timeout=2880,  # 48 hours
            WorkerType="G.1X",
            NumberOfWorkers=2,
            GlueVersion="3.0",
        )
        logger.info(f"Created Glue job: {job_name}")
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Updating existing Glue job: {job_name}")
        glue_client.update_job(
            JobName=job_name,
            JobUpdate={
                "Role": AWS_CONFIG["glue_role_arn"],
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": script_location,
                    "PythonVersion": "3.9",
                },
                "DefaultArguments": {
                    "--enable-metrics": "",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--job-language": "python",
                },
                "MaxRetries": 3,
                "Timeout": 2880,
                "WorkerType": "G.1X",
                "NumberOfWorkers": 2,
                "GlueVersion": "3.0",
            },
        )


def create_step_function(
    sfn_client, state_machine_name: str, definition_file: str, role_arn: str
) -> str:
    """Create or update AWS Step Function state machine."""
    try:
        # Load state machine definition
        with open(definition_file, "r") as f:
            definition = json.load(f)

        # Replace placeholders with actual values
        definition_str = (
            json.dumps(definition)
            .replace(
                "${GlueJobArn}",
                f"arn:aws:glue:{AWS_CONFIG['region']}:{AWS_CONFIG['account_id']}:job/{AWS_CONFIG['glue_job_name']}",
            )
            .replace("${GlueJobName}", AWS_CONFIG["glue_job_name"])
            .replace("${SNSTopicArn}", AWS_CONFIG["sns_topic_arn"])
        )

        # Create state machine
        response = sfn_client.create_state_machine(
            name=state_machine_name,
            definition=definition_str,
            roleArn=role_arn,
            type="STANDARD",
            loggingConfiguration={"level": "ALL", "includeExecutionData": True},
        )
        logger.info(f"Created Step Function: {state_machine_name}")
        return response["stateMachineArn"]

    except sfn_client.exceptions.StateMachineAlreadyExists:
        # Update existing state machine
        state_machine_arn = f"arn:aws:states:{AWS_CONFIG['region']}:{AWS_CONFIG['account_id']}:stateMachine:{state_machine_name}"
        sfn_client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=definition_str,
            roleArn=role_arn,
        )
        logger.info(f"Updated Step Function: {state_machine_name}")
        return state_machine_arn


def main():
    """Deploy AWS resources."""
    # Initialize AWS clients
    glue = boto3.client("glue", region_name=AWS_CONFIG["region"])
    sfn = boto3.client("stepfunctions", region_name=AWS_CONFIG["region"])

    # Get project root directory
    project_root = Path(__file__).parent.parent

    # Create Glue job
    script_location = f"s3://{AWS_CONFIG['s3_bucket']}/scripts/etl_job.py"
    create_glue_job(glue, AWS_CONFIG["glue_job_name"], script_location)

    # Create Step Function
    definition_file = project_root / "aws" / "step_function.json"
    state_machine_arn = create_step_function(
        sfn,
        "rental-marketplace-etl",
        str(definition_file),
        AWS_CONFIG["step_function_role_arn"],
    )

    logger.info("AWS resource deployment completed successfully")
    logger.info(f"Step Function ARN: {state_machine_arn}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
