"""
Create AWS Step Function for the rental marketplace ETL pipeline.
"""

import os
import json
import boto3
import logging
from pathlib import Path
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Add the project root to Python path
project_root = Path(__file__).parent.parent

# Load environment variables
load_dotenv(dotenv_path=project_root / ".env")

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "rental-marketplace-etl")
STEP_FUNCTION_ROLE_ARN = os.getenv("STEP_FUNCTION_ROLE_ARN")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")


def create_step_function():
    """Create AWS Step Function for the ETL pipeline."""
    try:
        # Initialize Step Functions client
        sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

        # Load state machine definition
        step_function_definition_path = project_root / "aws" / "step_function.json"
        with open(step_function_definition_path, "r") as f:
            definition = json.load(f)

        # Replace placeholders with actual values
        definition_str = (
            json.dumps(definition)
            .replace(
                "${GlueJobArn}",
                f"arn:aws:glue:{AWS_REGION}:{AWS_ACCOUNT_ID}:job/{GLUE_JOB_NAME}",
            )
            .replace("${GlueJobName}", GLUE_JOB_NAME)
            .replace("${SNSTopicArn}", SNS_TOPIC_ARN)
        )

        # Create or update state machine
        step_function_name = "rental-marketplace-etl"

        try:
            # Check if state machine already exists
            response = sfn.describe_state_machine(
                stateMachineArn=f"arn:aws:states:{AWS_REGION}:{AWS_ACCOUNT_ID}:stateMachine:{step_function_name}"
            )

            # Update existing state machine
            sfn.update_state_machine(
                stateMachineArn=response["stateMachineArn"],
                definition=definition_str,
                roleArn=STEP_FUNCTION_ROLE_ARN,
            )

            logger.info(f"Updated Step Function: {step_function_name}")
            step_function_arn = response["stateMachineArn"]

        except sfn.exceptions.StateMachineDoesNotExist:
            # Create new state machine
            response = sfn.create_state_machine(
                name=step_function_name,
                definition=definition_str,
                roleArn=STEP_FUNCTION_ROLE_ARN,
                type="STANDARD",
            )

            logger.info(f"Created Step Function: {step_function_name}")
            step_function_arn = response["stateMachineArn"]

        # Update .env file with Step Function ARN
        env_path = project_root / ".env"
        with open(env_path, "r") as f:
            env_content = f.read()

        if "STEP_FUNCTION_ARN=" in env_content:
            env_content = env_content.replace(
                "STEP_FUNCTION_ARN="
                + env_content.split("STEP_FUNCTION_ARN=")[1].split("\n")[0],
                f"STEP_FUNCTION_ARN={step_function_arn}",
            )
        else:
            env_content += f"\nSTEP_FUNCTION_ARN={step_function_arn}\n"

        with open(env_path, "w") as f:
            f.write(env_content)

        logger.info(f"Updated .env file with Step Function ARN: {step_function_arn}")

        return step_function_arn

    except Exception as e:
        logger.error(f"Failed to create Step Function: {str(e)}")
        raise


def main():
    """Main function."""
    try:
        # Create Step Function
        step_function_arn = create_step_function()

        logger.info(f"Step Function created successfully: {step_function_arn}")
        return True

    except Exception as e:
        logger.error(f"Failed to create Step Function: {str(e)}")
        return False


if __name__ == "__main__":
    main()
