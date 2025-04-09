"""
Script to set up IAM roles for the rental marketplace ETL pipeline.

This script creates the necessary IAM roles for AWS Glue and Step Functions
with the appropriate permissions, and updates the .env file with the correct ARNs.
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

# Import project modules
from src.config.settings import AWS_CONFIG

# AWS configuration
AWS_REGION = AWS_CONFIG["region"]
S3_BUCKET = AWS_CONFIG["s3_bucket"]


def get_account_id():
    """Get the current AWS account ID."""
    sts = boto3.client("sts", region_name=AWS_REGION)
    return sts.get_caller_identity()["Account"]


def create_glue_role(role_name="RentalMarketplaceGlueRole"):
    """
    Create IAM role for AWS Glue with necessary permissions.

    Args:
        role_name: Name of the IAM role to create

    Returns:
        str: ARN of the created or existing role
    """
    logger.info(f"Setting up IAM role for AWS Glue: {role_name}")

    iam = boto3.client("iam", region_name=AWS_REGION)
    account_id = get_account_id()

    # Define trust policy for Glue
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Create inline policy for S3 and Redshift access
    s3_redshift_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "redshift:DescribeClusters",
                    "redshift:DescribeClusterSubnetGroups",
                    "redshift:DescribeClusterSecurityGroups",
                    "redshift:DescribeClusterParameters",
                    "redshift:DescribeClusterParameterGroups",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    f"arn:aws:logs:{AWS_REGION}:{account_id}:log-group:/aws/glue/*"
                ]
            }
        ]
    }

    try:
        # Try to get the role (check if it exists)
        try:
            response = iam.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
            logger.info(f"IAM role already exists: {role_arn}")
        except iam.exceptions.NoSuchEntityException:
            # Role doesn't exist, create it
            response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for AWS Glue ETL jobs in the rental marketplace pipeline"
            )
            role_arn = response["Role"]["Arn"]
            logger.info(f"Created new IAM role: {role_arn}")

            # Wait for role to be available
            waiter = iam.get_waiter('role_exists')
            waiter.wait(RoleName=role_name)

        # Attach AWS managed policy for Glue
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        logger.info(f"Attached AWSGlueServiceRole policy to {role_name}")

        # Create or update inline policy for S3 and Redshift access
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="S3RedshiftAccess",
            PolicyDocument=json.dumps(s3_redshift_policy)
        )
        logger.info(f"Added S3 and Redshift access policy to {role_name}")

        return role_arn

    except Exception as e:
        logger.error(f"Failed to create or update IAM role: {str(e)}")
        raise


def create_step_function_role(role_name="RentalMarketplaceStepFunctionRole"):
    """
    Create IAM role for AWS Step Functions with necessary permissions.

    Args:
        role_name: Name of the IAM role to create

    Returns:
        str: ARN of the created or existing role
    """
    logger.info(f"Setting up IAM role for AWS Step Functions: {role_name}")

    iam = boto3.client("iam", region_name=AWS_REGION)
    account_id = get_account_id()

    # Define trust policy for Step Functions
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Create inline policy for Glue and SNS access
    step_function_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun"
                ],
                "Resource": f"arn:aws:glue:{AWS_REGION}:{account_id}:job/*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "sns:Publish"
                ],
                "Resource": f"arn:aws:sns:{AWS_REGION}:{account_id}:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    f"arn:aws:logs:{AWS_REGION}:{account_id}:log-group:/aws/states/*"
                ]
            }
        ]
    }

    try:
        # Try to get the role (check if it exists)
        try:
            response = iam.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
            logger.info(f"IAM role already exists: {role_arn}")
        except iam.exceptions.NoSuchEntityException:
            # Role doesn't exist, create it
            response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for AWS Step Functions in the rental marketplace pipeline"
            )
            role_arn = response["Role"]["Arn"]
            logger.info(f"Created new IAM role: {role_arn}")

            # Wait for role to be available
            waiter = iam.get_waiter('role_exists')
            waiter.wait(RoleName=role_name)

        # Create or update inline policy for Glue and SNS access
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="GlueSNSAccess",
            PolicyDocument=json.dumps(step_function_policy)
        )
        logger.info(f"Added Glue and SNS access policy to {role_name}")

        return role_arn

    except Exception as e:
        logger.error(f"Failed to create or update IAM role: {str(e)}")
        raise


def update_env_file(glue_role_arn, step_function_role_arn):
    """
    Update the .env file with the correct role ARNs.

    Args:
        glue_role_arn: ARN of the Glue role
        step_function_role_arn: ARN of the Step Function role
    """
    logger.info("Updating .env file with role ARNs")

    env_path = project_root / ".env"

    # Read current .env file
    with open(env_path, "r") as f:
        env_content = f.read()

    # Update or add Glue role ARN
    if "GLUE_ROLE_ARN=" in env_content:
        env_content = env_content.replace(
            "GLUE_ROLE_ARN=" + env_content.split("GLUE_ROLE_ARN=")[1].split("\n")[0],
            f"GLUE_ROLE_ARN={glue_role_arn}"
        )
    else:
        env_content += f"\nGLUE_ROLE_ARN={glue_role_arn}\n"

    # Update or add Step Function role ARN
    if "STEP_FUNCTION_ROLE_ARN=" in env_content:
        env_content = env_content.replace(
            "STEP_FUNCTION_ROLE_ARN=" + env_content.split("STEP_FUNCTION_ROLE_ARN=")[1].split("\n")[0],
            f"STEP_FUNCTION_ROLE_ARN={step_function_role_arn}"
        )
    else:
        env_content += f"\nSTEP_FUNCTION_ROLE_ARN={step_function_role_arn}\n"

    # Write updated content back to .env file
    with open(env_path, "w") as f:
        f.write(env_content)

    logger.info("Updated .env file successfully")


def add_pass_role_policy(user_name=None):
    """
    Add iam:PassRole permission to the current IAM user or role.

    Args:
        user_name: Name of the IAM user to add the policy to. If None, tries to detect current user.
    """
    iam = boto3.client("iam", region_name=AWS_REGION)
    sts = boto3.client("sts", region_name=AWS_REGION)

    # Get current identity
    identity = sts.get_caller_identity()
    account_id = identity["Account"]

    # Determine if we're running as a user or role
    arn = identity["Arn"]
    if ":user/" in arn:
        if user_name is None:
            user_name = arn.split("/")[-1]

        logger.info(f"Adding PassRole policy to IAM user: {user_name}")

        # Define policy for passing roles to Glue and Step Functions
        pass_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "iam:PassRole",
                    "Resource": [
                        f"arn:aws:iam::{account_id}:role/RentalMarketplaceGlueRole",
                        f"arn:aws:iam::{account_id}:role/RentalMarketplaceStepFunctionRole"
                    ],
                    "Condition": {
                        "StringEquals": {
                            "iam:PassedToService": [
                                "glue.amazonaws.com",
                                "states.amazonaws.com"
                            ]
                        }
                    }
                }
            ]
        }

        try:
            # Add inline policy to user
            iam.put_user_policy(
                UserName=user_name,
                PolicyName="PassRoleToGlueAndStepFunctions",
                PolicyDocument=json.dumps(pass_role_policy)
            )
            logger.info(f"Added PassRole policy to user {user_name}")
        except Exception as e:
            logger.error(f"Failed to add PassRole policy to user: {str(e)}")
            logger.warning("You may need to manually add the PassRole permission")
    else:
        logger.info("Running as an IAM role, no need to add PassRole policy")


def main():
    """Main function to set up IAM roles."""
    try:
        # Create IAM roles
        glue_role_arn = create_glue_role()
        step_function_role_arn = create_step_function_role()

        # Update .env file
        update_env_file(glue_role_arn, step_function_role_arn)

        # Add PassRole policy to current user
        add_pass_role_policy()

        logger.info("IAM roles setup completed successfully")
        logger.info(f"Glue Role ARN: {glue_role_arn}")
        logger.info(f"Step Function Role ARN: {step_function_role_arn}")
        logger.info("You can now run 'python main.py --deploy-aws' to deploy AWS resources")

        return True

    except Exception as e:
        logger.error(f"Failed to set up IAM roles: {str(e)}")
        return False


if __name__ == "__main__":
    main()
