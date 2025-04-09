"""
Set up Redshift database schema for the rental marketplace analytics.

This script creates the multi-layer architecture in Redshift:
1. Raw Layer - Data as close as possible to the source
2. Curated Layer - Cleaned and transformed data
3. Presentation Layer - Aggregated data for reporting
"""

import logging
import psycopg2
import sys
import os
import boto3
import time
import json
from pathlib import Path
from botocore.exceptions import ClientError

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Now import from src
from src.config.settings import REDSHIFT_CONFIG, AWS_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class RedshiftSetup:
    """Handles Redshift database setup and schema creation."""

    def __init__(self):
        """Initialize Redshift connection parameters."""
        # Get Redshift connection details from AWS Secrets Manager or environment variables
        self.connection_name = REDSHIFT_CONFIG["connection"]
        self.database = REDSHIFT_CONFIG["database"]
        self.host = os.getenv("REDSHIFT_HOST", "")
        self.port = int(os.getenv("REDSHIFT_PORT", "5439"))
        self.user = os.getenv("REDSHIFT_USER", "")
        self.password = os.getenv("REDSHIFT_PASSWORD", "")
        self.conn = None

        # If host is not provided, try to get it from the Redshift cluster
        if not self.host or not self.user or not self.password:
            self._get_redshift_connection_details()

    def _get_redshift_connection_details(self):
        """Get Redshift connection details from AWS Redshift."""
        try:
            # Initialize Redshift client
            redshift = boto3.client("redshift", region_name=AWS_CONFIG["region"])

            # Get clusters
            response = redshift.describe_clusters()

            # Find the cluster with the matching identifier
            for cluster in response.get("Clusters", []):
                if self.connection_name in cluster.get("ClusterIdentifier", ""):
                    self.host = cluster.get("Endpoint", {}).get("Address", "")
                    self.port = cluster.get("Endpoint", {}).get("Port", 5439)
                    self.user = cluster.get("MasterUsername", "")
                    # We can't get the password from the API, so it must be provided in env vars
                    logger.info(f"Found Redshift cluster: {self.host}:{self.port}")
                    return

            logger.warning(
                f"Could not find Redshift cluster with identifier containing '{self.connection_name}'"
            )
        except Exception as e:
            logger.warning(
                f"Failed to get Redshift connection details from AWS: {str(e)}"
            )
            logger.warning("Will try to use environment variables instead")

        # If we get here, we couldn't find the cluster or there was an error
        # For development/testing, we'll use a mock connection
        if not self.host or not self.user or not self.password:
            logger.warning("Using mock Redshift connection for development/testing")
            self.host = "localhost"
            self.port = 5439
            self.user = "admin"
            self.password = "password"

    def connect(self) -> None:
        """Establish connection to Redshift."""
        try:
            # Check if we have all the required connection details
            if not self.host or not self.user or not self.password:
                raise ValueError(
                    "Missing Redshift connection details. "
                    "Please set REDSHIFT_HOST, REDSHIFT_USER, and REDSHIFT_PASSWORD environment variables."
                )

            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.database,
                    user=self.user,
                    password=self.password,
                )
                logger.info("Connected to Redshift successfully")
            except Exception as conn_error:
                logger.warning(f"Failed to connect to Redshift: {str(conn_error)}")

        except Exception as e:
            logger.error(f"Failed to set up Redshift connection: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close Redshift connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Redshift")

    def execute_sql_file(self, file_path: str) -> None:
        """
        Execute SQL statements from a file.

        Args:
            file_path: Path to the SQL file
        """
        try:
            with open(file_path, "r") as f:
                sql = f.read()

            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                self.conn.commit()
                logger.info(f"Executed SQL file: {file_path}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to execute SQL file {file_path}: {str(e)}")
            raise

    def setup_schema(self) -> None:
        """Set up the complete Redshift schema."""
        # Get the SQL directory
        project_root = Path(__file__).parent.parent
        sql_dir = project_root / "sql" / "redshift"

        # Check if the directory exists
        if not sql_dir.exists():
            logger.error(f"SQL directory not found: {sql_dir}")
            raise FileNotFoundError(f"SQL directory not found: {sql_dir}")

        # List of SQL files to execute in order
        sql_files = [
            "00_create_schemas.sql",
            "01_create_raw_layer.sql",
            "02_create_curated_layer.sql",
            "03_create_presentation_layer.sql",
            "03_create_presentation_layer_metrics.sql",
        ]

        # Execute each SQL file
        for sql_file in sql_files:
            file_path = sql_dir / sql_file
            if not file_path.exists():
                logger.warning(f"SQL file not found: {file_path}")
                continue

            logger.info(f"Setting up schema from {file_path}")
            self.execute_sql_file(str(file_path))

    def populate_initial_data(self) -> None:
        """Populate the schema with initial data transformations."""
        # This method is intentionally left empty as data population
        # should be handled by the Glue jobs, not by this setup script.
        logger.info(
            "Data population will be handled by the ETL pipeline via Glue jobs."
        )


def create_iam_role_for_redshift() -> str:
    """Create an IAM role that allows Redshift to access other AWS services."""
    iam = boto3.client("iam")
    role_name = "RedshiftServiceRole"

    # Define the trust policy that allows Redshift to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        # Check if the role already exists
        try:
            response = iam.get_role(RoleName=role_name)
            logger.info(f"IAM role {role_name} already exists")
            return response["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            # Create the role if it doesn't exist
            response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Role for Redshift to access S3 and other AWS services",
            )
            logger.info(f"Created IAM role: {role_name}")

            # Attach policies to the role
            iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
            )
            logger.info("Attached S3 read policy to the role")

            # Wait for the role to be available
            time.sleep(10)

            return response["Role"]["Arn"]

    except Exception as e:
        logger.error(f"Error creating IAM role: {e}")
        raise


def create_security_group_for_redshift() -> str:
    """Create a security group for Redshift that allows inbound access on port 5439."""
    ec2 = boto3.client("ec2", region_name=AWS_CONFIG["region"])
    security_group_name = "redshift-security-group"

    try:
        # Check if the security group already exists
        response = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [security_group_name]}]
        )

        if response["SecurityGroups"]:
            security_group_id = response["SecurityGroups"][0]["GroupId"]
            logger.info(f"Security group {security_group_name} already exists")
            return security_group_id

        # Get the default VPC ID
        vpc_response = ec2.describe_vpcs(
            Filters=[{"Name": "isDefault", "Values": ["true"]}]
        )
        vpc_id = vpc_response["Vpcs"][0]["VpcId"]

        # Create the security group
        response = ec2.create_security_group(
            GroupName=security_group_name,
            Description="Security group for Redshift cluster",
            VpcId=vpc_id,
        )
        security_group_id = response["GroupId"]
        logger.info(f"Created security group: {security_group_name}")

        # Add inbound rule for Redshift port
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5439,
                    "ToPort": 5439,
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0",
                            "Description": "Redshift access from anywhere (Warning: Not secure for production)",
                        }
                    ],
                }
            ],
        )
        logger.info("Added inbound rule to allow Redshift access")

        return security_group_id

    except Exception as e:
        logger.error(f"Error creating security group: {e}")
        raise


def create_redshift_cluster(
    cluster_identifier: str,
    master_username: str,
    master_password: str,
    database_name: str,
    node_type: str = "dc2.large",
    num_nodes: int = 1,
) -> tuple:
    """Create a Redshift cluster or get existing cluster details."""
    try:
        redshift = boto3.client("redshift", region_name=AWS_CONFIG["region"])

        # Create IAM role for Redshift
        role_arn = create_iam_role_for_redshift()

        # Create security group for Redshift
        security_group_id = create_security_group_for_redshift()

        try:
            # Try to create new cluster
            response = redshift.create_cluster(
                ClusterIdentifier=cluster_identifier,
                NodeType=node_type,
                MasterUsername=master_username,
                MasterUserPassword=master_password,
                DBName=database_name,
                ClusterType="single-node" if num_nodes == 1 else "multi-node",
                NumberOfNodes=1 if num_nodes == 1 else num_nodes,
                PubliclyAccessible=True,
                VpcSecurityGroupIds=[security_group_id],
                IamRoles=[role_arn],
                Port=REDSHIFT_CONFIG["port"],
                Tags=[
                    {"Key": "Project", "Value": "RentalMarketplace"},
                ],
            )
            logger.info(f"Creating new Redshift cluster: {cluster_identifier}")
            status = "created"

        except ClientError as e:
            if "ClusterAlreadyExists" in str(e):
                logger.info(f"Redshift cluster {cluster_identifier} already exists")
                status = "exists"
            else:
                raise

        # Wait for the cluster to be available
        max_retries = (
            20  # Maximum number of retries (10 minutes at 30 seconds per retry)
        )
        retries = 0

        while retries < max_retries:
            try:
                response = redshift.describe_clusters(
                    ClusterIdentifier=cluster_identifier
                )
                cluster_status = response["Clusters"][0]["ClusterStatus"]

                # Check if the cluster has an endpoint (it won't until it's being provisioned)
                if "Endpoint" in response["Clusters"][0]:
                    endpoint = response["Clusters"][0]["Endpoint"]["Address"]
                    port = response["Clusters"][0]["Endpoint"]["Port"]

                    if cluster_status == "available":
                        logger.info("Redshift cluster is ready!")
                        logger.info(f"Endpoint: {endpoint}")
                        logger.info(f"Port: {port}")
                        return endpoint, port, status
                else:
                    logger.info("Cluster is being provisioned, no endpoint yet...")

                logger.info(
                    f"Waiting for Redshift cluster to be available... Status: {cluster_status}"
                )
                retries += 1
                time.sleep(30)

            except Exception as e:
                logger.error(f"Error checking cluster status: {e}")
                retries += 1
                time.sleep(30)

        # If we get here, the cluster didn't become available in time
        logger.error("Timed out waiting for Redshift cluster to become available")
        logger.error(
            "You can check the status in the AWS console and run the script again later"
        )
        raise TimeoutError("Timed out waiting for Redshift cluster")

    except ClientError as e:
        logger.error(f"Error with Redshift cluster: {e}")
        raise


def ensure_redshift_cluster_exists():
    """Ensure that a Redshift cluster exists and update the configuration."""
    try:
        # Check if we have credentials for Redshift
        if not REDSHIFT_CONFIG["user"] or not REDSHIFT_CONFIG["password"]:
            logger.warning(
                "Redshift username or password not set in environment variables"
            )
            logger.warning("Using default values for development/testing")
            REDSHIFT_CONFIG["user"] = "admin"
            REDSHIFT_CONFIG["password"] = "Admin123!"

        # Check if we already have a host
        if REDSHIFT_CONFIG["host"]:
            logger.info(f"Using existing Redshift host: {REDSHIFT_CONFIG['host']}")
            return True

        # Try to create or get the Redshift cluster
        logger.info("Creating or getting Redshift cluster...")
        try:
            endpoint, port, status = create_redshift_cluster(
                cluster_identifier=REDSHIFT_CONFIG["connection"],
                master_username=REDSHIFT_CONFIG["user"],
                master_password=REDSHIFT_CONFIG["password"],
                database_name=REDSHIFT_CONFIG["database"],
            )

            # Update the configuration with the actual endpoint
            REDSHIFT_CONFIG["host"] = endpoint
            REDSHIFT_CONFIG["port"] = port

            # Update the .env file with the Redshift details
            env_path = project_root / ".env"
            if env_path.exists():
                with open(env_path, "r") as f:
                    env_content = f.read()

                # Update or add Redshift host and port
                if "REDSHIFT_HOST=" in env_content:
                    env_content = env_content.replace(
                        "REDSHIFT_HOST="
                        + env_content.split("REDSHIFT_HOST=")[1].split("\n")[0],
                        f"REDSHIFT_HOST={endpoint}",
                    )
                else:
                    env_content += f"\nREDSHIFT_HOST={endpoint}\n"

                if "REDSHIFT_PORT=" in env_content:
                    env_content = env_content.replace(
                        "REDSHIFT_PORT="
                        + env_content.split("REDSHIFT_PORT=")[1].split("\n")[0],
                        f"REDSHIFT_PORT={port}",
                    )
                else:
                    env_content += f"REDSHIFT_PORT={port}\n"

                # Write updated content back to .env file
                with open(env_path, "w") as f:
                    f.write(env_content)

                logger.info("Updated .env file with Redshift details")

            return True

        except Exception as e:
            logger.error(f"Failed to create or get Redshift cluster: {e}")
            logger.warning("Using mock connection for development/testing")
            return False

    except Exception as e:
        logger.error(f"Error ensuring Redshift cluster exists: {e}")
        return False


def main():
    """Main function to set up Redshift schema."""
    try:
        # First, ensure that a Redshift cluster exists
        ensure_redshift_cluster_exists()

        redshift = RedshiftSetup()

        try:
            # Connect to Redshift
            redshift.connect()

            # Set up schema only
            redshift.setup_schema()

            # Note: Data population is handled by the ETL pipeline via Glue jobs
            # and not by this setup script

            logger.info("Redshift setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Redshift setup failed: {str(e)}")
            return False
        finally:
            # Disconnect from Redshift
            redshift.disconnect()
    except Exception as e:
        logger.error(f"Failed to set up Redshift: {str(e)}")
        return False


if __name__ == "__main__":
    main()
