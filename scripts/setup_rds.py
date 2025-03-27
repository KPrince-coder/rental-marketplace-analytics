"""
AWS RDS MySQL database setup script for the Rental Marketplace Analytics Pipeline.
"""

import boto3
import time
from botocore.exceptions import ClientError

from src.config.settings import DB_CONFIG


def get_rds_endpoint(db_instance_identifier: str) -> tuple:
    """
    Get the endpoint and port of an existing RDS instance.

    Args:
        db_instance_identifier: Unique identifier for the DB instance

    Returns:
        tuple: (endpoint, port)
    """
    rds = boto3.client("rds")
    response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
    instance = response["DBInstances"][0]
    endpoint = instance["Endpoint"]["Address"]
    port = instance["Endpoint"]["Port"]
    return endpoint, port


def create_rds_instance(
    db_instance_identifier: str,
    master_username: str,
    master_password: str,
    db_name: str,
    instance_class: str = "db.t3.micro",
    allocated_storage: int = 20,
    engine_version: str = "8.0.40",
) -> tuple:
    """
    Create an RDS MySQL instance or get existing instance details.

    Args:
        db_instance_identifier: Unique identifier for the DB instance
        master_username: Master username for the DB instance
        master_password: Master password for the DB instance
        db_name: Name of the initial database to create
        instance_class: The compute and memory capacity of the DB instance
        allocated_storage: The amount of storage (in gibibytes) to allocate

    Returns:
        tuple: (endpoint, port, status)
        status can be 'created' or 'exists'
    """
    try:
        rds = boto3.client("rds")

        try:
            # Try to create new instance
            response = rds.create_db_instance(
                DBName=db_name,
                DBInstanceIdentifier=db_instance_identifier,
                AllocatedStorage=allocated_storage,
                DBInstanceClass=instance_class,
                Engine="mysql",
                EngineVersion=engine_version,
                MasterUsername=master_username,
                MasterUserPassword=master_password,
                PubliclyAccessible=True,
                BackupRetentionPeriod=30,
                MultiAZ=False,
                AutoMinorVersionUpgrade=True,
                Port=DB_CONFIG["port"],
                Tags=[
                    {"Key": "Project", "Value": "RentalMarketplace"},
                ],
            )
            print(f"Creating new RDS instance: {db_instance_identifier}")
            status = "created"

        except ClientError as e:
            if "DBInstanceAlreadyExists" in str(e):
                print(f"RDS instance {db_instance_identifier} already exists")
                status = "exists"
            else:
                raise

        # Wait for the database to be available
        while True:
            response = rds.describe_db_instances(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance_status = response["DBInstances"][0]["DBInstanceStatus"]
            endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
            port = response["DBInstances"][0]["Endpoint"]["Port"]

            if instance_status == "available":
                print("\nDatabase is ready!")
                print(f"Endpoint: {endpoint}")
                print(f"Port: {port}")
                return endpoint, port, status

            print("Waiting for database to be available...")
            time.sleep(30)

    except ClientError as e:
        print(f"Error with RDS instance: {e}")
        raise


if __name__ == "__main__":
    # Replace these values with your desired configuration
    endpoint, port, status = create_rds_instance(
        db_instance_identifier="rental-marketplace-db",
        master_username=DB_CONFIG["user"],
        master_password=DB_CONFIG["password"],
        db_name=DB_CONFIG["database"],
    )
