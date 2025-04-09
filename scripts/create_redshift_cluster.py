"""
AWS Redshift cluster creation script for the Rental Marketplace Analytics Pipeline.

This script:
1. Creates a Redshift cluster if it doesn't exist
2. Creates the necessary IAM roles for Redshift
3. Configures security groups to allow access
4. Waits for the cluster to be available
5. Returns the endpoint and port for connecting to the cluster
"""

import boto3
import time
import json
import sys
from pathlib import Path
from botocore.exceptions import ClientError

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Now import from src
from src.config.settings import REDSHIFT_CONFIG, AWS_CONFIG


def get_redshift_endpoint(cluster_identifier: str) -> tuple:
    """
    Get the endpoint and port of an existing Redshift cluster.

    Args:
        cluster_identifier: Unique identifier for the Redshift cluster

    Returns:
        tuple: (endpoint, port)
    """
    redshift = boto3.client("redshift", region_name=AWS_CONFIG["region"])
    response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
    cluster = response["Clusters"][0]
    endpoint = cluster["Endpoint"]["Address"]
    port = cluster["Endpoint"]["Port"]
    return endpoint, port


def create_iam_role_for_redshift() -> str:
    """
    Create an IAM role that allows Redshift to access other AWS services.

    Returns:
        str: The ARN of the created IAM role
    """
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
            print(f"IAM role {role_name} already exists")
            return response["Role"]["Arn"]
        except iam.exceptions.NoSuchEntityException:
            # Create the role if it doesn't exist
            response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Role for Redshift to access S3 and other AWS services",
            )
            print(f"Created IAM role: {role_name}")

            # Attach policies to the role
            iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
            )
            print("Attached S3 read policy to the role")

            # Wait for the role to be available
            time.sleep(10)

            return response["Role"]["Arn"]

    except Exception as e:
        print(f"Error creating IAM role: {e}")
        raise


def create_security_group_for_redshift() -> str:
    """
    Create a security group for Redshift that allows inbound access on port 5439.

    Returns:
        str: The ID of the created security group
    """
    ec2 = boto3.client("ec2", region_name=AWS_CONFIG["region"])
    security_group_name = "redshift-security-group"

    try:
        # Check if the security group already exists
        response = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [security_group_name]}]
        )

        if response["SecurityGroups"]:
            security_group_id = response["SecurityGroups"][0]["GroupId"]
            print(f"Security group {security_group_name} already exists")
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
        print(f"Created security group: {security_group_name}")

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
        print("Added inbound rule to allow Redshift access")

        return security_group_id

    except Exception as e:
        print(f"Error creating security group: {e}")
        raise


def create_redshift_cluster(
    cluster_identifier: str,
    master_username: str,
    master_password: str,
    database_name: str,
    node_type: str = "dc2.large",
    num_nodes: int = 1,
) -> tuple:
    """
    Create a Redshift cluster or get existing cluster details.

    Args:
        cluster_identifier: Unique identifier for the Redshift cluster
        master_username: Master username for the Redshift cluster
        master_password: Master password for the Redshift cluster
        database_name: Name of the initial database to create
        node_type: The node type to be provisioned
        num_nodes: The number of compute nodes in the cluster

    Returns:
        tuple: (endpoint, port, status)
        status can be 'created' or 'exists'
    """
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
            print(f"Creating new Redshift cluster: {cluster_identifier}")
            status = "created"

        except ClientError as e:
            if "ClusterAlreadyExists" in str(e):
                print(f"Redshift cluster {cluster_identifier} already exists")
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
                        print("\nRedshift cluster is ready!")
                        print(f"Endpoint: {endpoint}")
                        print(f"Port: {port}")
                        return endpoint, port, status
                else:
                    print("Cluster is being provisioned, no endpoint yet...")

                print(
                    f"Waiting for Redshift cluster to be available... Status: {cluster_status}"
                )
                retries += 1
                time.sleep(30)

            except Exception as e:
                print(f"Error checking cluster status: {e}")
                retries += 1
                time.sleep(30)

        # If we get here, the cluster didn't become available in time
        print("Timed out waiting for Redshift cluster to become available")
        print(
            "You can check the status in the AWS console and run the script again later"
        )
        raise TimeoutError("Timed out waiting for Redshift cluster")

    except ClientError as e:
        print(f"Error with Redshift cluster: {e}")
        raise


def update_redshift_security_group(cluster_identifier: str):
    """
    Update the security group for the Redshift cluster to allow access from anywhere.

    Args:
        cluster_identifier: Unique identifier for the Redshift cluster
    """
    try:
        # Get the security group ID associated with the Redshift cluster
        redshift = boto3.client("redshift", region_name=AWS_CONFIG["region"])
        response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
        vpc_security_groups = response["Clusters"][0]["VpcSecurityGroups"]

        if not vpc_security_groups:
            print("No security groups found for the Redshift cluster")
            return

        security_group_id = vpc_security_groups[0]["VpcSecurityGroupId"]

        # Update the security group to allow access from anywhere
        ec2 = boto3.client("ec2", region_name=AWS_CONFIG["region"])

        # Check if the rule already exists
        response = ec2.describe_security_groups(GroupIds=[security_group_id])
        existing_rules = response["SecurityGroups"][0]["IpPermissions"]

        # Check if our desired rule already exists
        rule_exists = False
        for rule in existing_rules:
            if (
                rule.get("IpProtocol") == "tcp"
                and rule.get("FromPort") == 5439
                and rule.get("ToPort") == 5439
                and any(
                    ip_range.get("CidrIp") == "0.0.0.0/0"
                    for ip_range in rule.get("IpRanges", [])
                )
            ):
                rule_exists = True
                break

        if rule_exists:
            print("Universal access rule already exists for Redshift")
            return

        # Add inbound rule for all IPs if it doesn't exist
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

        print("Successfully added inbound rule to allow Redshift access from all IPs")
        print("WARNING: Your Redshift cluster is now accessible from anywhere!")
        print("This configuration should only be used for development/testing.")

    except Exception as e:
        print(f"Error updating security group: {e}")
        raise


def main():
    """Main function to create Redshift cluster and configure access."""
    try:
        # Print Redshift configuration for debugging
        print("Redshift Configuration:")
        print(f"User: {REDSHIFT_CONFIG['user']}")
        print(f"Database: {REDSHIFT_CONFIG['database']}")
        print(f"Host: {REDSHIFT_CONFIG['host']}")
        print(f"Port: {REDSHIFT_CONFIG['port']}")

        # Check if username and password are set
        if not REDSHIFT_CONFIG["user"] or not REDSHIFT_CONFIG["password"]:
            print("ERROR: Redshift username or password is not set in the .env file")
            print("Please set REDSHIFT_USER and REDSHIFT_PASSWORD in your .env file")
            return

        # Create Redshift cluster
        endpoint, port, status = create_redshift_cluster(
            cluster_identifier="rental-marketplace-redshift",
            master_username=REDSHIFT_CONFIG["user"],
            master_password=REDSHIFT_CONFIG["password"],
            database_name=REDSHIFT_CONFIG["database"],
        )

        # Update security group if needed
        update_redshift_security_group("rental-marketplace-redshift")

        # Update REDSHIFT_CONFIG with actual endpoint
        REDSHIFT_CONFIG["host"] = endpoint
        REDSHIFT_CONFIG["port"] = port

        print("\nRedshift cluster setup complete!")
        print(f"Endpoint: {endpoint}")
        print(f"Port: {port}")
        print(f"Database: {REDSHIFT_CONFIG['database']}")
        print(f"Username: {REDSHIFT_CONFIG['user']}")
        print(f"Status: {status}")

        print("\nNext steps:")
        print("1. Run scripts/setup_redshift.py to create the schema")
        print("2. Run the ETL pipeline to load data")

    except Exception as e:
        print(f"Failed to set up Redshift cluster: {e}")


if __name__ == "__main__":
    main()
