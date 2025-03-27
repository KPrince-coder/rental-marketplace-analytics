"""
Script to update RDS security group rules to allow MySQL access from anywhere.
WARNING: This opens database access to all IPs. Use only for development/testing.
"""

import boto3


def update_security_group_rules(security_group_id: str):
    """
    Update security group rules to allow MySQL access from anywhere.

    Args:
        security_group_id: The ID of the security group to update
    """
    try:
        # Initialize boto3 client
        ec2 = boto3.client("ec2")

        # First check if the rule already exists
        response = ec2.describe_security_groups(GroupIds=[security_group_id])
        existing_rules = response["SecurityGroups"][0]["IpPermissions"]

        # Check if our desired rule already exists
        rule_exists = False
        for rule in existing_rules:
            if (
                rule.get("IpProtocol") == "tcp"
                and rule.get("FromPort") == 3306
                and rule.get("ToPort") == 3306
                and any(
                    ip_range.get("CidrIp") == "0.0.0.0/0"
                    for ip_range in rule.get("IpRanges", [])
                )
            ):
                rule_exists = True
                break

        if rule_exists:
            print("Universal access rule already exists")
            return

        # Add inbound rule for all IPs if it doesn't exist
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 3306,
                    "ToPort": 3306,
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0",
                            "Description": "MySQL access from anywhere (Warning: Not secure for production)",
                        }
                    ],
                }
            ],
        )

        print("Successfully added inbound rule to allow access from all IPs")
        print("WARNING: Your database is now accessible from anywhere!")
        print("This configuration should only be used for development/testing.")

    except Exception as e:
        print(f"Error: {e}")
        raise


def get_rds_security_group():
    """
    Get the security group ID associated with the RDS instance.
    """
    try:
        rds = boto3.client("rds")
        response = rds.describe_db_instances(
            DBInstanceIdentifier="rental-marketplace-db"
        )

        # Get the first security group ID associated with the RDS instance
        security_group_id = response["DBInstances"][0]["VpcSecurityGroups"][0][
            "VpcSecurityGroupId"
        ]
        return security_group_id
    except Exception as e:
        print(f"Error getting RDS security group: {e}")
        raise


if __name__ == "__main__":
    try:
        # Get the security group ID automatically
        security_group_id = get_rds_security_group()
        print(f"Found RDS security group: {security_group_id}")

        # Update the security group rules
        update_security_group_rules(security_group_id)

        print("\nNext steps:")
        print("1. Wait a few minutes for the security group changes to propagate")
        print("2. Try connecting to your database again")
        print("3. For production, remember to restrict access to specific IP ranges")

    except Exception as e:
        print(f"Failed to update security group: {e}")
