# AWS RDS MySQL Setup Guide

## Prerequisites

1. AWS Account with appropriate permissions
2. AWS CLI configured with credentials
3. Python environment with boto3 installed

## Steps to Create RDS Instance

### 1. Security Group Setup

First, create a security group for your RDS instance:

1. Go to AWS Console > VPC > Security Groups
2. Create a new security group
   - Name: rental-marketplace-db-sg
   - Description: Security group for Rental Marketplace RDS
   - VPC: Select your default VPC
3. Add inbound rules:
   - Type: MySQL/Aurora (3306)
   - Source: Your IP address or application security group
   - Description: Allow MySQL access

### 2. Update Configuration

1. Copy `.env.example` to `.env`
2. Update the database configuration in `.env`:

```env
DB_HOST=<your-rds-endpoint>
DB_PORT=3306
DB_NAME=rental_marketplace
DB_USER=admin
DB_PASSWORD=<your-secure-password>
```

### 3. Run Setup Script

1. Install required packages:

```bash
pip install boto3
```

2. Update security group ID in `scripts/setup_rds.py`
   - Replace `<your-security-group-id>` with the ID of the security group created in step 1

3. Run the setup script:

```bash
python scripts/setup_rds.py
```

### 4. Post-Setup Configuration

1. Wait for the RDS instance to be available (10-15 minutes)
2. Note the endpoint URL and port from the script output
3. Update your application's `.env` file with the RDS endpoint
4. Test the connection using the application's database utilities

## Best Practices

- Keep the master password secure and never commit it to version control
- Enable automated backups
- Monitor the RDS instance using AWS CloudWatch
- Regularly review security group rules
- Consider using AWS Secrets Manager for credential management

## Cost Considerations

- The script uses `db.t3.micro` instance type (free tier eligible)
- 20GB storage allocated by default
- Enable storage autoscaling if needed
- Monitor costs through AWS Cost Explorer

## Troubleshooting

1. Connection issues:
   - Verify security group rules
   - Check network ACLs
   - Confirm VPC settings
   - Verify credentials in `.env`

2. Performance issues:
   - Monitor CloudWatch metrics
   - Check slow query logs
   - Consider upgrading instance class

## Cleanup

To delete the RDS instance:

1. Create final backup if needed
2. Use AWS Console or AWS CLI to delete the instance
3. Delete associated security groups
