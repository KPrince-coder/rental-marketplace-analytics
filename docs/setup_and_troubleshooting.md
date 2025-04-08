# Rental Marketplace Analytics - Setup and Troubleshooting Guide

This document provides detailed instructions for setting up the rental marketplace analytics pipeline and troubleshooting common issues.

## Setup Instructions

### 1. AWS Services Setup

#### 1.1 Amazon RDS (Aurora MySQL)

1. Create an Aurora MySQL cluster:

   ```bash
   aws rds create-db-cluster \
     --db-cluster-identifier rental-marketplace-cluster \
     --engine aurora-mysql \
     --engine-version 5.7.mysql_aurora.2.10.2 \
     --master-username admin \
     --master-user-password <your-password> \
     --db-subnet-group-name <your-subnet-group> \
     --vpc-security-group-ids <your-security-group-id>
   ```

2. Create a database instance:

   ```bash
   aws rds create-db-instance \
     --db-instance-identifier rental-marketplace-instance \
     --db-cluster-identifier rental-marketplace-cluster \
     --engine aurora-mysql \
     --db-instance-class db.r5.large
   ```

3. Create the database schema:

   ```bash
   python scripts/setup_rds.py
   ```

4. Load sample data:

   ```bash
   python -m jupyter notebook notebooks/load_csv_to_rds.ipynb
   ```

#### 1.2 Amazon S3

1. Create an S3 bucket:

   ```bash
   aws s3 mb s3://rental-marketplace-data
   ```

2. Create the required directories:

   ```bash
   aws s3api put-object --bucket rental-marketplace-data --key raw/
   aws s3api put-object --bucket rental-marketplace-data --key processed/
   aws s3api put-object --bucket rental-marketplace-data --key temp/
   ```

#### 1.3 Amazon Redshift

1. Create a Redshift cluster:

   ```bash
   aws redshift create-cluster \
     --cluster-identifier rental-marketplace-redshift \
     --node-type dc2.large \
     --number-of-nodes 2 \
     --master-username admin \
     --master-user-password <your-password> \
     --db-name rental_marketplace \
     --port 5439
   ```

2. Set up the Redshift schema:

   ```bash
   python scripts/setup_redshift.py
   ```

#### 1.4 AWS Glue

1. Create a Glue connection to Redshift:

   ```bash
   aws glue create-connection \
     --connection-input '{
       "Name": "redshift-connection",
       "ConnectionType": "JDBC",
       "ConnectionProperties": {
         "JDBC_CONNECTION_URL": "jdbc:redshift://<your-redshift-endpoint>:5439/rental_marketplace",
         "USERNAME": "admin",
         "PASSWORD": "<your-password>"
       }
     }'
   ```

2. Deploy the Glue job:

   ```bash
   python scripts/deploy_aws.py
   ```

#### 1.5 AWS Step Functions

1. Deploy the Step Function:

   ```bash
   python scripts/deploy_aws.py
   ```

### 2. Local Environment Setup

1. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:

   ```bash
   cp .env.example .env
   # Edit .env with your configuration values
   ```

## Running the Pipeline

### Local Execution

Run the ETL pipeline locally:

```bash
python main.py --listings-days 7 --activity-days 7 --metrics-days 30
```

### AWS Execution

Trigger the Step Function:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <your-step-function-arn> \
  --input '{"window_days": 7}'
```

## Accessing Analytics

1. Start Jupyter Notebook:

   ```bash
   jupyter notebook
   ```

2. Open the analytics notebooks:
   - `notebooks/rental_performance_metrics.ipynb`
   - `notebooks/user_engagement_metrics.ipynb`

3. Run the cells to generate the analytics reports

## Troubleshooting

### Common Issues

#### Database Connection Issues

**Problem**: Unable to connect to RDS or Redshift.

**Solution**:

1. Check security group settings to ensure proper inbound rules
2. Verify that the database credentials are correct in `.env`
3. Ensure the database instance is running and accessible

```bash
# Test RDS connection
mysql -h <rds-endpoint> -u admin -p

# Test Redshift connection
psql -h <redshift-endpoint> -p 5439 -U admin -d rental_marketplace
```

#### AWS Glue Job Failures

**Problem**: Glue job fails during execution.

**Solution**:

1. Check CloudWatch logs for detailed error messages
2. Verify IAM permissions for the Glue service role
3. Ensure S3 bucket and directories exist
4. Check Redshift connection parameters

```bash
# View Glue job logs
aws logs get-log-events \
  --log-group-name /aws/glue/jobs \
  --log-stream-name <job-run-id>
```

#### Step Function Failures

**Problem**: Step Function execution fails.

**Solution**:

1. Check the execution details in the AWS console
2. Verify that all required parameters are provided
3. Check IAM permissions for the Step Function role

#### Data Quality Issues

**Problem**: Data quality checks fail during pipeline execution.

**Solution**:

1. Review the quality reports in CloudWatch logs
2. Check source data for inconsistencies
3. Adjust validation thresholds if necessary

## Maintenance

### Regular Tasks

1. **Backup Database**:

   ```bash
   aws rds create-db-cluster-snapshot \
     --db-cluster-identifier rental-marketplace-cluster \
     --db-cluster-snapshot-identifier backup-$(date +%Y%m%d)
   ```

2. **Monitor Storage Usage**:

   ```bash
   aws s3 ls s3://rental-marketplace-data --recursive --summarize
   ```

3. **Update Dependencies**:

   ```bash
   pip install -r requirements.txt --upgrade
   ```

## Support

For additional support, please contact the development team or create an issue in the project repository.
