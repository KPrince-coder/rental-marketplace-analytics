# Using the Rental Marketplace Analytics Pipeline

This document explains how to use the `main.py` script to set up and run the entire rental marketplace analytics pipeline.

## Overview

The `main.py` script orchestrates the entire process:

1. Sets up the RDS MySQL database
2. Loads sample data into MySQL
3. Sets up the S3 bucket
4. Sets up the Redshift schema
5. Sets up IAM roles for AWS Glue and Step Functions
6. Deploys AWS Glue job and Step Function
7. Runs the ETL pipeline
8. Triggers the Step Function (optional)

## Prerequisites

Before running the script, make sure you have:

1. AWS credentials configured
2. Required Python packages installed (`pip install -r requirements.txt`)
3. Sample data files in the `data/` directory

## Command Line Options

### Setup Options

- `--setup-all`: Run all setup steps (RDS, data loading, S3, Redshift, IAM roles, AWS deployment)
- `--setup-rds`: Set up the RDS MySQL database
- `--load-data`: Load sample data into MySQL
- `--setup-s3`: Set up the S3 bucket
- `--setup-redshift`: Create the Redshift cluster and set up the schema
- `--setup-iam-roles`: Set up IAM roles for AWS Glue and Step Functions
- `--deploy-aws`: Deploy AWS Glue job and Step Function

### Pipeline Options

- `--run-pipeline`: Run the ETL pipeline locally
- `--trigger-step-function`: Trigger the AWS Step Function
- `--listings-days`: Number of days of listings data to process (default: 7)
- `--activity-days`: Number of days of user activity data to process (default: 7)
- `--metrics-days`: Number of days of metrics data to process (default: 30)

## Example Commands

### Complete Setup and Run

To set up everything and run the pipeline:

```bash
python main.py --setup-all --run-pipeline
```

### Step-by-Step Setup

If you prefer to run the setup steps individually:

```bash
# Set up RDS
python main.py --setup-rds

# Load sample data
python main.py --load-data

# Set up S3
python main.py --setup-s3

# Set up Redshift (creates cluster and schema)
python main.py --setup-redshift

# If you want to create just the Redshift cluster without the schema
python scripts/create_redshift_cluster.py

# Set up IAM roles for AWS Glue and Step Functions
python main.py --setup-iam-roles

# Deploy AWS resources
python main.py --deploy-aws
```

### Running the Pipeline

To run the ETL pipeline locally:

```bash
python main.py --run-pipeline --listings-days 14 --activity-days 14 --metrics-days 30
```

### Triggering the Step Function

To trigger the AWS Step Function:

```bash
python main.py --trigger-step-function
```

## Workflow Examples

### Development Workflow

During development, you might want to:

```bash
# Set up infrastructure once
python main.py --setup-all

# Then for each development iteration
python main.py --run-pipeline
```

### Production Workflow

For production use:

```bash
# Initial setup
python main.py --setup-all

# Regular ETL runs
python main.py --trigger-step-function
```

## Troubleshooting

If you encounter issues:

1. Check the logs in the `logs/` directory
2. Verify your AWS credentials and permissions
3. Ensure all required data files are in the `data/` directory
4. Check that the database connection parameters in `.env` are correct

### Common Issues

#### RDS Setup Issues

- If RDS creation fails, check your AWS account limits and permissions
- If you can't connect to RDS, run `python scripts/update_security_group.py` to update the security group

#### Redshift Setup Issues

- If Redshift creation fails, check your AWS account limits and permissions
- If you can't connect to Redshift, verify the security group allows access on port 5439
- If schema creation fails, ensure the Redshift cluster is fully available

## Notes

- The script will create resources in your AWS account, which may incur costs
- Make sure to clean up resources when you're done to avoid unnecessary charges
- The script handles errors and will exit with a non-zero status code if any step fails
