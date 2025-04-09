"""
AWS Glue job to load processed data from S3 to Redshift.

This job:
1. Reads processed data from S3 in Parquet format
2. Loads the data to Redshift tables in the curated schema

Usage:
    aws glue start-job-run --job-name processed-to-redshift --arguments '{"--s3_bucket":"your-bucket","--processed_prefix":"processed/","--redshift_connection":"your-redshift-connection","--redshift_database":"rental_marketplace","--redshift_schema":"curated","--region":"eu-west-1"}'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_bucket',
    'processed_prefix',
    'redshift_connection',
    'redshift_database',
    'redshift_schema',
    'region'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3 = boto3.client('s3', region_name=args['region'])

# List of tables to load to Redshift
tables = ["apartments", "apartment_attributes", "user_viewings", "bookings"]

# Define Redshift connection details
redshift_connection = args['redshift_connection']
redshift_database = args['redshift_database']
redshift_schema = args['redshift_schema']

# S3 base path
s3_processed_path = f"s3://{args['s3_bucket']}/{args['processed_prefix']}"

# Function to get the latest timestamp folder for a table
def get_latest_timestamp_folder(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    
    if 'CommonPrefixes' not in response:
        raise Exception(f"No folders found in {prefix}")
    
    # Get all timestamp folders
    timestamp_folders = [p['Prefix'] for p in response['CommonPrefixes']]
    
    # Sort by timestamp (assuming format YYYYMMDD_HHMMSS)
    timestamp_folders.sort(reverse=True)
    
    if not timestamp_folders:
        raise Exception(f"No timestamp folders found in {prefix}")
    
    return timestamp_folders[0]

# Loop through each table and load data to Redshift
for table in tables:
    print(f"Loading processed table: {table}")
    
    try:
        # Get the latest timestamp folder for this table
        table_prefix = f"{args['processed_prefix']}{table}/"
        latest_folder = get_latest_timestamp_folder(args['s3_bucket'], table_prefix)
        
        # Full S3 path to the latest data
        s3_table_path = f"s3://{args['s3_bucket']}/{latest_folder}"
        
        print(f"Reading processed data from: {s3_table_path}")
        
        # Read the Parquet data from S3
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_table_path]},
            format="parquet"
        )
        
        # Print schema and count for debugging
        df = dynamic_frame.toDF()
        print(f"Schema for processed table {table}:")
        df.printSchema()
        count = df.count()
        print(f"Row count for processed table {table}: {count}")
        
        # Define the Redshift table name
        redshift_table = f"{redshift_schema}.curated_{table}"
        
        # Write to Redshift
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_frame,
            catalog_connection=redshift_connection,
            connection_options={
                "dbtable": redshift_table,
                "database": redshift_database,
                "redshiftTmpDir": f"s3://{args['s3_bucket']}/temp/"
            },
            redshift_tmp_dir=f"s3://{args['s3_bucket']}/temp/"
        )
        
        print(f"Successfully loaded {count} processed rows to {redshift_table}")
        
    except Exception as e:
        print(f"Error loading processed table {table}: {str(e)}")

print("Processed data loading completed for all tables.")

# Commit the job
job.commit()
