"""
AWS Glue job to load data from S3 to Redshift.

This job:
1. Reads data from S3 in Parquet format
2. Loads the data to Redshift tables

Usage:
    aws glue start-job-run --job-name s3-to-redshift-loading --arguments '{"--s3_bucket":"your-bucket","--s3_prefix":"raw/","--redshift_connection":"your-redshift-connection","--redshift_database":"rental_marketplace","--redshift_schema":"public","--region":"eu-west-1"}'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3

# Get job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",
        "s3_prefix",
        "redshift_connection",
        "redshift_database",
        "redshift_schema",
        "region",
        "redshift_host",
        "redshift_port",
        "redshift_user",
        "redshift_password",
    ],
)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize S3 client
s3 = boto3.client("s3", region_name=args["region"])

# List of tables to load to Redshift with their corresponding Redshift table names
table_mapping = {
    "apartments": "raw_data.apartments",
    "apartment_attributes": "raw_data.apartment_attributes",
    "user_viewings": "raw_data.user_viewings",
    "bookings": "raw_data.bookings",
}

# Define Redshift connection details
redshift_connection = args["redshift_connection"]
redshift_database = args["redshift_database"]
redshift_schema = args["redshift_schema"]
redshift_host = args["redshift_host"]
redshift_port = args["redshift_port"]
redshift_user = args["redshift_user"]
redshift_password = args["redshift_password"]

# JDBC URL for Redshift
jdbc_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_database}"

# S3 base path
s3_base_path = f"s3://{args['s3_bucket']}/{args['s3_prefix']}"


# Function to get the latest timestamp folder for a table
def get_latest_timestamp_folder(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")

    if "CommonPrefixes" not in response:
        raise Exception(f"No folders found in {prefix}")

    # Get all timestamp folders
    timestamp_folders = [p["Prefix"] for p in response["CommonPrefixes"]]

    # Sort by timestamp (assuming format YYYYMMDD_HHMMSS)
    timestamp_folders.sort(reverse=True)

    if not timestamp_folders:
        raise Exception(f"No timestamp folders found in {prefix}")

    return timestamp_folders[0]


# Loop through each table and load data to Redshift
for source_table, target_table in table_mapping.items():
    print(f"Loading table: {source_table} to {target_table}")

    try:
        # Get the latest timestamp folder for this table
        table_prefix = f"{args['s3_prefix']}{source_table}/"
        latest_folder = get_latest_timestamp_folder(args["s3_bucket"], table_prefix)

        # Full S3 path to the latest data
        s3_table_path = f"s3://{args['s3_bucket']}/{latest_folder}"

        print(f"Reading data from: {s3_table_path}")

        # Read the Parquet data from S3
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_table_path]},
            format="parquet",
        )

        # Print schema and count for debugging
        df = dynamic_frame.toDF()
        print(f"Schema for table {source_table}:")
        df.printSchema()
        count = df.count()
        print(f"Row count for table {source_table}: {count}")

        # Define the Redshift table name
        redshift_table = target_table

        # Write to Redshift using direct JDBC connection
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "url": jdbc_url,
                "user": redshift_user,
                "password": redshift_password,
                "dbtable": redshift_table,
                "redshiftTmpDir": f"s3://{args['s3_bucket']}/temp/",
            },
            redshift_tmp_dir=f"s3://{args['s3_bucket']}/temp/",
        )

        print(f"Successfully loaded {count} rows to {redshift_table}")

    except Exception as e:
        print(f"Error loading table {source_table} to {target_table}: {str(e)}")

print("Data loading completed for all tables.")

# Commit the job
job.commit()
