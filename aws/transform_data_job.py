"""
AWS Glue job to transform data in S3.

This job:
1. Reads raw data from S3
2. Applies transformations
3. Writes transformed data back to S3

Usage:
    aws glue start-job-run --job-name transform-data --arguments '{"--s3_bucket":"your-bucket","--raw_prefix":"raw/","--processed_prefix":"processed/","--region":"eu-west-1"}'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, datediff, lit
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_bucket',
    'raw_prefix',
    'processed_prefix',
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

# S3 paths
s3_raw_path = f"s3://{args['s3_bucket']}/{args['raw_prefix']}"
s3_processed_path = f"s3://{args['s3_bucket']}/{args['processed_prefix']}"

# Generate a timestamp for this processing run
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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

# Process apartments data
def process_apartments(dynamic_frame):
    """Process apartments data."""
    df = dynamic_frame.toDF()
    
    # Apply transformations
    df = df.withColumn("processed_at", current_timestamp())
    
    # Calculate days since listing
    df = df.withColumn(
        "days_since_listing",
        when(
            col("listing_created_on").isNotNull(),
            datediff(current_timestamp(), col("listing_created_on"))
        ).otherwise(0)
    )
    
    # Standardize price (assuming price is in USD)
    df = df.withColumn(
        "price_usd",
        col("price")
    )
    
    return DynamicFrame.fromDF(df, glueContext, "processed_apartments")

# Process user_viewings data
def process_user_viewings(dynamic_frame):
    """Process user viewings data."""
    df = dynamic_frame.toDF()
    
    # Apply transformations
    df = df.withColumn("processed_at", current_timestamp())
    
    # Calculate viewing duration (if applicable)
    if "viewing_end_time" in df.columns and "viewed_at" in df.columns:
        df = df.withColumn(
            "viewing_duration_seconds",
            when(
                col("viewing_end_time").isNotNull(),
                datediff(col("viewing_end_time"), col("viewed_at")) * 86400
            ).otherwise(0)
        )
    
    return DynamicFrame.fromDF(df, glueContext, "processed_user_viewings")

# Process bookings data
def process_bookings(dynamic_frame):
    """Process bookings data."""
    df = dynamic_frame.toDF()
    
    # Apply transformations
    df = df.withColumn("processed_at", current_timestamp())
    
    # Calculate booking duration
    if "checkout_date" in df.columns and "checkin_date" in df.columns:
        df = df.withColumn(
            "booking_duration_days",
            when(
                col("checkout_date").isNotNull() & col("checkin_date").isNotNull(),
                datediff(col("checkout_date"), col("checkin_date"))
            ).otherwise(0)
        )
    
    return DynamicFrame.fromDF(df, glueContext, "processed_bookings")

# Process apartment_attributes data
def process_apartment_attributes(dynamic_frame):
    """Process apartment attributes data."""
    df = dynamic_frame.toDF()
    
    # Apply transformations
    df = df.withColumn("processed_at", current_timestamp())
    
    # Calculate price per square foot (if applicable)
    if "price_display" in df.columns and "square_feet" in df.columns:
        df = df.withColumn(
            "price_per_sqft",
            when(
                col("square_feet").isNotNull() & (col("square_feet") > 0) & col("price_display").isNotNull(),
                col("price_display") / col("square_feet")
            ).otherwise(0)
        )
    
    return DynamicFrame.fromDF(df, glueContext, "processed_apartment_attributes")

# Dictionary mapping table names to their processing functions
processing_functions = {
    "apartments": process_apartments,
    "user_viewings": process_user_viewings,
    "bookings": process_bookings,
    "apartment_attributes": process_apartment_attributes
}

# Process each table
for table, process_func in processing_functions.items():
    print(f"Processing table: {table}")
    
    try:
        # Get the latest timestamp folder for this table
        table_prefix = f"{args['raw_prefix']}{table}/"
        latest_folder = get_latest_timestamp_folder(args['s3_bucket'], table_prefix)
        
        # Full S3 path to the latest data
        s3_table_path = f"s3://{args['s3_bucket']}/{latest_folder}"
        
        print(f"Reading data from: {s3_table_path}")
        
        # Read the Parquet data from S3
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_table_path]},
            format="parquet"
        )
        
        # Print schema and count for debugging
        df = dynamic_frame.toDF()
        print(f"Schema for table {table}:")
        df.printSchema()
        count = df.count()
        print(f"Row count for table {table}: {count}")
        
        # Process the data
        processed_frame = process_func(dynamic_frame)
        
        # Create a destination path for the processed data
        processed_path = f"{s3_processed_path}{table}/{timestamp}/"
        
        # Write the processed data to S3
        glueContext.write_dynamic_frame.from_options(
            frame=processed_frame,
            connection_type="s3",
            connection_options={"path": processed_path},
            format="parquet"
        )
        
        print(f"Successfully processed {count} rows from {table} to {processed_path}")
        
    except Exception as e:
        print(f"Error processing table {table}: {str(e)}")

print("Data transformation completed for all tables.")

# Commit the job
job.commit()
