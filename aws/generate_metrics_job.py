"""
AWS Glue job to generate metrics and load them to Redshift.

This job:
1. Reads processed data from S3
2. Calculates various metrics
3. Loads the metrics to Redshift

Usage:
    aws glue start-job-run --job-name generate-metrics --arguments '{"--s3_bucket":"your-bucket","--processed_prefix":"processed/","--metrics_prefix":"metrics/","--redshift_connection":"your-redshift-connection","--redshift_database":"rental_marketplace","--redshift_schema":"presentation","--region":"eu-west-1"}'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from pyspark.sql.functions import col, count, avg, sum, min, max, lit, current_timestamp
from pyspark.sql import Window
import pyspark.sql.functions as F
from datetime import datetime

# Get job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",
        "processed_prefix",
        "metrics_prefix",
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

# S3 paths
s3_processed_path = f"s3://{args['s3_bucket']}/{args['processed_prefix']}"
s3_metrics_path = f"s3://{args['s3_bucket']}/{args['metrics_prefix']}"

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

# Generate a timestamp for this metrics run
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


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


# Load the latest processed data for each table
try:
    # Get apartments data
    apartments_prefix = f"{args['processed_prefix']}apartments/"
    apartments_folder = get_latest_timestamp_folder(
        args["s3_bucket"], apartments_prefix
    )
    apartments_path = f"s3://{args['s3_bucket']}/{apartments_folder}"

    apartments_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [apartments_path]},
        format="parquet",
    )
    apartments_df = apartments_frame.toDF()
    print(f"Loaded {apartments_df.count()} rows from apartments")

    # Get user_viewings data
    viewings_prefix = f"{args['processed_prefix']}user_viewings/"
    viewings_folder = get_latest_timestamp_folder(args["s3_bucket"], viewings_prefix)
    viewings_path = f"s3://{args['s3_bucket']}/{viewings_folder}"

    viewings_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [viewings_path]},
        format="parquet",
    )
    viewings_df = viewings_frame.toDF()
    print(f"Loaded {viewings_df.count()} rows from user_viewings")

    # Get bookings data
    bookings_prefix = f"{args['processed_prefix']}bookings/"
    bookings_folder = get_latest_timestamp_folder(args["s3_bucket"], bookings_prefix)
    bookings_path = f"s3://{args['s3_bucket']}/{bookings_folder}"

    bookings_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [bookings_path]},
        format="parquet",
    )
    bookings_df = bookings_frame.toDF()
    print(f"Loaded {bookings_df.count()} rows from bookings")

    # Calculate metrics

    # 1. Listing metrics
    listing_metrics = apartments_df.agg(
        count("id").alias("total_listings"),
        count(F.when(col("is_active") == True, True)).alias("active_listings"),
        avg("price_usd").alias("avg_price"),
        min("price_usd").alias("min_price"),
        max("price_usd").alias("max_price"),
        avg("days_since_listing").alias("avg_days_listed"),
    )

    # 2. Viewing metrics
    viewing_metrics = viewings_df.agg(
        count("*").alias("total_viewings"),
        count(F.when(col("is_wishlisted") == True, True)).alias("total_wishlisted"),
        count("user_id").alias("total_users"),
        count(F.when(col("viewing_duration_seconds").isNotNull(), True)).alias(
            "total_timed_viewings"
        ),
        avg("viewing_duration_seconds").alias("avg_viewing_duration"),
    )

    # 3. Booking metrics
    booking_metrics = bookings_df.agg(
        count("*").alias("total_bookings"),
        count(F.when(col("booking_status") == "confirmed", True)).alias(
            "confirmed_bookings"
        ),
        avg("total_price").alias("avg_booking_value"),
        sum("total_price").alias("total_booking_value"),
        avg("booking_duration_days").alias("avg_booking_duration"),
    )

    # Join all metrics
    metrics_df = listing_metrics.crossJoin(viewing_metrics).crossJoin(booking_metrics)

    # Add calculated metrics
    metrics_df = metrics_df.withColumn(
        "booking_conversion_rate",
        F.when(
            col("total_viewings") > 0, col("total_bookings") / col("total_viewings")
        ).otherwise(0),
    )

    metrics_df = metrics_df.withColumn(
        "wishlist_rate",
        F.when(
            col("total_viewings") > 0, col("total_wishlisted") / col("total_viewings")
        ).otherwise(0),
    )

    metrics_df = metrics_df.withColumn(
        "avg_viewings_per_listing",
        F.when(
            col("active_listings") > 0, col("total_viewings") / col("active_listings")
        ).otherwise(0),
    )

    # Add timestamp
    metrics_df = metrics_df.withColumn("generated_at", current_timestamp())
    metrics_df = metrics_df.withColumn("metrics_date", F.current_date())

    # Convert to DynamicFrame
    metrics_frame = DynamicFrame.fromDF(metrics_df, glueContext, "metrics")

    # Write metrics to S3
    metrics_output_path = f"{s3_metrics_path}{timestamp}/"

    glueContext.write_dynamic_frame.from_options(
        frame=metrics_frame,
        connection_type="s3",
        connection_options={"path": metrics_output_path},
        format="parquet",
    )

    print(f"Successfully wrote metrics to {metrics_output_path}")

    # Write metrics to Redshift
    redshift_table = "presentation.presentation_metrics"

    # Write to Redshift using direct JDBC connection
    glueContext.write_dynamic_frame.from_options(
        frame=metrics_frame,
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

    print(f"Successfully loaded metrics to {redshift_table}")

except Exception as e:
    print(f"Error generating metrics: {str(e)}")

print("Metrics generation completed.")

# Commit the job
job.commit()
