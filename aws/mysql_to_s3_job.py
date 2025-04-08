"""
AWS Glue job to extract data from MySQL and load it to S3.

This job:
1. Connects to a MySQL database
2. Extracts data from specified tables
3. Writes the data to S3 in Parquet format

Usage:
    aws glue start-job-run --job-name mysql-to-s3-extraction --arguments '{"--mysql_host":"your-mysql-host","--mysql_port":"3306","--mysql_database":"rental_marketplace","--mysql_user":"your-user","--mysql_password":"your-password","--s3_bucket":"your-bucket","--s3_prefix":"raw/","--region":"eu-west-1"}'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'mysql_host',
    'mysql_port',
    'mysql_database',
    'mysql_user',
    'mysql_password',
    's3_bucket',
    's3_prefix',
    'region'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# List of tables to extract from the MySQL database
tables = ["apartments", "apartment_attributes", "user_viewings", "bookings"]

# Define JDBC connection details for MySQL
jdbc_url = f"jdbc:mysql://{args['mysql_host']}:{args['mysql_port']}/{args['mysql_database']}"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_user = args['mysql_user']
jdbc_password = args['mysql_password']

# Generate a timestamp for this extraction
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
s3_base_path = f"s3://{args['s3_bucket']}/{args['s3_prefix']}"

# Loop through each table and extract data
for table in tables:
    print(f"Extracting table: {table}")
    
    try:
        # Read the table from the MySQL database using the JDBC connection
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", jdbc_driver) \
            .option("dbtable", table) \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .load()
        
        # Print schema and count for debugging
        print(f"Schema for table {table}:")
        df.printSchema()
        count = df.count()
        print(f"Row count for table {table}: {count}")
        
        # Convert the DataFrame to a DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, table)
        
        # Create a destination path for each table within the main S3 output path
        # Include timestamp for versioning
        table_s3_output_path = f"{s3_base_path}{table}/{timestamp}/"
        
        # Write the DynamicFrame to S3 in Parquet format
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": table_s3_output_path},
            format="parquet"
        )
        
        print(f"Successfully extracted {count} rows from {table} to {table_s3_output_path}")
        
    except Exception as e:
        print(f"Error extracting table {table}: {str(e)}")

print("Data extraction completed for all tables.")

# Commit the job
job.commit()
