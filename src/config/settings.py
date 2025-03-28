"""
Configuration settings for the Rental Marketplace Analytics Pipeline.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv(
        "DB_HOST", "rental-marketplace-db.czaiaq68azf6.eu-west-1.rds.amazonaws.com"
    ),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "rental_marketplace"),
    "port": int(os.getenv("DB_PORT", 3306)),
}


# AWS Configuration
AWS_CONFIG = {
    "region": os.getenv("AWS_REGION", "eu-west-1"),
    "s3_bucket": os.getenv("S3_BUCKET"),
    "redshift_cluster": os.getenv("REDSHIFT_CLUSTER"),
    "redshift_database": os.getenv("REDSHIFT_DATABASE"),
    "redshift_user": os.getenv("REDSHIFT_USER"),
    "redshift_password": os.getenv("REDSHIFT_PASSWORD"),
    "redshift_port": int(os.getenv("REDSHIFT_PORT", 5439)),
}

# ETL Configuration
ETL_CONFIG = {
    "batch_size": int(os.getenv("ETL_BATCH_SIZE", 1000)),
    "max_retries": int(os.getenv("ETL_MAX_RETRIES", 3)),
    "timeout": int(os.getenv("ETL_TIMEOUT", 300)),
}

# Logging Configuration
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": os.getenv("LOG_FILE", "pipeline.log"),
}
