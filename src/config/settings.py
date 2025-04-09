"""
Configuration settings for the rental marketplace ETL pipeline.
"""

import os
from typing import Dict, Any

# Database configuration
DB_CONFIG: Dict[str, Any] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME", "rental_marketplace"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

# AWS configuration
AWS_CONFIG: Dict[str, Any] = {
    "region": os.getenv("AWS_REGION", "eu-west-1"),
    "s3_bucket": os.getenv("S3_BUCKET", "rental-marketplace-data"),
    "glue_role_arn": os.getenv("GLUE_ROLE_ARN", ""),
    "step_function_role_arn": os.getenv("STEP_FUNCTION_ROLE_ARN", ""),
    "step_function_arn": os.getenv("STEP_FUNCTION_ARN", ""),
}

# Redshift configuration
REDSHIFT_CONFIG: Dict[str, Any] = {
    "connection": os.getenv("REDSHIFT_CONNECTION", "rental-marketplace-redshift"),
    "database": os.getenv("REDSHIFT_DATABASE", "rental_marketplace"),
    "host": os.getenv("REDSHIFT_HOST", ""),
    "port": int(os.getenv("REDSHIFT_PORT", "5439")),
    "user": os.getenv("REDSHIFT_USER", "admin"),
    "password": os.getenv("REDSHIFT_PASSWORD", ""),
    "schema_raw": "public",
    "schema_curated": "curated",
    "schema_presentation": "presentation",
}
