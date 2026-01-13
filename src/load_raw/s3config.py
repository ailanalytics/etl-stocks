import os
import boto3
from utils.custom_exceptions import ConfigError

# -------------------------------------
# AWS config
# -------------------------------------

s3_bucket = os.getenv("S3_RAW_BUCKET")
if not s3_bucket:
    raise ConfigError("S3 bucket not set in environment")

aws_region = os.getenv("AWS_REGION")

client = boto3.client(
    "s3",
    region_name=aws_region
)

# -------------------------------------
# S3 helpers
# -------------------------------------

def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise