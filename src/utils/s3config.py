import os
import boto3
from dotenv import load_dotenv
from src.utils.custom_exceptions import *

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv()

# --------------------------------------------------
# AWS config
# --------------------------------------------------

s3_bucket = os.getenv("S3_RAW_BUCKET")

if not s3_bucket:
    raise ConfigError("S3 bucket not set in environment")

aws_region = os.getenv("AWS_REGION")

client = boto3.client(
    "s3",
    region_name=aws_region
)

# --------------------------------------------------
# S3 helpers
# --------------------------------------------------

def s3_key_exists(bucket: str, key: str) -> bool:

    """
    Checks if S3 bucket exists
    
    :param bucket: S3 Bucket
    :type bucket: str
    :param key: Path key
    :type key: str
    :return: True/False
    :rtype: bool
    """

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise