"""S3 client helpers for the FASM pipeline.

Two credential sets are used:
  - AirFire (AWS_*)      — reads source data from AFE_BUCKET
  - EPA     (EPA_AWS_*)  — writes status/layer artifacts to EPA_BUCKET
"""
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AFE_BUCKET = os.getenv("AFE_BUCKET")
EPA_ACCESS_KEY = os.getenv("EPA_AWS_ACCESS_KEY")
EPA_SECRET_ACCESS_KEY = os.getenv("EPA_AWS_SECRET_ACCESS_KEY")
EPA_BUCKET = os.getenv("EPA_BUCKET")

# Optional endpoint/region overrides. Unset -> None -> boto3 uses AWS defaults.
# Set AWS_ENDPOINT_URL to point at a non-AWS / S3-compatible store (MinIO, a VPC
# endpoint, etc.) without changing code.
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or None
AWS_REGION = os.getenv("AWS_REGION") or None
EPA_ENDPOINT_URL = os.getenv("EPA_ENDPOINT_URL") or None
EPA_REGION = os.getenv("EPA_REGION") or None


def init_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
    )


def init_epa_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=EPA_ACCESS_KEY,
        aws_secret_access_key=EPA_SECRET_ACCESS_KEY,
        endpoint_url=EPA_ENDPOINT_URL,
        region_name=EPA_REGION,
    )


def airfire_exports_bucket():
    return AFE_BUCKET


def fasm_layers_bucket():
    return EPA_BUCKET
