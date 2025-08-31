import os, io, mimetypes
import boto3

def s3_client(region=None):
    return boto3.client("s3", region_name=region or os.getenv("AWS_REGION", "<REGION>"))

def upload_file_to_s3(local_path: str, bucket: str, key: str, region=None):
    cli = s3_client(region)
    extra_args = {}
    ctype, _ = mimetypes.guess_type(local_path)
    if ctype: extra_args["ContentType"] = ctype
    cli.upload_file(local_path, bucket, key, ExtraArgs=extra_args)
    return f"s3://{bucket}/{key}"
