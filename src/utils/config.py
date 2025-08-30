import os
AWS_REGION = os.getenv("AWS_REGION", "<REGION>")
S3_BUCKET  = os.getenv("S3_BUCKET", "alexa-ml-<ACCOUNT_ID>-<REGION>")
