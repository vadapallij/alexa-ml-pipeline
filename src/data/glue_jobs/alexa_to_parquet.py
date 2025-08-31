import sys, os, json, re
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_BUCKET','RAW_PREFIX','CURATED_PREFIX'])
S3_BUCKET = args['S3_BUCKET']
RAW_PREFIX = args['RAW_PREFIX']            # e.g., 'data/processed/alexa_reviews_clean.csv'
CURATED_PREFIX = args['CURATED_PREFIX']    # e.g., 'data/curated/alexa_reviews/'

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Contract: we need a "text" column; map common aliases to "text"
ALIASES = ["verified_reviews","review_text","text","content","feedback","reviews","summary"]

# Read CSV (single file)
raw_path = f"s3://{S3_BUCKET}/{RAW_PREFIX}"
df = spark.read.option("header", True).csv(raw_path)

# Normalize columns
def to_snake(c): 
    return re.sub(r"_+","_", re.sub(r"[^0-9A-Za-z_]","_", c.strip().replace(" ","_").lower()))
df = df.select([F.col(c).alias(to_snake(c)) for c in df.columns])

# Ensure text column exists
text_col = None
for a in ALIASES:
    if a in df.columns:
        text_col = a; break
if text_col is None:
    raise Exception(f"No text-like column found. Columns: {df.columns}")

df = df.withColumnRenamed(text_col, "text")

# Coerce a few common fields
if "rating" in df.columns:
    df = df.withColumn("rating", F.col("rating").cast(T.DoubleType()))
if "date" in df.columns:
    df = df.withColumn("date", F.to_timestamp("date"))

# Add ingest_date partition (UTC day)
df = df.withColumn("ingest_date", F.lit(datetime.utcnow().strftime("%Y-%m-%d")))

# Write Parquet partitioned by ingest_date
out_path = f"s3://{S3_BUCKET}/{CURATED_PREFIX}"
(df
 .repartition(1)
 .write
 .mode("overwrite")
 .partitionBy("ingest_date")
 .parquet(out_path))

print(f"Wrote Parquet to: {out_path}")
