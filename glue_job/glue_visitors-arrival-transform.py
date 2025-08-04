import sys
import re
import os
import boto3
from itertools import chain
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    regexp_replace, regexp_extract, col, trim, lit, create_map, when
)

def get_latest_s3_file_from_pattern(src_path):
    match = re.match(r's3://([^/]+)/(.+)', src_path)
    if not match:
        raise ValueError("Invalid S3 path: " + src_path)
    bucket, key_pattern = match.group(1), match.group(2)
    prefix = key_pattern.split('*')[0]
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    candidates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Simple wildcard match using fnmatch
            import fnmatch
            if fnmatch.fnmatch(key, key_pattern):
                candidates.append((key, obj['LastModified']))
    if not candidates:
        raise Exception(f"No files found matching pattern: {src_path}")
    # Pick the latest file by LastModified
    latest_key = sorted(candidates, key=lambda x: x[1], reverse=True)[0][0]
    return bucket, latest_key

def move_s3_file(src_bucket, src_key, processed_prefix):
    s3 = boto3.client('s3')
    processed_key = f"{processed_prefix.rstrip('/')}/{os.path.basename(src_key)}"
    s3.copy_object(
        Bucket=src_bucket,
        CopySource={'Bucket': src_bucket, 'Key': src_key},
        Key=processed_key
    )
    s3.delete_object(Bucket=src_bucket, Key=src_key)
    print(f"✅ Original file moved to: s3://{src_bucket}/{processed_key}")

# Get parameters
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'SRC_PATH', 'OUTPUT_PATH', 'PROCESSED_PREFIX']
)
src_path = args['SRC_PATH']  # e.g. s3://bucket/prefix/*.csv
output_path = args['OUTPUT_PATH']  # e.g. s3://bucket/processed/
processed_prefix = args['PROCESSED_PREFIX']  # e.g. visitors-arrivals-staging-zone-processed/

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get the latest file matching the pattern
bucket, key = get_latest_s3_file_from_pattern(src_path)
file_path = f"s3://{bucket}/{key}"

# Extract year from filename
match = re.search(r'(\d{4})', os.path.basename(key))
if not match:
    raise Exception(f"No year found in filename: {file_path}")
year = int(match.group(1))

df = spark.read.option("header", True).csv(file_path)
df = df.toDF(*[c.strip() for c in df.columns])
df = df.select([regexp_replace(col(c), "-", "").alias(c) for c in df.columns])
df = df.drop("Rank", "Jan-Oct Total", "% Share")
df = df.withColumnRenamed("RankingType", "Type")
df = df.withColumn("Year", lit(year))
# Extract the last word from 'Type', but keep original if extraction fails
extracted_type = regexp_extract(trim(col("Type")), r"(\w+)$", 1)
df = df.withColumn(
    "Type",
    when(extracted_type != "", extracted_type).otherwise(trim(col("Type")))
)
# Filter out empty or null Type
df = df.filter(col("Type").isNotNull() & (col("Type") != ""))

month_cols = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October'
]
stack_expr = ", ".join([f"'{m}', `{m}`" for m in month_cols])
df = df.selectExpr("Country", "Type", "Year", f"stack({len(month_cols)}, {stack_expr}) as (Month, Arrivals)")
month_map = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5,
    "June": 6, "July": 7, "August": 8, "September": 9, "October": 10
}
month_expr = create_map([lit(x) for x in chain(*month_map.items())])
df = df.withColumn("Month", trim(col("Month")))
df = df.withColumn("Month", month_expr.getItem(col("Month")).cast("int"))
df = df.withColumn("Arrivals", col("Arrivals").cast("int"))
df = df.dropna(subset=["Country", "Type", "Month", "Arrivals"])

df.write.mode("overwrite").partitionBy("Year", "Month", "Type").parquet(output_path)

move_s3_file(bucket, key, processed_prefix)

job.commit() 