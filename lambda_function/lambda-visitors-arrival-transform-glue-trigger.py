import json
import boto3
import urllib.parse
import os

glue = boto3.client('glue')

def lambda_handler(event, context):
    # Get S3 event info
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(record['s3']['object']['key'])
    
    # Get Glue job name from environment variable
    glue_job_name = os.environ['GLUE_JOB_NAME']
    
    # Optional: Pass S3 info as arguments to Glue job
    arguments = {
        '--S3_BUCKET': bucket,
        '--S3_KEY': key
    }
    
    # Start Glue job
    response = glue.start_job_run(
        JobName=glue_job_name,
        Arguments=arguments
    )
    
    print(f"Started Glue job {glue_job_name} for s3://{bucket}/{key}")
    return {
        'statusCode': 200,
        'body': json.dumps('Glue job triggered successfully!')
    }