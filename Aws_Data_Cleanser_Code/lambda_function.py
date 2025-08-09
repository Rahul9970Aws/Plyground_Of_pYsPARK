import json
import boto3
from data_cleanser import DataCleanser
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('Lambda handler execution started...')
    # logger.info(f"Incoming event: {json.dumps(event)}")

    # Initialize AWS clients
    s3 = boto3.client('s3')
    glue_client = boto3.client('glue', region_name='us-east-1')

    # Extract bucket & file key from event (supports S3 event trigger or manual test)
    try:
        if "Records" in event:  # Event from S3 trigger
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            file_path = event['Records'][0]['s3']['object']['key']
        else:  # Manual testing / custom event
            bucket_name = event['bucket']
            file_path = event['file_key']
    except KeyError as e:
        logger.error(f"Invalid event format: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps("Invalid event format")
        }

    glue_job_name = 'bulk_data_cleanser'
    logger.info(f"Processing file: s3://{bucket_name}/{file_path}")

    # Prepare Glue job parameters
    job_parameters = {
        '--SOURCE_S3_BUCKET': bucket_name,
        '--TARGET_S3_PATH': file_path,
        '--job_name':'data_cleanser',
        '--PROCESS_DATE': datetime.today().strftime('%Y-%m-%d')
    }

    # Read file from S3 and count lines
    try:
        src_obj = s3.get_object(Bucket=bucket_name, Key=file_path)
        body = src_obj['Body'].read().decode('utf-8').splitlines()
        count = len(body)
        logger.info(f"File line count: {count}")
    except Exception as e:
        logger.error(f"Error reading file from S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error reading file: {str(e)}")
        }

    # Decide whether to start Glue or run locally
    try:
        if count > 15:
            logger.info(f"Count > 15. Starting Glue job: {glue_job_name}")
            response = glue_client.start_job_run(
                JobName=glue_job_name,
                Arguments=job_parameters
            )
            logger.info(f"Glue job started. Job Run ID: {response['JobRunId']}")
            return {
                'statusCode': 200,
                'body': json.dumps(f"Glue job {glue_job_name} started successfully.")
            }
        else:
            logger.info("Count <= 15. Running DataCleanser locally.")
            response=DataCleanser.main(bucket_name, file_path,logger)
            logger.info(f"i got response from data_cleanser : {response}")
            return {
                'statusCode': 200,
                'body': json.dumps("DataCleanser job completed successfully.")
            }

    except Exception as e:
        logger.error(f"Error running job: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error running job: {str(e)}")
        }
