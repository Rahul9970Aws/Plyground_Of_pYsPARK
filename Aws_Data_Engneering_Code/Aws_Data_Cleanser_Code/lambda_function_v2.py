import json
import boto3
import logging
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime

# In-memory log buffer
log_stream = StringIO()

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

memory_handler = logging.StreamHandler(log_stream)
memory_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(memory_handler)

# AWS S3 client
s3_client = boto3.client('s3')

# Fixed bucket names for your use case
SOURCE_BUCKET = "raw-data-layer-ext"
DESTINATION_BUCKET = "bronze-layer-project"
ARCHIVE_BUCKET = "archive-bucket-ext"
LOG_BUCKET = "log-files-bkt"

def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_filename = f"Logs/policy_cleansed_loader_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        s3_client.put_object(
            Bucket=LOG_BUCKET,
            Key=log_filename,
            Body=log_contents.encode("utf-8")
        )
        logger.info(f"Logs uploaded to S3 at: s3://{LOG_BUCKET}/{log_filename}")
    except Exception as e:
        logger.error(f"Failed to upload logs to S3: {e}")

def move_all_files_to_archive():
    try:
        archive_subfolder = "RAW_ARCHIVE/"
        today_date = datetime.now().strftime("%Y-%m-%d")
        date_folder = archive_subfolder + today_date + "/"

        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)

        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]
                copy_source = {"Bucket": SOURCE_BUCKET, "Key": file_key}
                destination_key = date_folder + file_key

                s3_client.copy_object(Bucket=ARCHIVE_BUCKET, CopySource=copy_source, Key=destination_key)
                s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=file_key)

                logger.info(f"Moved file '{file_key}' to archive '{ARCHIVE_BUCKET}/{destination_key}'.")
        else:
            logger.info("No files found in the source bucket.")

    except Exception as e:
        logger.error(f"Error moving files to archive: {e}")

def lambda_handler(event, context):
    logger.info('Lambda triggered for file processing.')

    # Log bucket values
    logger.info(f"SOURCE_BUCKET: {SOURCE_BUCKET}")
    logger.info(f"DESTINATION_BUCKET: {DESTINATION_BUCKET}")
    logger.info(f"ARCHIVE_BUCKET: {ARCHIVE_BUCKET}")
    logger.info(f"LOG_BUCKET: {LOG_BUCKET}")

    try:
        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        files = response.get('Contents', [])

        if not files:
            logger.info("No files found in source bucket.")
            upload_logs_to_s3()
            return {
                'statusCode': 200,
                'body': json.dumps(f"No files found in '{SOURCE_BUCKET}'.")
            }

        for file in files:
            file_key = file.get('Key')
            logger.info(f"Processing file: {file_key}")
            filename_without_ext = file_key.split('/')[-1].split('.')[0]
            destination_prefix = f"{filename_without_ext}/"
            response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
            file_content = response['Body'].read()

            # Read into DataFrame
            try:
                if file_key.endswith('.csv'):
                    df = pd.read_csv(BytesIO(file_content))
                elif file_key.endswith('.json'):
                    df = pd.read_json(BytesIO(file_content), lines=True)
                elif file_key.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(BytesIO(file_content))
                else:
                    logger.warning(f"Unsupported format: {file_key}")
                    continue
            except Exception as e:
                logger.error(f"Failed to read '{file_key}': {e}")
                continue

            # Convert to Parquet
            parquet_buffer = BytesIO()
            try:
                df.to_parquet(parquet_buffer, index=False)
            except Exception as e:
                logger.error(f"Failed to convert to Parquet: {e}")
                continue

            parquet_buffer.seek(0)
            destination_key = f"{destination_prefix}{filename_without_ext}.parquet"

            s3_client.upload_fileobj(parquet_buffer, DESTINATION_BUCKET, destination_key)
            logger.info(f"Uploaded '{file_key}' as Parquet to '{DESTINATION_BUCKET}/{destination_key}'")

        # Archive source files
        move_all_files_to_archive()

        # Upload logs
        upload_logs_to_s3()

        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed and moved files to '{DESTINATION_BUCKET}'")
        }

    except Exception as e:
        logger.error(f"Lambda failed: {e}")
        upload_logs_to_s3()
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
