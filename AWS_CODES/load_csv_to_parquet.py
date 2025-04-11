import json
import boto3
import os
import logging
import pandas as pd
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def move_files_to_archive(s3_client, archive_bucket, file_key):
    try:
        source_bucket = os.environ.get('processing_layer')
        copy_source = {'Bucket': source_bucket, 'Key': file_key}

        # Copy the object to the archive bucket
        s3_client.copy_object(
            Bucket=archive_bucket,
            CopySource=copy_source,
            Key=file_key
        )

        # Delete the object from the source bucket
        s3_client.delete_object(Bucket=source_bucket, Key=file_key)
        logger.info(f"Moved file '{file_key}' from '{source_bucket}' to archive bucket '{archive_bucket}'.")
    except Exception as e:
        logger.error(f"Error moving file '{file_key}' to archive bucket '{archive_bucket}': {e}")

def lambda_handler(event, context):
    logger.info('START OF EVENT')
    s3_client = boto3.client('s3')

    source_bucket = os.environ.get('processing_layer')
    destination_bucket = os.environ.get('ploicy_Cleansed')
    archive_bucket=os.environ.get('archive_layer')

    if not source_bucket:
        logger.error("The 'processing_layer' environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps("Error: 'processing_layer' environment variable not found.")
        }

    if not destination_bucket:
        logger.error("The 'ploicy_Cleansed' environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps("Error: 'ploicy_Cleansed' environment variable not found.")
        }

    logger.info(f"Source Bucket: {source_bucket}")
    logger.info(f"Destination Bucket: {destination_bucket}")

    try:
        response = s3_client.list_objects_v2(Bucket=source_bucket)
        logger.info('LISTING OBJECTS IN SOURCE BUCKET')
        files = response.get('Contents', [])

        if files:
            for file in files:
                file_key = file.get('Key')
                logger.info(f"Processing file: {file_key}")
                filename_without_extension = os.path.splitext(os.path.basename(file_key))[0]
                destination_prefix = f"{filename_without_extension}/"
                logger.info(f"Destination Prefix: {destination_prefix}")
                response = s3_client.get_object(Bucket=source_bucket, Key=file_key)
                file_content = response['Body'].read()
                if file_key.endswith('.csv'):
                    try:
                        df = pd.read_csv(BytesIO(file_content))
                    except Exception as e:
                        logger.error(f"Error reading CSV file '{file_key}': {e}")
                        continue
                elif file_key.endswith('.json'):
                    try:
                        df = pd.read_json(BytesIO(file_content), lines=True)
                    except Exception as e:
                        logger.error(f"Error reading JSON file '{file_key}': {e}")
                        continue
                elif file_key.endswith(('.xlsx', '.xls')):
                    try:
                        df = pd.read_excel(BytesIO(file_content))
                    except Exception as e:
                        logger.error(f"Error reading Excel file '{file_key}': {e}")
                        continue
                else:
                    logger.warning(f"Unsupported file format for '{file_key}'. Skipping.")
                    continue

                # Convert DataFrame to Parquet format in memory
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                destination_key = f"{destination_prefix}{os.path.splitext(os.path.basename(file_key))[0]}.parquet"

                # Upload the Parquet file to the destination bucket
                s3_client.upload_fileobj(parquet_buffer, destination_bucket, destination_key)
                logger.info(f"Successfully converted and loaded '{file_key}' to '{destination_bucket}/{destination_key}'")
                
                move_files_to_archive(archive_bucket, file_key, s3_client)  #to move to archive and cleanse the existing bucket

            return {
                'statusCode': 200,
                'body': json.dumps(f"Successfully processed and loaded files to '{destination_bucket}' with folders based on filenames.")
            }
        else:
            logger.info(f"No files found in '{source_bucket}'.")
            return {
                'statusCode': 200,
                'body': json.dumps(f"No files found in '{source_bucket}'.")
            }

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing files: {str(e)}")
        }
