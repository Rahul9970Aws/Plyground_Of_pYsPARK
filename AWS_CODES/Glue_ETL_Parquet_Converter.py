import sys
import boto3
import logging
from io import StringIO
from datetime import datetime
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Glue_ETL_Parquet_Converter").getOrCreate()

# S3 client
s3_client = boto3.client("s3")

# Buckets
SOURCE_BUCKET = "raw-data-layer-ext"
DESTINATION_BUCKET = "bronze-layer-project"
ARCHIVE_BUCKET = "archive-bucket-ext"
LOG_BUCKET = "log-files-bkt"

# Set up in-memory log
log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

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
        logger.error(f"Failed to upload logs: {e}")

def move_files_to_archive(files):
    archive_path_prefix = f"s3://{ARCHIVE_BUCKET}/RAW_ARCHIVE/{datetime.now().strftime('%Y-%m-%d')}/"
    for file in files:
        try:
            src_key = file['Key']
            src_path = f"s3://{SOURCE_BUCKET}/{src_key}"
            dest_path = f"{archive_path_prefix}{src_key}"

            s3_client.copy_object(
                Bucket=ARCHIVE_BUCKET,
                CopySource={'Bucket': SOURCE_BUCKET, 'Key': src_key},
                Key=f"RAW_ARCHIVE/{datetime.now().strftime('%Y-%m-%d')}/{src_key}"
            )
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=src_key)

            logger.info(f"Moved file '{src_path}' to archive: {dest_path}")
        except Exception as e:
            logger.error(f"Failed to archive '{src_key}': {e}")

def process_files():
    try:
        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        files = response.get("Contents", [])

        if not files:
            logger.info("No files found in source bucket.")
            return

        for file in files:
            file_key = file['Key']
            logger.info(f"Processing file: {file_key}")
            filename_without_ext = file_key.split("/")[-1].split(".")[0]
            destination_prefix = f"s3://{DESTINATION_BUCKET}/{filename_without_ext}/"

            input_path = f"s3://{SOURCE_BUCKET}/{file_key}"

            try:
                if file_key.endswith(".csv"):
                    df = spark.read.option("header", "true").csv(input_path)
                elif file_key.endswith(".json"):
                    df = spark.read.json(input_path)
                elif file_key.endswith((".xls", ".xlsx")):
                    logger.warning(f"Skipping Excel file (not supported by Spark directly): {file_key}")
                    continue
                else:
                    logger.warning(f"Unsupported file format: {file_key}")
                    continue
            except Exception as e:
                logger.error(f"Failed to read '{file_key}': {e}")
                continue

            try:
                df.write.mode("overwrite").parquet(f"{destination_prefix}{filename_without_ext}.parquet")
                logger.info(f"Parquet written to: {destination_prefix}{filename_without_ext}.parquet")
            except Exception as e:
                logger.error(f"Failed to write parquet for '{file_key}': {e}")
                continue

        move_files_to_archive(files)

    except Exception as e:
        logger.error(f"Job failed: {e}")

    finally:
        upload_logs_to_s3()

# Run the Glue job
process_files()
