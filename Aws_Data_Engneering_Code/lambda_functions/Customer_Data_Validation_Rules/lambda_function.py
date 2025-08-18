import json
import re
import pandas as pd
from io import StringIO
import boto3
import logging
from generic_db_function import database_connection, shutdown_connection, insert_records
from generic_validator import DataValidator
from aws_utility import upload_df_to_s3, get_email_templete

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def main(bucket_name, file_path, logger):
    logger.info("Started main()")
    logger.info("Initializing S3 client...")
    s3 = boto3.client("s3")

    logger.info(f"Fetching file: s3://{bucket_name}/{file_path}")
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)

    logger.info("Reading file content...")
    data = obj['Body'].read().decode('utf-8')

    logger.info("Converting to DataFrame...")
    src_df = pd.read_csv(StringIO(data))

    logger.info("Running data validations...")
    validator = DataValidator(src_df, logger=logger)
    valid_df, invalid_df = validator.run_all_validations()
    logger.info("Completed data validations in main()")
    
    # DB connection
    db_connection = database_connection(logger)
    logger.info("Database connection established...")
    
    insert_records(valid_df, db_connection, logger=logger)
    logger.info("Records inserted to Database...")
    
    shutdown_connection(logger)
    return valid_df, invalid_df
    

def lambda_handler(event, context):
    lambda_name = context.function_name
    logger.info("Lambda execution started")
    from datetime import datetime
    
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    
    bucket_name = "de-project-poc-us-east-1"
    file_key = "Customer_poc_raw/raw_customer_info/customer_data_validation.csv"
    target_file_key = f"Curoupt_Records/Customer/{date_str}/Customer_invalid_records.csv"
    
    targeted_s3_file_path=f"s3://de-project-poc-us-east-1/Curoupt_Records/Customer/{date_str}/Customer_invalid_records.csv"
    logger.info("Calling main() function...")
    valid_df, invalid_df = main(bucket_name, file_key, logger)

    valid_count = len(valid_df)
    invalid_count = len(invalid_df)

    logger.info("Loading invalid records to S3 bucket...")
    upload_df_to_s3(invalid_df, bucket_name, target_file_key, logger=logger)
    
    if invalid_count:
        logger.info("Found Invalid Records Sending Email....")
        get_email_templete(lambda_name,targeted_s3_file_path,valid_count,invalid_count,logger=logger)
        
    
    
    logger.info(f"Lambda execution finished. Valid: {valid_count}, Invalid: {invalid_count}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "valid_records": valid_count,
            "invalid_records": invalid_count
        })
    }
