from io import StringIO
import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

class DataCleanser():
    def validate_columns(df, logger):
        logger.info("Dropping duplicate customer_id records")
        df_duplicate = df.dropDuplicates(["customer_id"])
        
        logger.info("Dropping rows with nulls in key columns")
        df_cleaned = df_duplicate.na.drop(subset=["customer_id", "first_name", "last_name", "address"])
        
        logger.info("Creating full_name column")
        df_validated = df_cleaned.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        
        logger.info("Dropping first_name and last_name columns")
        df_final = df_validated.drop("first_name", "last_name")
        return df_final

    def db_connector(logger):
        try:
            logger.info("Connecting to Snowflake")
            sfOptions = {
                "sfURL": 'DZAKLEE-HQ11452.snowflakecomputing.com',
                "sfDatabase": "UNIFIED_DATA_LAKE",
                "sfSchema": "RAW_DATA",
                "sfWarehouse": "COMPUTE_WH",
                "sfUser": "rahul",
                "sfPassword": "xyz",
                "dbtable": "CUSTOMER_DETAILS"
            }
            logger.info("Snowflake connection established successfully")
            return sfOptions
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}")
            raise

    def data_loader_snowflake(conn, df, logger):
        try:
            logger.info("Writing DataFrame to Snowflake")
            df.write \
                .format("snowflake") \
                .options(**conn) \
                .mode("overwrite") \
                .save()
            logger.info("Write to Snowflake completed successfully :)")
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {e}")
            raise

    def main(bucket, filename, logger):
        try:
            logger.info("Creating Spark session for Glue job")
            spark = SparkSession.builder.appName("GlueSnowflakeLoader").getOrCreate()
            
            s3_path = f"s3://{bucket}/{filename}"
            logger.info(f"Reading CSV file from {s3_path}")
            
            df = spark.read.csv(s3_path, header=True, inferSchema=True)
            logger.info(f"Initial DataFrame count: {df.count()}")
            
            validate_records = DataCleanser.validate_columns(df, logger)
            logger.info(f"Validated DataFrame count: {validate_records.count()}")
            
            conn = DataCleanser.db_connector(logger)
            DataCleanser.data_loader_snowflake(conn, validate_records, logger)
            
            logger.info("Data load process completed successfully")
        except Exception as e:
            logger.error(f"Error in main process: {e}")
            raise

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger()
    args = getResolvedOptions(sys.argv, [
            'SOURCE_S3_BUCKET',
            'TARGET_S3_PATH',
            'JOB_NAME',
            'PROCESS_DATE'
            
        ])
    bucket = args['SOURCE_S3_BUCKET']
    target_path = args['TARGET_S3_PATH']
    DataCleanser.main(bucket,target_path, logger)
