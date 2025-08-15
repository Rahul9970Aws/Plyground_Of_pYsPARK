import os
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# ------------------ Arguments ------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_run_id = os.environ.get('AWS_GLUE_JOB_RUN_ID', "UNKNOWN_RUN_ID")

# ------------------ Glue Context ------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------ Snowflake Reader ------------------
def snowflake_dw_reader(logger, table_name):
    try:
        logger.info(f"Reading table {table_name} from Snowflake")
        sfOptions = {
            "sfURL": "DZAKLEE-HQ11452.snowflakecomputing.com",
            "sfUser": "rahul",
            "sfPassword": "Rahul123456789",
            "sfDatabase": "UNIFIED_DATA_LAKE",
            "sfSchema": "DW",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN",
        }
        df = (
            spark.read.format("snowflake")
            .options(**sfOptions)
            .option("dbtable", table_name)
            .load()
        )
        logger.info(f"Successfully read {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error during Snowflake connection: {e}")
        raise

# ------------------ Snowflake Audit Table ------------------
def snowflake_audit_table(logger, table_name, src_df, request):
    sfOptions = {
        "sfURL": "DZAKLEE-HQ11452.snowflakecomputing.com",
        "sfUser": "rahul",
        "sfPassword": "Rahul123456789",
        "sfDatabase": "UNIFIED_DATA_LAKE",
        "sfSchema": "DW",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"    
    }

    if request == 'read':
        try:
            logger.info(f"Reading audit record from Snowflake table: {table_name}")
            audit_df = (
                spark.read.format("snowflake")
                .options(**sfOptions)
                .option("dbtable", table_name)
                .load()
            )
            logger.info(f"Successfully read {table_name}")
            return audit_df
        except Exception as e:
            logger.error(f"Error reading Snowflake audit table: {e}")
            raise
    else:
        try:
            if src_df.count() == 0:
                logger.warning("Source DataFrame is empty, skipping audit insert.")
                return

            last_rundate = src_df.select(F.max("LAST_UPDATED_DATE")).collect()[0][0]
            schema = StructType([
                StructField("jobname", StringType(), True),
                StructField("runid", StringType(), True),
                StructField("lastrundate", TimestampType(), True)
            ])

            audit_df = spark.createDataFrame(
                [(args['JOB_NAME'], 'job_run_id', last_rundate)],
                schema=schema
            )

            audit_df.write \
                .format("snowflake") \
                .options(**sfOptions) \
                .option("dbtable", table_name) \
                .mode("append") \
                .save()

            logger.info("Audit record inserted successfully.")
        except Exception as e:
            logger.error(f"Error inserting into Snowflake audit table: {e}")
            raise

# ------------------ Incremental Loader ------------------
def incremental_loader(logger, src_df, audit_df):
    try:
        logger.info("Incremental loading started...")
        if audit_df.count() == 0:
            logger.warning("No audit records found, loading full data.")
            return src_df
        audit_last_rundate = audit_df.select(F.max("LASTRUNDATE")).collect()[0][0]
        filtered_df = src_df.filter(src_df["LAST_UPDATED_DATE"] > audit_last_rundate)
        logger.info(f"Number of records to be loaded: {filtered_df.count()}")
        return filtered_df
    except Exception as e:
        logger.error(f"Error during incremental loading: {e}")
        raise

# ------------------ S3 Parquet Loader ------------------
def s3_parquet_loader(logger, final_df):
    s3_bucket_name = 'de-project-poc-us-east-1'
    try:
        if final_df.count() == 0:
            logger.warning("Final DataFrame is empty, skipping S3 write.")
            return
        logger.info(f"Writing DataFrame to s3://{s3_bucket_name}/customer.parquet ...")
        final_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .parquet(f"s3://{s3_bucket_name}/customer.parquet")
        logger.info(f"Data written successfully to s3://{s3_bucket_name}/customer.parquet")
    except Exception as e:
        logger.error(f"Error writing to S3 file: {e}")

# ------------------ Main ------------------
def main(logger):
    src_df = snowflake_dw_reader(logger, 'UNIFIED_DATA_LAKE.DW.CUSTOMER')
    audit_table_name = 'UNIFIED_DATA_LAKE.PUBLIC.GLUE_PROCESS_LOGS'
    snow_audit_df = snowflake_audit_table(logger, audit_table_name, src_df, 'read')
    final_df = incremental_loader(logger, src_df, snow_audit_df)
    s3_parquet_loader(logger, final_df)
    snowflake_audit_table(logger, audit_table_name, src_df, 'write')

if __name__ == "__main__":
    main(logger)
    job.commit()
 
