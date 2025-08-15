
import os
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# Get only JOB_NAME from args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# JOB_RUN_ID from environment
job_run_id = os.environ.get('AWS_GLUE_JOB_RUN_ID')

# Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def snowflake_audit_table(table_name):
    sfOptions = {
        "sfURL": "DZAKLEE-HQ11452.snowflakecomputing.com",
        "sfUser": "rahul",
        "sfPassword": "Rahul123456789",
        "sfDatabase": "UNIFIED_DATA_LAKE",
        "sfSchema": "DW",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN",
    }
    audit_df = spark.createDataFrame(
        [(args['JOB_NAME'], job_run_id, datetime.now())],
        ["jobname", "runid", "lastrundate"]
    )
    audit_df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()
    logger.info("✅ Audit record inserted successfully.")

def main():
    snowflake_audit_table("JOB_AUDIT")

if __name__ == "__main__":
    main()
    job.commit()
