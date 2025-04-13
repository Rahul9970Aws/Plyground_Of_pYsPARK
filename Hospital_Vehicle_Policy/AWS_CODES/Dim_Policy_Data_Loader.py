import sys
import boto3
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp
import pandas as pd
import io

# Setup logging
log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.StreamHandler(log_stream)
    ]
)
logger = logging.getLogger(__name__)

# Get job args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 client
s3_client = boto3.client("s3")

# ========================= #
#     Helper Functions      #
# ========================= #

def validate_and_prepare_df(dataframe):
    logger.info("Validating and preparing input DataFrame...")
    df_cleaned = dataframe.filter(
        col("policy_id").isNotNull() &
        col("policy_type").isNotNull() &
        col("coverage_type").isNotNull() &
        col("start_date").isNotNull() &
        col("end_date").isNotNull() &
        col("annual_premium").isNotNull()
    )
    df_with_audit = df_cleaned.withColumn("insert_date", current_timestamp()) \
                              .withColumn("update_date", current_timestamp())
    logger.info("Validation complete. Audit columns added.")
    return df_with_audit


def s3_file_reader(bucket_name, object_name):
    logger.info(f"Reading file from S3: s3://{bucket_name}/{object_name}")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        data = io.BytesIO(response['Body'].read())
        logger.info("File successfully read into memory.")
        return data
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"NoSuchKey: s3://{bucket_name}/{object_name} not found.")
        return None
    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        raise


def upload_csv_to_s3(df, bucket_name='silver-layer-project', object_name='Dim_Policy_Data/Policy_Data.csv'):
    logger.info(f"Uploading final CSV to s3://{bucket_name}/{object_name}")
    try:
        pandas_df = df.toPandas()
        csv_buffer = io.StringIO()
        pandas_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=object_name
        )
        logger.info("Upload successful.")
        return {"statusCode": 200, "body": f"File uploaded successfully to s3://{bucket_name}/{object_name}"}
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return {"statusCode": 500, "body": f"Error uploading file: {str(e)}"}


def incremental_type2(cleansed_df):
    logger.info("Starting SCD Type 2 processing...")

    source_df = cleansed_df.withColumn("is_active", lit(True)) \
                           .withColumn("end_date_version", lit(None).cast("timestamp"))

    target_data = s3_file_reader('silver-layer-project', 'Dim_Policy_Data/Policy_Data.csv')

    if target_data is None:
        logger.warning("No existing dimension file found. Treating all records as new inserts.")
        return source_df

    logger.info("Reading existing dimension data from CSV...")
    pdf_dim = pd.read_csv(target_data)
    dim_df = spark.createDataFrame(pdf_dim)
    logger.info("Dimension data loaded.")

    dim_current_df = dim_df.filter(col("is_active") == True)

    dim_alias = dim_current_df.alias("dim")
    src_alias = source_df.alias("src")

    join_cond = dim_alias["policy_id"] == src_alias["policy_id"]

    joined_df = dim_alias.join(src_alias, join_cond, "outer") \
        .select(
            "dim.*", "src.policy_type", "src.coverage_type", "src.start_date", "src.end_date", "src.annual_premium"
        )

    logger.info("Identifying unchanged records...")
    unchanged = joined_df.filter(
        (col("dim.policy_id").isNotNull()) &
        (col("src.policy_type") == col("dim.policy_type")) &
        (col("src.coverage_type") == col("dim.coverage_type")) &
        (col("src.start_date") == col("dim.start_date")) &
        (col("src.end_date") == col("dim.end_date")) &
        (col("src.annual_premium") == col("dim.annual_premium"))
    ).selectExpr(
        "policy_id", "policy_type", "coverage_type", "start_date", "end_date", "annual_premium",
        "insert_date", "current_timestamp() as update_date", "true as is_active", "null as end_date_version"
    )

    logger.info("Identifying changed records...")
    changed = joined_df.filter(
        (col("dim.policy_id").isNotNull()) &
        (
            (col("src.policy_type") != col("dim.policy_type")) |
            (col("src.coverage_type") != col("dim.coverage_type")) |
            (col("src.start_date") != col("dim.start_date")) |
            (col("src.end_date") != col("dim.end_date")) |
            (col("src.annual_premium") != col("dim.annual_premium"))
        )
    )

    expired = changed.selectExpr(
        "policy_id", "dim.policy_type as policy_type", "dim.coverage_type as coverage_type",
        "dim.start_date as start_date", "dim.end_date as end_date", "dim.annual_premium as annual_premium",
        "insert_date", "current_timestamp() as update_date", "false as is_active", "current_timestamp() as end_date_version"
    )

    new_versions = changed.selectExpr(
        "policy_id", "src.policy_type as policy_type", "src.coverage_type as coverage_type",
        "src.start_date as start_date", "src.end_date as end_date", "src.annual_premium as annual_premium",
        "current_timestamp() as insert_date", "null as update_date", "true as is_active", "null as end_date_version"
    )

    logger.info("Identifying new inserts...")
    new_inserts = joined_df.filter(
        col("dim.policy_id").isNull() & col("policy_id").isNotNull()
    ).selectExpr(
        "policy_id", "src.policy_type as policy_type", "src.coverage_type as coverage_type",
        "src.start_date as start_date", "src.end_date as end_date", "src.annual_premium as annual_premium",
        "current_timestamp() as insert_date", "null as update_date", "true as is_active", "null as end_date_version"
    )

    final_df = unchanged.unionByName(expired, allowMissingColumns=True) \
                        .unionByName(new_versions, allowMissingColumns=True) \
                        .unionByName(new_inserts, allowMissingColumns=True)

    logger.info("SCD Type 2 processing complete.")
    return final_df


def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_filename = f"Logs/Policy_Data_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        s3_client.put_object(
            Bucket='log-files-bkt',
            Key=log_filename,
            Body=log_contents.encode("utf-8")
        )
        logger.info(f"Logs uploaded to S3 at: s3://log-files-bkt/{log_filename}")
    except Exception as e:
        logger.error(f"Failed to upload logs to S3: {e}")


# ========================= #
#         Main Job          #
# ========================= #

def Main(bucket, key):
    logger.info(f"Starting ETL for file: s3://{bucket}/{key}")
    spark_df = spark.read.parquet(f"s3://{bucket}/{key}")

    logger.info("Cleaning and auditing input DataFrame...")
    cleansed_df = validate_and_prepare_df(spark_df)

    logger.info("Applying SCD Type 2 transformation...")
    final_df = incremental_type2(cleansed_df)

    logger.info("Uploading transformed data to S3...")
    result = upload_csv_to_s3(final_df)
    logger.info(result["body"])

    logger.info("ETL pipeline completed successfully.")
    upload_logs_to_s3()


if __name__ == "__main__":
    source_bucket = "bronze-layer-project"
    source_object = "policy_data/policy_data.parquet"
    Main(source_bucket, source_object)
    job.commit()
