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
    handlers=[logging.StreamHandler(sys.stdout), logging.StreamHandler(log_stream)]
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

def validate_and_prepare_df(dataframe):
    logger.info("Validating and preparing input DataFrame...")
    df_cleaned = dataframe.filter(
        col("hospital_id").isNotNull() &
        col("hospital_name").isNotNull() &
        col("location").isNotNull()
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
        return data
    except s3_client.exceptions.NoSuchKey:
        logger.warning("File not found.")
        return None
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise

def upload_csv_to_s3(df, bucket_name='silver-layer-project', object_name='Dim_Hospital/hospital_data.csv'):
    logger.info(f"Uploading transformed data to s3://{bucket_name}/{object_name}")
    try:
        pandas_df = df.toPandas()
        csv_buffer = io.StringIO()
        pandas_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_name)
        logger.info("Upload successful.")
        return {"statusCode": 200, "body": f"Uploaded to s3://{bucket_name}/{object_name}"}
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return {"statusCode": 500, "body": str(e)}

def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_file = f"Logs/Hospital_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        s3_client.put_object(Bucket='log-files-bkt', Key=log_file, Body=log_contents.encode("utf-8"))
        logger.info(f"Logs uploaded: s3://log-files-bkt/{log_file}")
    except Exception as e:
        logger.error(f"Log upload failed: {e}")

def scd_type2_hospital(cleansed_df):
    logger.info("Performing SCD Type 2 logic for hospital data...")

    source_df = cleansed_df.withColumn("is_active", lit(True)).withColumn("end_date", lit(None).cast("timestamp"))

    target_data = s3_file_reader('silver-layer-project', 'Dim_Hospital/hospital_data.csv')

    if target_data is None:
        logger.warning("No existing data. Treating all as new records.")
        return source_df

    pdf_dim = pd.read_csv(target_data)
    dim_df = spark.createDataFrame(pdf_dim)
    dim_current_df = dim_df.filter(col("is_active") == True)

    dim = dim_current_df.alias("dim")
    src = source_df.alias("src")

    join_cond = dim["hospital_id"] == src["hospital_id"]

    joined = dim.join(src, join_cond, "outer") \
        .select(
            dim["hospital_id"].alias("dim_hospital_id"),
            src["hospital_id"].alias("src_hospital_id"),
            dim["hospital_name"].alias("dim_hospital_name"),
            src["hospital_name"].alias("src_hospital_name"),
            dim["location"].alias("dim_location"),
            src["location"].alias("src_location"),
            dim["accreditation"].alias("dim_accreditation"),
            src["accreditation"].alias("src_accreditation"),
            dim["insert_date"].alias("dim_insert_date"),
            src["insert_date"].alias("src_insert_date")
        )

    unchanged = joined.filter(
        (col("dim_hospital_id").isNotNull()) & (col("src_hospital_id").isNotNull()) &
        (col("dim_hospital_name") == col("src_hospital_name")) &
        (col("dim_location") == col("src_location")) &
        (col("dim_accreditation") == col("src_accreditation"))
    ).selectExpr(
        "dim_hospital_id as hospital_id", "dim_hospital_name as hospital_name",
        "dim_location as location", "dim_accreditation as accreditation",
        "dim_insert_date as insert_date", "current_timestamp() as update_date",
        "true as is_active", "null as end_date"
    )

    changed = joined.filter(
        (col("dim_hospital_id").isNotNull()) & (col("src_hospital_id").isNotNull()) & (
            (col("dim_hospital_name") != col("src_hospital_name")) |
            (col("dim_location") != col("src_location")) |
            (col("dim_accreditation") != col("src_accreditation"))
        )
    )

    expired = changed.selectExpr(
        "dim_hospital_id as hospital_id", "dim_hospital_name as hospital_name",
        "dim_location as location", "dim_accreditation as accreditation",
        "dim_insert_date as insert_date", "current_timestamp() as update_date",
        "false as is_active", "current_timestamp() as end_date"
    )

    new_versions = changed.selectExpr(
        "src_hospital_id as hospital_id", "src_hospital_name as hospital_name",
        "src_location as location", "src_accreditation as accreditation",
        "current_timestamp() as insert_date", "null as update_date",
        "true as is_active", "null as end_date"
    )

    new_inserts = joined.filter(col("dim_hospital_id").isNull() & col("src_hospital_id").isNotNull()) \
        .selectExpr(
            "src_hospital_id as hospital_id", "src_hospital_name as hospital_name",
            "src_location as location", "src_accreditation as accreditation",
            "current_timestamp() as insert_date", "null as update_date",
            "true as is_active", "null as end_date"
        )

    final_df = unchanged.unionByName(expired, allowMissingColumns=True) \
                        .unionByName(new_versions, allowMissingColumns=True) \
                        .unionByName(new_inserts, allowMissingColumns=True)

    logger.info("SCD Type 2 transformation complete.")
    return final_df

def Main(bucket, key):
    logger.info(f"Running Hospital ETL for file: s3://{bucket}/{key}")
    df = spark.read.parquet(f"s3://{bucket}/{key}")
    cleaned_df = validate_and_prepare_df(df)
    scd_df = scd_type2_hospital(cleaned_df)
    result = upload_csv_to_s3(scd_df)
    logger.info(result["body"])
    upload_logs_to_s3()

if __name__ == "__main__":
    source_bucket = "bronze-layer-project"
    source_object = "hospital_data/hospital_data.parquet"
    Main(source_bucket, source_object)
    job.commit()
