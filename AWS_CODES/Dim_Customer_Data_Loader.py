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
        col("address").isNotNull() & 
        col("first_name").isNotNull() & 
        col("last_name").isNotNull()
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


def upload_csv_to_s3(df, bucket_name='silver-layer-project', object_name='Dim_Customer/Dim_Customer_data.csv'):
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


def incemenatl_type2(cleansed_df):
    logger.info("Starting SCD Type 2 processing...")

    source_df = cleansed_df.withColumn("is_active", lit(True)) \
                           .withColumn("end_date", lit(None).cast("timestamp"))

    target_data = s3_file_reader('silver-layer-project', 'Dim_Customer/Dim_Customer_data.csv')

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

    join_cond = dim_alias["customer_id"] == src_alias["customer_id"]

    joined_df = dim_alias.join(src_alias, join_cond, "outer") \
                         .select(
                             dim_alias["customer_id"].alias("dim_customer_id"),
                             src_alias["customer_id"].alias("src_customer_id"),
                             dim_alias["first_name"].alias("dim_first_name"),
                             src_alias["first_name"].alias("src_first_name"),
                             dim_alias["last_name"].alias("dim_last_name"),
                             src_alias["last_name"].alias("src_last_name"),
                             dim_alias["dob"].alias("dim_dob"),
                             src_alias["dob"].alias("src_dob"),
                             dim_alias["gender"].alias("dim_gender"),
                             src_alias["gender"].alias("src_gender"),
                             dim_alias["address"].alias("dim_address"),
                             src_alias["address"].alias("src_address"),
                             dim_alias["city"].alias("dim_city"),
                             src_alias["city"].alias("src_city"),
                             dim_alias["state"].alias("dim_state"),
                             src_alias["state"].alias("src_state"),
                             dim_alias["zip"].alias("dim_zip"),
                             src_alias["zip"].alias("src_zip"),
                             dim_alias["insert_date"].alias("dim_insert_date"),
                             src_alias["insert_date"].alias("src_insert_date")
                         )

    logger.info("Identifying unchanged records...")
    unchanged = joined_df.filter(
        (joined_df["dim_customer_id"].isNotNull()) &
        (joined_df["src_customer_id"].isNotNull()) &
        (joined_df["dim_first_name"] == joined_df["src_first_name"]) &
        (joined_df["dim_last_name"] == joined_df["src_last_name"]) &
        (joined_df["dim_dob"] == joined_df["src_dob"]) &
        (joined_df["dim_gender"] == joined_df["src_gender"]) &
        (joined_df["dim_address"] == joined_df["src_address"]) &
        (joined_df["dim_city"] == joined_df["src_city"]) &
        (joined_df["dim_state"] == joined_df["src_state"]) &
        (joined_df["dim_zip"] == joined_df["src_zip"])
    ).selectExpr(
        "dim_customer_id as customer_id", "dim_first_name as first_name", "dim_last_name as last_name",
        "dim_dob as dob", "dim_gender as gender", "dim_address as address", "dim_city as city",
        "dim_state as state", "dim_zip as zip", "dim_insert_date as insert_date",
        "current_timestamp() as update_date", "true as is_active", "null as end_date"
    )

    logger.info("Identifying changed records...")
    changed = joined_df.filter(
        (joined_df["dim_customer_id"].isNotNull()) & 
        (joined_df["src_customer_id"].isNotNull()) & 
        (
            (joined_df["dim_first_name"] != joined_df["src_first_name"]) |
            (joined_df["dim_last_name"] != joined_df["src_last_name"]) |
            (joined_df["dim_dob"] != joined_df["src_dob"]) |
            (joined_df["dim_gender"] != joined_df["src_gender"]) |
            (joined_df["dim_address"] != joined_df["src_address"]) |
            (joined_df["dim_city"] != joined_df["src_city"]) |
            (joined_df["dim_state"] != joined_df["src_state"]) |
            (joined_df["dim_zip"] != joined_df["src_zip"])
        )
    )

    expired_records = changed.selectExpr(
        "dim_customer_id as customer_id", "dim_first_name as first_name", "dim_last_name as last_name",
        "dim_dob as dob", "dim_gender as gender", "dim_address as address", "dim_city as city",
        "dim_state as state", "dim_zip as zip", "dim_insert_date as insert_date",
        "current_timestamp() as update_date", "false as is_active", "current_timestamp() as end_date"
    )

    new_versions = changed.selectExpr(
        "src_customer_id as customer_id", "src_first_name as first_name", "src_last_name as last_name",
        "src_dob as dob", "src_gender as gender", "src_address as address", "src_city as city",
        "src_state as state", "src_zip as zip",
        "current_timestamp() as insert_date", "null as update_date", "true as is_active", "null as end_date"
    )

    logger.info("Identifying new inserts...")
    new_inserts = joined_df.filter(
        joined_df["dim_customer_id"].isNull() & joined_df["src_customer_id"].isNotNull()
    ).selectExpr(
        "src_customer_id as customer_id", "src_first_name as first_name", "src_last_name as last_name",
        "src_dob as dob", "src_gender as gender", "src_address as address", "src_city as city",
        "src_state as state", "src_zip as zip",
        "current_timestamp() as insert_date", "null as update_date", "true as is_active", "null as end_date"
    )

    logger.info("Combining all records into final dimension DataFrame...")
    final_df = unchanged.unionByName(expired_records, allowMissingColumns=True) \
                        .unionByName(new_versions, allowMissingColumns=True) \
                        .unionByName(new_inserts, allowMissingColumns=True)

    logger.info("SCD Type 2 processing complete.")
    return final_df


def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_filename = f"Logs/Dimn_customer_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
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
    final_df = incemenatl_type2(cleansed_df)

    logger.info("Uploading transformed data to S3...")
    result = upload_csv_to_s3(final_df)
    logger.info(result["body"])

    logger.info("ETL pipeline completed successfully.")
    upload_logs_to_s3()


if __name__ == "__main__":
    source_bucket = "bronze-layer-project"
    source_object = "customer_data/customer_data.parquet"
    Main(source_bucket, source_object)
    job.commit()
