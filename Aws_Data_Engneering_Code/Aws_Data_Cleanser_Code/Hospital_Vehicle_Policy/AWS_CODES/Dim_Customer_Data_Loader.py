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
# Configure Logging with in-memory stream for upload
import io

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

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3_client = boto3.client("s3")


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
    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        raise


def upload_csv_to_s3(df, bucket_name='policy-dimesion-bucket', object_name='Dim_Customer/Dim_Customer_data.csv'):
    logger.info(f"Uploading final CSV to s3://{bucket_name}/{object_name}")
    try:
        pandas_df = df.toPandas()   # Convert Spark DataFrame to Pandas DataFrame
        csv_buffer = io.StringIO()          # Write the pandas DataFrame to a CSV buffer
        pandas_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=object_name
        )

        logger.info("Upload successful.")
        return {
            "statusCode": 200,
            "body": f"File uploaded successfully to s3://{bucket_name}/{object_name}"
        }
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return {
            "statusCode": 500,
            "body": f"Error uploading file: {str(e)}"
        }



def incemenatl_type2(cleansed_df):
    logger.info("Starting SCD Type 2 processing...")

    source_df = cleansed_df.withColumn("is_active", lit(True)) \
                           .withColumn("end_date", lit(None).cast("timestamp"))
    target_data = s3_file_reader('policy-dimesion-bucket', 'Dim_Customer/dim_customer_data.csv')

    logger.info("Reading existing dimension data from CSV...")
    pdf_dim = pd.read_csv(target_data)
    dim_df = spark.createDataFrame(pdf_dim)
    logger.info("Dimension data loaded.")

    dim_current_df = dim_df.filter(col("is_active") == True)


    dim_alias = dim_current_df.alias("dim")
    src_alias = source_df.alias("src")

    # Renaming conflicting columns to avoid ambiguity
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
    ).select("dim_customer_id", "dim_first_name", "dim_last_name", "dim_dob", "dim_gender", "dim_address", 
             "dim_city", "dim_state", "dim_zip", "dim_insert_date")

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

    logger.info("Expiring old records...")
    expired_records = changed.select(
        changed["dim_customer_id"],
        changed["dim_first_name"],
        changed["dim_last_name"],
        changed["dim_dob"],
        changed["dim_gender"],
        changed["dim_address"],
        changed["dim_city"],
        changed["dim_state"],
        changed["dim_zip"],
        changed["dim_insert_date"],
        current_timestamp().alias("update_date"),
        lit(False).alias("is_active")
    )

    logger.info("Creating new versions of changed records...")
    new_versions = changed.select(
        changed["src_customer_id"],
        changed["src_first_name"],
        changed["src_last_name"],
        changed["src_dob"],
        changed["src_gender"],
        changed["src_address"],
        changed["src_city"],
        changed["src_state"],
        changed["src_zip"],
        current_timestamp().alias("insert_date"),
        lit(None).cast("timestamp").alias("update_date"),
        lit(True).alias("is_active")
    )

    logger.info("Identifying new inserts...")
    new_inserts = joined_df.filter(
        joined_df["dim_customer_id"].isNull() & joined_df["src_customer_id"].isNotNull()
    ).select(
        joined_df["src_customer_id"],
        joined_df["src_first_name"],
        joined_df["src_last_name"],
        joined_df["src_dob"],
        joined_df["src_gender"],
        joined_df["src_address"],
        joined_df["src_city"],
        joined_df["src_state"],
        joined_df["src_zip"],
        current_timestamp().alias("insert_date"),
        lit(None).cast("timestamp").alias("update_date"),
        lit(True).alias("is_active")
    )

    logger.info("Combining all records into final dimension DataFrame...")
    final_df = unchanged.unionByName(expired_records, allowMissingColumns=True).unionByName(new_versions, allowMissingColumns=True).unionByName(new_inserts, allowMissingColumns=True)
    logger.info("SCD Type 2 processing complete.")
    return final_df


def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_filename = f"Logs/Dimn_customer_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        s3_client.put_object(
            Bucket='myawsgeneralbucket1826',
            Key=log_filename,
            Body=log_contents.encode("utf-8")
        )
        logger.info(f"Logs uploaded to S3 at: s3://myawsgeneralbucket1826/{log_filename}")
    except Exception as e:
        logger.error(f"Failed to upload logs to S3: {e}")


def Main(bucket, key):
    logger.info(f"Starting ETL for file: s3://{bucket}/{key}")
    # Read data into Spark DataFrame directly from S3 Parquet file
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


# Run script
if __name__ == "__main__":
    source_bucket = "ploicy-cleansed-layer"
    source_object = "customer_data/customer_data.parquet"
    Main(source_bucket, source_object)
    job.commit()
