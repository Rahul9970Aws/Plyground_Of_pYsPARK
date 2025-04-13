import sys
import boto3
import logging
from datetime import datetime
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

# Glue setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client("s3")

def validate_and_prepare_df(df):
    logger.info("Validating vehicle data...")
    df_cleaned = df.filter(
        col("vehicle_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("make").isNotNull() &
        col("model").isNotNull() &
        col("year").isNotNull() &
        col("registration_no").isNotNull()
    )
    df_with_audit = df_cleaned.withColumn("insert_date", current_timestamp()) \
                              .withColumn("update_date", current_timestamp())
    return df_with_audit

def s3_file_reader(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return io.BytesIO(response['Body'].read())
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"{key} not found in bucket {bucket}. Treating as first load.")
        return None
    except Exception as e:
        logger.error(f"Error reading {key}: {str(e)}")
        raise

def upload_csv_to_s3(df, bucket='silver-layer-project', key='Dim_Vehicle/vehicle_data.csv'):
    try:
        csv_buffer = io.StringIO()
        df.toPandas().to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"File uploaded to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload CSV: {e}")
        raise

def incremental_type2(source_df):
    source_df = source_df.withColumn("is_active", lit(True)).withColumn("end_date", lit(None).cast("timestamp"))
    dim_data = s3_file_reader("silver-layer-project", "Vehicle/vehicle_data.csv")

    if dim_data is None:
        return source_df

    dim_df = spark.createDataFrame(pd.read_csv(dim_data))
    dim_current = dim_df.filter(col("is_active") == True)

    src = source_df.alias("src")
    dim = dim_current.alias("dim")

    join_cond = dim["vehicle_id"] == src["vehicle_id"]

    joined = dim.join(src, join_cond, "outer").select(
        dim["vehicle_id"].alias("dim_vid"), src["vehicle_id"].alias("src_vid"),
        dim["customer_id"].alias("dim_cid"), src["customer_id"].alias("src_cid"),
        dim["make"].alias("dim_make"), src["make"].alias("src_make"),
        dim["model"].alias("dim_model"), src["model"].alias("src_model"),
        dim["year"].alias("dim_year"), src["year"].alias("src_year"),
        dim["registration_no"].alias("dim_reg"), src["registration_no"].alias("src_reg"),
        dim["insert_date"].alias("dim_insert_date")
    )

    unchanged = joined.filter(
        (col("dim_vid").isNotNull()) & (col("src_vid").isNotNull()) &
        (col("dim_cid") == col("src_cid")) &
        (col("dim_make") == col("src_make")) &
        (col("dim_model") == col("src_model")) &
        (col("dim_year") == col("src_year")) &
        (col("dim_reg") == col("src_reg"))
    ).selectExpr(
        "dim_vid as vehicle_id", "dim_cid as customer_id", "dim_make as make",
        "dim_model as model", "dim_year as year", "dim_reg as registration_no",
        "dim_insert_date as insert_date", "current_timestamp() as update_date",
        "true as is_active", "null as end_date"
    )

    changed = joined.filter(
        (col("dim_vid").isNotNull()) & (col("src_vid").isNotNull()) &
        (
            (col("dim_cid") != col("src_cid")) |
            (col("dim_make") != col("src_make")) |
            (col("dim_model") != col("src_model")) |
            (col("dim_year") != col("src_year")) |
            (col("dim_reg") != col("src_reg"))
        )
    )

    expired = changed.selectExpr(
        "dim_vid as vehicle_id", "dim_cid as customer_id", "dim_make as make",
        "dim_model as model", "dim_year as year", "dim_reg as registration_no",
        "dim_insert_date as insert_date", "current_timestamp() as update_date",
        "false as is_active", "current_timestamp() as end_date"
    )

    new_versions = changed.selectExpr(
        "src_vid as vehicle_id", "src_cid as customer_id", "src_make as make",
        "src_model as model", "src_year as year", "src_reg as registration_no",
        "current_timestamp() as insert_date", "null as update_date",
        "true as is_active", "null as end_date"
    )

    new_records = joined.filter(col("dim_vid").isNull() & col("src_vid").isNotNull()).selectExpr(
        "src_vid as vehicle_id", "src_cid as customer_id", "src_make as make",
        "src_model as model", "src_year as year", "src_reg as registration_no",
        "current_timestamp() as insert_date", "null as update_date",
        "true as is_active", "null as end_date"
    )

    final_df = unchanged.unionByName(expired, allowMissingColumns=True) \
                        .unionByName(new_versions, allowMissingColumns=True) \
                        .unionByName(new_records, allowMissingColumns=True)

    return final_df

def upload_logs_to_s3():
    try:
        log_stream.seek(0)
        log_contents = log_stream.getvalue()
        log_filename = f"Logs/Vehicle_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        s3_client.put_object(Bucket="log-files-bkt", Key=log_filename, Body=log_contents.encode("utf-8"))
        logger.info("Logs uploaded to S3.")
    except Exception as e:
        logger.error(f"Log upload failed: {e}")

def Main(bucket, key):
    logger.info(f"Running vehicle ETL for s3://{bucket}/{key}")
    df = spark.read.parquet(f"s3://{bucket}/{key}")
    validated_df = validate_and_prepare_df(df)
    scd_df = incremental_type2(validated_df)
    upload_csv_to_s3(scd_df)
    upload_logs_to_s3()

if __name__ == "__main__":
    source_bucket = "bronze-layer-project"
    source_key = "vehicle_data/vehicle_data.parquet"
    Main(source_bucket, source_key)
    job.commit()
