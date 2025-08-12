import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, sum, max ,avg

# @params: [JOB_NAME] is a required parameter for Glue jobs
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Standard logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def db_connector():
    try:
        logger.info("Attempting to connect to Snowflake")
        sfOptions = {
            "sfURL": "DZAKLEE-HQ11452.snowflakecomputing.com",
            "sfUser": "rahul",
            "sfPassword": "Rahul123456789",
            "sfDatabase": "UNIFIED_DATA_LAKE",
            "sfSchema": "RAW_DATA",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN",
        }
        logger.info("Snowflake connection options prepared successfully")
        return sfOptions
    except Exception as e:
        logger.error(f"Error during Snowflake connection setup: {e}")
        raise

def read_raw_data(conn):
    try:
        logger.info("Reading raw data from FACT_USER_EVENTS in Snowflake")
        
        raw_data_query = "(SELECT log_id, user_id, event_date,event_timestamp, event_type, quantity,unit_price, total_amount FROM UNIFIED_DATA_LAKE.DW.FACT_USER_EVENTS) AS raw_data_alias"
        conn_raw = conn.copy()
        conn_raw["dbtable"] = raw_data_query
        df = spark.read.format("snowflake").options(**conn_raw).load()

        logger.info("Reading event dimension data from DIM_EVENT_TYPE in Snowflake")
        event_query = "(SELECT EVENT_TYPE_KEY, EVENT_TYPE_NAME, DESCRIPTION FROM UNIFIED_DATA_LAKE.RAW_DATA.DIM_EVENT_TYPE) AS raw_event_type"
        conn_event = conn.copy()
        conn_event["dbtable"] = event_query
        event_df = spark.read.format("snowflake").options(**conn_event).load()

        logger.info("Raw data and event dimension table read successfully")
        return df, event_df
    except Exception as e:
        logger.error(f"Failed to read data from Snowflake: {e}")
        raise

def combine_user_metrics_single_function(src_df, event_df):
    logger.info("Combining user metrics")
    src_df.show(5)
    event_df.show(5)
    # The 'event_date' column is created here
    event_new_df = src_df.join(
    event_df.select(col("EVENT_TYPE_KEY"), col("EVENT_TYPE_NAME")),
    on=src_df.EVENT_TYPE == event_df.EVENT_TYPE_NAME,
        how="left"
    ).drop("EVENT_TYPE_NAME")
    event_new_df.show(6)
    # 'event_date' is now available in this DataFrame and can be used for aggregation
    login_counts_df = event_new_df.groupBy("user_id", "event_date").agg(
        countDistinct("log_id").alias("login_count")
    )

    checkout_df = event_new_df.filter(col("event_type") == "checkout")
    
    daily_checkout_summary = checkout_df.groupBy("user_id", "event_date").agg(
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_amount"),
        avg("unit_price").alias("unit_price"),
        max(col("EVENT_TYPE_KEY")).alias("checkout_event_key")
    )
    
    combined_df = login_counts_df.join(
        daily_checkout_summary,
        on=["user_id", "event_date"],
        how="left"
    )
    
    filled_df = combined_df.na.fill(0, subset=["total_quantity", "total_amount","unit_price"])
    
    final_df = filled_df.select(
        "user_id",
        col("login_count").alias("count_of_logid"),
        "event_date",
        col("checkout_event_key").alias("event_type_key"),
        "total_quantity",
        "unit_price",
        "total_amount"
    ).orderBy("user_id", "event_date")
    
    logger.info("User metrics combined successfully")
    return final_df

def main():
    try:
        logger.info("Starting the data processing job")
        
        conn = db_connector()
        
        raw_df, event_df = read_raw_data(conn)
        
        raw_df.printSchema()

        event_df.printSchema()

        result_df = combine_user_metrics_single_function(raw_df, event_df)
        
        logger.info("Data processed successfully. Loading data to the target table...")
        
        # This is where the output schema is defined, separate from the input schema
        conn['sfSchema'] = 'DW'
        
        result_df.write \
            .format("snowflake") \
            .options(**conn) \
            .option("dbtable", 'FACT_DAILY_USER_METRICS') \
            .mode("append") \
            .save()
            
        logger.info("Data written successfully to Snowflake")
    
    except Exception as e:
        logger.error(f"Job failed with error: {e}")

if __name__ == '__main__':
    main()
    job.commit() # This command signals to Glue that the job has completed successfully
