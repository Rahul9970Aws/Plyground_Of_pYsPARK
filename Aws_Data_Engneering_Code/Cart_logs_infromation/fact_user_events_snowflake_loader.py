import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, explode, lit , when
from pyspark.sql.types import TimestampType

# Standard AWS Glue setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Standard logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def db_connector(logger):
    """
    Establishes Snowflake connection options.
    """
    try:
        logger.info("Connecting to Snowflake")
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
        logger.error(f"Snowflake connection failed: {e}")
        raise

def table_reader(conn, tablename, logger):
    """
    Reads a table from Snowflake into a Spark DataFrame.
    """
    try:
        logger.info(f"Reading Snowflake table: {tablename}")
        df = spark.read \
            .format("snowflake") \
            .options(**conn) \
            .option("dbtable", tablename) \
            .load()
        logger.info(f"Successfully read table {tablename} from Snowflake")
        return df
    except Exception as e:
        logger.error(f"Failed to read table {tablename}: {e}")
        raise




def build_fact_data(src_df, product_df):
    """
    Joins the source data with the product dimension table.
    For 'checkout' events, it uses the category from product_df;
    otherwise, it uses the category from the source DataFrame.
    """
    product_column = "product_id_hash"
    
    # Select product_id, product_id_hash, and category from product_df for joining
    product_lookup = product_df.select("product_id", product_column, "category").alias("p")

    # Join the source data with the product dimension table
    # Since both dataframes have a 'category' column, you need to be careful with selection
    joined_df = src_df.alias("m").join(
        product_lookup,
        on="product_id",
        how="left"
    )

    # Use a single `select` to create the new `category` column and choose all final columns
    new_df = joined_df.select(
        col("m.log_id"),
        col("m.user_id"),
        to_date(col("m.timestamp")).alias("event_date"),
        col("m.timestamp").alias("event_timestamp"),
        col("m.event_type"),
        col("p.product_id_hash"),
        # Use 'when' with explicit aliases for the category columns
        when(col("m.event_type") == "checkout", col("p.category")).otherwise(col("m.category")).alias("category"),
        col("m.quantity"),
        col("m.price"),
        col("m.total_amount")
    )

    return new_df

def main(logger):
    """
    Main execution logic for the Glue job.
    """
    try:
        s3_path = "s3://de-project-poc-us-east-1/Cart_Logs_raw_data/logsdetails.json"
        logger.info(f"Reading JSON data from S3 path: {s3_path}")

        # The previous error indicates that Spark is having trouble reading the JSON file.
        # It's likely the file is a single JSON array spanning multiple lines, rather than
        # JSON Lines. The `multiLine` option tells Spark to read the entire file
        # as a single object, which should resolve the parsing error.
        df = spark.read.option("multiLine", "true").json(s3_path)

        # --- DEBUGGING STEP ---
        # Print the schema and a few rows to inspect the DataFrame after initial read.
        # This will help you confirm if the 'timestamp' column exists or if the data is corrupt.
        logger.info("Schema of the initial DataFrame:")
        df.printSchema()
        logger.info("First 5 rows of the initial DataFrame:")
        df.show(5, truncate=False)
        # --- END DEBUGGING STEP ---

        # Check if the 'timestamp' column exists before trying to cast it.
        # The original error occurred here because the column was not found.
        if "timestamp" in df.columns:
            df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
            logger.info("Successfully cast timestamp column.")
        else:
            logger.error("The 'timestamp' column was not found after reading the JSON file. Check the source data structure.")
            raise ValueError("Required 'timestamp' column is missing from the source data.")

        logger.info(f"Successfully read JSON data with {df.count()} records")

        # Separate checkout and non-checkout events
        checkout_df = df.filter(col("event_type") == "checkout").select(
            "log_id", "user_id", "timestamp", "event_type", "category", "total_amount", explode("cart_items").alias("cart_item")
        )

        checkout_expanded = checkout_df.select(
            "log_id",
            "user_id",
            "timestamp",
            "event_type",
            "category",
            col("cart_item.product_id").alias("product_id"),
            col("cart_item.price").alias("price"),
            col("cart_item.quantity").alias("quantity"),
            "total_amount"
        )
        
        non_checkout_df = df.filter(col("event_type") != "checkout").drop("cart_items")

        # Add missing columns to non_checkout_df (product_id, price, quantity) with nulls
        non_checkout_filled = non_checkout_df

        # Select columns in order
        non_checkout_final = non_checkout_filled.select(
            "log_id", "user_id", "timestamp", "event_type", "category", "price", "quantity", "total_amount", "product_id"
        )
        checkout_final = checkout_expanded.select(
            "log_id", "user_id", "timestamp", "event_type", "category", "price", "quantity", "total_amount", "product_id"
        )

        processed_df = non_checkout_final.unionByName(checkout_final)

        logger.info(f"Processed DataFrame count: {processed_df.count()}")

        # Read product dimension from Snowflake
        dim_product = 'DIM_PRODUCT_DETAILS'
        logger.info("Connecting to Snowflake Database....")
        conn = db_connector(logger)
        logger.info(f"Extracting Data from the {dim_product}")
        product_df = table_reader(conn, dim_product, logger)

        logger.info("Building the fact data by joining with product dimension...")
        result_df = build_fact_data(processed_df, product_df)
        logger.info(f"Fact data count after join: {result_df.count()}")

        logger.info("Writing result DataFrame to Snowflake table DW.FACT_USER_EVENTS...")
        result_df.write \
            .format("snowflake") \
            .options(**conn) \
            .option("dbtable", 'DW.FACT_USER_EVENTS') \
            .mode("append") \
            .save()
        logger.info("Data written successfully to Snowflake")

    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == '__main__':
    try:
        main(logger)
        job.commit()
    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        # Optionally, you can add a job.set_failure_state() here if needed in a real scenario
        raise
