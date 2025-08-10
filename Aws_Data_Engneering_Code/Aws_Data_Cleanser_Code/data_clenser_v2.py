from io import StringIO

class DataCleanser():

    def validate_columns(df):
        # Drop duplicate customer_id rows
        df_duplicate = df.drop_duplicates(subset='customer_id', keep='first')

        # Drop rows with null values in key columns
        df_cleaned = df_duplicate.dropna(subset=['customer_id', 'first_name', 'last_name', 'address'])

        # Create full_name column (fix applied here)
        df_cleaned['full_name'] = df_cleaned['first_name'] + ' ' + df_cleaned['last_name']

        # Drop original name columns
        df_final = df_cleaned.drop(columns=['first_name', 'last_name'])

        return df_final

    def db_connector():
        try:
            sfOptions = {
                "sfURL": 'DZAKLEE-HQ11452.snowflakecomputing.com',
                "sfDatabase": "UNIFIED_DATA_LAKE",
                "sfSchema": "RAW_DATA",
                "sfWarehouse": "COMPUTE_WH",
                "sfUser": "rahul",
                "sfPassword": "Rahul123456789",
                "dbtable": "CUSTOMER_DETAILS"
            }
            return sfOptions
        except Exception as e:
            print(f"connection setup failed: {e}")
            raise

    def data_loader_snowflake(conn, df, logger):
        try:
            logger.info("Writing DataFrame to Snowflake")

            # This assumes df is a Spark DataFrame
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
        import boto3
        import pandas as pd
        from pyspark.sql import SparkSession

        s3 = boto3.client('s3')
        logger.info(f"reading file from s3://{bucket}/{filename}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=filename)
            body = obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(body))

            # Validate and clean
            validate_records = DataCleanser.validate_columns(df)
            logger.info("Validation of Data is Completed...")

            # Connect to Snowflake
            logger.info("Connecting to Snowflake Database...")
            conn = DataCleanser.db_connector()
            logger.info("Connected to Database...")

            # Convert pandas DataFrame to Spark DataFrame
            spark = SparkSession.builder.appName("DataCleanserJob").getOrCreate()
            spark_df = spark.createDataFrame(validate_records)

            # Load to Snowflake
            DataCleanser.data_loader_snowflake(conn, spark_df, logger)
            logger.info("Data loaded to Snowflake db...")

        except Exception as e:
            logger.error(f"error in reading or processing file: {e}")
