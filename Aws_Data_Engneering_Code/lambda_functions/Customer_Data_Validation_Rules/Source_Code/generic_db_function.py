import pandas as pd
import boto3
import pg8000.native
from io import StringIO
import json


connection = None

def database_connection(logger):
    global connection
    try:
        logger.info("Starting database connection...")
        
        # Fetch parameter from SSM
        ssm = boto3.client("ssm")
        parameter = ssm.get_parameter(
            Name="/myapp/dev/secrets",
            WithDecryption=True
        )
        logger.info("Fetched DB credentials from SSM")

        # Parse JSON value
        config = json.loads(parameter["Parameter"]["Value"])

        # Extract values
        db_host = config["host"]
        db_name = config["dbname"]
        db_user = config["user"]
        db_password = config["password"]

        # Establish connection
        connection = pg8000.native.Connection(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_name,
            port=5432,
            ssl_context=True
        )
        logger.info("Database connection established successfully")
        return connection

    except Exception as e:
        logger.error(f"Error while connecting to database: {str(e)}")
        raise


def shutdown_connection(logger):
    global connection
    if connection:
        try:
            logger.info("Closing database connection...")
            connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")


def insert_records(df, connection, logger):
    if connection is None:
        logger.error("No active DB connection. Call database_connection() first.")
        return

    try:
        logger.info("insert_records() started...")

        for _, row in df.iterrows():
            query = """
                INSERT INTO public.dim_customer
                (customer_id, full_name, email, age, date_of_birth, phone_number, country)
                VALUES (:customer_id, :full_name, :email, :age, :date_of_birth, :phone_number, :country)
                ON CONFLICT (customer_id) DO NOTHING
                """
            connection.run(
                query,
                customer_id=row["customer_id"],
                full_name=row["full_name"],
                email=row["email"],
                age=row["age"],
                date_of_birth=row["date_of_birth"],
                phone_number=row["phone_number"],
                country=row["country"]
            )

        logger.info("insert_records() completed successfully.")

    except Exception as e:
        logger.error(f"Error in insert_records(): {str(e)}")
