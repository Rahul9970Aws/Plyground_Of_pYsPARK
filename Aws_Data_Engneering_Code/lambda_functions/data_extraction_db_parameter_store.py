import pandas as pd
import boto3
import pg8000.native
from io import StringIO
import json

def lambda_handler(event, context):
    try:
        # ✅ Fetch parameter from SSM
        ssm = boto3.client("ssm")
        parameter = ssm.get_parameter(
            Name="/store/db-credentials/db-config",
            WithDecryption=True
        )
        
        # Parse JSON value
        config = json.loads(parameter["Parameter"]["Value"])

        # Extract values
        db_host = config["DB_HOST"]
        db_name = config["DB_NAME"]
        db_user = config["DB_USER"]
        db_password = config["DB_PASSWORD"]
        bucket_name = config["S3_BUCKET"]
        s3_key = config["S3_KEY"]

        # ✅ Connect to Neon DB
        conn = pg8000.native.Connection(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_name,
            port=5432,
            ssl_context=True
        )

        query = "SELECT * FROM playing_with_neon;"

        # Run query -> rows + column names
        result = conn.run(query)  # list of tuples
        cols = [c["name"] for c in conn.columns]  # ✅ get column names from metadata

        # Convert to DataFrame
        df = pd.DataFrame(result, columns=cols)

        conn.close()

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

        return {
            "statusCode": 200,
            "body": f"Data exported successfully to s3://{bucket_name}/{s3_key}"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
