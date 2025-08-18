import pandas as pd
import boto3
from io import StringIO
import json
from botocore.exceptions import ClientError

def upload_df_to_s3(df,bucket_name,file_key,logger=None):
    try:
        if logger:
            logger.info("upload_df_to_s3() started...")

        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )

        if logger:
            logger.info(f"File uploaded to s3://{bucket_name}/{file_key}")
            logger.info("upload_df_to_s3() completed successfully.")

    except Exception as e:
        if logger:
            logger.error(f"Error in upload_df_to_s3(): {str(e)}")
        else:
            print(f"Error: {str(e)}")

def get_email_templete(lambda_name,s3_file_path,valid_count,invalid_count,logger=None):
    subject = f"Lambda {lambda_name} - Data Validation Report"
    body_text = (
        f"Lambda Function: {lambda_name}\n"
        f"Valid Records: {valid_count}\n"
        f"Invalid Records: {invalid_count}\n"
        f"Corrupt file stored at: {s3_file_path}\n"
    )
    body_html = f"""
    <html>
    <body>
        <h2>Lambda Data Validation Report</h2>
        <p><b>Lambda Function:</b> {lambda_name}</p>
        <p><b>Valid Records:</b> {valid_count}</p>
        <p><b>Invalid Records:</b> {invalid_count}</p>
        <p><b>Corrupt File Path:</b> <a href="{s3_file_path}">{s3_file_path}</a></p>
    </body>
    </html>
    """

    send_email_ses(
        to_email=["nayan19.inf@gmail.com"],
        from_email="nayananda.sm@gmail.com",  # must be verified in SES
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        logger=logger
    )


def send_email_ses(to_email, from_email, subject, body_text, body_html=None, logger=None):
    try:
        ses_client = boto3.client("ses", region_name="us-east-1")  # Change region if needed

        if not body_html:
            body_html = f"<html><body><p>{body_text}</p></body></html>"

        # Ensure to_email is always a list of strings
        if isinstance(to_email, str):
            to_addresses = [to_email]
        elif isinstance(to_email, list):
            to_addresses = to_email
        else:
            raise ValueError("to_email must be a string or list of strings")

        response = ses_client.send_email(
            Source=from_email,
            Destination={
                "ToAddresses": to_addresses
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": body_text},
                    "Html": {"Data": body_html}
                }
            }
        )

        if logger:
            logger.info(f"Email sent! Message ID: {response['MessageId']}")
        return response

    except ClientError as e:
        if logger:
            logger.error(f"Error sending email: {e.response['Error']['Message']}")
        else:
            print(f"Error sending email: {e.response['Error']['Message']}")
        return None

