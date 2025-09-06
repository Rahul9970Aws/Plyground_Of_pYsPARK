## write a python program to read files from s3 where s3 has mutiple files with employee1, empl,emp2,employee2 having same column strucuture and 
## witre thr data from above 4 files data in a single file.

import boto3
import pandas as pd
from io import *

bucket_name  = 'data-engineering-generic-bkt'
key = 's3_vs_code_validation/'

ACCESS_KEY = "-----------"
SECRET_KEY = "---------------"
REGION = "us-east-1"   # example region

final_data = []
# Create S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)
response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=key)
files = response['Contents']
for file in files:
    if file['Key'].endswith(".csv"):
        obj = s3_client.get_object(Bucket=bucket_name,Key=file['Key'])

        try:
            df = pd.read_csv(obj['Body'], encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(obj['Body'], encoding="latin1")  # fallback

        print(f"file: {file['Key']} and size: {len(df)}")
        final_data.append(df)
final_df = pd.concat(final_data)
csv_buffer = StringIO()
final_df.to_csv(csv_buffer,index=False)
thg_key = 's3_vs_code_validation/final_cust_df.csv'
s3_client.put_object(Bucket=bucket_name,Key = thg_key,Body=csv_buffer.getvalue())








