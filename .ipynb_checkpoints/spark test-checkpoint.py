from pyspark.sql import SparkSession
import logging
import boto3
import io
from logging.handlers import MemoryHandler
import datetime

class MyDataProcessor:
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger("data_logger")
        logger.setLevel(logging.INFO)
        handler = self._create_s3_handler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _create_s3_handler(self):
        class S3Handler(MemoryHandler):
            def __init__(self, bucket, prefix):
                super().__init__(1024)
                self.bucket = bucket
                self.prefix = prefix
                self.s3 = boto3.client('s3')

            def flush(self):
                if self.buffer:
                    log_content = '\n'.join([self.format(record) for record in self.buffer])
                    key = f"{self.prefix}/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                    try:
                        self.s3.put_object(Bucket=self.bucket, Key=key, Body=log_content)
                    except Exception as e:
                        logging.error(f"S3 upload error: {e}")
                    self.buffer.clear()

        return S3Handler(self.bucket, self.prefix)

    def process_data(self, spark):
        self.logger.info("Starting data processing.")
        try:
            data = [("A", 1), ("B", 2)]
            df = spark.createDataFrame(data, ["col1", "col2"])
            df.show()
            self.logger.info("Data processed successfully.")
            self.logger.handlers[0].flush()
        except Exception as e:
            self.logger.error(f"Error: {e}")
            self.logger.handlers[0].flush()

if __name__ == "__main__":
    bucket = "myawsgeneralbucket1826 "  # Replace with your S3 bucket name
    prefix = "Logs"  # Replace with your desired S3 prefix

    try:
        spark = SparkSession.getActiveSession() #get the existing spark session.
        if spark is None:
            raise ValueError("SparkSession not found.")

        processor = MyDataProcessor(bucket, prefix)
        processor.process_data(spark)

    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")