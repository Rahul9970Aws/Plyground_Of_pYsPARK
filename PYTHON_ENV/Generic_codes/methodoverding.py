from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, isnan, udf
from pyspark.sql.types import BooleanType
import re
import logging
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataValidator:
    logger = logging.getLogger(__name__)

    @staticmethod
    def remove_duplicates(spark: SparkSession, df: DataFrame, columns: list = None) -> DataFrame:
        DataValidator.logger.info("Removing duplicates from DataFrame.")
        deduped_df = df.dropDuplicates(columns) if columns else df.dropDuplicates()
        DataValidator.logger.info("Duplicates removed.")
        return deduped_df

    @staticmethod
    def check_null_nan(spark: SparkSession, df: DataFrame) -> DataFrame:
        DataValidator.logger.info("Checking for null/NaN values in DataFrame.")
        null_nan_counts = [(column_name, df.filter(col(column_name).isNull()).count(), df.filter(isnan(col(column_name))).count()) for column_name in df.columns]
        result_df = spark.createDataFrame(null_nan_counts, ["column", "null_count", "nan_count"])
        DataValidator.logger.info("Null/NaN checks completed.")
        return result_df

    @staticmethod
    def is_valid_email_regex(email: str) -> bool:
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        is_valid = bool(re.match(email_regex, email))
        DataValidator.logger.debug(f"Email '{email}' validation result: {is_valid}")
        return is_valid

    @staticmethod
    def validate_email_udf(spark: SparkSession, df: DataFrame, email_column: str) -> DataFrame:
        DataValidator.logger.info(f"Validating email addresses in column '{email_column}'.")
        is_valid_email_udf_spark = udf(DataValidator.is_valid_email_regex, BooleanType())
        validated_df = df.withColumn("email_valid", is_valid_email_udf_spark(col(email_column)))
        DataValidator.logger.info(f"Email validation completed for column '{email_column}'.")
        return validated_df

    @staticmethod
    def validate_phone_number_regex(phone_number: str) -> bool:
        phone_regex = r"^\+?[1-9]\d{1,14}$"
        is_valid = bool(re.match(phone_regex, str(phone_number)))
        DataValidator.logger.debug(f"Phone number '{phone_number}' regex validation result: {is_valid}")
        return is_valid

    @staticmethod
    def validate_phone_number_length(phone_number: str) -> bool:
        number = str(phone_number).replace("+", "").replace("-", "").replace(" ", "")
        is_valid = len(number) >= 7 and len(number) <= 15
        DataValidator.logger.debug(f"Phone number '{phone_number}' length validation result: {is_valid}")
        return is_valid

    @staticmethod
    def validate_phone_number_udf(spark: SparkSession, df: DataFrame, phone_column: str, method="regex") -> DataFrame:
        DataValidator.logger.info(f"Validating phone numbers in column '{phone_column}' using {method} method.")
        validate_func = DataValidator.validate_phone_number_regex if method == "regex" else DataValidator.validate_phone_number_length
        validate_udf_spark = udf(validate_func, BooleanType())
        validated_df = df.withColumn("phone_valid", validate_udf_spark(col(phone_column)))
        DataValidator.logger.info(f"Phone number validation completed for column '{phone_column}'.")
        return validated_df

def main(spark, input_path):
    try:
        # Read data from input_path
        input_df = spark.read.csv(input_path, header=True, inferSchema=True) #read as csv.
        # Data Validation
        deduped_df = DataValidator.remove_duplicates(spark, input_df)
        null_nan_counts_df = DataValidator.check_null_nan(spark, deduped_df)
        validated_email_df = DataValidator.validate_email_udf(spark, deduped_df, "email")
        # validated_phone_regex_df = DataValidator.validate_phone_number_udf(spark, validated_email_df, "phone")
        # validated_phone_length_df = DataValidator.validate_phone_number_udf(spark, validated_phone_regex_df, "phone", method="length")

        # Write the validated data back to output_path
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        output_location = f"dbfs:/mnt/myawsgeneralbucket1826/OutputFolder/validated_data_{timestamp}.parquet" #change output location.
        # validated_phone_length_df.write.parquet(output_location)
        # logging.info(f"Validated data saved to: {output_location}")

        return validated_email_df
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataValidation").getOrCreate()

    # Databricks specific input and output paths.
    input_path = "dbfs:/mnt/myawsgeneralbucket1826/SourceFolder/employees.csv" #replace with your input path.

    result_df = main(spark, input_path)
    if result_df is not None:
        result_df.show() #show the dataframe.
