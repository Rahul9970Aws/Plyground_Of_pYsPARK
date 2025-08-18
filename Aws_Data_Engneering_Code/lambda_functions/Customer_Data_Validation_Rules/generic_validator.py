import pandas as pd
import re
from datetime import datetime
import time

class DataValidator:
    def __init__(self, df, logger=None):
        self.df = df.copy()
        self.logger = logger

    def _log_wrapper(self, func, func_name):
        """Helper to log start, end, and duration of a validation function."""
        if self.logger:
            self.logger.info(f"{func_name} validation started")
        start = time.time()
        result = func()
        end = time.time()
        if self.logger:
            self.logger.info(f"{func_name} validation completed in {end - start:.2f} seconds")
        return result

    def validate_customer_id(self):
        return self._log_wrapper(
            lambda: self.df["customer_id"].apply(lambda x: isinstance(x, int) and x > 0),
            "Customer ID"
        )

    def validate_full_name(self):
        return self._log_wrapper(
            lambda: self.df["full_name"].apply(lambda x: bool(re.match(r"^[A-Za-z ]+$", str(x))) if pd.notnull(x) else False),
            "Full Name"
        )

    def validate_email(self):
        pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        return self._log_wrapper(
            lambda: self.df["email"].apply(lambda x: bool(re.match(pattern, str(x))) if pd.notnull(x) else False),
            "Email"
        )

    def validate_age_range(self):
        return self._log_wrapper(
            lambda: self.df["age"].apply(lambda x: isinstance(x, int) and 1 <= x <= 120),
            "Age Range"
        )

    def validate_date_of_birth(self):
        def check_date(dob):
            try:
                dob = datetime.strptime(str(dob), "%Y-%m-%d")
                return dob < datetime.now() and dob.year > 1900
            except:
                return False
        return self._log_wrapper(
            lambda: self.df["date_of_birth"].apply(check_date),
            "Date of Birth"
        )

    def validate_phone_number(self):
        return self._log_wrapper(
            lambda: self.df["phone_number"].astype(str).apply(lambda x: x.isdigit() and len(x) == 10),
            "Phone Number"
        )

    def validate_country(self):
        allowed = {"USA", "UK", "India", "Canada", "Australia", "Ireland"}
        return self._log_wrapper(
            lambda: self.df["country"].isin(allowed),
            "Country"
        )

    def check_missing_values(self):
        mandatory = ["customer_id", "full_name", "email", "age", "date_of_birth", "phone_number", "country"]
        return self._log_wrapper(
            lambda: self.df[mandatory].notnull().all(axis=1),
            "Missing Values"
        )

    def check_duplicates(self):
        return self._log_wrapper(
            lambda: ~self.df.duplicated(subset=["full_name", "date_of_birth"], keep=False),
            "Duplicate"
        )

    def check_age_consistency(self):
        def check(row):
            try:
                dob = datetime.strptime(str(row["date_of_birth"]), "%Y-%m-%d")
                today = datetime.today()
                calculated_age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                return abs(calculated_age - int(row["age"])) <= 1
            except:
                return False
        return self._log_wrapper(
            lambda: self.df.apply(check, axis=1),
            "Age Consistency"
        )

    def run_all_validations(self):
        if self.logger:
            self.logger.info("All validations started")

        results = pd.DataFrame({
            "customer_id_valid": self.validate_customer_id(),
            "full_name_valid": self.validate_full_name(),
            "email_valid": self.validate_email(),
            "age_valid": self.validate_age_range(),
            "dob_valid": self.validate_date_of_birth(),
            "phone_valid": self.validate_phone_number(),
            "country_valid": self.validate_country(),
            "not_null": self.check_missing_values(),
            "no_duplicates": self.check_duplicates(),
            "age_consistency": self.check_age_consistency()
        }, index=self.df.index)

        combined_df = self.df.join(results)
        mask_valid = results.all(axis=1)

        valid_df = combined_df[mask_valid].reset_index(drop=True)
        invalid_df = combined_df[~mask_valid].reset_index(drop=True)

        if self.logger:
            self.logger.info("All validations completed")

        return valid_df, invalid_df
