CREATE OR REPLACE TABLE DW.DIM_CUSTOMER (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    dob DATE,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,
    insert_date TIMESTAMP_NTZ,
    update_date TIMESTAMP_NTZ,
    is_active BOOLEAN
);
