import pandas as pd
import logging
import os
import json
import sys
from datetime import datetime
from Utility.generic_db_connection import connect_from_config

# Setup logging
log_dir = 'C:/Users/nayan/Documents/GitHub/mypython_env/Logs'
script_name = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'{script_name}.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def read_params(json_path, job_name):
    with open(json_path, 'r') as file:
        all_params = json.load(file)
    return all_params.get(job_name)

def read_data_and_export_csv(job_name, params, config_path):
    db_type = params.get("db_type")
    table_name = params.get("table_name")

    if not db_type or not table_name:
        logging.error("'db_type' and 'table_name' must be defined for job.")
        return

    query = f"SELECT * FROM {table_name}"
    output_file_name = f"{job_name}.csv"
    output_dir = 'C:/Users/nayan/Documents/GitHub/mypython_env/Outputs'
    output_csv_path = os.path.join(output_dir, output_file_name)

    logging.info(f"Starting job: {job_name}")
    logging.info(f"Table: {table_name}")
    logging.info(f"Query: {query}")
    logging.info(f"Output: {output_csv_path}")

    start_time = datetime.now()
    logging.info(f"Started at: {start_time}")

    engine = connect_from_config(db_type, config_path=config_path, logging=logging)
    if engine is None:
        logging.error("Engine creation failed.")
        return

    try:
        df = pd.read_sql_query(query, engine)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Rows fetched: {len(df)}")
        logging.info(f"Exported: {output_csv_path}")
        print(f"Data exported for job '{job_name}' to: {output_csv_path}")

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")

    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Ended at: {end_time} (Duration: {duration:.2f}s)")

# Entry point
if __name__ == '__main__':
    job_name = input("Enter job name (e.g., customer_export): ").strip()

    param_file_path = 'C:/Users/nayan/Documents/GitHub/mypython_env/params.json'
    config_path = 'C:/Users/nayan/Documents/GitHub/mypython_env/Utility/config.ini'

    params = read_params(param_file_path, job_name)

    if params is None:
        print(f"No configuration found for job: {job_name}")
        logging.error(f"No configuration found for job: {job_name}")
    else:
        read_data_and_export_csv(job_name, params, config_path)
