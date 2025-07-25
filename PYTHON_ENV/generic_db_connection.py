import configparser
import logging
import os
import sqlite3
import pymysql
import psycopg2
import pyodbc
import oracledb
import snowflake.connector
from sqlalchemy import create_engine


def connect_from_config(db_type, config_path='config.ini', logging=logging):
    config = configparser.ConfigParser()
    config.read(config_path)

    logging.info(f"Db_type is {db_type} and path is {config_path}")
    section = db_type.lower()

    if section not in config:
        error_msg = f"Section [{section}] not found in {config_path}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    db_config = config[section]
    host = db_config.get('host')
    port = db_config.getint('port', fallback=None)
    dbname = db_config.get('dbname')
    user = db_config.get('user')
    password = db_config.get('password')

    try:
        if section == "sqlite":
            conn = sqlite3.connect(dbname)
            msg = "Connected to SQLite"
            print(msg)
            logging.info(msg)
            return conn  # DB-API2 is fine for sqlite

        elif section == "mysql":
            port = port or 3306
            url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(url)
            msg = "Connected to MySQL "
            print(msg)
            logging.info(msg)
            return engine

        elif section == "postgresql":
            port = port or 5432
            url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            engine = create_engine(url)
            msg = "Connected to PostgreSQL"
            print(msg)
            logging.info(msg)
            return engine

        elif section == "mssql":
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host},{port or 1433};"
                f"DATABASE={dbname};"
                f"UID={user};"
                f"PWD={password}"
            )
            conn = pyodbc.connect(conn_str)
            msg = "Connected to MS SQL Server"
            print(msg)
            logging.info(msg)
            return conn

        elif section == "oracle":
            dsn = oracledb.makedsn(host, port or 1521, service_name=dbname)
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            msg = "Connected to Oracle"
            print(msg)
            logging.info(msg)
            return conn

        elif section == "snowflake":
            conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=db_config.get('account'),
                warehouse=db_config.get('warehouse'),
                database=dbname,
                schema=db_config.get('schema'),
                role=db_config.get('role')
            )
            msg = "Connected to Snowflake"
            print(msg)
            logging.info(msg)
            return conn

        else:
            error_msg = f"Unsupported database type: {db_type}"
            logging.error(error_msg)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Failed to connect to {db_type}: {e}"
        print(error_msg)
        logging.error(error_msg)
        return None
