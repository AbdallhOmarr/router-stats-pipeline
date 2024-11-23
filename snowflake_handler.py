import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import pandas as pd 

# Configure logging
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO)

class SnowflakeHandler:
    def __init__(self) -> None:
        # Load credentials from environment variables
        load_dotenv(".env")
        self.load_credentials_from_env()
        
    def load_credentials_from_env(self):
        """Load Snowflake credentials from environment variables."""
        self.credentials = {
            "username": os.getenv("SNOWFLAKE_USERNAME"),
            "password": os.getenv("SNOWSQL_PWD"),
            "account": os.getenv("ACCOUNT_IDENTIFIER"),
            "warehouse": os.getenv("WAREHOUSE"),
            "database": os.getenv("DATABASE"),
            "schema": os.getenv("SCHEMA"),
        }
        assert all(self.credentials.values()), "Error: One or more Snowflake credentials are missing or empty."
        logging.info("All credentials loaded successfully.")

    def connect(self):
        """Establish a connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(
                user=self.credentials["username"],
                password=self.credentials["password"],
                account=self.credentials["account"],
                warehouse=self.credentials["warehouse"],
                database=self.credentials["database"],
                schema=self.credentials["schema"]
            )
            self.conn.cursor().execute(f"USE WAREHOUSE {self.credentials['warehouse']}")
            logging.info("Successfully connected to Snowflake.")
            return self.conn
        except Exception as e:
            logging.error(f"Failed to connect to Snowflake: {e}")
            raise

    def df_to_table(self, df, table_name='raw_data'):
        """Insert a Pandas DataFrame into a Snowflake table."""
        try:
            # Ensure connection is established
            if not hasattr(self, 'conn'):
                logging.error("No active connection found. Please call the connect() method first.")
                return
            

            # Insert DataFrame into Snowflake table
            success, nchunks, nrows, _ = write_pandas(self.conn, df, table_name)
            if success:
                logging.info(f"Successfully inserted {nrows} rows into table '{table_name}'.")
            else:
                logging.error(f"Data insertion failed for table '{table_name}'.")
        except Exception as e:
            logging.error(f"Failed to insert data into {table_name}: {e}")
            raise

    def close_connection(self):
        """Close the Snowflake connection."""
        if hasattr(self, 'conn'):
            self.conn.close()
            logging.info("Snowflake connection closed.")
