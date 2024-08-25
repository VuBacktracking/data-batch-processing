import os
import logging
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

# Load environment variables from the .env file
load_dotenv(".env")

# Configure logging
logging.basicConfig(
    filename="logs/datawarehouse/schema_creation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def main():
    # Initialize the PostgresSQLClient with connection parameters
    try:
        pc = PostgresSQLClient(
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        logging.info("PostgresSQLClient initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize PostgresSQLClient: {e}")
        return

    # SQL statements to create schemas
    create_staging_schema = """
                                DROP SCHEMA IF EXISTS staging;
                                CREATE SCHEMA IF NOT EXISTS staging;
                                """
    create_production_schema = """
                                DROP SCHEMA IF EXISTS production;
                                CREATE SCHEMA IF NOT EXISTS production;
                                """

    # Execute schema creation queries with error handling
    try:
        logging.info("Creating 'staging' schema...")
        pc.execute_query(create_staging_schema)
        logging.info("'staging' schema created successfully.")
        
        logging.info("Creating 'production' schema...")
        pc.execute_query(create_production_schema)
        logging.info("'production' schema created successfully.")
        
    except Exception as e:
        logging.error(f"Failed to create schema: {e}")
        print(f"Failed to create schema with error: {e}")

if __name__ == "__main__":
    main()