import sys
import os
import logging
from glob import glob
from minio import S3Error
from concurrent.futures import ThreadPoolExecutor

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import load_cfg
from utils.minio import MinioClient

# Parameters & Arguments
CFG_FILE = "./config/minio.yaml"

# Set up logging
logging.basicConfig(
    filename='logs/extract_load.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define function
def extract_load(endpoint, access_key, secret_key, year):
    try:
        # Load configuration
        cfg = load_cfg(CFG_FILE)
        datalake_cfg = cfg.get("datalake")
        nyc_data_cfg = cfg.get("nyc_data")

        # Validate configuration
        if not datalake_cfg or not nyc_data_cfg:
            raise ValueError("Missing 'datalake' or 'nyc_data' configuration in YAML file.")

        # Initialize MinIO client
        client = MinioClient(
            endpoint_url=endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

        # Create bucket if it doesn't exist
        client.create_bucket(datalake_cfg["bucket_name_raw"])

        # Upload files from yellow and green directories
        base_path = os.path.join(nyc_data_cfg["folder_path"], year)
        parquet_dirs = ["yellow", "green"]

        all_parquet_files = []
        for directory in parquet_dirs:
            parquet_files = glob(os.path.join(base_path, directory, "*.parquet"))
            if parquet_files:
                logger.info(f"Found {len(parquet_files)} Parquet files in {directory}.")
                all_parquet_files.extend(parquet_files)
            else:
                logger.warning(f"No Parquet files found in {os.path.join(base_path, directory)}.")

        if not all_parquet_files:
            logger.warning(f"No Parquet files found for year {year}.")
            return
        def upload_file(fp):
            try:
                # Determine the directory and object name
                directory_name = os.path.basename(os.path.dirname(fp))
                object_name = os.path.join(year, directory_name, os.path.basename(fp))  # Including year in the path
                logger.info(f"Uploading {fp} to {object_name}")

                client_minio = client.create_conn()
                client_minio.fput_object(
                    bucket_name=datalake_cfg["bucket_name_raw"],
                    object_name=object_name,
                    file_path=fp,
                )
                logger.info(f"Successfully uploaded {fp}")
            except S3Error as e:
                logger.error(f"Failed to upload {fp}: {e}")
                raise
        # Use ThreadPoolExecutor to upload files in parallel
        with ThreadPoolExecutor() as executor:
            executor.map(upload_file, all_parquet_files)

    except Exception as e:
        logger.error(f"Error in extract_load: {e}")
        raise

if __name__ == "__main__":
    try:
        # Load configuration
        cfg = load_cfg(CFG_FILE)
        datalake_cfg = cfg.get("datalake")

        # Retrieve environment variables
        MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
        MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
        MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')

        # Validate environment variables
        if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY or not MINIO_ENDPOINT:
            logger.error("Missing MinIO environment variables.")
            raise EnvironmentError("MinIO environment variables not set correctly.")

        # Call the function with the year parameter
        year = "2024"  # This can be passed as a parameter or retrieved from args
        extract_load(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, year)

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
