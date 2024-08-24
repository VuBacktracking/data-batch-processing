import sys
import os
import warnings
import logging

from pyspark import SparkConf, SparkContext
from dotenv import load_dotenv

load_dotenv(".env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.helpers import load_cfg
from utils.minio import MinioClient

# Configuration for logging
logging.basicConfig(filename='logs/delta_convert.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
warnings.filterwarnings('ignore')

CFG_FILE = "./config/minio.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BUCKET_NAME_PROCESSED = datalake_cfg['bucket_name_processed']
BUCKET_NAME_DELTA = datalake_cfg['bucket_name_delta']


def delta_convert(endpoint_url, access_key, secret_key):
    """
    Convert parquet files to delta format and store in the MinIO bucket.
    """
    try:
        from pyspark.sql import SparkSession
        from delta.pip_utils import configure_spark_with_delta_pip

        # Create a Spark session with Delta support and S3A configurations
        builder = SparkSession.builder \
            .appName("Converting to Delta Lake") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars", "jars/hadoop-aws-3.3.4.jar, jars/aws-java-sdk-bundle-1.12.769.jar")

        # Configure Spark with Delta and extra packages
        spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
        logging.info('Spark session successfully created.')

    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return

    try:
        # Initialize MinioClient and ensure the delta bucket exists
        client = MinioClient(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key
        )
        client.create_bucket(BUCKET_NAME_DELTA)
        logging.info(f"Bucket '{BUCKET_NAME_DELTA}' created or already exists.")

    except Exception as e:
        logging.error(f"Failed to initialize MinIO client or create bucket: {e}")
        return

    # Convert Parquet files to Delta format
    try:
        # List all Parquet files in the '2024/green' and '2024/yellow' directories
        parquet_files = client.list_parquet_files(BUCKET_NAME_PROCESSED, prefix='2024/')
        for file in parquet_files:
            try:
                path_read = f"s3a://{BUCKET_NAME_PROCESSED}/{file}"
                logging.info(f"Reading parquet file: {file}")

                # Read Parquet file into a Spark DataFrame
                df = spark.read.parquet(path_read)

                # Define the path to save Delta format based on the directory structure
                folder_name = file.split('/')[1]  # 'green' or 'yellow'
                delta_path = f"s3a://{BUCKET_NAME_DELTA}/2024/{folder_name}/{os.path.basename(file)}"
                logging.info(f"Saving delta file to: {delta_path}")

                # Write the DataFrame to Delta format
                df.write \
                  .format("delta") \
                  .mode("overwrite") \
                  .save(delta_path)

                logging.info(f"Successfully converted {file} to delta format.")

            except Exception as e:
                logging.error(f"Failed to process file {file}: {e}")
                continue

    except Exception as e:
        logging.error(f"Failed to list or convert files: {e}")

    finally:
        if 'spark' in locals():
            spark.stop()
            logging.info("Spark session stopped.")



if __name__ == "__main__":
    delta_convert(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)