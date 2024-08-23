import sys
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import os
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.helpers import load_cfg
from utils.minio import MinioClient

# Configuration for logging
logging.basicConfig(filename='logs/transform.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

CFG_FILE = "config/minio.yaml"
YEARS = ["2024"]
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

spark = SparkSession.builder \
    .appName("Taxi Data Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "jars/hadoop-aws-3.3.4.jar, jars/aws-java-sdk-bundle-1.12.769.jar") \
    .getOrCreate()

def drop_column(df, file):
    """
    Drop columns 'store_and_fwd_flag'
    """
    try:
        if "store_and_fwd_flag" in df.columns:
            df = df.drop("store_and_fwd_flag")
            logger.info(f"Dropped column store_and_fwd_flag from file: {file}")
        else:
            logger.info(f"Column store_and_fwd_flag not found in file: {file}")
    except Exception as e:
        logger.error(f"Error dropping column from file {file}: {str(e)}")
        raise
    return df

def merge_taxi_zone(df, file, lookup_path):
    """
    Merge dataset with taxi zone lookup
    """
    try:
        df_lookup = spark.read.csv(lookup_path, header=True, inferSchema=True)
        
        def merge_and_rename(df, location_id, lat_col, long_col):
            df = df.join(df_lookup, df[location_id] == df_lookup["LocationID"], "left") \
                   .drop("LocationID", "Borough", "service_zone", "zone") \
                   .withColumnRenamed("latitude", lat_col) \
                   .withColumnRenamed("longitude", long_col)
            return df

        if "pickup_latitude" not in df.columns:
            df = merge_and_rename(df, "PULocationID", "pickup_latitude", "pickup_longitude")

        if "dropoff_latitude" not in df.columns:
            df = merge_and_rename(df, "DOLocationID", "dropoff_latitude", "dropoff_longitude")

        df = df.dropna()
        logger.info(f"Merged data from file: {file}")
    except Exception as e:
        logger.error(f"Error merging taxi zone for file {file}: {str(e)}")
        raise
    return df

def process(df, file):
    """
    Process data for green and yellow taxi
    """
    try:
        if "green" in file:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
                   .drop("trip_type")
        elif "yellow" in file:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
                   .drop("Airport_fee")

        df = df.withColumn("payment_type", col("payment_type").cast("int")) \
               .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
               .withColumn("PULocationID", col("PULocationID").cast("int")) \
               .withColumn("VendorID", col("VendorID").cast("int"))

        df = df.dropna()
        logger.info(f"Processed data from file: {file}")
    except Exception as e:
        logger.error(f"Error processing data for file {file}: {str(e)}")
        raise
    return df

def transform_data(spark, endpoint_url, access_key, secret_key, bucket_raw, bucket_processed, lookup_path):
    """
    Transform data using Spark from MinIO
    """
    try:
        client = MinioClient(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key
        )

        client.create_bucket(bucket_processed)

        for year in YEARS:
            # List all parquet files in the green and yellow directories
            green_files = client.list_parquet_files(bucket_raw, f"{year}/green/")
            yellow_files = client.list_parquet_files(bucket_raw, f"{year}/yellow/")

            for file in green_files:
                green_taxi_path = f"s3a://{bucket_raw}/{file}"

                try:
                    logger.info(f"Reading parquet file for green taxis: {green_taxi_path}")
                    green_df = spark.read.parquet(green_taxi_path)

                    green_df = drop_column(green_df, file)
                    green_df = merge_taxi_zone(green_df, file, lookup_path)
                    green_df = process(green_df, file)

                    # Construct the output path to match the raw data structure with the same filename
                    file_name = os.path.basename(file)  # Extracts the file name from the path
                    green_output_path = f"s3a://{bucket_processed}/{year}/green/{file_name}"
                    green_df.write.mode("overwrite").parquet(green_output_path)
                    logger.info(f"Saved processed data to {green_output_path}")
                except Exception as e:
                    logger.error(f"Error processing green taxi file {file} for year {year}: {str(e)}")
                    continue

            for file in yellow_files:
                yellow_taxi_path = f"s3a://{bucket_raw}/{file}"

                try:
                    logger.info(f"Reading parquet file for yellow taxis: {yellow_taxi_path}")
                    yellow_df = spark.read.parquet(yellow_taxi_path)

                    yellow_df = drop_column(yellow_df, file)
                    yellow_df = merge_taxi_zone(yellow_df, file, lookup_path)
                    yellow_df = process(yellow_df, file)

                    # Construct the output path to match the raw data structure with the same filename
                    file_name = os.path.basename(file)  # Extracts the file name from the path
                    yellow_output_path = f"s3a://{bucket_processed}/{year}/yellow/{file_name}"
                    yellow_df.write.mode("overwrite").parquet(yellow_output_path)
                    logger.info(f"Saved processed data to {yellow_output_path}")
                except Exception as e:
                    logger.error(f"Error processing yellow taxi file {file} for year {year}: {str(e)}")
                    continue

        logger.info("Finished transforming all data.")
    except Exception as e:
        logger.error(f"Error in transforming data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        cfg = load_cfg(CFG_FILE)
        datalake_cfg = cfg["datalake"]

        BUCKET_RAW = datalake_cfg['bucket_name_raw']
        BUCKET_PROCESSED = datalake_cfg['bucket_name_processed']
        TAXI_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "data", "taxi_lookup.csv")

        transform_data(spark, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_RAW, BUCKET_PROCESSED, TAXI_LOOKUP_PATH)
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        sys.exit(1)
