from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
import os
from dotenv import load_dotenv
load_dotenv(".env")

def print_dataframe_schema(endpoint_url, access_key, secret_key, bucket_name, file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Print DataFrame Schema") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars", "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.769.jar") \
        .getOrCreate()

    # Load the DataFrame from MinIO
    df = spark.read.parquet(f"s3a://{bucket_name}/{file_path}")
    df.printSchema()
    df.show(5)
    # Count null values in each column
    null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
    null_counts.show()

if __name__ == "__main__":
    # Retrieve environment variables
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
    BUCKET_NAME = 'processed-taxi'
    FILE_PATH = "2024/green/green_tripdata_2024-01.parquet"

    print_dataframe_schema(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_NAME, FILE_PATH)
