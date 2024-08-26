import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.helpers import load_cfg
from utils.minio import MinioClient

logging.basicConfig(filename='logs/datalake_to_dw.log',
                    level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

# Parameters & Arguments

## Minio
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

## PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DB_STAGING_TABLE = os.getenv("DB_STAGING_TABLE")

CFG_FILE = "./config/minio.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]
BUCKET_NAME = datalake_cfg['bucket_name_processed']

def create_spark_session():
    """
        Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try: 
        spark = (SparkSession.builder.config("spark.executor.memory", '4g') \
                        .config(
                            "spark.jars", 
                            "jars/postgresql-42.7.3.jar,jars/aws-java-sdk-bundle-1.12.769.jar,jars/hadoop-aws-3.3.4.jar",
                        )
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Batch Processing Application")
                        .getOrCreate()
        )
        
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
        Establish the necessary configurations to access MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created due to exception: {e}")


def processing_dataframe(df, file_path):
    """
        Process data before loading to staging area
    """
    from pyspark.sql import functions as F 

    # Add columns year, month, and day of week (dow)
    df_temp = df.withColumn('year', F.year('pickup_datetime')) \
            .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
            .withColumn('dow', F.date_format('pickup_datetime', 'EEEE'))

    # Aggregate data
    df_final = df_temp.groupBy(
        'year', 'month', 'dow',
        F.col('VendorID').alias('vendor_id'),
        F.col('RatecodeID').alias('rate_code_id'),
        F.col('PULocationID').alias('pickup_location_id'),
        F.col('DOLocationID').alias('dropoff_location_id'),
        F.col('payment_type').alias('payment_type_id'),
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_latitude',
        'pickup_longitude',
        'dropoff_latitude',
        'dropoff_longitude'
        ).agg(
            F.sum('passenger_count').alias('passenger_count'),
            F.sum('trip_distance').alias('trip_distance'),
            F.sum('extra').alias('extra'),
            F.sum('mta_tax').alias('mta_tax'),
            F.sum('fare_amount').alias('fare_amount'),
            F.sum('tip_amount').alias('tip_amount'),
            F.sum('tolls_amount').alias('tolls_amount'),
            F.sum('total_amount').alias('total_amount'),
            F.sum('improvement_surcharge').alias('improvement_surcharge'),
            F.sum('congestion_surcharge').alias('congestion_surcharge'),
        )

    # Add 'service_type' column based on file path
    if 'yellow' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(1))  # Yellow taxi
    elif 'green' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(2))  # Green taxi

    # # Reorder columns to have year, month, dow at the front
    # columns = [
    #     'year', 'month', 'dow', 'vendor_id', 'rate_code_id', 'pickup_location_id',
    #     'dropoff_location_id', 'payment_type_id', 'pickup_datetime', 'dropoff_datetime',
    #     'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude',
    #     'passenger_count', 'trip_distance', 'extra', 'mta_tax', 'fare_amount', 
    #     'tip_amount', 'tolls_amount', 'total_amount', 'improvement_surcharge',
    #     'congestion_surcharge', 'service_type'
    # ]
    
    # # Select columns in the specified order
    # df_final = df_final.select(*columns)
    logging.info("Data processed successfully")
    return df_final


def load_to_staging_table(df):
    """
        Save processed data to Staging Area (PostgreSQL)
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # Write data to PostgreSQL
    df.write.jdbc(url=URL, table=DB_STAGING_TABLE, mode='append', properties=properties)


# Main Execution Logic
# Main

if __name__ == "__main__":
    start_time = time.time()

    spark = create_spark_session()
    load_minio_config(spark.sparkContext)

    client = MinioClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    # Process both 'yellow' and 'green' directories for the year 2024
    for service_type in ['yellow', 'green']:
        folder_prefix = f"2024/{service_type}/"
        
        # List all Parquet files in the folder
        parquet_files = client.list_parquet_files(BUCKET_NAME, prefix=folder_prefix)
        
        for file in parquet_files:
            # Construct the correct file path
            path = f"s3a://{BUCKET_NAME}/{file}"
            logging.info(f"Reading parquet file: {path}")

            try:
                # Read the Parquet file into a Spark DataFrame
                df = spark.read.parquet(path)
                
                # Process the DataFrame and assign the correct service type
                df_final = processing_dataframe(df, service_type)
                
                # Load the processed DataFrame into the staging table
                load_to_staging_table(df_final)
            
            except Exception as e:
                logging.error(f"Failed to process file {file}: {e}")
                traceback.print_exc(file=sys.stderr)

    logging.info(f"Time to process: {time.time() - start_time}")
    logging.info("Batch processing successfully!")