import os
import logging
from minio import Minio
from minio.error import S3Error

class MinioClient:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key


        # Set up logging
        logging.basicConfig(
            filename='logs/minio_client.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def create_conn(self):
        try:
            # Print the endpoint URL for debugging
            print(f"Connecting to MinIO at {self.endpoint_url}")

            client = Minio(
                endpoint=self.endpoint_url,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False,  # or True if you're using HTTPS
            )
            self.logger.info("Connection to MinIO established successfully.")
            return client
        except S3Error as e:
            self.logger.error(f"Error establishing connection to MinIO: {e}")
            raise

    def create_bucket(self, bucket_name):
        client = self.create_conn()
        try:
            found = client.bucket_exists(bucket_name=bucket_name)
            if not found:
                client.make_bucket(bucket_name=bucket_name)
                self.logger.info(f"Bucket {bucket_name} created successfully.")
            else:
                self.logger.info(f"Bucket {bucket_name} already exists, skipping creation.")
        except S3Error as e:
            self.logger.error(f"Error in bucket creation/check: {e}")
            raise

    def list_parquet_files(self, bucket_name, prefix=""):
        client = self.create_conn()
        try:
            objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            
            self.logger.info(f"Listed Parquet files in bucket {bucket_name} with prefix '{prefix}': {parquet_files}")
            return parquet_files
        except S3Error as e:
            self.logger.error(f"Error listing objects in bucket {bucket_name}: {e}")
            raise
