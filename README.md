# Data Batch Processing

## Overview

* Persist data to PostgreSQL.
* Monitor changes to data using the Debezium Connector.
* Stream data from a Kafka topic using PySpark (Spark Streaming).
* Convert the streaming data to Delta Lake format.
* Write the Delta Lake data to MinIO (AWS Object Storage).
* Query the data with Trino.
* Display the results in DBeaver.

## Project structure

## System Architecture

<p align = "center">
    <img src="assets/architecture.png" alt="workflow">
</p>

## Prequisites 

Before runing this script, ensure you have the following installed.\
**Note**:  The project was setup on Ubuntu 22.04 OS.

* Ubuntu 22.04 (prefered, but you can use Ubuntu 20.04)
* Python 3.10
* Apache Spark (installed locally)
* Apache Airflow
* Docker
* Minio
* DBeaver CE

## Start

1. **Clone the repository**
```bash
$ git clone https://github.com/VuBacktracking/data-batch-processing.git
$ cd stream-data-processing
```

2. **Start our data streaming infrastructure**
```bash
$ sudo service docker start
$ docker compose -f docker-compose.yaml up -d
```

3. **Setup environment**
```bash
$ python3 -m venv .venv
$ pip install -r requirements.txt
```

Create `.env` file and paste your MINIO keys, SPARK_HOME in it.
```ini
# Minio
MINIO_ACCESS_KEY=minio_access_key
MINIO_SECRET_KEY=minio_secret_key
MINIO_ENDPOINT=localhost:9000

# PostgreSQL
POSTGRES_DB=v9
POSTGRES_USER=v9
POSTGRES_PASSWORD=v9
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
DB_STAGING_TABLE=staging.nyc_taxi
```

4. **Services**

* Postgres is accessible on the default port 5432.
* Trino: http://localhost:8084.
* MinIO: http://localhost:9001.

## How to use?

- **Step 1. Start Debezium Connection**
```bash
cd debezium
bash run-cdc.sh register_connector conf/products-cdc-config.json
```

You should see the connection is running like the image below in the port http://localhost:8085.

<p align = "center">
    <img src="assets/debezium-connect.png" width = 80%>
</p>

- **Step 2. Create table and insert data into Database**

```bash
python3 database-operations/create_table.py
python3 database-operations/insert_table.py
```

In the PostgreSQL connection, you should see the database `v9` and the table `products` like the image below.

<p align = "center">
    <img src="assets/postgres.png" width = 80%>
</p>

- **Step 3. Start Streaming Data to MinIO**
```bash
python3 stream_processing/delta-to-minio.py
```

After putting data to MinIO storage, you can go to the port http://localhost:9001 and see the result like this image

<p align = "center">
    <img src="assets/minio.png" width = 80%>
</p>

## Read streaming data with Trino and Dbeaver

### Connect Trino in Dbeaver

<p align = "center">
    <img src="assets/trino_connect.png" width = 80%>
</p>

### Query with Dbeaver

Create your Trino schema and table in Dbeaver

```sql
-- Create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS lakehouse.products
WITH (location = 's3://datalake/');

-- Create the products table
CREATE TABLE IF NOT EXISTS lakehouse.products.products (
    id VARCHAR,
    name VARCHAR,
    original_price DOUBLE,
    price DOUBLE,
    fulfillment_type VARCHAR,
    brand VARCHAR,
    review_count INTEGER,
    rating_average DOUBLE,
    favourite_count INTEGER,
    current_seller VARCHAR,
    number_of_images INTEGER,
    category VARCHAR,
    quantity_sold INTEGER,
    discount DOUBLE
) WITH (
    location = 's3://datalake/products/'
);
```

<p align = "center">
    <img src="assets/trino_dbeaver.png" width = 80%>
</p>