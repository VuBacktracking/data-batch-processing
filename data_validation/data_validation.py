import great_expectations as gx
from dotenv import load_dotenv
import os

load_dotenv(".env")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DB_STAGING_TABLE = os.getenv("DB_STAGING_TABLE")

# Create a DataContext as an entry point to the GX Python API
context = gx.get_context()

# Define the datasource name and connection string
datasource_name = "nyc-taxi-datasource"
connection_string = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Check if the datasource already exists before adding it
if datasource_name not in context.datasources:
    pg_datasource = context.sources.add_postgres(
        name=datasource_name, connection_string=connection_string
    )
    pg_datasource.add_table_asset(
        name="postgres_taxi_data", table_name="nyc_taxi", schema_name="staging"
    )
else:
    pg_datasource = context.datasources[datasource_name]

# Build a batch request to load data from the table
batch_request = pg_datasource.get_asset("postgres_taxi_data").build_batch_request()

# Define and add an expectation suite
expectation_suite_name = "validate_trip_data"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

# Get a validator for the table data
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# Add expectations to the validator
validator.expect_column_values_to_not_be_null("vendor_id")
validator.expect_column_values_to_not_be_null("rate_code_id")
validator.expect_column_values_to_not_be_null("dropoff_location_id")
validator.expect_column_values_to_not_be_null("pickup_location_id")
validator.expect_column_values_to_not_be_null("payment_type_id")
validator.expect_column_values_to_not_be_null("service_type")
validator.expect_column_values_to_not_be_null("pickup_latitude")
validator.expect_column_values_to_not_be_null("pickup_longitude")
validator.expect_column_values_to_not_be_null("dropoff_latitude")
validator.expect_column_values_to_not_be_null("dropoff_longitude")

validator.expect_column_values_to_be_between("trip_distance", min_value=0, max_value=100)
validator.expect_column_values_to_be_between("extra", min_value=0, max_value=3)

# Save the expectation suite
validator.save_expectation_suite(discard_failed_expectations=False)

# Similar to a single file, create a checkpoint to validate the result
# Define the checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="staging_tripdata_asset_checkpoint",
    validator=validator
)

# Get the result after validator
checkpoint_result = checkpoint.run()

# Quick view on the validation result
context.view_validation_result(checkpoint_result)