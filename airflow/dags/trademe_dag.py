import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from google.oauth2 import service_account

import requests
from typing import Iterator
import dlt
from dlt.common import json as dlt_json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl

from google.cloud import storage

# Import functions from oauth_api.py
from scripts.oauth_api import OAuthAPI, get_watchlist_data, get_rental_properties, process_trademe_dates, upload_to_gcs

from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings




# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define constants
GCS_BUCKET = "trademe-raw-data"
PROJECT_ID = "trademe-viewer"
BIGQUERY_DATASET_STAGING = "trademe_staging"
BIGQUERY_DATASET_PROD = "trademe_prod"
SERVICE_ACCOUNT_PATH = "/opt/airflow/keys/trademe-viewer-service-account.json"

@provide_session
def create_gcp_conn(session=None):
    conn_id = 'google_cloud_default'
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if not existing_conn:
        conn = Connection(
            conn_id=conn_id,
            conn_type='google_cloud_platform',
            # extra='{"key_path": "/opt/airflow/keys/trademe-viewer-service-account.json", "project": "trademe-viewer"}'
            extra = f'{{"key_path": "{SERVICE_ACCOUNT_PATH}", "project": "{PROJECT_ID}"}}'

        )
        session.add(conn)
        session.commit()

create_gcp_conn()

# Define functions for each step in the pipeline
def check_authentication(**kwargs):
    """Check if authentication is valid"""
    api = OAuthAPI()
    is_authenticated = api.authenticate()
    if not is_authenticated:
        raise Exception("Authentication failed. Cannot proceed with data extraction.")
    return is_authenticated

def extract_watchlist_data(**kwargs):
    """Extract watchlist data from TradeMe API"""
    api = OAuthAPI()
    watchlist_data = get_watchlist_data(api)
    
    # Generate timestamp for the file
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"watchlist_{formatted_time}.json"
    
    # Save data to local file
    with open(file_name, 'w') as f:
        json.dump(watchlist_data, f, indent=4)
    
    # Return file name and count for logging
    return {
        "file_name": file_name,
        "count": len(watchlist_data),
        "timestamp": formatted_time
    }

def extract_rental_data(**kwargs):
    """Extract rental properties data from TradeMe API"""
    api = OAuthAPI()
    rental_params = {
        'region': 15,  # Auckland
        'price_max': 1000,
        'bedrooms_min': 2
    }
    rental_data = get_rental_properties(api)
    # rental_data = get_rental_properties(api, rental_params)
    
    # Generate timestamp for the file (use same timestamp as watchlist for consistency)
    ti = kwargs['ti']
    watchlist_info = ti.xcom_pull(task_ids='extract_watchlist_data')
    formatted_time = watchlist_info['timestamp']
    file_name = f"rentals_{formatted_time}.json"
    
    # Save data to local file
    with open(file_name, 'w') as f:
        json.dump(rental_data, f, indent=4)
    
    # Return file name and count for logging
    return {
        "file_name": file_name,
        "count": len(rental_data),
        "timestamp": formatted_time
    }

def upload_watchlist_to_gcs(**kwargs):
    """Upload watchlist data to Google Cloud Storage"""
    ti = kwargs['ti']
    watchlist_info = ti.xcom_pull(task_ids='extract_watchlist_data')
    file_name = watchlist_info['file_name']
    
    # Define GCS path
    gcs_path = f"watchlist/{file_name}"
    
    # Upload to GCS
    upload_to_gcs(GCS_BUCKET, file_name, gcs_path)
    
    return {
        "gcs_path": f"gs://{GCS_BUCKET}/{gcs_path}",
        "file_name": file_name
    }

def upload_rentals_to_gcs(**kwargs):
    """Upload rental data to Google Cloud Storage"""
    ti = kwargs['ti']
    rental_info = ti.xcom_pull(task_ids='extract_rental_data')
    file_name = rental_info['file_name']
    
    # Define GCS path
    gcs_path = f"rentals/{file_name}"
    
    # Upload to GCS
    upload_to_gcs(GCS_BUCKET, file_name, gcs_path)
    
    return {
        "gcs_path": f"gs://{GCS_BUCKET}/{gcs_path}",
        "file_name": file_name
    }
    


# # Define a standalone transformer to read data from a JSON file.
# @dlt.transformer(standalone=True)
# def read_json(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
#     for file_obj in items:
#         with file_obj.open() as f:
#             yield json.load(f)

# files_resource = filesystem(file_glob="**/*.json")
# files_resource.apply_hints(incremental=dlt.sources.incremental("modification_date"))
# json_resource = files_resource | read_json()
# pipeline = dlt.pipeline(pipeline_name="s3_to_duckdb", dataset_name="json_data", destination="duckdb")

# info = pipeline.run(json_resource, write_disposition="replace")
# print(info)
    
def upload_watchlist_to_bq():
    # Initialize GCS client with your service account
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
    bucket = storage_client.bucket(GCS_BUCKET)
    prefix = "watchlist"

    # Helper function to extract datetime from filename
    def extract_datetime(filename, prefix):
        try:
            # Example: rentals_2025-04-18_08-40-15.json
            datetime_str = filename.replace(f"{prefix}_", "").replace(".json", "")
            date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d_%H-%M-%S")
            return date
        except Exception:
            return None
        
    # def dlt_read_json(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    #     for file_obj in items:
    #         with file_obj.open() as f:
    #             yield dlt_json.load(f)
    
    # List files under the given prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Print the filenames
    urls = [blob.name for blob in blobs]
    print(f"URLS: {urls}")
    
    valid_urls = []
    for url in urls:
        dt = extract_datetime(url.split("/")[-1], prefix)
        if dt:
            valid_urls.append((url, dt))

    if not valid_urls:
        raise ValueError("No valid datetime found in any of the URLs")

    # Get the one with the latest datetime
    latest_url, _ = max(valid_urls, key=lambda item: item[1])

    # Upload only the latest file
    # file_name = latest_url.split("/")[-1]
    # gcs_blob = bucket.blob(file_name)

    print(f"Downloading latest file {latest_url}")
    # response = requests.get(latest_url)
    # gcs_blob.upload_from_string(response.content)
    
    # blob = bucket.blob(latest_url)
    # # here you get string
    # str_json = blob.download_as_text()
    # # now you have as dict
    # dict_result = json.loads(str_json)

    # Define a DLT resource that reads the single latest JSON file from GCS
    @dlt.resource(name=f"{prefix}_data", write_disposition="replace")
    def json_source():
        # file_path = f"gs://{GCS_BUCKET}/{file_name}"
        file_path = f"gs://{GCS_BUCKET}/{latest_url}"
        # reader = (filesystem(file_path) | read_jsonl()).with_name(prefix)
        blob = bucket.blob(latest_url)
        # here you get string
        str_json = blob.download_as_text()
        # now you have as dict
        reader = json.loads(str_json)

        row_count = 0
        for row in reader:
            row_count += 1
            yield row

        print(f"Total rows yielded: {row_count}")

    # Create and run the DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="watchlist",
        dataset_name=BIGQUERY_DATASET_STAGING,
        destination="bigquery"
    )

    info = pipeline.run(json_source())
    
    # return {
    #     "Total rows yielded": info
    # }




# Create the DAG
with DAG(
    'trademe_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for TradeMe data',
    schedule_interval=datetime.timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['trademe', 'etl'],
) as dag:
    
    # Task 1: Check Authentication
    # check_auth_task = PythonOperator(
    #     task_id='check_authentication',
    #     python_callable=check_authentication,
    # )
    
    # # Task 2a: Extract Watchlist Data
    # extract_watchlist_task = PythonOperator(
    #     task_id='extract_watchlist_data',
    #     python_callable=extract_watchlist_data,
    # )
    
    # # Task 2b: Extract Rental Data
    # extract_rental_task = PythonOperator(
    #     task_id='extract_rental_data',
    #     python_callable=extract_rental_data,
    # )
    
    # # Task 3a: Upload Watchlist to GCS
    # upload_watchlist_gcs_task = PythonOperator(
    #     task_id='upload_watchlist_to_gcs',
    #     python_callable=upload_watchlist_to_gcs,
    # )
    
    # # Task 3b: Upload Rentals to GCS
    # upload_rentals_gcs_task = PythonOperator(
    #     task_id='upload_rentals_to_gcs',
    #     python_callable=upload_rentals_to_gcs,
    # )
    
    # create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_staging_dataset",
    #     dataset_id=BIGQUERY_DATASET_STAGING,
    #     project_id=PROJECT_ID,
    #     location="US",  # Change this to your preferred location
    #     exists_ok=True
    # )
    
    # create_prod_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_prod_dataset",
    #     dataset_id=BIGQUERY_DATASET_PROD,
    #     project_id=PROJECT_ID,
    #     location="US",  # Change this to your preferred location
    #     exists_ok=True
    # )
    
    upload_watchlist_to_bq_task = PythonOperator(
        task_id='upload_watchlist_to_bq',
        python_callable=upload_watchlist_to_bq,
    )
    
    # Task 4a: Load Watchlist from GCS to BigQuery Staging
    # load_watchlist_to_bq_task = GCSToBigQueryOperator(
    #     task_id='load_watchlist_to_bigquery_staging',
    #     bucket=GCS_BUCKET,
    #     source_objects=["watchlist/{{ ti.xcom_pull(task_ids='upload_watchlist_to_gcs')['file_name'] }}"],
    #     destination_project_dataset_table=f"{PROJECT_ID}:{BIGQUERY_DATASET_STAGING}.watchlist",
    #     schema_fields=[
    #         {'name': 'ListingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'Category', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'StartPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'BuyNowPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'StartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'EndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'ListingLength', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'HasGallery', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'AsAt', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'CategoryPath', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'PictureHref', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'RegionId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         # Add more fields as needed
    #     ],
    #     write_disposition='WRITE_TRUNCATE',
    #     source_format='NEWLINE_DELIMITED_JSON',
    #     autodetect=True,  # Automatically detect schema
    # )
    
    # # Task 4b: Load Rentals from GCS to BigQuery Staging
    # load_rentals_to_bq_task = GCSToBigQueryOperator(
    #     task_id='load_rentals_to_bigquery_staging',
    #     bucket=GCS_BUCKET,
    #     source_objects=["rentals/{{ ti.xcom_pull(task_ids='upload_rentals_to_gcs')['file_name'] }}"],
    #     destination_project_dataset_table=f"{PROJECT_ID}:{BIGQUERY_DATASET_STAGING}.rentals",
    #     schema_fields=[
    #         {'name': 'ListingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'Category', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'StartPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'StartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'EndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'IsFeatured', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'HasGallery', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'IsBold', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'IsHighlighted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'AsAt', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'CategoryPath', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'PictureHref', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'RegionId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'SuburbId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Suburb', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'RentPerWeek', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Bedrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'Bathrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         # Add more fields as needed
    #     ],
    #     write_disposition='WRITE_TRUNCATE',
    #     source_format='NEWLINE_DELIMITED_JSON',
    #     autodetect=True,  # Automatically detect schema
    # )
    
    # Task 5a: Transform Watchlist data (dbt-like transformation)
    transform_watchlist_task = BigQueryInsertJobOperator(
        task_id='transform_watchlist_to_prod',
        configuration={
            'query': {
                'query': f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET_PROD}.watchlist_data` AS
                SELECT
                    listing_id,
                    title,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', start_date) AS start_date,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', end_date) AS end_date,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', as_at) AS as_at,
                    CAST(REGEXP_REPLACE(price_display, '[^0-9]', '', 'g') AS INTEGER) AS price_display,
                    CAST(REGEXP_MATCHES(title, '\\d+(?= bedrooms)')[1] AS INTEGER) AS bedrooms,
                    region_id,
                    region,
                    suburb
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.watchlist_data`
                """,
                'useLegacySql': False,
            }
        },
    )
    
    # # Task 5b: Transform Rentals data (dbt-like transformation)
    # transform_rentals_task = BigQueryInsertJobOperator(
    #     task_id='transform_rentals_to_prod',
    #     configuration={
    #         'query': {
    #             'query': f"""
    #             CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET_PROD}.rentals` AS
    #             SELECT
    #                 ListingId,
    #                 Title,
    #                 Category,
    #                 StartPrice,
    #                 PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', StartDate) AS StartDate,
    #                 PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', EndDate) AS EndDate,
    #                 IsFeatured,
    #                 HasGallery,
    #                 IsBold,
    #                 IsHighlighted,
    #                 PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', AsAt) AS AsAt,
    #                 CategoryPath,
    #                 PictureHref,
    #                 RegionId,
    #                 Region,
    #                 SuburbId,
    #                 Suburb,
    #                 RentPerWeek,
    #                 Bedrooms,
    #                 Bathrooms,
    #                 CURRENT_TIMESTAMP() AS etl_timestamp
    #             FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.rentals`
    #             """,
    #             'useLegacySql': False,
    #         }
    #     },
    # )
    
    # # Define task dependencies
    # check_auth_task >> [extract_watchlist_task, extract_rental_task]
    # extract_watchlist_task >> upload_watchlist_gcs_task >> load_watchlist_to_bq_task >> transform_watchlist_task
    # extract_rental_task >> upload_rentals_gcs_task >> load_rentals_to_bq_task >> transform_rentals_task

# check_auth_task >> extract_watchlist_task >> upload_watchlist_gcs_task >> create_staging_dataset >> 
upload_watchlist_to_bq_task >> transform_watchlist_task
