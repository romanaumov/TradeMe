import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from google.oauth2 import service_account

# Import functions from oauth_api.py
from scripts.oauth_api import OAuthAPI, get_watchlist_data, get_rental_properties, process_trademe_dates, upload_to_gcs

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
    check_auth_task = PythonOperator(
        task_id='check_authentication',
        python_callable=check_authentication,
    )
    
    # Task 2a: Extract Watchlist Data
    extract_watchlist_task = PythonOperator(
        task_id='extract_watchlist_data',
        python_callable=extract_watchlist_data,
    )
    
    # Task 2b: Extract Rental Data
    extract_rental_task = PythonOperator(
        task_id='extract_rental_data',
        python_callable=extract_rental_data,
    )
    
    # Task 3a: Upload Watchlist to GCS
    upload_watchlist_gcs_task = PythonOperator(
        task_id='upload_watchlist_to_gcs',
        python_callable=upload_watchlist_to_gcs,
    )
    
    # Task 3b: Upload Rentals to GCS
    upload_rentals_gcs_task = PythonOperator(
        task_id='upload_rentals_to_gcs',
        python_callable=upload_rentals_to_gcs,
    )
    
    # Task 4a: Load Watchlist from GCS to BigQuery Staging
    load_watchlist_to_bq_task = GCSToBigQueryOperator(
        task_id='load_watchlist_to_bigquery_staging',
        bucket=GCS_BUCKET,
        source_objects=["watchlist/{{ ti.xcom_pull(task_ids='upload_watchlist_to_gcs')['file_name'] }}"],
        destination_project_dataset_table=f"{PROJECT_ID}:{BIGQUERY_DATASET_STAGING}.watchlist",
        schema_fields=[
            {'name': 'ListingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'StartPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'BuyNowPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'StartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'EndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ListingLength', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'HasGallery', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'AsAt', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CategoryPath', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PictureHref', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegionId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'},
            # Add more fields as needed
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,  # Automatically detect schema
    )
    
    # Task 4b: Load Rentals from GCS to BigQuery Staging
    load_rentals_to_bq_task = GCSToBigQueryOperator(
        task_id='load_rentals_to_bigquery_staging',
        bucket=GCS_BUCKET,
        source_objects=["rentals/{{ ti.xcom_pull(task_ids='upload_rentals_to_gcs')['file_name'] }}"],
        destination_project_dataset_table=f"{PROJECT_ID}:{BIGQUERY_DATASET_STAGING}.rentals",
        schema_fields=[
            {'name': 'ListingId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'StartPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'StartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'EndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'IsFeatured', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'HasGallery', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'IsBold', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'IsHighlighted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'AsAt', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CategoryPath', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PictureHref', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegionId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'SuburbId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Suburb', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RentPerWeek', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Bedrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Bathrooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            # Add more fields as needed
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,  # Automatically detect schema
    )
    
    # Task 5a: Transform Watchlist data (dbt-like transformation)
    transform_watchlist_task = BigQueryInsertJobOperator(
        task_id='transform_watchlist_to_prod',
        configuration={
            'query': {
                'query': f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET_PROD}.watchlist` AS
                SELECT
                    ListingId,
                    Title,
                    Category,
                    StartPrice,
                    BuyNowPrice,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', StartDate) AS StartDate,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', EndDate) AS EndDate,
                    ListingLength,
                    HasGallery,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', AsAt) AS AsAt,
                    CategoryPath,
                    PictureHref,
                    RegionId,
                    Region,
                    CURRENT_TIMESTAMP() AS etl_timestamp
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.watchlist`
                """,
                'useLegacySql': False,
            }
        },
    )
    
    # Task 5b: Transform Rentals data (dbt-like transformation)
    transform_rentals_task = BigQueryInsertJobOperator(
        task_id='transform_rentals_to_prod',
        configuration={
            'query': {
                'query': f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET_PROD}.rentals` AS
                SELECT
                    ListingId,
                    Title,
                    Category,
                    StartPrice,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', StartDate) AS StartDate,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', EndDate) AS EndDate,
                    IsFeatured,
                    HasGallery,
                    IsBold,
                    IsHighlighted,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', AsAt) AS AsAt,
                    CategoryPath,
                    PictureHref,
                    RegionId,
                    Region,
                    SuburbId,
                    Suburb,
                    RentPerWeek,
                    Bedrooms,
                    Bathrooms,
                    CURRENT_TIMESTAMP() AS etl_timestamp
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.rentals`
                """,
                'useLegacySql': False,
            }
        },
    )
    
    # Define task dependencies
    check_auth_task >> [extract_watchlist_task, extract_rental_task]
    extract_watchlist_task >> upload_watchlist_gcs_task >> load_watchlist_to_bq_task >> transform_watchlist_task
    extract_rental_task >> upload_rentals_gcs_task >> load_rentals_to_bq_task >> transform_rentals_task
