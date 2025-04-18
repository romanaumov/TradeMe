
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import dlt

# Import functions from oauth_api.py
from scripts.oauth_api import OAuthAPI, get_watchlist_data, get_rental_properties, upload_to_gcs

from airflow.models import Connection
from airflow.utils.db import provide_session


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

    print(f"Downloading latest file {latest_url}")

    # Define a DLT resource that reads the single latest JSON file from GCS
    @dlt.resource(name=f"{prefix}_data", write_disposition="replace")
    def json_source():
        blob = bucket.blob(latest_url)
        str_json = blob.download_as_text()
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
    print(f"Pipeline info: {info}")


def upload_rentals_to_bq():
    # Initialize GCS client with your service account
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
    bucket = storage_client.bucket(GCS_BUCKET)
    prefix = "rentals"

    # Helper function to extract datetime from filename
    def extract_datetime(filename, prefix):
        try:
            # Example: rentals_2025-04-18_08-40-15.json
            datetime_str = filename.replace(f"{prefix}_", "").replace(".json", "")
            date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d_%H-%M-%S")
            return date
        except Exception:
            return None
    
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

    print(f"Downloading latest file {latest_url}")

    # Define a DLT resource that reads the single latest JSON file from GCS
    @dlt.resource(name=f"{prefix}_data", write_disposition="replace")
    def json_source():
        blob = bucket.blob(latest_url)
        str_json = blob.download_as_text()
        reader = json.loads(str_json)

        row_count = 0
        for row in reader:
            row_count += 1
            yield row

        print(f"Total rows yielded: {row_count}")

    # Create and run the DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rentals",
        dataset_name=BIGQUERY_DATASET_STAGING,
        destination="bigquery"
    )

    info = pipeline.run(json_source())
    print(f"Pipeline info: {info}")



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
    
    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_staging_dataset",
        dataset_id=BIGQUERY_DATASET_STAGING,
        project_id=PROJECT_ID,
        location="US",  # Change this to your preferred location
        exists_ok=True
    )
    
    create_prod_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_prod_dataset",
        dataset_id=BIGQUERY_DATASET_PROD,
        project_id=PROJECT_ID,
        location="US",  # Change this to your preferred location
        exists_ok=True
    )
    
    upload_watchlist_to_bq_task = PythonOperator(
        task_id='upload_watchlist_to_bq',
        python_callable=upload_watchlist_to_bq,
    )
    
    upload_rentals_to_bq_task = PythonOperator(
        task_id='upload_rentals_to_bq',
        python_callable=upload_rentals_to_bq,
    )
    
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
                    CAST(REGEXP_REPLACE(price_display, r'[^0-9]', '') AS INT64) AS price_display,
                    CAST(REGEXP_EXTRACT(title, r'(\\d+) bedrooms') AS INT64) AS bedrooms,
                    region_id,
                    region,
                    suburb
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.watchlist_data`
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
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET_PROD}.rentals_data` AS
                SELECT
                    listing_id,
                    title,
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S (NZT)', start_date) AS start_date,
                    region_id,
                    region,
                    district_id,
                    district,
                    suburb_id,
                    suburb,
                    bathrooms,
                    bedrooms,
                    rent_per_week,
                    pets_okay
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.rentals_data`
                """,
                'useLegacySql': False,
            }
        },
    )
    
    # # Define task dependencies
    check_auth_task >> [extract_watchlist_task, extract_rental_task]
    extract_watchlist_task >> upload_watchlist_gcs_task >> create_staging_dataset >> upload_watchlist_to_bq_task >> create_prod_dataset >> transform_watchlist_task
    extract_rental_task >> upload_rentals_gcs_task >> create_staging_dataset >> upload_rentals_to_bq_task >> create_prod_dataset >> transform_rentals_task
    
