import os
import json
import urllib.parse
import requests
import dlt
import time
import datetime
import re
from typing import Dict, Iterator, List, Optional, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = "~/prod/airflow/keys/trademe-viewer-service-account.json"
PROJECT_ID = "trademe-viewer"
BUCKET_NAME = "trademe-raw-data"
BASE_URL = "https://api.tmsandbox.co.nz"
CALLBACK_URL = "https://developer.trademe.co.nz"
AUTHORIZATION_URL = "https://secure.tmsandbox.co.nz/Oauth/Authorize"

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """Create a requests session with retry capabilities"""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def convert_trademe_date(date_str):
    """Convert TradeMe date format (/Date(timestamp)/) to human-readable format"""
    if not date_str:
        return None
    
    # Extract timestamp from /Date(1574800782810)/
    match = re.search(r'/Date\((\d+)\)/', date_str)
    if not match:
        return date_str
    
    # Convert milliseconds to seconds and create datetime object
    timestamp_ms = int(match.group(1))
    timestamp_sec = timestamp_ms / 1000
    
    # Convert to NZ timezone (UTC+12)
    dt = datetime.datetime.fromtimestamp(timestamp_sec)
    # Format as human-readable string
    return dt.strftime('%Y-%m-%d %H:%M:%S (NZT)')

def process_trademe_dates(item):
    """Process all date fields in a TradeMe item"""
    if isinstance(item, dict):
        for key, value in list(item.items()):
            if isinstance(value, str) and '/Date(' in value:
                item[key] = convert_trademe_date(value)
            elif isinstance(value, (dict, list)):
                process_trademe_dates(value)
    elif isinstance(item, list):
        for i in range(len(item)):
            process_trademe_dates(item[i])
    
    return item

class OAuthAPI:
    def __init__(self):
        self.base_url = BASE_URL
        self.callback_url = CALLBACK_URL
        
        self.credentials = {
            "consumer_key": None,
            "consumer_secret": None,
            "base_url": self.base_url,
            "callback_url": self.callback_url,
            "oauth_token": None,
            "oauth_token_secret": None,
            "oauth_verifier": None
        }
        
        # Load existing credentials and update auth header
        self.load_credentials()
        self.auth_header = self.create_oauth_header()
        
    def get_request_token(self):
        """Step 1: Get temporary request tokens"""
        url = f"{self.base_url}/Oauth/RequestToken"
        
        # Create OAuth header
        auth_params = {
            'oauth_callback': self.callback_url,
            'oauth_consumer_key': self.consumer_key,
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_signature': f'{self.consumer_secret}&'
        }
        
        # Build Authorization header - only encode oauth_callback
        header_params = auth_params.copy()
        if 'oauth_callback' in header_params:
            header_params['oauth_callback'] = urllib.parse.quote(header_params['oauth_callback'])
        
        auth_header = 'OAuth ' + ','.join([
            f'{k}="{v}"' for k, v in header_params.items()
        ])
        
        # Make request
        response = requests.post(
            f"{url}?scope=MyTradeMeRead,MyTradeMeWrite",
            headers={'Authorization': auth_header}
        )
        
        print("\n=== Request Token Response ===")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        # Parse response
        if response.status_code == 200:
            return dict(urllib.parse.parse_qsl(response.text))
        return None

    def get_authorization_url(self, oauth_token):
        """Step 2: Get authorization URL"""
        return f"{AUTHORIZATION_URL}?oauth_token={oauth_token}"

    def get_access_token(self, oauth_token, oauth_token_secret, oauth_verifier):
        """Step 3: Get final access token"""
        url = f"{self.base_url}/Oauth/AccessToken"
        
        # Create OAuth header
        auth_params = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_token': oauth_token,
            'oauth_verifier': oauth_verifier,
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_signature': f'{self.consumer_secret}&{oauth_token_secret}'
        }
        
        # Build Authorization header
        header_params = auth_params.copy()
        if 'oauth_callback' in header_params:
            header_params['oauth_callback'] = urllib.parse.quote(header_params['oauth_callback'])
        
        auth_header = 'OAuth ' + ','.join([
            f'{k}="{v}"' for k, v in header_params.items()
        ])
        
        # Make request
        response = requests.post(
            url,
            headers={'Authorization': auth_header}
        )
        
        print("\n=== Access Token Response ===")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        # Parse response
        if response.status_code == 200:
            return dict(urllib.parse.parse_qsl(response.text))
        return None
    
    def save_credentials(self, creds):
        """Save credentials to file"""
        with open('credentials.json', 'w') as f:
            json.dump(creds, f, indent=4)
        print("Credentials updated and saved.")
        
    def load_credentials(self):
        """Load credentials from file or initialize new ones"""
        try:
            if os.path.exists('credentials.json'):
                with open('credentials.json') as f:
                    data = json.load(f)
                
                # Update credentials with loaded data
                self.credentials.update({
                    "consumer_key": data.get('consumer_key', self.consumer_key),
                    "consumer_secret": data.get('consumer_secret', self.consumer_secret),
                    "base_url": data.get('base_url', self.base_url),
                    "callback_url": data.get('callback_url', self.callback_url),
                    "oauth_token": data.get('oauth_token'),
                    "oauth_token_secret": data.get('oauth_token_secret'),
                    "oauth_verifier": data.get('oauth_verifier')
                })
                print("Credentials loaded successfully.")
            else:
                print("No credentials.json found. Will create new file with initial values.")
                self.save_credentials(self.credentials)

        except (KeyError, json.JSONDecodeError) as e:
            print(f"Error loading credentials: {e}")
            print("Creating new credentials file with initial values.")
            self.save_credentials(self.credentials)
            
    def authenticate(self):
        """Complete OAuth authentication flow"""
        if not self.credentials.get('oauth_token') or not self.credentials.get('oauth_token_secret'):
            print("No valid OAuth tokens found. Proceeding to get new tokens.")
            
            # Step 1: Get request token
            print("\nStep 1: Getting Request Token...")
            request_token_response = self.get_request_token()
            
            if request_token_response:
                oauth_token = request_token_response['oauth_token']
                oauth_token_secret = request_token_response['oauth_token_secret']
                
                # Update and save credentials with request tokens
                self.credentials['oauth_token'] = oauth_token
                self.credentials['oauth_token_secret'] = oauth_token_secret
                self.save_credentials(self.credentials)
                
                # Step 2: Get authorization URL
                print("\nStep 2: Authorization URL")
                auth_url = self.get_authorization_url(oauth_token)
                print(f"Please visit this URL to authorize: {auth_url}")
                
                # Wait for user to authorize and get verifier code
                print("\nPlease visit the above URL and authorize the application.")
                print("Copy a verifier code oauth_verifier from URL above.")
                oauth_verifier = input("Enter the verifier code: ")
                
                # Update and save credentials with verifier
                self.credentials['oauth_verifier'] = oauth_verifier
                self.save_credentials(self.credentials)
                
                # Step 3: Get access token
                print("\nStep 3: Getting Permanent Access Token...")
                access_token_response = self.get_access_token(
                    oauth_token,
                    oauth_token_secret,
                    oauth_verifier
                )
                
                if access_token_response:
                    access_token = access_token_response['oauth_token']
                    access_token_secret = access_token_response['oauth_token_secret']
                    
                    # Update and save credentials with access tokens
                    self.credentials['oauth_token'] = access_token
                    self.credentials['oauth_token_secret'] = access_token_secret
                    self.save_credentials(self.credentials)
                    
                    # Update auth header with new tokens
                    self.auth_header = self.create_oauth_header()
                    return True
                return False
            return False
        else:
            print("Using existing OAuth tokens.")
            return True
                
    def create_oauth_header(self):
        """Create OAuth 1.0 header for requests"""
        auth_params = {
            'oauth_consumer_key': self.credentials['consumer_key'],
            'oauth_token': self.credentials.get('oauth_token', ''),
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_signature': f"{self.credentials['consumer_secret']}&{self.credentials.get('oauth_token_secret', '')}"
        }
        
        # Build Authorization header
        header_params = auth_params.copy()
        if 'oauth_callback' in header_params:
            header_params['oauth_callback'] = urllib.parse.quote(header_params['oauth_callback'])
        
        auth_header = 'OAuth ' + ','.join([
            f'{k}="{v}"' for k, v in header_params.items() if v  # Skip empty values
        ])
        
        return auth_header

    def get_request_headers(self):
        """Return headers for API requests"""
        return {'Authorization': self.auth_header}
        
    def api_request(self, endpoint, method="GET", params=None, data=None):
        """Generic API request method for adding new endpoints easily"""
        # Ensure we're authenticated
        if not self.authenticate():
            print(f"Authentication failed. Cannot access {endpoint}.")
            return None
            
        url = f"{self.base_url}{endpoint}"
        
        # Create a retry session for more robust API calls
        session = requests_retry_session()
        
        # Make request
        if method.upper() == "GET":
            response = session.get(
                url,
                params=params,
                headers=self.get_request_headers()
            )
        elif method.upper() == "POST":
            response = session.post(
                url,
                params=params,
                json=data,
                headers=self.get_request_headers()
            )
        elif method.upper() == "PUT":
            response = session.put(
                url,
                params=params,
                json=data,
                headers=self.get_request_headers()
            )
        elif method.upper() == "DELETE":
            response = session.delete(
                url,
                params=params,
                headers=self.get_request_headers()
            )
        else:
            print(f"Unsupported HTTP method: {method}")
            return None
        
        print(f"\n=== {endpoint} Response ===")
        print(f"Status Code: {response.status_code}")
        
        return response


def get_watchlist_data(api: OAuthAPI) -> List[Dict[str, Any]]:
    """Fetch all watchlist items with pagination"""
    page = 1
    page_size = 50
    has_more = True
    all_items = []
    
    while has_more:
        # Use the generic API request method
        endpoint = "/v1/MyTradeMe/Watchlist/All.json"
        params = {'page': page, 'page_size': page_size}
        
        print(f"Fetching watchlist page {page}")
        
        # Make request
        response = api.api_request(endpoint, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.status_code}")
            print(f"Response: {response.text}")
            break
            
        try:
            data = response.json()
            items = data.get('List', [])
            total_count = data.get('TotalCount', 0)
            current_page_size = data.get('PageSize', page_size)
            
            # Add items from this page
            all_items.extend(items)
                
            # Calculate if there are more pages
            if current_page_size > 0:
                total_pages = (total_count + current_page_size - 1) // current_page_size
            else:
                total_pages = 1  # Avoid division by zero
            has_more = page < total_pages and len(items) > 0  # Stop if we get an empty page
            page += 1
            
            print(f"Processed {len(items)} items. Total count: {total_count}. Pages: {page-1}/{total_pages}")
                    
        except Exception as e:
            print(f"Error processing page {page}: {e}")
            break
    
    # Process all date fields in the data
    for item in all_items:
        process_trademe_dates(item)
    
    return all_items

def get_rental_properties(api: OAuthAPI, search_params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Fetch rental properties with pagination"""
    if search_params is None:
        search_params = {}
    
    # Default search parameters
    default_params = {
        'page': 1,
        'page_size': 50,
        'sort_order': 'Default'
    }
    
    # Merge default params with provided params
    params = {**default_params, **search_params}
    page = params.pop('page')
    page_size = params.pop('page_size')
    
    has_more = True
    all_items = []
    
    while has_more:
        # Update page parameter
        current_params = {**params, 'page': page, 'page_size': page_size}
        
        # Use the generic API request method
        endpoint = "/v1/Search/Property/Rental.json"
        
        print(f"Fetching rental properties page {page}")
        
        # Make request
        response = api.api_request(endpoint, params=current_params)
        
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.status_code}")
            print(f"Response: {response.text}")
            break
            
        try:
            data = response.json()
            items = data.get('List', [])
            total_count = data.get('TotalCount', 0)
            current_page_size = data.get('PageSize', page_size)
            
            # Add items from this page
            all_items.extend(items)
                
            # Calculate if there are more pages
            if current_page_size > 0:
                total_pages = (total_count + current_page_size - 1) // current_page_size
            else:
                total_pages = 1  # Avoid division by zero
            has_more = page < total_pages and len(items) > 0  # Stop if we get an empty page
            page += 1
            
            print(f"Processed {len(items)} items. Total count: {total_count}. Pages: {page-1}/{total_pages}")
                    
        except Exception as e:
            print(f"Error processing page {page}: {e}")
            break
    
    # Process all date fields in the data
    for item in all_items:
        process_trademe_dates(item)
    
    return all_items


def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str):
    """Upload a file to Google Cloud Storage"""
    # Initialize GCS client with credentials
    credentials = service_account.Credentials.from_service_account_file(
        os.path.expanduser(SERVICE_ACCOUNT_FILE)
    )
    client = storage.Client(credentials=credentials, project=PROJECT_ID)
    
    # Get bucket
    bucket = client.bucket(bucket_name)
    
    # Create blob and upload file
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    
    print(f"File {source_file} uploaded to gs://{bucket_name}/{destination_blob}")


def run_pipeline():
    """Run the full ETL pipeline"""
    # Initialize API
    api = OAuthAPI()
    
    # Ensure authentication is completed
    if not api.authenticate():
        raise Exception("Authentication failed. Cannot proceed with data extraction.")
    
    # Step 1: Extract data from TradeMe API
    print("\n=== Step 1: Extracting data from TradeMe API ===")
    
    # Get watchlist data
    print("\n--- Fetching Watchlist Data ---")
    watchlist_data = get_watchlist_data(api)
    print(f"Extracted {len(watchlist_data)} items from watchlist")
    
    # Get rental properties data
    print("\n--- Fetching Rental Properties Data ---")
    rental_params = {
        'region': 15,  # Auckland
        'price_max': 1000,
        'bedrooms_min': 2
    }
    # rental_data = get_rental_properties(api, rental_params)
    rental_data = get_rental_properties(api)
    print(f"Extracted {len(rental_data)} rental properties")
    
    # Step 2: Save data to local JSON files with human-readable timestamp
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    
    # Save watchlist data
    watchlist_file = f"watchlist_{formatted_time}.json"
    print(f"\n=== Step 2a: Saving watchlist data to local file {watchlist_file} ===")
    with open(watchlist_file, 'w') as f:
        json.dump(watchlist_data, f, indent=4)
    print(f"Watchlist data saved to {watchlist_file}")
    
    # Save rental data
    rental_file = f"rentals_{formatted_time}.json"
    print(f"\n=== Step 2b: Saving rental data to local file {rental_file} ===")
    with open(rental_file, 'w') as f:
        json.dump(rental_data, f, indent=4)
    print(f"Rental data saved to {rental_file}")
    
    # Step 3: Upload to Google Cloud Storage
    print("\n=== Step 3: Uploading to Google Cloud Storage ===")
    
    # Upload watchlist data
    watchlist_gcs_path = f"watchlist/{watchlist_file}"
    try:
        upload_to_gcs(BUCKET_NAME, watchlist_file, watchlist_gcs_path)
        print(f"Successfully uploaded watchlist data to gs://{BUCKET_NAME}/{watchlist_gcs_path}")
    except Exception as e:
        print(f"Error uploading watchlist data to GCS: {e}")
    
    # Upload rental data
    rental_gcs_path = f"rentals/{rental_file}"
    try:
        upload_to_gcs(BUCKET_NAME, rental_file, rental_gcs_path)
        print(f"Successfully uploaded rental data to gs://{BUCKET_NAME}/{rental_gcs_path}")
    except Exception as e:
        print(f"Error uploading rental data to GCS: {e}")
    
    # Step 4: Load data into dlt pipeline
    print("\n=== Step 4: Loading data into dlt pipeline ===")
    try:
        # Set up local directory for dlt filesystem destination
        dlt_data_dir = os.path.join(os.getcwd(), "dlt_data")
        os.makedirs(dlt_data_dir, exist_ok=True)
        
        # Set environment variables for dlt
        os.environ["BUCKET_URL"] = dlt_data_dir
        
        # Define a dlt pipeline with filesystem destination
        pipeline = dlt.pipeline(
            pipeline_name="trademe_pipeline",
            destination="filesystem",
            dataset_name="trademe_data"
        )
        
        # Run the pipeline with each dataset separately
        print("Loading watchlist data into dlt pipeline...")
        watchlist_info = pipeline.run(
            watchlist_data,
            table_name="watchlist",  # Explicitly provide the table name
            write_disposition="replace",
            loader_file_format="jsonl"  # Use JSON Lines format
        )
        
        print("Loading rental data into dlt pipeline...")
        rental_info = pipeline.run(
            rental_data,
            table_name="rentals",  # Explicitly provide the table name
            write_disposition="replace",
            loader_file_format="jsonl"  # Use JSON Lines format
        )
        
        # Combine load info
        load_info = {
            "watchlist": watchlist_info,
            "rentals": rental_info
        }
        
        print(f"Pipeline run completed successfully.")
        print(f"Data location: {os.path.join(dlt_data_dir, pipeline.pipeline_name, pipeline.dataset_name)}")
        
        # Print summary
        # print(f"Load info: {load_info}")
        
        dlt_info = load_info
    except Exception as e:
        print(f"Error running dlt pipeline: {e}")
        print("This step is optional and doesn't affect the main functionality.")
        dlt_info = None
    
    return {
        "watchlist_file": watchlist_file,
        "rental_file": rental_file,
        "watchlist_gcs_path": f"gs://{BUCKET_NAME}/{watchlist_gcs_path}",
        "rental_gcs_path": f"gs://{BUCKET_NAME}/{rental_gcs_path}",
        "dlt_info": dlt_info
    }


if __name__ == "__main__":
    run_pipeline()
