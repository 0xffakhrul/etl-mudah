from datetime import datetime, timedelta
from airflow import DAG
import requests
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time
from typing import List, Dict, Any
from queue import Queue
from threading import Lock
import concurrent.futures
import psycopg2.extras
from io import StringIO
import csv

# Constants
REGIONS = [
    "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "10", "11", "12", "13", "14", "15"
]
MAX_RESULTS_PER_REGION = 10000  # Hard limit per region
BATCH_SIZE = 200  # API limit per request
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 10  # seconds
MAX_RETRY_DELAY = 60  # seconds
RATE_LIMIT_DELAY = 3  # seconds between requests

# Rate limiting utilities
request_lock = Lock()
last_request_time = time.time()

def wait_for_rate_limit():
    """Ensure minimum delay between requests."""
    global last_request_time
    with request_lock:
        current_time = time.time()
        time_since_last_request = current_time - last_request_time
        if time_since_last_request < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - time_since_last_request)
        last_request_time = time.time()

def fetch_batch(region: str, offset: int) -> Dict[str, Any]:
    """Fetch a single batch of listings with exponential backoff retry."""
    url = "https://search.mudah.my/v1/search"
    params = {
        "category": "1020",
        "condition": "1",
        "from": offset,
        "limit": BATCH_SIZE,
        "region": region,
        "type": "sell"
    }
    
    retry_delay = INITIAL_RETRY_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            wait_for_rate_limit()  # Rate limit protection
            response = requests.get(url, params=params)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                logging.warning(f"Rate limited on region {region}, waiting {retry_after} seconds")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
            logging.warning(f"Retry {attempt + 1} after error: {str(e)}, waiting {retry_delay} seconds")
            time.sleep(retry_delay)
    
    raise Exception(f"Max retries exceeded for region {region}")

def fetch_region_data_parallel(regions: List[str]) -> List[Dict[str, Any]]:
    """Fetch listings for multiple regions in parallel with thread pooling."""
    all_listings = []
    seen_listing_ids = set()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all regions to thread pool
        future_to_region = {
            executor.submit(fetch_single_region, region): region 
            for region in regions
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                listings = future.result()
                # Deduplicate while collecting
                new_listings = [
                    listing for listing in listings 
                    if listing['listing_id'] not in seen_listing_ids
                ]
                seen_listing_ids.update(
                    listing['listing_id'] for listing in new_listings
                )
                all_listings.extend(new_listings)
                logging.info(f"Added {len(new_listings)} unique listings from region {region}")
            except Exception as e:
                logging.error(f"Error processing region {region}: {str(e)}")
    
    return all_listings

def process_listing(listing: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single listing into the desired format."""
    attrs = listing['attributes']
    mileage = attrs.get('mileage', {})
    
    return {
        "listing_id": listing['id'],
        "title": attrs.get('subject', ''),
        "price": attrs.get('price', 0),
        "make": attrs.get('make_name', ''),
        "model": attrs.get('model_name', ''),
        "year": attrs.get('manufactured_year', ''),
        "mileage_min": mileage.get('gte', '0'),
        "mileage_max": mileage.get('lte', '0'),
        "transmission": attrs.get('transmission_name', ''),
        "fuel_type": attrs.get('fueltype', ''),
        "car_type": attrs.get('car_type_name', ''),
        "location": attrs.get('region_name', ''),
        "seller_name": attrs.get('name', ''),
        "listing_date": attrs.get('date', ''),
        "image_count": attrs.get('image_count', 0),
        "ad_url": attrs.get('adview_url', ''),
        "region_id": attrs.get('region_id', '')
    }

def fetch_single_region(region: str) -> List[Dict[str, Any]]:
    """Fetch all listings for a single region."""
    listings = []
    offset = 0
    
    try:
        while len(listings) < MAX_RESULTS_PER_REGION:
            data = fetch_batch(region, offset)
            batch = data['data']
            
            if not batch:
                break
            
            processed_batch = [process_listing(listing) for listing in batch]
            listings.extend(processed_batch)
            
            if len(batch) < BATCH_SIZE:
                break
                
            offset += len(batch)
            
    except Exception as e:
        logging.error(f"Error in batch fetch for region {region}: {str(e)}")
        
    return listings

def get_car_listings(**context):
    """Main function using parallel processing."""
    all_listings = fetch_region_data_parallel(REGIONS)
    context['task_instance'].xcom_push(key='car_listings', value=all_listings)
    logging.info(f"Total unique listings collected: {len(all_listings)}")
    return len(all_listings)

def fast_insert_car_listings_into_postgres(**context):
    """Optimized bulk insert using COPY command with deduplication."""
    try:
        listings = context['task_instance'].xcom_pull(key='car_listings', task_ids='fetch_car_listings')
        
        if not listings:
            logging.error("No car listings found in XCom")
            raise ValueError("No car listings found in XCom")
        
        postgres_hook = PostgresHook(postgres_conn_id='cars_connection')
        conn = postgres_hook.get_conn()
        
        # Create temporary table for new data
        create_temp_table_sql = """
        CREATE TEMP TABLE temp_listings (
            listing_id BIGINT NOT NULL,
            title TEXT NOT NULL,
            price NUMERIC,
            make TEXT,
            model TEXT,
            year TEXT,
            mileage_min TEXT,
            mileage_max TEXT,
            transmission TEXT,
            fuel_type TEXT,
            car_type TEXT,
            location TEXT,
            seller_name TEXT,
            listing_date TIMESTAMP,
            image_count INTEGER,
            ad_url TEXT,
            region_id TEXT
        ) ON COMMIT DROP;
        """
        
        with conn.cursor() as cur:
            cur.execute(create_temp_table_sql)
            
            # Prepare data for COPY
            output = StringIO()
            writer = csv.writer(output, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            # Keep track of seen listing_ids to handle duplicates
            seen_listing_ids = set()
            deduplicated_listings = []
            
            for listing in listings:
                if listing['listing_id'] not in seen_listing_ids:
                    seen_listing_ids.add(listing['listing_id'])
                    deduplicated_listings.append(listing)
                    writer.writerow([
                        listing['listing_id'],
                        listing['title'],
                        listing['price'],
                        listing['make'],
                        listing['model'],
                        listing['year'],
                        listing['mileage_min'],
                        listing['mileage_max'],
                        listing['transmission'],
                        listing['fuel_type'],
                        listing['car_type'],
                        listing['location'],
                        listing['seller_name'],
                        listing['listing_date'],
                        listing['image_count'],
                        listing['ad_url'],
                        listing['region_id']
                    ])
            
            output.seek(0)
            
            # Bulk insert using COPY
            cur.copy_expert(
                "COPY temp_listings FROM STDIN WITH CSV DELIMITER E'\t' QUOTE '\"'",
                output
            )
            
            # Upsert from temporary table to main table
            # Add a subquery to ensure we only get one row per listing_id
            cur.execute("""
                INSERT INTO car_listings (
                    listing_id, title, price, make, model, year,
                    mileage_min, mileage_max, transmission, fuel_type,
                    car_type, location, seller_name, listing_date,
                    image_count, ad_url, region_id
                )
                SELECT DISTINCT ON (listing_id) *
                FROM temp_listings
                ON CONFLICT (listing_id) DO UPDATE 
                SET 
                    price = EXCLUDED.price,
                    mileage_min = EXCLUDED.mileage_min,
                    mileage_max = EXCLUDED.mileage_max,
                    image_count = EXCLUDED.image_count,
                    updated_at = CURRENT_TIMESTAMP;
            """)
            
        conn.commit()
        logging.info(f"Successfully inserted/updated {len(deduplicated_listings)} listings")
        
    except Exception as e:
        logging.error(f"Error in fast_insert_car_listings_into_postgres: {str(e)}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_mudah_listings_optimized',
    default_args=default_args,
    description='Optimized DAG to fetch car listings from all regions on Mudah.my',
    schedule_interval=timedelta(hours=12),
    catchup=False
)

# Create table task
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='cars_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS car_listings (
        id SERIAL PRIMARY KEY,
        listing_id BIGINT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        price NUMERIC,
        make TEXT,
        model TEXT,
        year TEXT,
        mileage_min TEXT,
        mileage_max TEXT,
        transmission TEXT,
        fuel_type TEXT,
        car_type TEXT,
        location TEXT,
        seller_name TEXT,
        listing_date TIMESTAMP,
        image_count INTEGER,
        ad_url TEXT,
        region_id TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_region_id ON car_listings(region_id);
    CREATE INDEX IF NOT EXISTS idx_listing_date ON car_listings(listing_date);
    """,
    dag=dag,
)

# Fetch listings task
fetch_car_listings_task = PythonOperator(
    task_id='fetch_car_listings',
    python_callable=get_car_listings,
    provide_context=True,
    dag=dag,
)

# Insert listings task
insert_listings_task = PythonOperator(
    task_id='insert_listings',
    python_callable=fast_insert_car_listings_into_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_car_listings_task >> create_table_task >> insert_listings_task