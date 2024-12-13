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
MAX_RESULTS_PER_REGION = 10000
BATCH_SIZE = 40  # API limit per request for motorcycles
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 10
MAX_RETRY_DELAY = 60
RATE_LIMIT_DELAY = 3

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
    """Fetch a single batch of motorcycle listings with exponential backoff retry."""
    url = "https://search.mudah.my/v1/search"
    params = {
        "category": "1040",  # Motorcycle category
        "condition": "1",
        "from": offset,
        "limit": BATCH_SIZE,
        "region": region,
        "type": "sell",
        "include": "extra_images,body"
    }
    
    retry_delay = INITIAL_RETRY_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            wait_for_rate_limit()
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

def process_listing(listing: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single motorcycle listing into the desired format."""
    attrs = listing['attributes']
    
    return {
        "listing_id": listing['id'],
        "title": attrs.get('subject', ''),
        "price": attrs.get('price', 0),
        "make": attrs.get('motorcycle_make_name', ''),
        "model": attrs.get('motorcycle_model_name', ''),
        "year": attrs.get('manufactured_year', ''),
        "location": attrs.get('region_name', ''),
        "seller_name": attrs.get('name', ''),
        "listing_date": attrs.get('date', ''),
        "image_count": attrs.get('image_count', 0),
        "ad_url": attrs.get('adview_url', ''),
        "region_id": attrs.get('region_id', '')
    }

def fetch_region_data_parallel(regions: List[str]) -> List[Dict[str, Any]]:
    """Fetch listings for multiple regions in parallel with thread pooling."""
    all_listings = []
    seen_listing_ids = set()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_region = {
            executor.submit(fetch_single_region, region): region 
            for region in regions
        }
        
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                listings = future.result()
                new_listings = [
                    listing for listing in listings 
                    if listing['listing_id'] not in seen_listing_ids
                ]
                seen_listing_ids.update(
                    listing['listing_id'] for listing in new_listings
                )
                all_listings.extend(new_listings)
                logging.info(f"Added {len(new_listings)} unique motorcycle listings from region {region}")
            except Exception as e:
                logging.error(f"Error processing region {region}: {str(e)}")
    
    return all_listings

def fetch_single_region(region: str) -> List[Dict[str, Any]]:
    """Fetch all motorcycle listings for a single region."""
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

def get_motorcycle_listings(**context):
    """Main function using parallel processing."""
    all_listings = fetch_region_data_parallel(REGIONS)
    context['task_instance'].xcom_push(key='motorcycle_listings', value=all_listings)
    logging.info(f"Total unique motorcycle listings collected: {len(all_listings)}")
    return len(all_listings)

def fast_insert_motorcycle_listings_into_postgres(**context):
    """Optimized bulk insert using COPY command with deduplication."""
    try:
        listings = context['task_instance'].xcom_pull(
            key='motorcycle_listings', 
            task_ids='fetch_motorcycle_listings'
        )
        
        if not listings:
            logging.error("No motorcycle listings found in XCom")
            raise ValueError("No motorcycle listings found in XCom")
        
        postgres_hook = PostgresHook(postgres_conn_id='motorcycles_connection')
        conn = postgres_hook.get_conn()
        
        # Create temporary table
        create_temp_table_sql = """
        CREATE TEMP TABLE temp_listings (
            listing_id BIGINT NOT NULL,
            title TEXT NOT NULL,
            price NUMERIC,
            make TEXT,
            model TEXT,
            year TEXT,
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
            
            output = StringIO()
            writer = csv.writer(output, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
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
                        listing['location'],
                        listing['seller_name'],
                        listing['listing_date'],
                        listing['image_count'],
                        listing['ad_url'],
                        listing['region_id']
                    ])
            
            output.seek(0)
            
            cur.copy_expert(
                "COPY temp_listings FROM STDIN WITH CSV DELIMITER E'\t' QUOTE '\"'",
                output
            )
            
            cur.execute("""
                INSERT INTO motorcycle_listings (
                    listing_id, title, price, make, model, year,
                    location, seller_name, listing_date,
                    image_count, ad_url, region_id
                )
                SELECT DISTINCT ON (listing_id) *
                FROM temp_listings
                ON CONFLICT (listing_id) DO UPDATE 
                SET 
                    price = EXCLUDED.price,
                    image_count = EXCLUDED.image_count,
                    updated_at = CURRENT_TIMESTAMP;
            """)
            
        conn.commit()
        logging.info(f"Successfully inserted/updated {len(deduplicated_listings)} motorcycle listings")
        
    except Exception as e:
        logging.error(f"Error in fast_insert_motorcycle_listings_into_postgres: {str(e)}")
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
    'fetch_and_store_mudah_motorcycle_listings',
    default_args=default_args,
    description='DAG to fetch motorcycle listings from all regions on Mudah.my',
    schedule_interval=timedelta(hours=12),
    catchup=False
)

# Create table task
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='motorcycles_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS motorcycle_listings (
        id SERIAL PRIMARY KEY,
        listing_id BIGINT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        price NUMERIC,
        make TEXT,
        model TEXT,
        year TEXT,
        location TEXT,
        seller_name TEXT,
        listing_date TIMESTAMP,
        image_count INTEGER,
        ad_url TEXT,
        region_id TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_motorcycle_region_id ON motorcycle_listings(region_id);
    CREATE INDEX IF NOT EXISTS idx_motorcycle_listing_date ON motorcycle_listings(listing_date);
    """,
    dag=dag,
)

# Migration task to remove unused columns
# migrate_table_task = PostgresOperator(
#     task_id='migrate_table',
#     postgres_conn_id='motorcycles_connection',
#     sql="""
#     ALTER TABLE motorcycle_listings
#     DROP COLUMN IF EXISTS condition,
#     DROP COLUMN IF EXISTS body,
#     DROP COLUMN IF EXISTS seller_type,
#     DROP COLUMN IF EXISTS company_ad,
#     DROP COLUMN IF EXISTS store_verified,
#     DROP COLUMN IF EXISTS subarea;

#     DROP INDEX IF EXISTS idx_motorcycle_make;
#     """,
#     dag=dag,
# )

# Fetch listings task
fetch_motorcycle_listings_task = PythonOperator(
    task_id='fetch_motorcycle_listings',
    python_callable=get_motorcycle_listings,
    provide_context=True,
    dag=dag,
)

# Insert listings task
insert_listings_task = PythonOperator(
    task_id='insert_listings',
    python_callable=fast_insert_motorcycle_listings_into_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_motorcycle_listings_task >> create_table_task >> insert_listings_task
