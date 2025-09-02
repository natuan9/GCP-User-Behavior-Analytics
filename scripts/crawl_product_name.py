# product_data_processing.py - Modified với Threading và Checkpoint Save

import pymongo
import csv
import logging
import configparser
import os
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import sys
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Set up logging for better tracking and error reporting ---
def setup_logging(log_file, error_log_file):
    """Configures logging to write to a main log file and a separate error log file."""
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(console_handler)

# --- Load configuration from INI file ---
def load_config(filename='../config/config.ini'):
    """Loads configuration from the specified INI file."""
    config = configparser.ConfigParser()
    if not os.path.exists(filename):
        logging.error(f"Configuration file '{filename}' not found. Please create it.")
        raise FileNotFoundError(f"Configuration file '{filename}' not found.")
    config.read(filename)
    return config

# --- MongoDB connection function ---
def connect_to_mongodb(mongo_uri, db_name):
    """Establishes a connection to MongoDB and returns the database object."""
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client[db_name]
        logging.info("Successfully connected to MongoDB.")
        return db, client
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Could not connect to MongoDB: {e}")
        return None, None

# --- Function to get unique product URLs ---
def get_unique_product_urls(summary_collection, unique_ids_file, event_collections):
    """Extracts unique product IDs and URLs from a MongoDB collection or a file."""
    product_urls = {}
    if os.path.exists(unique_ids_file):
        logging.info(f"Loading unique product IDs and URLs from file: '{unique_ids_file}'...")
        try:
            with open(unique_ids_file, 'r') as f:
                product_urls = json.load(f)
            logging.info(f"Loaded {len(product_urls)} unique product IDs from file.")
            return product_urls
        except Exception as e:
            logging.error(f"Error loading unique product IDs from file: {e}. Re-extracting from MongoDB.")

    logging.info(f"Extracting unique product IDs and URLs from the 'summary' collection for events: {event_collections}")
    try:
        query = {"collection": {"$in": event_collections}}
        cursor = summary_collection.find(query, {"product_id": 1, "viewing_product_id": 1, "current_url": 1, "referrer_url": 1, "collections": 1, "_id": 0})
        
        for doc in cursor:
            collection_name = doc.get('collection')
            
            if collection_name == 'product_view_all_recommend_clicked':
                product_id = doc.get('viewing_product_id')
                url = doc.get('referrer_url')
            else:
                product_id = doc.get('product_id') or doc.get('viewing_product_id')
                url = doc.get('current_url')
            
            if product_id and url and product_id not in product_urls:
                product_urls[product_id] = url
        
        with open(unique_ids_file, 'w') as f:
            json.dump(product_urls, f, indent=4)
        
        logging.info(f"Finished extracting. Found {len(product_urls)} unique product IDs. Saved to '{unique_ids_file}'.")
        return product_urls
    except Exception as e:
        logging.error(f"Error while fetching data from the 'summary' collection: {e}")
        return None

# --- Thread-safe data handler for checkpoint saves ---
class ThreadSafeDataHandler:
    def __init__(self, failed_output_file, processed_ids_file, success_output_file):
        self.failed_output_file = failed_output_file
        self.processed_ids_file = processed_ids_file
        self.success_output_file = success_output_file
        self.processed_ids = set()
        self.processed_count = 0
        self.successful_count = 0
        self.failed_count = 0
        self.failed_data = []
        self.success_data = []
        self.lock = threading.Lock()
        
        # Initialize failed CSV file with header
        self._init_failed_csv()
        
    def _init_failed_csv(self):
        """Initialize failed CSV file with header if it doesn't exist."""
        if not os.path.exists(self.failed_output_file):
            with open(self.failed_output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["product_id", "url", "error"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
    
    def add_success(self, product_id, product_name, url):
        """Add successful crawl result."""
        with self.lock:
            self.success_data.append({
                "product_id": product_id,
                "product_name": product_name,
                "url": url
            })
            self.processed_ids.add(product_id)
            self.processed_count += 1
            self.successful_count += 1
            
    def add_failure(self, product_id, url, error_message):
        """Add failed crawl result and immediately write to CSV."""
        with self.lock:
            error_record = {"product_id": product_id, "url": url, "error": error_message}
            self.failed_data.append(error_record)
            self.processed_ids.add(product_id)
            self.processed_count += 1
            self.failed_count += 1
            
            # Immediately write failed record to CSV
            try:
                with open(self.failed_output_file, 'a', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ["product_id", "url", "error"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(error_record)
            except Exception as e:
                logging.error(f"Error writing failed record to CSV: {e}")
    
    def checkpoint_save(self, force=False):
        """Save checkpoint data every 100 records or when forced."""
        with self.lock:
            if (self.processed_count % 100 == 0 and self.processed_count > 0) or force:
                try:
                    # Save processed IDs
                    with open(self.processed_ids_file, 'w') as f:
                        json.dump(list(self.processed_ids), f)
                    
                    # Save successful products to CSV
                    if self.success_data:
                        fieldnames = ["product_id", "product_name", "url"]
                        with open(self.success_output_file, 'w', newline='', encoding='utf-8') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(self.success_data)
                    
                    # Log checkpoint info (only at checkpoints)
                    logging.info(f"📊 Checkpoint {self.processed_count}: ✅ {self.successful_count} success | ❌ {self.failed_count} failed")
                    
                    return True
                except Exception as e:
                    logging.error(f"Error saving checkpoint: {e}")
                    return False
            return False

# --- Single URL crawling function for threading ---
def crawl_single_url(product_id, url, data_handler, crawl_delay_min, crawl_delay_max, retry_delay):
    """Crawls a single URL and processes the data."""

    session = requests.Session()
    
    try:
        if not urlparse(url).scheme:
            url = f"https://www.glamira.vn/{url.lstrip('/')}"
        
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        product_name_element = soup.find('h1', class_='page-title') 
        
        if product_name_element:
            product_name = product_name_element.text.strip()
            data_handler.add_success(product_id, product_name, url)
        else:
            error_msg = f"Product name not found for product_id '{product_id}' on URL: {url}"
            data_handler.add_failure(product_id, url, error_msg)
            
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error for product_id '{product_id}' at URL '{url}': {e}"
        data_handler.add_failure(product_id, url, error_msg)
        if e.response and e.response.status_code in (429, 503, 504):
            time.sleep(retry_delay)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Could not connect to product_id '{product_id}' at URL '{url}': {e}"
        data_handler.add_failure(product_id, url, error_msg)
        
    except Exception as e:
        error_msg = f"An unexpected error occurred while processing product_id '{product_id}' at URL '{url}': {e}"
        data_handler.add_failure(product_id, url, error_msg)
    
    finally:
        session.close()
        # Randomize the delay to avoid being detected
        time.sleep(random.uniform(crawl_delay_min, crawl_delay_max))
        
        # Check for checkpoint save
        data_handler.checkpoint_save()

# --- Multi-threaded crawling function with checkpoint saves ---
def crawl_and_process_urls_threaded(crawl_list, processed_ids, output_files, 
                                  crawl_delay_min, crawl_delay_max, retry_delay, max_workers=5):
    """Crawls URLs using multiple threads with checkpoint saves."""
    
    data_handler = ThreadSafeDataHandler(
        output_files['failed'], 
        output_files['processed_ids'],
        output_files['success']
    )
    data_handler.processed_ids = processed_ids.copy()
    
    total_to_crawl = len(crawl_list)
    logging.info(f"🚀 Starting threaded crawl with {max_workers} workers for {total_to_crawl} URLs...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all crawling tasks
        futures = []
        for product_id, url in crawl_list:
            future = executor.submit(
                crawl_single_url, product_id, url, data_handler,
                crawl_delay_min, crawl_delay_max, retry_delay
            )
            futures.append(future)
        
        # Wait for all tasks to complete with progress tracking
        completed = 0
        for future in as_completed(futures):
            try:
                future.result()
                completed += 1
                
                # Log progress every 50 completions (less verbose than every single crawl)
                if completed % 50 == 0:
                    progress_pct = (completed / total_to_crawl) * 100
                    logging.info(f"🔄 Progress: {completed}/{total_to_crawl} URLs completed ({progress_pct:.1f}%)")
                    
            except Exception as e:
                logging.error(f"Thread execution error: {e}")
                completed += 1
    
    # Final checkpoint save
    data_handler.checkpoint_save(force=True)
    
    logging.info(f"🎉 Threaded crawling completed!")
    logging.info(f"   📊 Total processed: {data_handler.processed_count}")
    logging.info(f"   ✅ Successful: {data_handler.successful_count}")
    logging.info(f"   ❌ Failed: {data_handler.failed_count}")
    
    return data_handler.success_data, data_handler.failed_data

# --- Function to save successful data to CSV (final save) ---
def save_successful_data(product_data, output_file):
    """Saves final list of successful product records to a CSV file."""
    if product_data:
        logging.info(f"💾 Final save: Writing {len(product_data)} successful product records to '{output_file}'...")
        fieldnames = ["product_id", "product_name", "url"]
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(product_data)
        logging.info("✅ Final CSV file saved successfully.")
    else:
        logging.warning("⚠️  No product data to save. The process finished with no successful results.")

# --- Function to print final summary ---
def print_summary(product_urls, successful_crawls, failed_crawls_current_run, failed_output_file):
    """Prints a final summary of the crawling process."""
    total_products = len(product_urls)
    logging.info("\n--- CRAWLING SUMMARY ---")
    logging.info(f"Total unique products to crawl: {total_products}")
    logging.info(f"Successfully crawled (this run): {successful_crawls}")
    logging.info(f"Failed to crawl (this run): {failed_crawls_current_run}")
    
    total_failed_in_file = 0
    if os.path.exists(failed_output_file):
        try:
            with open(failed_output_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                total_failed_in_file = sum(1 for row in reader) - 1
                if total_failed_in_file < 0: total_failed_in_file = 0
        except Exception as e:
            logging.error(f"Could not read from '{failed_output_file}' to get total failed count: {e}")
    
    logging.info(f"Total failed records in '{failed_output_file}': {total_failed_in_file}")
    logging.info("------------------------")

# --- Main function to process product data ---
def process_product_data():
    """
    Orchestrates the entire product data extraction and crawling process with threading.
    """
    try:
        config = load_config()
    except FileNotFoundError:
        return

    setup_logging(config['script_logic']['log_file'], config['script_logic']['error_log_file'])
    logging.info("🔥 Starting threaded product data processing with checkpoint saves...")

    mongo_uri = config['mongodb']['mongo_uri']
    db_name = config['mongodb']['db_name']
    product_output_file = config['script_logic']['product_output_file']
    failed_output_file = config['script_logic']['failed_output_file']
    event_collections = [col.strip() for col in config['script_logic']['event_collections'].split(',')]
    unique_product_ids_file = config['script_logic']['unique_product_ids_file']
    processed_product_ids_file = config['script_logic']['processed_product_ids_file']
    crawl_delay_min = float(config['script_logic']['crawl_delay_min_seconds'])
    crawl_delay_max = float(config['script_logic']['crawl_delay_max_seconds'])
    retry_delay = int(config['script_logic']['retry_delay_seconds'])
    
    # Get max_workers from config with default value
    max_workers = int(config['script_logic'].get('max_workers', 5))

    db, client = connect_to_mongodb(mongo_uri, db_name)
    if db is None:
        return

    summary_collection = db['summary']
    product_urls = get_unique_product_urls(summary_collection, unique_product_ids_file, event_collections)
    if not product_urls:
        client.close()
        return

    processed_ids = set()
    if os.path.exists(processed_product_ids_file):
        try:
            with open(processed_product_ids_file, 'r') as f:
                processed_ids = set(json.load(f))
            logging.info(f"📂 Loaded {len(processed_ids)} previously processed product IDs.")
        except Exception as e:
            logging.error(f"Error loading processed IDs from '{processed_product_ids_file}': {e}. Starting from scratch.")

    crawl_list = [item for item in product_urls.items() if item[0] not in processed_ids]
    
    output_files = {
        'failed': failed_output_file,
        'processed_ids': processed_product_ids_file,
        'success': product_output_file
    }
    
    logging.info(f"📋 Ready to crawl {len(crawl_list)} new products using {max_workers} threads")
    logging.info(f"   (Skipping {len(processed_ids)} already processed products)")
    
    # Use threaded crawling with checkpoint saves
    product_data, failed_data_current_run = crawl_and_process_urls_threaded(
        crawl_list, processed_ids, output_files, 
        crawl_delay_min, crawl_delay_max, retry_delay, max_workers
    )

    # Final save - ensures the final complete dataset is saved
    save_successful_data(product_data, product_output_file)
    
    print_summary(product_urls, len(product_data), len(failed_data_current_run), failed_output_file)

    client.close()
    logging.info("🎉 Threaded product data processing complete. MongoDB connection closed.")


# --- Script entry point ---
if __name__ == "__main__":
    process_product_data()