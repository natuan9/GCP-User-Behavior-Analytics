# IP Location Processing Script - Optimized with Resumption

import pymongo
import IP2Location
import os
import logging
import json
import configparser
from datetime import datetime

# --- Set up logging for better tracking and error reporting ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load configuration from INI file ---
def load_config(filename='config.ini'):
    """Loads configuration from the specified INI file."""
    config = configparser.ConfigParser()
    if not os.path.exists(filename):
        logging.error(f"Configuration file '{filename}' not found. Please create it.")
        raise FileNotFoundError(f"Configuration file '{filename}' not found.")
    config.read(filename)
    return config

# Load the config at the start of the script
try:
    config = load_config()
except FileNotFoundError:
    exit()

# Extract config values
MONGO_URI = config['mongodb']['mongo_uri']
DB_NAME = config['mongodb']['db_name']
SOURCE_COLLECTION_NAME = config['mongodb']['source_collection_name']
TARGET_COLLECTION_NAME = config['mongodb']['target_collection_name']

IP2LOCATION_DB_PATH = config['ip2location']['ip2location_db_path']

BATCH_SIZE = int(config['script_logic']['batch_size'])
UNIQUE_IPS_FILE = config['script_logic']['unique_ips_file']

# --- Main function to process IP data ---
def process_ip_locations():
    """
    Connects to MongoDB, extracts unique IPs (from file or DB),
    looks up their location, and stores the results in a new collection.
    The script can resume from where it left off after a restart.
    """
    logging.info("Starting IP location processing...")

    # 1. Connect to MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        source_collection = db[SOURCE_COLLECTION_NAME]
        target_collection = db[TARGET_COLLECTION_NAME]
        logging.info("Successfully connected to MongoDB.")
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Could not connect to MongoDB: {e}")
        return

    # Check if the source collection exists and has data using find_one()
    if not source_collection.find_one({}):
        logging.error(f"Source collection '{SOURCE_COLLECTION_NAME}' is empty or does not exist. Exiting.")
        client.close()
        return

    # 2. Get unique IPs from file or from MongoDB
    unique_ips = []
    if os.path.exists(UNIQUE_IPS_FILE):
        logging.info(f"Loading unique IPs from existing file: '{UNIQUE_IPS_FILE}'...")
        try:
            with open(UNIQUE_IPS_FILE, 'r') as f:
                unique_ips = json.load(f)
            logging.info(f"Loaded {len(unique_ips)} unique IPs from file.")
        except Exception as e:
            logging.error(f"Error loading unique IPs from file: {e}. Falling back to MongoDB extraction.")
    
    if not unique_ips:
        logging.info(f"Extracting unique IPs from '{SOURCE_COLLECTION_NAME}' using aggregation pipeline...")
        try:
            pipeline = [
                {"$group": {"_id": "$ip"}},
                {"$project": {"ip": "$_id", "_id": 0}}
            ]
            unique_ips_cursor = source_collection.aggregate(pipeline, allowDiskUse=True)
            unique_ips = [doc['ip'] for doc in unique_ips_cursor]
            
            with open(UNIQUE_IPS_FILE, 'w') as f:
                json.dump(unique_ips, f)
            
            logging.info(f"Successfully extracted and saved {len(unique_ips)} IPs to '{UNIQUE_IPS_FILE}'.")
        except Exception as e:
            logging.error(f"Error extracting unique IPs with aggregation: {e}")
            client.close()
            return

    # 3. Use IP2Location to get location data and process in batches
    try:
        ip_db = IP2Location.IP2Location(IP2LOCATION_DB_PATH)
        logging.info(f"Successfully loaded IP2Location database file: {IP2LOCATION_DB_PATH}")
    except FileNotFoundError:
        logging.error(f"IP2Location database file not found at '{IP2LOCATION_DB_PATH}'. Please ensure the file is in the correct path.")
        client.close()
        return

    # Get a list of IPs already processed to resume from where we left off
    logging.info("Checking for already processed IPs in the target collection...")
    try:
        processed_ips_cursor = target_collection.find({}, {"ip": 1, "_id": 0})
        processed_ips = {doc['ip'] for doc in processed_ips_cursor}
        logging.info(f"Found {len(processed_ips)} IPs already processed.")
    except Exception as e:
        logging.error(f"Error fetching processed IPs: {e}")
        processed_ips = set()

    # Filter out IPs that have already been processed
    ips_to_process = [ip for ip in unique_ips if ip not in processed_ips]
    logging.info(f"Starting to process {len(ips_to_process)} remaining IPs...")
    
    location_data_batch = []
    processed_count = len(processed_ips)
    batch_count = 0
    
    for ip in ips_to_process:
        if ip:
            try:
                record = ip_db.get_all(ip)
                if record:
                    location_entry = {
                        "ip": ip,
                        "country_code": record.country_short,
                        "country_name": record.country_long,
                        "region_name": record.region,
                        "city_name": record.city,
                        "last_updated": datetime.utcnow()
                    }
                    location_data_batch.append(location_entry)
            except Exception as e:
                logging.warning(f"Could not process IP '{ip}': {e}")
        
        # Check if the batch is full, then insert
        batch_count += 1
        if batch_count >= BATCH_SIZE:
            try:
                target_collection.insert_many(location_data_batch)
                processed_count += len(location_data_batch)
                logging.info(f"Successfully inserted a batch of {len(location_data_batch)} records. Total processed: {processed_count}")
            except pymongo.errors.BulkWriteError as bwe:
                logging.error(f"Bulk write error occurred: {bwe.details['writeErrors']}")
            location_data_batch = [] # Clear the batch for the next round
            batch_count = 0

    # Insert any remaining records in the last batch
    if location_data_batch:
        logging.info(f"Inserting final batch of {len(location_data_batch)} records.")
        try:
            target_collection.insert_many(location_data_batch)
            processed_count += len(location_data_batch)
            logging.info(f"Successfully inserted final batch. Total processed: {processed_count}")
        except pymongo.errors.BulkWriteError as bwe:
            logging.error(f"Bulk write error occurred: {bwe.details['writeErrors']}")

    # Create an index on the 'ip' field for faster lookups (do this only once)
    logging.info("Creating index on 'ip' field if it doesn't exist...")
    try:
        target_collection.create_index("ip", unique=True)
        logging.info("Index created successfully.")
    except pymongo.errors.OperationFailure as e:
        logging.warning(f"Index creation failed, possibly because it already exists: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during index creation: {e}")

    client.close()
    logging.info("Processing complete. MongoDB connection closed.")

# --- Script entry point ---
if __name__ == "__main__":
    process_ip_locations()
