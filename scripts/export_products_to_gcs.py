from pymongo import MongoClient
from google.cloud import storage
import logging
import json
import os
from datetime import datetime

# --- Configuration Section ---
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB_NAME = "glamira_db"
MONGO_COLLECTION_NAME = "products"
GCS_BUCKET_NAME = "raw-glamira-data"
GCS_EXPORT_PATH_PREFIX = "exports/products/products"
LOCAL_FILE_PATH = "../data/products.jsonl"
BATCH_SIZE = 1000

# --- Helper Functions ---
def get_mongo_connection():
    """Establishes a connection to MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db

def extract_data(collection_name, batch_size):
    """Extracts documents from a MongoDB collection in batches."""
    db = get_mongo_connection()
    collection = db[collection_name]

    cursor = collection.find({}).batch_size(batch_size)
    for doc in cursor:
        yield doc

def write_to_jsonl(docs, file_path):
    """Writes documents to a JSONL file."""
    with open(file_path, "w", encoding='utf-8') as f:
        for doc in docs:
            # Convert ObjectId to string for JSON serialization
            doc['_id'] = str(doc['_id'])
            
            # Handle special float values if they exist
            if 'collection' in doc and isinstance(doc['collection'], float):
                if doc['collection'] == float('inf'):
                    doc['collection'] = "infinity" 
                elif doc['collection'] == float('nan'):
                    doc['collection'] = "nan"
            
            f.write(json.dumps(doc) + '\n')

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Uploads a file to a specified Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

def export_to_gcs():
    """Main function to orchestrate the export process."""
    logging.info("Starting export from MongoDB")

    # Generate a timestamped destination file name
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    gcs_destination_blob = f"{GCS_EXPORT_PATH_PREFIX}_{timestamp}.jsonl"
        
    docs = extract_data(MONGO_COLLECTION_NAME, batch_size=BATCH_SIZE)
    write_to_jsonl(docs, LOCAL_FILE_PATH)
    
    upload_to_gcs(GCS_BUCKET_NAME, LOCAL_FILE_PATH, gcs_destination_blob)
    
    logging.info("Export successfully")

if __name__ == "__main__":
    export_to_gcs()
