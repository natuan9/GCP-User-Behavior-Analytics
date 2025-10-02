from pymongo import MongoClient
from google.cloud import storage
import logging
import json
import os
from datetime import datetime

# --- Configuration Section ---
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB_NAME = "glamira_db"
MONGO_COLLECTION_NAME = "summary"
GCS_BUCKET_NAME = "raw-glamira-data"
GCS_EXPORT_PATH_PREFIX = "exports/user_behaviors/user_behaviors"
LOCAL_FILE_PATH = "../data/user_behaviors.jsonl"
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

    cursor = collection.find({}, batch_size=batch_size)
    for doc in cursor:
        yield doc

def clean_empty_option(cart_products):
    cleaned = []
    for cp in cart_products:
        if isinstance(cp.get("option"), str):
            if cp["option"] == "":
                cp.pop("option")
        cleaned.append(cp)
    return cleaned

def write_to_jsonl(docs, file_path):
    """Writes documents to a JSONL file."""
    with open(file_path, "w", encoding='utf-8') as f:
        for doc in docs:
            doc['_id'] = str(doc['_id'])

            if "cart_products" in doc:
                # Clean empty option 
                doc["cart_products"] = clean_empty_option(doc["cart_products"])
            
            if "option" in doc and isinstance(doc["option"], dict):
                if "category id" in doc["option"]:
                    doc["option"]["category_id"] = doc["option"].pop("category id")
            f.write(json.dumps(doc) + "\n")

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
