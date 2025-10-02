import logging
from google.cloud import bigquery
import functions_framework
from datetime import datetime

# BigQuery settings
PROJECT_ID = "my-glamira-project"
DATASET_ID = "glamira_dataset"
TABLE_ID = "raw_ip_locations"
SCHEMA_PATH = "ip_locations_schema.json"
EVENT_TABLE_ID = "event_metadata"

@functions_framework.cloud_event
def bigquery_load_ip_locations(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]

    bucket_name = data["bucket"]
    file_path = data["name"]

    # Only process matching files
    if not (file_path.startswith('exports/ip_locations/ip_locations_') and file_path.endswith('.jsonl')):
        print(f"Ignoring file {file_path}. It does not match the required naming convention.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    # ---------- Check if event_id already processed ----------
    event_table_ref = client.dataset(DATASET_ID).table(EVENT_TABLE_ID)
    query = f"""
        SELECT COUNT(*) as cnt 
        FROM `{PROJECT_ID}.{DATASET_ID}.{EVENT_TABLE_ID}`
        WHERE event_id = @event_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    )
    results = client.query(query, job_config=job_config).result()
    if next(results).cnt > 0:
        print(f"Event {event_id} already processed. Skipping load.")
        return

    # ---------- Load data to BigQuery ----------
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    with open(SCHEMA_PATH, "r") as schema_file:
        schema = client.schema_from_json(schema_file)

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )

    uri = f"gs://{bucket_name}/{file_path}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=load_config)
    print(f"Starting load job for file: {uri}")
    load_job.result()
    print(f"Successfully loaded file {file_path} to table {table_ref.path}")

    # ---------- Save event_id as processed ----------
    rows_to_insert = [
        {"event_id": event_id, "processed_at": datetime.utcnow().isoformat()}
    ]
    client.insert_rows_json(event_table_ref, rows_to_insert)
    print(f"Saved event_id {event_id} to event_metadata")
