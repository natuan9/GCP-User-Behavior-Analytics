import logging
from google.cloud import bigquery
import functions_framework

PROJECT_ID = "my-glamira-project"
DATASET_ID = "glamira_dataset"
TABLE_ID = "raw_user_behaviors"
EVENT_TABLE_ID = "event_metadata"  # Bảng lưu event_id
SCHEMA_PATH = "user_behaviors_schema.json"

@functions_framework.cloud_event
def bigquery_load_user_behaviors(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]  # lấy event_id duy nhất

    bucket_name = data["bucket"]
    file_path = data["name"]

    if not (file_path.startswith('exports/user_behaviors/user_behaviors_') and file_path.endswith('.jsonl')):
        print(f"Ignoring file {file_path}. It does not match the required naming convention.")
        return

    print(f"New file uploaded: gs://{bucket_name}/{file_path} | event_id={event_id}")

    client = bigquery.Client(project=PROJECT_ID)

    # --- Kiểm tra event_id đã xử lý chưa ---
    check_query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{PROJECT_ID}.{DATASET_ID}.{EVENT_TABLE_ID}`
        WHERE event_id = @event_id
    """
    job = client.query(check_query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    ))
    result = list(job.result())[0]
    if result.cnt > 0:
        print(f"Event {event_id} already processed. Skipping.")
        return

    # --- Tiến hành load data ---
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    with open(SCHEMA_PATH, "r") as schema_file:
        schema = client.schema_from_json(schema_file)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )

    uri = f"gs://{bucket_name}/{file_path}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print(f"Starting load job for file: {uri}")
    load_job.result()
    print(f"Successfully loaded file {file_path} to table {table_ref.path}")

    # --- Ghi lại event_id đã xử lý ---
    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{EVENT_TABLE_ID}` (event_id)
        VALUES (@event_id)
    """
    client.query(insert_query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    )).result()
