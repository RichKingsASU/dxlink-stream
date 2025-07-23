from google.cloud import bigquery
import base64
import json

def pubsub_to_bq(event, context):
    client = bigquery.Client()
    dataset_id = "market_data"
    table_id = "events"

    # Parse Pub/Sub message
    data = base64.b64decode(event['data']).decode('utf-8')
    record = json.loads(data)

    row = {
        "received_at": record.get("received_at"),
        "event_type": record.get("event_type"),
        "symbol": record.get("symbol"),
        "raw_event": json.dumps(record.get("raw_event")),
    }

    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"BigQuery insert errors: {errors}")
    else:
        print(f"Inserted row: {row}")
