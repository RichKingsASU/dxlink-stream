from google.cloud import bigquery
import base64
import json
import logging

logging.basicConfig(level=logging.INFO)

def pubsub_to_bq(event, context):
    logging.info(f"Received event: {event}")
    client = bigquery.Client()
    dataset_id = "dxlink"
    table_id = "ticks_raw"

    # The Pub/Sub message is passed as the `event` parameter.
    # The `data` is in `event['data']`.
    # The `attributes` are in `event['attributes']`.
    # The `message_id` is in `context.event_id`.
    # The `publish_time` is in `context.timestamp`.
    # The `subscription_name` can be extracted from the resource name.
    # The resource name is in the format `projects/project-id/subscriptions/subscription-id`.
    subscription_name = context.resource['name'].split('/')[-1]

    row = {
        "subscription_name": subscription_name,
        "message_id": context.event_id,
        "publish_time": context.timestamp,
        "data": base64.b64decode(event['data']).decode('utf-8'),
        "attributes": json.dumps(event.get('attributes', {})),
    }

    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        logging.error(f"BigQuery insert errors: {errors}")
    else:
        logging.info(f"Inserted row: {row}")
