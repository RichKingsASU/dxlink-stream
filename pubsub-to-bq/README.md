# pubsub-to-bq

**Google Cloud Function:** Consumes DXLink streamer events from Pub/Sub and writes them to BigQuery.

## Deployment

Deploy with:

```bash
gcloud functions deploy pubsub_to_bq \
  --runtime python310 \
  --trigger-topic=my-streaming-ticks \
  --entry-point=pubsub_to_bq \
  --region=us-central1 \
  --memory=256MB
```

## Schema

- `received_at` (TIMESTAMP)
- `event_type` (STRING)
- `symbol` (STRING)
- `raw_event` (STRING/JSON)

## Usage

Make sure the BigQuery dataset and table exist before deploying.
