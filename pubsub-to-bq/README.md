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
  --memory=256MB \
  --set-env-vars BIGQUERY_DATASET=my_dataset,BIGQUERY_TABLE=my_table
```

### Environment Variables

- `BIGQUERY_DATASET`: The BigQuery dataset ID (default: `market_data`)
- `BIGQUERY_TABLE`: The BigQuery table ID (default: `events`)

## Schema

- `received_at` (TIMESTAMP)
- `event_type` (STRING)
- `symbol` (STRING)
- `raw_event` (STRING/JSON)

## Usage

Make sure the BigQuery dataset and table exist before deploying.
