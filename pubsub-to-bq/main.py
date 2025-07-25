#!/usr/bin/env python3
# pubsub_to_bq.py
#
# Google Cloud Function: Pub/Sub → BigQuery Ingestion
#
# This function is triggered by messages on a Pub/Sub topic. It decodes the
# base64-encoded payload, parses the JSON content, and streams each record
# as a row into a BigQuery table.
#
# Environment Variables:
#   BQ_DATASET — BigQuery dataset name (default: "market_data")
#   BQ_TABLE   — BigQuery table name   (default: "events")
#
# Usage:
#   1. Deploy to Cloud Functions with Pub/Sub trigger.
#   2. Set `BQ_DATASET` and `BQ_TABLE` via function’s environment settings.
#   3. Publish JSON messages to the configured topic; each message must be a
#      valid JSON object, e.g.:
#        {"event_type":"trade","symbol":"SPY","raw_event":{"price":555.25,"size":100}}
#
# Author: Your Name
# Created: 2025-07-24
# Version: 1.0.0
# License: Apache-2.0
#
import os
import base64
import json
import logging
from datetime import datetime
from google.cloud import bigquery

# Configure the logger for structured output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read dataset and table IDs from environment variables for flexibility
BQ_DATASET = os.getenv("BQ_DATASET", "market_data")
BQ_TABLE   = os.getenv("BQ_TABLE",   "events")

def pubsub_to_bq(event, context):
    """
    Entry point for the Cloud Function.
    Trigger: Pub/Sub message on the configured topic.
    Action: Decodes, parses, and writes the message payload to BigQuery.
    """
    client = bigquery.Client()

    # Decode the Pub/Sub payload (base64-encoded)
    raw_payload = event.get("data", "")
    try:
        data_str = base64.b64decode(raw_payload).decode("utf-8")
    except Exception as exc:
        logger.error("Failed to base64-decode Pub/Sub payload: %s", exc)
        return

    logger.info("Raw Pub/Sub message: %s", data_str)

    # Parse JSON safely
    try:
        record = json.loads(data_str)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in Pub/Sub message: %s; payload: %s", exc, data_str)
        return

    logger.info("Parsed record: %s", record)

    # Build the row with a UTC timestamp fallback
    row = {
        "received_at": record.get("received_at") or datetime.utcnow().isoformat() + "Z",
        "event_type":  record.get("event_type"),
        "symbol":      record.get("symbol"),
        "raw_event":   json.dumps(record.get("raw_event", {})),
    }
    logger.info("Prepared BigQuery row: %s", row)

    # Reference the target table
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)

    # Insert the row into BigQuery with robust error handling
    try:
        errors = client.insert_rows_json(table_ref, [row])
    except Exception as exc:
        logger.error("Exception during BigQuery insert: %s", exc, exc_info=True)
        return

    if errors:
        logger.error("BigQuery insert errors: %s", errors)
    else:
        logger.info("Successfully inserted row into %s.%s", BQ_DATASET, BQ_TABLE)