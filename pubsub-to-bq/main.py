import os, json
from datetime import datetime, timezone
from flask import Flask, request, abort
from cloudevents.http import from_http
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()
TABLE = os.getenv("BQ_TABLE")  # should be set to "ttbot-466703.market_data.events"

@app.route("/", methods=["POST"])
def receive_event():
    # parse CloudEvent
    try:
        event = from_http(request.headers, request.get_data())
    except Exception:
        abort(400, "invalid CloudEvent")

    # extract CloudEvent attributes + payload
    data = event.data or {}
    evt_type = event["type"]
    evt_time = event["time"]          # CE time header → ISO8601
    now_utc = datetime.now(timezone.utc).isoformat()

    # build a row covering all non‐nullable columns
    row = {
      "received_at": now_utc,
      "event_type": evt_type,
      "symbol": data.get("symbol"),
      "raw_event": data.get("raw_event"),
      "payload": data,
      "event_utc": evt_time
    }

    errors = bq.insert_rows_json(TABLE, [row])
    if errors:
      app.logger.error("BQ insert errors: %s", errors)
      abort(500, "BigQuery insert failed")

    return ("", 204)
