import os
import json
from flask import Flask, request, abort
from cloudevents.http import from_http
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()
TABLE = os.getenv("BQ_TABLE", "ttbot-466703.market_data.utbot_signals")

@app.route("/", methods=["POST"])
def receive_event():
    # parse CloudEvent from the HTTP request
    try:
        event = from_http(request.headers, request.get_data())
    except Exception:
        abort(400, "invalid CloudEvent")

    # extract payload and UTC timestamp
    data = event.data
    ts = event.get("ce-time", None)

    # insert into BigQuery
    row = {"payload": data, "event_utc": ts}
    errors = bq.insert_rows_json(TABLE, [row])
    if errors:
        app.logger.error("BQ insert errors: %s", errors)
        abort(500, "BigQuery insert failed")

    return ("", 204)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
