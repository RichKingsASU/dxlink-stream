import os, json
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests
from google.cloud import pubsub_v1

app = Flask(__name__)
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
ACCOUNT = os.getenv("TT_ACCOUNT_NUMBER")
BASE    = os.getenv("TT_BASE_URL", "https://api.tastytrade.com")
TOKEN   = os.getenv("TT_AUTH_TOKEN")  # or read from Secret Manager at startup
HEADERS = {"Accept": "application/json", "Content-Type": "application/json",
           "Authorization": TOKEN}

pub = pubsub_v1.PublisherClient()
topic_orders       = pub.topic_path(PROJECT, "orders")
topic_order_events = pub.topic_path(PROJECT, "order-events")

def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")

def publish(topic_path, payload):
    payload.setdefault("ts_utc", now_utc())
    pub.publish(topic_path, json.dumps(payload).encode("utf-8"))

@app.post("/orders")
def create_order():
    body = request.get_json(silent=True) or {}
    url = f"{BASE}/accounts/{ACCOUNT}/orders"
    r = requests.post(url, headers=HEADERS, json=body, timeout=30)
    data = {"action": "create", "request": body, "status": r.status_code,
            "response": (r.json() if r.content else {})}
    publish(topic_orders, data)
    # echo back TT result
    return jsonify(data["response"]), r.status_code

@app.post("/orders/cancel")
def cancel_order():
    # minimal example: TT cancel patterns vary by order type
    oid = request.args.get("order_id")
    url = f"{BASE}/accounts/{ACCOUNT}/orders/{oid}/cancel"
    r = requests.post(url, headers=HEADERS, timeout=30)
    data = {"action": "cancel", "order_id": oid, "status": r.status_code,
            "response": (r.json() if r.content else {})}
    publish(topic_order_events, data)
    return jsonify(data["response"]), r.status_code

@app.get("/orders/live")
def live_orders():
    # REST read (you’ll also get fills via account streamer)
    url = f"{BASE}/accounts/{ACCOUNT}/live-orders"
    r = requests.get(url, headers=HEADERS, timeout=30)
    data = {"action": "read-live-orders", "status": r.status_code,
            "response": (r.json() if r.content else {})}
    publish(topic_order_events, data)
    return jsonify(data["response"]), r.status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
