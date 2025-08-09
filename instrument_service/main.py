import os, json
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests
from google.cloud import pubsub_v1

app = Flask(__name__)
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
BASE    = os.getenv("TT_BASE_URL", "https://api.tastytrade.com")
TOKEN   = os.getenv("TT_AUTH_TOKEN")
HEADERS = {"Accept": "application/json", "Content-Type": "application/json",
           "Authorization": TOKEN}
pub = pubsub_v1.PublisherClient()
topic_instr = pub.topic_path(PROJECT, "instruments")

def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")

def publish(payload):
    payload.setdefault("ts_utc", now_utc())
    pub.publish(topic_instr, json.dumps(payload).encode("utf-8"))

@app.get("/instruments/equities")
def equities():
    symbol = request.args.get("symbol", "AAPL")
    url = f"{BASE}/instruments/equities?symbol={symbol}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    data = {"action": "equities", "symbol": symbol, "status": r.status_code,
            "response": (r.json() if r.content else {})}
    publish(data)
    return jsonify(data["response"]), r.status_code

@app.get("/option-chains")
def option_chains():
    symbol = request.args.get("symbol", "AAPL")
    url = f"{BASE}/option-chains/{symbol}"
    r = requests.get(url, headers=HEADERS, timeout=45)
    data = {"action": "option-chains", "symbol": symbol, "status": r.status_code,
            "response": (r.json() if r.content else {})}
    publish(data)
    return jsonify(data["response"]), r.status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
