#!/usr/bin/env python3
import sys
import json
import asyncio
import logging
import requests
import websockets
from datetime import datetime, timezone
from google.cloud import secretmanager, pubsub_v1
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# —————————————————————————————————————————————————————
# CONFIGURATION
GCP_PROJECT_ID      = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or "tt-production-468500"
PUBSUB_TOPIC        = os.getenv("PUBSUB_TOPIC", "my-streaming-ticks")
STREAM_SYMBOL       = os.getenv("STREAM_SYMBOL", "SPY")
SESSION_SECRET_NAME = os.getenv("SESSION_SECRET_NAME", "tastytrade-session-token")
KEEPALIVE_INTERVAL  = int(os.getenv("KEEPALIVE_INTERVAL", "30"))
LOGLEVEL            = os.getenv("LOGLEVEL", "INFO")

QUOTE_TOKEN_URL = "https://api.tastyworks.com/api-quote-tokens"

# Logging configuration
logging.basicConfig(
    level=getattr(logging, LOGLEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)

logging.info("🚀 Starting dxlink-stream for symbol '%s' to Pub/Sub topic '%s'", STREAM_SYMBOL, PUBSUB_TOPIC)

# —————————————————————————————————————————————————————
def access_secret(secret_id, version_id="latest"):
    """Fetch secret from Google Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": secret_path})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error("❌ Failed to access secret '%s': %s", secret_id, e)
        sys.exit(1)

def get_session_token():
    """Get session token from Google Secret Manager."""
    token = access_secret(SESSION_SECRET_NAME).replace("\u2011", "-").strip()
    if not token:
        logging.error("❌ Session token secret '%s' is empty!", SESSION_SECRET_NAME)
        sys.exit(1)
    return token

def publish_event_pubsub(event: dict):
    """Publish a market event to Pub/Sub."""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)
        data = json.dumps(event).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result(timeout=5)
        logging.info("✅ Published event to Pub/Sub: %s", event)
    except Exception as e:
        logging.error("❌ Pub/Sub publish failed: %s", e)

async def stream_symbol(symbol: str):
    """Main DXLink streaming logic."""
    while True:  # Infinite loop to restart if DXLink drops connection
        session_token = get_session_token()
        logging.info("🔑 Using session token from Secret Manager '%s'.", SESSION_SECRET_NAME)
        # 1) Fetch DXLink quote token
        try:
            resp = requests.get(
                QUOTE_TOKEN_URL,
                headers={"Authorization": session_token},
                timeout=10
            )
            resp.raise_for_status()
        except Exception as e:
            logging.error("❌ Failed to fetch DXLink quote token: %s", e)
            await asyncio.sleep(15)
            continue

        info = resp.json().get("data", {})
        quote_token = info.get("token")
        ws_url      = info.get("dxlink-url")
        if not (quote_token and ws_url):
            logging.error("❌ Invalid quote token response: %s", resp.json())
            await asyncio.sleep(15)
            continue

        logging.info("🌐 Connecting to DXLink WebSocket at %s", ws_url)

        try:
            async with websockets.connect(ws_url) as ws:
                # SETUP
                await ws.send(json.dumps({
                    "type": "SETUP",
                    "channel": 0,
                    "version": "0.1-PY/1.0",
                    "keepaliveTimeout": 60,
                    "acceptKeepaliveTimeout": 60
                }))
                await ws.recv()

                # AUTHORIZE
                await ws.send(json.dumps({
                    "type": "AUTH",
                    "channel": 0,
                    "token": quote_token
                }))
                await ws.recv()

                # OPEN CHANNEL
                channel_id = 1
                await ws.send(json.dumps({
                    "type": "CHANNEL_REQUEST",
                    "channel": channel_id,
                    "service": "FEED",
                    "parameters": {"contract": "AUTO"}
                }))
                await ws.recv()

                # FEED_SETUP
                await ws.send(json.dumps({
                    "type": "FEED_SETUP",
                    "channel": channel_id,
                    "acceptAggregationPeriod": 0.1,
                    "acceptDataFormat": "COMPACT",
                    "acceptEventFields": {
                        "Trade":   ["eventType","eventSymbol","price","dayVolume","size"],
                        "Quote":   ["eventType","eventSymbol","bidPrice","askPrice","bidSize","askSize"],
                        "Summary": ["eventType","eventSymbol","openInterest","dayOpenPrice","dayHighPrice","dayLowPrice","prevDayClosePrice"]
                    }
                }))
                await ws.recv()

                # SUBSCRIBE
                await ws.send(json.dumps({
                    "type": "FEED_SUBSCRIPTION",
                    "channel": channel_id,
                    "reset": True,
                    "add": [
                        {"type": "Trade",   "symbol": symbol},
                        {"type": "Quote",   "symbol": symbol},
                        {"type": "Summary", "symbol": symbol}
                    ]
                }))

                # KEEPALIVE task
                async def keepalive():
                    while True:
                        await asyncio.sleep(KEEPALIVE_INTERVAL)
                        await ws.send(json.dumps({"type": "KEEPALIVE", "channel": 0}))
                asyncio.create_task(keepalive())

                logging.info("▶️ Streaming events for symbol '%s'...", symbol)

                # RECEIVE & PUBLISH TO PUBSUB
                async for message in ws:
                    try:
                        evt = json.loads(message)
                    except json.JSONDecodeError:
                        logging.warning("⚠️ Non-JSON message: %s", message)
                        continue

                    # Tag with ingestion timestamp (timezone-aware)
                    evt["received_at"] = datetime.now(timezone.utc).isoformat()
                    logging.info("DXLink event: %s", evt)
                    publish_event_pubsub(evt)
        except Exception as e:
            logging.error("❌ Exception during DXLink streaming: %s", e)
            await asyncio.sleep(15)
            continue

# HTTP server for Cloud Run liveness/health check
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

def run_http_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
    server.serve_forever()

def main():
    # Start HTTP health check server in a background thread
    threading.Thread(target=run_http_server, daemon=True).start()
    try:
        asyncio.run(stream_symbol(STREAM_SYMBOL))
    except KeyboardInterrupt:
        logging.info("🛑 Interrupted by user, shutting down")
    except Exception as e:
        logging.error("❌ Uncaught exception: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()