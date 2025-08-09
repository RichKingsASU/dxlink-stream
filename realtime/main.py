import os
import json
import threading
from collections import deque
from flask import Flask, jsonify
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# ─── CONFIG ──────────────────────────────────────────────
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT") or "tt-production-468500"
TOPIC      = os.getenv("PUBSUB_TOPIC", "my-streaming-ticks")
SUB_ID     = os.getenv("PUBSUB_SUBSCRIPTION", "")
SYMBOL     = os.getenv("SYMBOL", "SPY")
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
MULTIPLIER = float(os.getenv("MULTIPLIER", "3.0"))

# ─── STATE ───────────────────────────────────────────────
prices = deque(maxlen=ATR_PERIOD+1)
trs    = deque(maxlen=ATR_PERIOD)
position = None

app = Flask(__name__)


def compute_atr():
    return sum(trs) / ATR_PERIOD if len(trs) == ATR_PERIOD else None


def handle_message(message: pubsub_v1.subscriber.message.Message):
    global position

    # Decode UTF-8 JSON payload
    try:
        payload = message.data.decode("utf-8")
        data = json.loads(payload)
    except Exception as e:
        app.logger.error("Bad JSON payload %r: %s", message.data, e)
        message.ack()
        return

    # Process only matching symbol
    try:
        if data.get("symbol") != SYMBOL:
            message.ack()
            return

        price = float(data["raw_event"]["price"])
        prices.append(price)

        if len(prices) > 1:
            prev = prices[-2]
            # True range (simplified)
            tr = max(price - prev, abs(price - prev), abs(prev - price))
            trs.append(tr)

        atr = compute_atr()
        if atr is not None:
            upper = price + MULTIPLIER * atr
            lower = price - MULTIPLIER * atr

            if price > upper and position != "LONG":
                position = "LONG"
                app.logger.info(f"[SIGNAL] BUY {SYMBOL} at {price:.2f} ATR={atr:.2f}")
            elif price < lower and position != "SHORT":
                position = "SHORT"
                app.logger.info(f"[SIGNAL] SELL {SYMBOL} at {price:.2f} ATR={atr:.2f}")

    except Exception as e:
        app.logger.error("Error processing %r: %s", data, e)
    finally:
        message.ack()


def start_subscriber():
    client = pubsub_v1.SubscriberClient()
    subscription_name = SUB_ID or f"{TOPIC}-rt"
    sub_path = client.subscription_path(PROJECT_ID, subscription_name)

    # Create subscription if it doesn't exist
    if not SUB_ID:
        try:
            client.create_subscription(name=sub_path,
                                       topic=f"projects/{PROJECT_ID}/topics/{TOPIC}")
        except AlreadyExists:
            app.logger.info("Subscription %s already exists", subscription_name)

    client.subscribe(sub_path, callback=handle_message).result()


@app.route("/")
def health():
    return jsonify(status="ok"), 200


if __name__ == "__main__":
    # Start Pub/Sub listener thread
    threading.Thread(target=start_subscriber, daemon=True).start()

    # Run Flask app (in production, Cloud Run will use Gunicorn automatically)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
