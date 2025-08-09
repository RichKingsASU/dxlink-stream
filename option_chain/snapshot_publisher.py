import asyncio, json, os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.cloud import pubsub_v1
from tastytrade import Session, DXLinkStreamer
from tastytrade.dxfeed import Greeks, Quote
from tastytrade.instruments import get_option_chain
from tastytrade.utils import get_tasty_monthly
from decimal import Decimal

PHX = ZoneInfo("America/Phoenix")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def now_fields():
    now = datetime.now(timezone.utc)
    return {
        "received_at": now.isoformat(timespec="microseconds"),
        "received_at_epoch_ms": int(now.timestamp() * 1000),
        "received_at_phx": now.astimezone(PHX).isoformat(timespec="microseconds"),
    }

def _extract_event_time_ms(model_dict: dict):
    # Try millis first
    for k in ("event_time_millis", "eventTime", "event_time_ms"):
        v = model_dict.get(k)
        if v is not None:
            try: return int(v)
            except: pass
    # Try ISO strings
    for k in ("event_time", "eventTimeISO", "time", "timestamp"):
        v = model_dict.get(k)
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v.replace("Z","+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                pass
    return None

def attach_latency(msg: dict) -> dict:
    evt_ms = _extract_event_time_ms(msg)
    if evt_ms is not None:
        msg["event_time_epoch_ms"] = evt_ms
        msg["ingest_latency_ms"] = msg["received_at_epoch_ms"] - evt_ms
    else:
        msg["event_time_epoch_ms"] = None
        msg["ingest_latency_ms"] = None
    return msg

async def run(symbol: str):
    # Auth to tastytrade API (session handles DXLink token internally)
    user = os.environ["TT_USER"]; pw = os.environ["TT_PASS"]
    session = Session(user, pw)

    # Pick a reasonable slice of the chain (tasty monthly expiry)
    chain = get_option_chain(session, symbol)          # full nested chain
    expiry = get_tasty_monthly()                       # ≈45 DTE helper
    options = chain.get(expiry, [])                    # list of instruments
    option_syms = [o.streamer_symbol for o in options] # symbols for DXLink

    publisher = pubsub_v1.PublisherClient()
    project = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
    topic_greeks = publisher.topic_path(project, os.getenv("OPTIONS_GREEKS_TOPIC","options-greeks"))
    topic_quotes = publisher.topic_path(project, os.getenv("OPTIONS_QUOTES_TOPIC","options-quotes"))

    async with DXLinkStreamer(session) as streamer:
        # Subscribe to option greeks and (optionally) quotes
        await streamer.subscribe(Greeks, option_syms)
        await streamer.subscribe(Quote, [symbol] + option_syms)  # drop if you only want greeks

        async def pump_greeks():
            async for g in streamer.listen(Greeks):
                payload = attach_latency({**g.model_dump(), "underlying": symbol, "event_type": "GREEKS", **now_fields()})
                publisher.publish(topic_greeks, json.dumps(payload, cls=DecimalEncoder).encode("utf-8"))

        async def pump_quotes():
            async for q in streamer.listen(Quote):
                payload = attach_latency({**q.model_dump(), "event_type": "QUOTE", **now_fields()})
                publisher.publish(topic_quotes, json.dumps(payload, cls=DecimalEncoder).encode("utf-8"))

        await asyncio.gather(pump_greeks(), pump_quotes())

if __name__ == "__main__":
    if "TT_USER" not in os.environ or "TT_PASS" not in os.environ:
        print("Please set the TT_USER and TT_PASS environment variables.")
        exit(1)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", type=str, help="Underlying symbol to stream")
    args = parser.parse_args()
    asyncio.run(run(args.symbol))
