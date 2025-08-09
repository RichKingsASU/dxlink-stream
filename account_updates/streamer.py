import asyncio, json, os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.cloud import pubsub_v1
from tastytrade import Session, Account, AlertStreamer
from tastytrade.order import PlacedOrder

PHX = ZoneInfo("America/Phoenix")

def now_fields():
    now = datetime.now(timezone.utc)
    return {
        "received_at": now.isoformat(timespec="microseconds"),
        "received_at_epoch_ms": int(now.timestamp() * 1000),
        "received_at_phx": now.astimezone(PHX).isoformat(timespec="microseconds"),
    }

async def run():
    user = os.environ["TT_USER"]; pw = os.environ["TT_PASS"]
    session = Session(user, pw)

    accounts = Account.get(session)  # list of accounts for this user
    publisher = pubsub_v1.PublisherClient()
    project = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
    topic = publisher.topic_path(project, os.getenv("ACCOUNT_EVENTS_TOPIC","account-events"))

    async with AlertStreamer(session) as streamer:
        # Orders/balances/positions + optional public watchlists and quote alerts
        await streamer.subscribe_accounts(accounts)
        # await streamer.subscribe_public_watchlists()
        # await streamer.subscribe_quote_alerts()

        async for order in streamer.listen(PlacedOrder):
            msg = {**order.model_dump(), "event_type": "ACCOUNT_ORDER", **now_fields()}
            publisher.publish(topic, json.dumps(msg).encode("utf-8"))

if __name__ == "__main__":
    if "TT_USER" not in os.environ or "TT_PASS" not in os.environ:
        print("Please set the TT_USER and TT_PASS environment variables.")
        exit(1)
    asyncio.run(run())