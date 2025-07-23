# dxlink-stream

A minimal Python streamer for Tastytrade DXLink quotes,
publishing all events to Google Pub/Sub.

## Usage

1. **Store your Tastytrade session token in Google Secret Manager:**
   - Secret name: `tastytrade-session-token`
2. **Set up your Pub/Sub topic** in Google Cloud (default: `my-streaming-ticks`)
3. **Build and run the container:**

   ```bash
   docker build -t dxlink-stream .
   docker run --rm \
     -e PUBSUB_TOPIC=my-streaming-ticks \
     -e STREAM_SYMBOL=SPY \
     dxlink-stream
