FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY stream_dxlink_pubsub.py .

CMD ["python", "stream_dxlink_pubsub.py"]
