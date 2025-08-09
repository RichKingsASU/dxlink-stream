import os
import requests
import sys
from google.cloud import secretmanager
import logging

logging.basicConfig(level=logging.INFO)

GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or "tt-production-468500"

def access_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

def set_secret(secret_id, value):
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}"
    payload = value.encode("UTF-8")
    client.add_secret_version(parent=parent, payload={"data": payload})

def refresh_token(request):
    logging.info("Starting token refresh.")
    TW_LOGIN = access_secret("tastytrade-login")
    TW_PASSWORD = access_secret("tastytrade-password")
    TW_URL = "https://api.tastyworks.com/sessions"
    payload = {
        "login": TW_LOGIN,
        "password": TW_PASSWORD,
        "remember-me": True
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    logging.info(f"Requesting new session token from {TW_URL}")
    resp = requests.post(TW_URL, json=payload, headers=headers)
    logging.info(f"Response status code: {resp.status_code}")
    logging.info(f"Response content: {resp.content}")
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logging.error(f"Tastytrade auth failed ({resp.status_code}): {resp.text}")
        return f"Error: {resp.text}", 400

    session_token = resp.json()["data"].get("session-token")
    if not session_token:
        logging.error("No session-token in response.")
        return "No session-token in response.", 500

    # Update the session token in Secret Manager
    set_secret("tastytrade-session-token", session_token)
    logging.info("✅ Refreshed session token in Secret Manager.")
    return "OK", 200

if __name__ == "__main__":
    refresh_token(None)
