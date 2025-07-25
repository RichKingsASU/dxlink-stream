import requests
import sys
from google.cloud import secretmanager

GCP_PROJECT_ID = "ttbot-466703"

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

    resp = requests.post(TW_URL, json=payload, headers=headers)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print(f"Tastytrade auth failed ({resp.status_code}): {resp.text}")
        return f"Error: {resp.text}", 400

    session_token = resp.json()["data"].get("session-token")
    if not session_token:
        return "No session-token in response.", 500

    # Update the session token in Secret Manager
    set_secret("tastytrade-session-token", session_token)
    print("✅ Refreshed session token in Secret Manager.")
    return "OK", 200
