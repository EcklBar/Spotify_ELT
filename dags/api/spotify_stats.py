import requests
import base64
import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

REDIRECT_URI = "http://127.0.0.1:8000/callback"
SCOPE = "user-read-recently-played"
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"

def get_auth_url():
    return (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={client_id}"
        f"&scope={SCOPE}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        self.server.auth_code = query.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authorization successful! You can close this tab.")
        
    def log_message(self, format, *args):
        pass
    
    
def get_authorization_code():
    webbrowser.open(get_auth_url())
    server = HTTPServer(("127.0.0.1", 8000), CallbackHandler)
    server.handle_request()
    return server.auth_code


def exchange_code_for_tokens(code):
    # Encode credentials
    auth_string = f"{client_id}:{client_secret}"
    auth_base64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    # Request access token
    response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={"grant_type": "authorization_code",
              "code": code,
              "redirect_uri": REDIRECT_URI,
              },
    )

    return response.json()

if __name__ == "__main__":
    code = get_authorization_code()
    tokens = exchange_code_for_tokens(code)
    print(f"Access Token: {tokens['access_token'][:20]}...")
    print(f"Refresh Token: {tokens['refresh_token']}")
    print("\nSave your refresh token in your .env file as REFRESH_TOKEN")