import requests
import os
import base64
import json

from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_access_token():
    
    token_url = "https://accounts.spotify.com/api/token"
    
    try: 
        # Encode credentials
        auth_string = f"{client_id}:{client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        # Request access token
        response = requests.post(
            token_url,
            headers={
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={"grant_type": "client_credentials"}
            )

        response.raise_for_status()

        token = response.json()["access_token"]
        
        return token
    
    except requests.exceptions.RequestException as e:
     		raise e

def get_artist(token):
    
    try: 
    
        artist_id = "3QSQFmccmX81fWCUSPTS7y"  # Dean Lewis
        headers = {"Authorization": f"Bearer {token}"}
        base_url = f"https://api.spotify.com/v1/artists/{artist_id}"

        response = requests.get(base_url, headers=headers)

        response.raise_for_status()

        response_json = response.json()

        print(json.dumps(response_json, indent=4))
        
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    token = get_access_token()
    print(get_artist(token))