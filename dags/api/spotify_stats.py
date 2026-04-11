from textwrap import indent
import requests
import os 
import base64
import json

from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
refresh_token = os.getenv("SPOTIFY_REFRESH_TOKEN")

def get_access_token():
    
    try: 
        
        auth_url = f"https://api.spotify.com/v1/me/player/recently-played"
        token_url = f"https://accounts.spotify.com/api/token"
        # Encode credentials
        auth_string = f"{client_id}:{client_secret}"
        auth_base64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

        # Request access token
        response = requests.post(token_url,
            headers={
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={"grant_type": "refresh_token",
                "refresh_token": refresh_token,
                },
        )

        token = response.json()["access_token"]

        # Get recently played tracks
        data_response = requests.get(auth_url,headers={"Authorization": f"Bearer {token}"},)

        print(json.dumps(data_response.json(), indent=4))
        
    except requests.exceptions.RequestException as e:
        raise e 
    