import requests
import os
import base64
import json

from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_access_token():
    
    try: 
        
        token_url = "https://accounts.spotify.com/api/token"
        
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
        # Print the response
        response_json = response.json()
        # print(json.dumps(response_json, indent=4))
        
        token = response_json["access_token"]
        
        return token
    
    except requests.exceptions.RequestException as e:
     		raise e

def get_top_ten_artists_by_genre(token, genre, limit=10):
    
    try:
        
        url = f"https://api.spotify.com/v1/search?q=genre:{genre}&type=artist&limit={limit}"
        headers = {"Authorization": f"Bearer {token}"}
        
        # A GET request
        response = requests.get(url, headers=headers)
        
        response.raise_for_status()

        response_json = response.json()
        
        artists = response_json["artists"]["items"]
        
        print(artists[0].keys())
        
        return artists
    
    except requests.exceptions.RequestException as e:
        raise e 
    

if __name__ == "__main__":
    token = get_access_token()
    get_top_ten_artists_by_genre(token, "pop", 10)