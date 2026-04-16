import requests
import os
import base64
import json

from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
artist_names = json.loads(os.getenv("ARTIST_NAMES"))

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


def get_artist_ids(token, artist_names):
    
    try: 
        
        headers = {"Authorization": f"Bearer {token}"}
        artists = []
        
        for name in artist_names:
            url = f"https://api.spotify.com/v1/search?q={name}&type=artist&limit=1"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            response_json = response.json()
            
            artist = response_json["artists"]["items"][0]
            artists.append({
                "artist_id": artist["id"],
                "artist_name": artist["name"]
            })
        
        return artists
    
    except requests.exceptions.RequestException as e: 
        raise e 


def get_artist_albums(token, artists):
    
    try:
        
        headers = {"Authorization": f"Bearer {token}"}
        all_ablums = []
        
        for artist in artists:
            url = f"https://api.spotify.com/v1/artists/{artist['artist_id']}/albums"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            response_json = response.json()
            
            albums = response_json["items"]
            
            
        
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    token = get_access_token()
    print(get_artist_ids(token, artist_names))