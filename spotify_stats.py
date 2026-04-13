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

def get_artist_ids(token, artist_names):
    
    try:
         
        headers = {"Authorization": f"Bearer {token}"}
        artist_ids = []

        for name in artist_names:
            url = f"https://api.spotify.com/v1/search?q={name}&type=artist&limit=1"

            response = requests.get(url, headers=headers)
        
            response.raise_for_status()

            artist = response.json()["artists"]["items"][0]
            artist_ids.append(artist["id"])

            print(f"{artist['name']} → {artist['id']}")

        return artist_ids
        
    except requests.exceptions.RequestException as e:
        raise e

def get_artist_album(token, artist_ids):
    
    headers = {"Authorization": f"Bearer {token}"}
    all_albums = []
    
    for artist_id in artist_ids: 
        url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
        
        response = requests.get(url, headers=headers)
        
        response.raise_for_status()
        
        albums = response.json()["items"]
        all_albums.extend(albums)
        
        print(json.dumps(albums, indent=4))
    
    return all_albums
    

if __name__ == "__main__":
    token = get_access_token()
    artist_ids = get_artist_ids(token, artist_names)
    print(get_artist_album(token, artist_ids))