import requests
import base64
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env")

from airflow.decorators import task
from airflow.models import Variable

client_id = Variable.get("SPOTIFY_CLIENT_ID")
client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
artist_names = json.loads(Variable.get("ARTIST_NAMES"))

@task
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

@task
def get_artist_ids(token, artist_names):
    
    try: 
        
        headers = {"Authorization": f"Bearer {token}"}
        artists = []
        
        for name in artist_names:
            url = f"https://api.spotify.com/v1/search?q={name}&type=artist&limit=1"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            response_json = response.json()
            # print(json.dumps(response_json, indent=4))
            
            artist = response_json["artists"]["items"][0]
            artists.append({
                "artist_id": artist["id"],
                "artist_name": artist["name"]
            })
        
        return artists
    
    except requests.exceptions.RequestException as e: 
        raise e 

@task
def get_artist_albums(token, artists):
    
    try:
        
        headers = {"Authorization": f"Bearer {token}"}
        album_list = []
        
        for artist in artists:
            url = f"https://api.spotify.com/v1/artists/{artist['artist_id']}/albums"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            response_json = response.json()
            # print(json.dumps(response_json, indent=4))
            for album in response_json.get("items", []):
                album_list.append({
                    "album_id": album["id"],
                    "album_name": album["name"],
                    "artist_id": artist["artist_id"],
                    "album_release_date": album["release_date"],
                    "album_total_tracks": album["total_tracks"],
                    "album_url": album["external_urls"]["spotify"]
                })
            
        return album_list
        
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def get_album_tracks(token, albums):
    
    try:
        
        headers = {"Authorization": f"Bearer {token}"}
        tracks_list = []
        
        for album in albums:
            url = f"https://api.spotify.com/v1/albums/{album['album_id']}/tracks"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            response_json = response.json()
            # print(json.dumps(response_json, indent=4))
            
            for track in response_json.get("items", []):
                tracks_list.append({
                    "track_id": track["id"],
                    "track_name": track["name"],
                    "album_id": album["album_id"],
                    "artist_id": album["artist_id"],
                    "duration_ms": track["duration_ms"],
                    "disc_number": track["disc_number"],
                    "track_number": track["track_number"],
                    "explicit": track["explicit"],
                })
            
        return tracks_list
            
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def save_to_json(artists, albums, tracks):
    file_path = f"./data/Spotify_data_{date.today()}.json"
    
    data = {
        "artists": artists,
        "albums": albums,
        "tracks": tracks,
    }

    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    token = get_access_token()
    artists = get_artist_ids(token, artist_names)
    albums = get_artist_albums(token, artists)
    tracks = get_album_tracks(token, albums)
    save_to_json(artists, albums, tracks)