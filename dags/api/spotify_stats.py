import requests
import base64
import json
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = Variable.get("SPOTIFY_REFRESH_TOKEN")

TOKEN_URL = "https://accounts.spotify.com/api/token"
RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"


@task
def get_access_token():
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_base64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
        },
    )

    response.raise_for_status()

    return response.json()["access_token"]


@task
def get_recently_played(access_token):
    response = requests.get(
        RECENTLY_PLAYED_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": 50},
    )

    response.raise_for_status()

    return response.json()["items"]


@task
def extract_track_data(raw_data):
    extracted_data = []

    for item in raw_data:
        track = item["track"]
        artists = ", ".join([artist["name"] for artist in track["artists"]])

        track_data = {
            "track_id": track["id"],
            "track_name": track["name"],
            "artist_name": artists,
            "album_name": track["album"]["name"],
            "played_at": item["played_at"],
            "duration_ms": track["duration_ms"],
            "popularity": track["popularity"],
        }

        extracted_data.append(track_data)

    return extracted_data


@task
def save_to_json(extracted_data):
    file_path = f"./data/spotify_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
