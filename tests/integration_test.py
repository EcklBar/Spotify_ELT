import requests
import pytest
import psycopg2


def test_spotify_api_response(airflow_variable):
    client_id = airflow_variable("spotify_client_id")
    client_secret = airflow_variable("spotify_client_secret")

    import base64
    auth_string = f"{client_id}:{client_secret}"
    auth_base64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            headers={
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
        )
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"Request to Spotify API failed: {e}")


def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()