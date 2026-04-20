from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.spotify_stats import (
    get_access_token,
    get_artist_ids,
    get_artist_albums,
    get_album_tracks,
    save_to_json,
    artist_names,
)

from datawarehouse.dwh import staging_table, core_table


# Define the local timezone
local_tz = pendulum.timezone("Asia/Jerusalem")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag:
    
    # Define tasks
    token = get_access_token()
    artists = get_artist_ids(token, artist_names)
    albums = get_artist_albums(token, artists)
    tracks = get_album_tracks(token, albums)
    save_to_json_task = save_to_json(artists, albums, tracks)
    
    # Define dependencies
    token >> artists >> albums >> tracks >> save_to_json_task
    
    
# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    schedule="0 15 * * *",
    catchup=False,
) as dag:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()
    
    # Define dependencies
    update_staging >> update_core