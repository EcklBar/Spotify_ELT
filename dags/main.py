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
from dataquality.soda import spotify_elt_data_quality


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

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:
    
    # Define tasks
    token = get_access_token()
    artists = get_artist_ids(token, artist_names)
    albums = get_artist_albums(token, artists)
    tracks = get_album_tracks(token, albums)
    save_to_json_task = save_to_json(artists, albums, tracks)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    save_to_json_task >> trigger_update_db


# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Define dependencies
    update_staging >> update_core >> trigger_data_quality


# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = spotify_elt_data_quality(staging_schema)
    soda_validate_core = spotify_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core