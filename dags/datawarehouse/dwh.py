from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_tables, get_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_row, update_row, delete_rows
from datawarehouse.data_transformation import transform_track, transform_album

import logging
from airflow.decorators import task


logger = logging.getLogger(__name__)

# Column definitions for each table
ARTIST_COLUMNS = ["artist_id", "artist_name"]
ALBUM_COLUMNS = ["album_id", "album_name", "artist_id", "album_release_date", "album_total_tracks", "album_url"]
SONG_COLUMNS = ["track_id", "track_name", "album_id", "artist_id", "duration_ms", "disc_number", "track_number", "explicit"]
