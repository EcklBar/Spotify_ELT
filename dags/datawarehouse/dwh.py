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


@task
def staging_table():
    
    schema = "staging"
    conn, cur = None, None 
    
    
    try: 
        
        conn, cur = get_conn_cursor()
        artists, albums, tracks = load_data()
        
        create_schema(schema)
        create_tables(schema)
        
        # Artists
        artist_ids = get_ids(cur, schema, "artists", "artist_id")
        
        for row in artists:
            
            if len(artist_ids) == 0:
                insert_row(cur, conn, schema, "artists", row, ARTIST_COLUMNS)
                
            else:
                if row["artist_id"] in artist_ids:
                    update_row(cur, conn, schema, "artists", row, "artist_id", ["artist_name"])
                else:
                    insert_row(cur, conn, schema, "artists", row, ARTIST_COLUMNS)
        
        ids_in_json = {row["artist_id"] for row in artists}
        ids_to_delete = set(artist_ids) - ids_in_json
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, "artists", "artist_id", ids_to_delete)
            
            
        # Albums
        album_ids = get_ids(cur, schema, "albums", "album_id")
        
        for row in albums:
            
            if len(album_ids) == 0:
                insert_row(cur, conn, schema, "albums", row, ALBUM_COLUMNS)
                
            else: 
                if row["album_id"] in album_ids:
                    update_row(cur, conn, schema, "albums", row, "album_id", ["album_name", "album_total_tracks"])
                else:
                    insert_row(cur, conn, schema, "albums", row, ALBUM_COLUMNS)
        
        ids_in_json = {row["album_id"] for row in albums}
        ids_to_delete = set(album_ids) - ids_in_json
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, "albums", "album_id", ids_to_delete)



        # Songs
        track_ids = get_ids(cur, schema, "songs", "track_id")
        
        for row in tracks:
            
            if len(track_ids) == 0:
                insert_row(cur, conn, schema, "songs", row, SONG_COLUMNS)
                
            else:
                if row["track_id"] in track_ids:
                    update_row(cur, conn, schema, "songs", row, "track_id", ["track_name"])
                else:
                    insert_row(cur, conn, schema, "songs", row, SONG_COLUMNS)

        ids_in_json = {row["track_id"] for row in tracks}
        ids_to_delete = set(track_ids) - ids_in_json
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, "songs", "track_id", ids_to_delete)

        
        logger.info(f"{schema} tables update completed")

    except Exception as e:
        logger.error(f"Error updating {schema} tables: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(cur, conn)
            
            
@task
def core_table():

    schema = "core"
    conn, cur = None, None
    
    try: 
        
        conn, cur = get_conn_cursor()
        
        create_schema(schema)
        create_tables(schema)
        
        # Artists — no transformation needed
        artist_ids = get_ids(cur, schema, "artists", "artist_id")
        
        current_artist_ids = set()
        
        cur.execute("SELECT * FROM staging.artists;")
        rows = cur.fetchall()
        
        for row in rows:
            
            row = dict(row)
            current_artist_ids.add(row["artist_id"])
            
            if len(artist_ids) == 0: 
                insert_row(cur, conn, schema, "artists", row, ARTIST_COLUMNS)
            else:
                if row["artist_id"] in artist_ids:
                    update_row(cur, conn, schema, "artists", row, "artist_id", ["artist_name"])
                else: 
                    insert_row(cur, conn, schema, "artists", row, ARTIST_COLUMNS)
        
        ids_to_delete = set(artist_ids) - current_artist_ids
        
        if ids_to_delete:
             delete_rows(cur, conn, schema, "artists", "artist_id", ids_to_delete)
             
        # Albums — transform release_date
        album_ids = get_ids(cur, schema, "albums", "album_id")
        
        current_album_ids = set()
        
        cur.execute("SELECT * FROM staging.albums;")
        rows = cur.fetchall()
        
        for row in rows:
            
            transformed_row = transform_album(dict(row))
            current_album_ids.add(transformed_row["album_id"])
            
            if len(album_ids) == 0: 
                insert_row(cur, conn, schema, "albums", transformed_row, ALBUM_COLUMNS)
            else:
                if transformed_row["album_id"] in album_ids:
                    update_row(cur, conn, schema, "albums", transformed_row, "album_id", ["album_name", "album_total_tracks"])
                else:
                    insert_row(cur, conn, schema, "albums", transformed_row, ALBUM_COLUMNS)

        
        ids_to_delete = set(album_ids) - current_album_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, "albums", "album_id", ids_to_delete)
            
        
        # Songs — transform duration + track_type
        current_track_ids = set()
        
        core_song_columns = ["track_id", "track_name", "album_id", "artist_id", "duration", "track_type", "disc_number", "track_number", "explicit"]
        track_ids = get_ids(cur, schema, "songs", "track_id")
        
        cur.execute("SELECT * FROM staging.songs;")
        rows = cur.fetchall()

        for row in rows:
            
            transformed_row = transform_track(dict(row))
            current_track_ids.add(transformed_row["track_id"])

            if len(track_ids) == 0:
                insert_row(cur, conn, schema, "songs", transformed_row, core_song_columns)
            else:
                if transformed_row["track_id"] in track_ids:
                    update_row(cur, conn, schema, "songs", transformed_row, "track_id", ["track_name"])
                else:
                    insert_row(cur, conn, schema, "songs", transformed_row, core_song_columns)

        ids_to_delete = set(track_ids) - current_track_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, "songs", "track_id", ids_to_delete)

        logger.info(f"{schema} tables update completed")

    except Exception as e:
        logger.error(f"Error updating {schema} tables: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(cur, conn)