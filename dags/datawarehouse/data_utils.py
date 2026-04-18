from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id= "postgres_db_spotify_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cursor(cur, conn):
    cur.close()
    conn.close()


def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(cur, conn)


def create_tables(schema):
    conn, cur = get_conn_cursor()
    
    if schema == "staging":
        artists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.artists (
                "artist_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "artist_name" TEXT NOT NULL
            );
        """
        albums_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.albums (
                "album_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "album_name" TEXT NOT NULL,
                "artist_id" VARCHAR(22) NOT NULL,
                "artist_name" TEXT NOT NULL,
                "album_release_date" VARCHAR(10) NOT NULL,
                "album_total_tracks" INT NOT NULL,
                "album_url" TEXT NOT NULL
            );
        """
        song_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.songs (
                "track_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "track_name" TEXT NOT NULL,
                "album_id" VARCHAR(22) NOT NULL,
                "album_name" TEXT NOT NULL,
                "artist_id" VARCHAR(22) NOT NULL,
                "artist_name" TEXT NOT NULL,
                "duration_ms" INT NOT NULL,
                "disc_number" INT NOT NULL,
                "track_number" INT NOT NULL,
                "explicit" BOOLEAN NOT NULL
            );
        """
    else: 
        artists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.artists (
                "artist_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "artist_name" TEXT NOT NULL
            );
        """
        albums_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.albums (
                "album_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "album_name" TEXT NOT NULL,
                "artist_id" VARCHAR(22) NOT NULL,
                "artist_name" TEXT NOT NULL,
                "album_release_date" DATE NOT NULL,
                "album_total_tracks" INT NOT NULL,
                "album_url" TEXT NOT NULL
            );
        """
        song_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.songs (
                "track_id" VARCHAR(22) PRIMARY KEY NOT NULL,
                "track_name" TEXT NOT NULL,
                "album_id" VARCHAR(22) NOT NULL,
                "album_name" TEXT NOT NULL,
                "artist_id" VARCHAR(22) NOT NULL,
                "artist_name" TEXT NOT NULL,
                "duration" TIME NOT NULL,
                "track_type" VARCHAR(10) NOT NULL,
                "disc_number" INT NOT NULL,
                "track_number" INT NOT NULL,
                "explicit" BOOLEAN NOT NULL
            );
        """
    
    cur.execute(artists_sql)
    cur.execute(albums_sql)
    cur.execute(song_sql)
    
    conn.commit()
    
    close_conn_cursor(cur, conn)

    
def get_ids(cur, schema, table_name, id_column):
    cur.execute(f"""SELECT "{id_column}" FROM {schema}.{table_name};""")
    rows = cur.fetchall()
    
    ids = [row[id_column] for row in rows]
    
    return ids