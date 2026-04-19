import logging

logger = logging.getLogger(__name__)


def insert_row(cur, conn, schema, table, row, columns):
    
    try: 
        
        cols = ", ".join([f'"{c}"' for c in columns])
        vals = ", ".join([f"%({c}s)" for c in columns])
        
        cur.execute(
            f"""INSERT INTO {schema}.{table} ({cols}) VALUES ({vals});""",
            row,
        )
        conn.commit()
        
    except Exception as e: 
        logger.error(f"Error inserting row into {schema}.{table}: {e}")
        raise e