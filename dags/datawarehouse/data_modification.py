import logging

logger = logging.getLogger(__name__)


def insert_row(cur, conn, schema, table, row, columns):
    
    try: 
        
        cols = ", ".join([f'"{c}"' for c in columns])
        vals = ", ".join([f"%({c})s" for c in columns])
        
        cur.execute(
            f"""INSERT INTO {schema}.{table} ({cols}) VALUES ({vals});""",
            row,
        )
        conn.commit()
        
        logger.info(f"Inserted row with {columns[0]}: {row[columns[0]]}")
        
    except Exception as e: 
        logger.error(f"Error inserting row into {schema}.{table}: {e}")
        raise e

    
def update_row(cur, conn, schema, table, row, id_column, update_columns):
    
    try: 
        
        set_clause = ", ".join([f'"{c}" = %({c})s' for c in update_columns])
        
        cur.execute(
            f"""UPDATE {schema}.{table} set {set_clause} WHERE "{id_column}"  = %({id_column})s;""",
            row,
        )
        conn.commit()
        
        logger.info(f"Update row with {id_column}: {row[id_column]}")
    
    except Exception as e:
        logger.error(f"Error updating row in {schema}.{table}: {e}")
        raise e
    

def delete_rows(cur, conn, schema, table, id_column, ids_to_delete):
    
    try: 
        
        ids_str = " ,".join([f"'{id}'" for id in ids_to_delete])
        
        cur.execute(
            f"""DELETE FROM {schema}.{table} WHERE "{id_column}" IN ({ids_str});"""
        )
        conn.commit()
        
        logger.info(f"Deleted rows with {id_column}: {ids_to_delete}")
        
    except Exception as e:
        logger.error(f"Error deleting rows from {schema}.{table}: {e}")
        raise e