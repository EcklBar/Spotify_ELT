from datetime import datetime, timedelta


def transform_track(row):
    duration_ms = row["duration_ms"]
    duration_td = timedelta(milliseconds=duration_ms)
    
    row["duration"] = (datetime.min + duration_td).time()
    row["track_type"] = "Short" if duration_td.total_seconds() < 60 else "Normal"
    
    return row


def transform_album(row):
    release_date = row["album_release_date"]
    
    if len(release_date) == 4:
        row["album_release_date"] = datetime.strptime(release_date, "%Y").date()
    elif len(release_date) == 7:
        row["album_release_date"] = datetime.strptime(release_date, "%Y-%m").date()
    else:
        row["album_release_date"] = datetime.strptime(release_date, "%Y-%m-%d").date()
        
    return row