def calculate_duration(start_time, end_time):
    if start_time:
        duration = end_time - start_time
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        secs = total_seconds % 60
        formatted_duration = f"{hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        formatted_duration = "00:00:00"

    return formatted_duration