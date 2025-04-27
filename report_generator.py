import pandas as pd
from datetime import datetime, timedelta
import pytz
import sqlite3
import os

DATABASE_PATH = "store_monitoring.db"

def get_db_connection():
    """Create a new SQLite connection for the current thread."""
    return sqlite3.connect(DATABASE_PATH)

def get_max_timestamp():
    """Get the latest timestamp from store_status as the 'current' time."""
    conn = get_db_connection()
    try:
        query = "SELECT MAX(timestamp_utc) as max_ts FROM store_status;"
        max_ts_df = pd.read_sql(query, conn)
        return pd.to_datetime(max_ts_df['max_ts'].iloc[0])
    finally:
        conn.close()

def convert_to_local_time(df, timezone_map):
    """Convert UTC timestamps to local time based on store timezone."""
    df['local_time'] = df.apply(
        lambda row: row['timestamp_utc'].astimezone(timezone_map.get(row['store_id'], pytz.timezone('America/Chicago'))),
        axis=1
    )
    return df

def interpolate_status(df, start_time, end_time):
    """Interpolate uptime/downtime between observations within a time range."""
    df = df.sort_values('timestamp_utc')
    total_minutes = (end_time - start_time).total_seconds() / 60
    if len(df) == 0:
        return 0, total_minutes  # Assume downtime if no data
    uptime_minutes = 0
    for i in range(len(df) - 1):
        start = df.iloc[i]['timestamp_utc']
        end = df.iloc[i + 1]['timestamp_utc']
        if start < start_time:
            start = start_time
        if end > end_time:
            end = end_time
        if start < end:  # Ensure there's a valid interval
            duration = (end - start).total_seconds() / 60
            if df.iloc[i]['status'] == 'active':
                uptime_minutes += duration
    # Add the last segment (assume status holds till end_time)
    if len(df) > 0:
        last_time = df.iloc[-1]['timestamp_utc']
        if last_time < end_time and last_time >= start_time:
            duration = (end_time - last_time).total_seconds() / 60
            if df.iloc[-1]['status'] == 'active':
                uptime_minutes += duration
    downtime_minutes = total_minutes - uptime_minutes
    return uptime_minutes, downtime_minutes

def calculate_uptime_downtime_extended(store_data, start_time, end_time, bh_dict, store_tz):
    """
    Calculate uptime and downtime within business hours for extended time ranges (day/week).
    Returns uptime and downtime in minutes.
    """
    uptime_minutes = 0
    downtime_minutes = 0
    current_local = start_time.astimezone(store_tz)
    end_local = end_time.astimezone(store_tz)
    one_day = timedelta(days=1)

    while current_local < end_local:
        next_day = min(end_local, current_local + one_day)
        day = current_local.dayofweek
        if day in bh_dict:
            start_bh, end_bh = bh_dict[day]
            # Construct full business hour timestamps for the current day
            bh_start_full = current_local.replace(hour=start_bh.hour, minute=start_bh.minute, second=0, microsecond=0)
            bh_end_full = current_local.replace(hour=end_bh.hour, minute=end_bh.minute, second=59, microsecond=999999)
            # Adjust if business hours span across days (rare, but handle it)
            if start_bh.day != current_local.day:
                bh_start_full += timedelta(days=(start_bh.day - current_local.day) % 7)
            if end_bh.day != current_local.day:
                bh_end_full += timedelta(days=(end_bh.day - current_local.day) % 7)
            # Find overlap with business hours for this day
            segment_start = max(current_local, bh_start_full)
            segment_end = min(next_day, bh_end_full)
            if segment_start < segment_end:
                segment_start_utc = segment_start.astimezone(pytz.UTC)
                segment_end_utc = segment_end.astimezone(pytz.UTC)
                relevant_data = store_data[
                    (store_data['timestamp_utc'] >= segment_start_utc) & 
                    (store_data['timestamp_utc'] <= segment_end_utc)
                ]
                up_min, down_min = interpolate_status(relevant_data, segment_start_utc, segment_end_utc)
                uptime_minutes += up_min
                downtime_minutes += down_min
        current_local = next_day

    return uptime_minutes, downtime_minutes

def generate_report(report_id):
    """Generate the uptime/downtime report for last hour, day, and week."""
    current_time = get_max_timestamp()
    one_hour_ago = current_time - timedelta(hours=1)
    one_day_ago = current_time - timedelta(days=1)
    one_week_ago = current_time - timedelta(weeks=1)

    # Load data from DB with a new connection
    conn = get_db_connection()
    try:
        store_status = pd.read_sql("SELECT * FROM store_status", conn)
        business_hours = pd.read_sql("SELECT * FROM business_hours", conn)
        timezone_data = pd.read_sql("SELECT * FROM timezone", conn)
    finally:
        conn.close()

    store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'])
    timezone_map = dict(zip(timezone_data['store_id'], timezone_data['timezone_str'].apply(pytz.timezone)))

    store_status = convert_to_local_time(store_status, timezone_map)
    result = []

    stores = store_status['store_id'].unique()
    for store_id in stores:
        store_tz = timezone_map.get(store_id, pytz.timezone('America/Chicago'))
        store_bh = business_hours[business_hours['store_id'] == store_id]
        if store_bh.empty:
            # Assume 24/7 if no business hours data
            bh_dict = {i: (pd.Timestamp("00:00", tz=store_tz), pd.Timestamp("23:59:59", tz=store_tz)) for i in range(7)}
        else:
            bh_dict = {
                row['dayOfWeek']: (
                    pd.Timestamp(f"{row['start_time_local']}", tz=store_tz),
                    pd.Timestamp(f"{row['end_time_local']}", tz=store_tz)
                )
                for _, row in store_bh.iterrows()
            }

        store_data = store_status[store_status['store_id'] == store_id]
        uptime_1h, downtime_1h = 0, 0
        uptime_1d, downtime_1d = 0, 0
        uptime_1w, downtime_1w = 0, 0

        # Last hour (original logic to ensure it works as before)
        local_current = current_time.astimezone(store_tz)
        local_one_hour_ago = one_hour_ago.astimezone(store_tz)
        day = local_current.dayofweek
        if day in bh_dict:
            start_bh, end_bh = bh_dict[day]
            if local_one_hour_ago.time() >= start_bh.time() and local_current.time() <= end_bh.time():
                relevant_data = store_data[
                    (store_data['timestamp_utc'] >= one_hour_ago) & 
                    (store_data['timestamp_utc'] <= current_time)
                ]
                uptime_1h, downtime_1h = interpolate_status(relevant_data, one_hour_ago, current_time)

        # Last day (extended logic)
        uptime_1d, downtime_1d = calculate_uptime_downtime_extended(store_data, one_day_ago, current_time, bh_dict, store_tz)

        # Last week (extended logic)
        uptime_1w, downtime_1w = calculate_uptime_downtime_extended(store_data, one_week_ago, current_time, bh_dict, store_tz)

        result.append({
            'store_id': store_id,
            'uptime_last_hour': round(uptime_1h, 2),  # in minutes
            'uptime_last_day': round(uptime_1d / 60, 2),  # convert to hours
            'uptime_last_week': round(uptime_1w / 60, 2),  # convert to hours
            'downtime_last_hour': round(downtime_1h, 2),  # in minutes
            'downtime_last_day': round(downtime_1d / 60, 2),  # convert to hours
            'downtime_last_week': round(downtime_1w / 60, 2)  # convert to hours
        })

    report_df = pd.DataFrame(result)
    report_path = f"reports/{report_id}.csv"
    os.makedirs("reports", exist_ok=True)
    report_df.to_csv(report_path, index=False)
    return report_path