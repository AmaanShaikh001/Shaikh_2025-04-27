import pandas as pd
import sqlite3
import os

# SQLite database connection
DATABASE_PATH = "store_monitoring.db"

def load_data_to_db(data_folder):
    """
    Load CSV data into SQLite database.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        # Load store status data
        store_status = pd.read_csv(os.path.join(data_folder, "store_status.csv"))
        store_status.to_sql("store_status", conn, if_exists="replace", index=False)
        print("Loaded store_status.csv to database.")

        # Load business hours data
        business_hours = pd.read_csv(os.path.join(data_folder, "business_hours.csv"))
        business_hours.to_sql("business_hours", conn, if_exists="replace", index=False)
        print("Loaded business_hours.csv to database.")

        # Load timezone data
        timezone = pd.read_csv(os.path.join(data_folder, "timezone.csv"))
        timezone.to_sql("timezone", conn, if_exists="replace", index=False)
        print("Loaded timezone.csv to database.")
    finally:
        conn.close()

if __name__ == "__main__":
    data_folder = "./"  # Assuming CSVs are in the same folder as the script
    load_data_to_db(data_folder)