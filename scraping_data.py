import requests
import time
import json
import zipfile
import os
import pandas as pd
import datetime
from io import BytesIO
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from enum import Enum  # Import Enum for GTFSDataType

# API Key (Replace with your actual key)
API_KEY = "NKg1pSpaOzGgENc338qcqCsQTNnOanAoOtN3"

# GTFS-RT Endpoints
TRIP_UPDATES_URI = "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses"
VEHICLE_POSITIONS_URI = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"
TIMETABLE_URI = "https://api.transport.nsw.gov.au/v1/publictransport/timetables/complete/gtfs"

# JSON File Checkpoint Paths
TRIP_UPDATES_FILE = "trip_updates_data.json"
VEHICLE_POSITIONS_FILE = "vehicle_positions_data.json"

# ==============================
# Define GTFS Data Types (Fix for NameError)
# ==============================
class GTFSDataType(Enum):
    TRIP_UPDATES = (TRIP_UPDATES_URI, TRIP_UPDATES_FILE, "DYNAMIC")
    VEHICLE_POSITIONS = (VEHICLE_POSITIONS_URI, VEHICLE_POSITIONS_FILE, "DYNAMIC")
    TIMETABLE = (TIMETABLE_URI, None, "STATIC")  # No file path needed for static data

# ==============================
# Function: Fetch GTFS-RT Data
# ==============================
def fetch_gtfs_data(url):
    """Fetch GTFS-RT or static GTFS data from the API."""
    try:
        response = requests.get(url, headers={'Authorization': f'apikey {API_KEY}'}, timeout=10)
        if response.status_code == 200:
            return response.content
        print(f"Failed to fetch data from {url}, Status Code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
    return None

# ==================================
# Function: Load Existing Checkpoint
# ==================================
def load_existing_data(file_path):
    """Load historical data from a checkpoint file if available."""
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (json.JSONDecodeError, IOError):
            print(f"Error reading {file_path}. Starting fresh.")
    return []

# ==================================================
# Function: Parse & Append GTFS-RT Data with History
# ==================================================
def parse_gtfs_realtime_data(data, file_path):
    """Parses GTFS-RT protobuf data, converts to JSON, and appends to checkpoint."""
    if not data:
        print(f"No data available for {file_path}.")
        return

    # Decode protobuf data
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    # Convert to dictionary with timestamp
    feed_dict = MessageToDict(feed)
    feed_dict["timestamp"] = datetime.datetime.now().isoformat()

    # Load existing data and ensure it's a list
    existing_data = load_existing_data(file_path)

    # Append only if new data is different
    if existing_data and existing_data[-1] == feed_dict:
        print(f"No new data for {file_path}. Skipping update.")
        return

    existing_data.append(feed_dict)

    # Save updated data with a checkpoint
    try:
        with open(file_path, "w") as f:
            json.dump(existing_data, f, indent=4)
        print(f"Data appended to {file_path}. File size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
    except IOError as e:
        print(f"Error saving data: {e}")

# ====================================
# Function: Process GTFS Static Data
# ====================================
def process_gtfs_static_data(data):
    """Extracts and processes GTFS static data from a ZIP file."""
    if not data:
        print("No GTFS static data available.")
        return

    zip_file_path = "gtfs_static_data.zip"

    # Save ZIP file
    with open(zip_file_path, "wb") as f:
        f.write(data)

    # Extract ZIP file
    extract_folder = "gtfs_static"
    os.makedirs(extract_folder, exist_ok=True)
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_folder)

    # Load key GTFS files into DataFrames
    files_to_read = ["trips.txt", "stops.txt", "routes.txt"]
    dataframes = {}
    for file in files_to_read:
        file_path = os.path.join(extract_folder, file)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            dataframes[file.replace(".txt", "")] = df
            print(f"Loaded {file} with {df.shape[0]} records.")

    return dataframes

# ==============================================
# Function: Run GTFS Scraper on a Fixed Interval
# ==============================================
def real_time_gtfs(interval=180):
    """Continuously fetch, parse, and store GTFS-RT data every interval seconds."""
    while True:
        for data_type in GTFSDataType:  
            uri, file, data_category = data_type.value
            print(f"Fetching {data_type.name}...")

            data = fetch_gtfs_data(uri)

            if data_category == "DYNAMIC":
                parse_gtfs_realtime_data(data, file)
            elif data_category == "STATIC":
                process_gtfs_static_data(data)

        print(f"Waiting {interval} seconds before next update...\n")
        time.sleep(interval)

# Start the real-time GTFS parser
real_time_gtfs()
