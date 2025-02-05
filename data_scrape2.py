import requests
import time
import json
import zipfile
import os
import csv
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

# API Key
API_KEY = "NKg1pSpaOzGgENc338qcqCsQTNnOanAoOtN3"

# GTFS-RT Endpoints
TRIP_UPDATES_URI = "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses"
VEHICLE_POSITIONS_URI = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"
TIMETABLE_URI = "https://api.transport.nsw.gov.au/v1/publictransport/timetables/complete/gtfs"

# Function to fetch GTFS-RT data
def fetch_gtfs_data(url, is_gtfs_static=False):
    try:
        response = requests.get(url, headers={'Authorization': f'apikey {API_KEY}'})
        if response.status_code == 200:
            if is_gtfs_static:
                return response.content  # Return ZIP file content
            return response.content  # Return protobuf data
        else:
            print(f"Failed to fetch data from {url}, Status Code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Function to parse and save GTFS-RT data
def parse_gtfs_realtime_data(data, data_type="trip_updates"):
    if not data:
        print(f"No {data_type} data available.")
        return
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)  # Decode protobuf data

    # Convert to dictionary
    feed_dict = MessageToDict(feed)

    # Save to JSON file
    file_name = f"{data_type}_data.json"
    with open(file_name, "w") as f:
        json.dump(feed_dict, f, indent=4)

    print(f"{data_type.capitalize()} data saved to {file_name}")

# Function to extract and process GTFS static data
def process_gtfs_static_data(data):
    if not data:
        print("No GTFS static data available.")
        return

    zip_file_path = "gtfs_static_data.zip"

    # Save ZIP file
    with open(zip_file_path, "wb") as f:
        f.write(data)

    print(f"GTFS Static Data saved to {zip_file_path}")

    # Extract ZIP file
    extract_folder = "gtfs_static"
    os.makedirs(extract_folder, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_folder)

    print(f"Extracted GTFS static files to {extract_folder}")

    # Load key GTFS files into CSV (without pandas)
    files_to_read = ["trips.txt", "stops.txt", "routes.txt"]
    dataframes = {}

    for file in files_to_read:
        file_path = os.path.join(extract_folder, file)
        if os.path.exists(file_path):
            with open(file_path, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                dataframes[file.replace(".txt", "")] = list(reader)
            print(f"Loaded {file} with {len(dataframes[file.replace('.txt', '')])} records.")

    return dataframes

# Real-time loop to fetch & parse data continuously
def real_time_gtfs(interval=180):
    while True:
        print("Fetching Trip Updates...")
        parse_gtfs_realtime_data(fetch_gtfs_data(TRIP_UPDATES_URI), "trip_updates")

        print("Fetching Vehicle Positions...")
        parse_gtfs_realtime_data(fetch_gtfs_data(VEHICLE_POSITIONS_URI), "vehicle_positions")

        print("Fetching GTFS Static Timetable...")
        gtfs_static_data = fetch_gtfs_data(TIMETABLE_URI, is_gtfs_static=True)
        dataframes = process_gtfs_static_data(gtfs_static_data)

        # Example: Print first few rows of trips data
        if dataframes and "trips" in dataframes:
            print(dataframes["trips"][:5])  # Show first 5 rows

        print(f"Waiting {interval} seconds before next update...\n")
        time.sleep(interval)

# Start real-time GTFS parsing (updates every 180 seconds)
real_time_gtfs()
