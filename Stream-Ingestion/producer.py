import json
import pandas as pd
import time 
import io
import requests 
import dataclasses
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer



# Process to push to redpanda
# ----- arrival dataset from TfL API -----
@dataclass
class tlfArrival:
    id            : str
    operationType          : int
    vehicleId                : str
    naptanId                 : str
    stationName              : str
    lineId                   : str
    lineName                 : str
    platformName             : str
    direction                : str
    bearing                  : str
    tripId                   : str
    baseVersion              : str
    destinationNaptanId      : str
    destinationName          : str
    timestamp                : str
    timeToStation            : int
    currentLocation          : str
    towards                  : str
    expectedArrival          : str
    timeToLive               : str
    modeName                 : str
    timing_source            : str
    timing_insert            : str
    timing_read              : str
    timing_sent              : str
    timing_received          : str

@dataclass
class tflLineStatus:
    id                   : str
    name                 :str
    modeName             : str
    statusSeverity       : int
    statusSeverityDescription    : str
    reason                       : str   # Disruptions often have a 'reason' string
    created                      : str

@dataclass
class tflStopPoint:
    naptanId    : str
    commonName  : str
    lat         : float
    lon         : float
    placeType   : str


# Get all bus line IDs
url_lines = "https://api.tfl.gov.uk/Line/Mode/bus"
lines = requests.get(url_lines).json()
line_ids = [line["id"] for line in lines][:100]



def trip_serializer(trip):
    trip_dict = dataclasses.asdict(trip)
    return json.dumps(trip_dict).encode('utf-8')

def trip_deserializer(data):
    json_str = data.decode('utf-8')
    trip_dict = json.loads(json_str)
    return tlfArrival(**trip_dict)


# def dataframe_to_parquet(df):
#     """Converts a pandas DataFrame into a Parquet byte stream."""
#     if df.empty:
#         return None
#     table = pa.Table.from_pandas(df)
#     buf = io.BytesIO()
#     pq.write_table(table, buf)
#     return buf.getvalue()

server = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=trip_serializer  
)

topic_name1 = "tfl-arrivals"
topic_name2 = "tfl-line-status"
topic_name3 = "tfl-stoppoints"

def fetch_and_send_tlf_arrivals():
    # Get arrivals for busses 
    all_data = []
    for line_id in line_ids:
        url = f"https://api.tfl.gov.uk/Line/{line_id}/Arrivals"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if isinstance(data, list):
                all_data.extend(data)
        except:
            continue

    # --- SAFETY GATE ---
    if not all_data:
        print("No arrival data found in this window. Skipping...")
        return

    # Keep only dicts
    clean_data = [d for d in all_data if isinstance(d, dict)]
    
    if not clean_data:
        return

    df_arrivals = pd.DataFrame(clean_data)

    # Check if 'timing' column exists (TfL sometimes omits it if empty)
    if 'timing' in df_arrivals.columns:
        timing_df = pd.json_normalize(df_arrivals['timing'])
        timing_df = timing_df.add_prefix('timing_')
        
        # Ensure only the columns we want exist in timing_df to avoid errors
        timing_cols = ['timing_source', 'timing_insert', 'timing_read', 'timing_sent', 'timing_received']
        for col in timing_cols:
            if col not in timing_df.columns:
                timing_df[col] = None # Fill missing timing fields with None
        
        timing_df = timing_df[timing_cols].copy()
        df_arrivals.drop(columns=['timing'], inplace=True)
        line_data = pd.concat([df_arrivals.reset_index(drop=True), 
                               timing_df.reset_index(drop=True)], axis=1)
    else:
        # If no timing column, create empty ones so schema doesn't break
        for col in ['timing_source', 'timing_insert', 'timing_read', 'timing_sent', 'timing_received']:
            df_arrivals[col] = None
        line_data = df_arrivals

    columns = ['id', 'operationType', 'vehicleId', 'naptanId', 'stationName',
        'lineId', 'lineName', 'platformName', 'direction', 'bearing', 'tripId',
        'baseVersion', 'destinationNaptanId', 'destinationName', 'timestamp',
        'timeToStation', 'currentLocation', 'towards', 'expectedArrival',
        'timeToLive', 'modeName', 'timing_source', 'timing_insert',
        'timing_read', 'timing_sent', 'timing_received'] 
    
    # Filter only columns that actually exist to prevent "KeyError"
    # existing_columns = [c for c in columns if c in line_data.columns]
    for col in columns:
        if col not in line_data.columns:
            line_data[col] = None

    line_data = line_data[columns]
    
    line_data = line_data.fillna({
    "operationType": 0,
    "timeToStation": 0,
    "vehicleId": "",
    "naptanId": "",
    "stationName": "",
    "lineId": "",
    "lineName": "",
    "platformName": "",
    "direction": "",
    "bearing": "",
    "tripId": "",
    "baseVersion": "",
    "destinationNaptanId": "",
    "destinationName": "",
    "timestamp": "",
    "currentLocation": "",
    "towards": "",
    "expectedArrival": "",
    "timeToLive": "",
    "modeName": "",
    "timing_source": "",
    "timing_insert": "",
    "timing_read": "",
    "timing_sent": "",
    "timing_received": ""
    })

    line_data = line_data.astype({
    "operationType": "int64",
    "timeToStation": "int64"
    })
    # Convert DataFrame to a list of JSON-like dictionaries
    json_data = line_data.to_dict(orient='records') 

    for row in json_data:
        # This turns the dict into a tlfArrival object so trip_serializer works
        arrival_obj = tlfArrival(**row) 
        producer.send(topic_name1, value=arrival_obj)

    producer.flush()
    print(f"Sent {len(line_data)} arrivals to {topic_name1}")
   



def fetch_and_send_line_status():
    url = "https://api.tfl.gov.uk/Line/Mode/bus/Status"
    try:
        response = requests.get(url, timeout=15).json()
    except:
        print("Failed to fetch Line Status API. Skipping...")
        return

    if not response:
        return


    if isinstance(response, dict):
        response = [response]
        
    df = pd.DataFrame(response)
    
    # Check if lineStatuses exists and isn't empty
    if 'lineStatuses' in df.columns:
        df = df.explode('lineStatuses')
        
        # Filter out rows where lineStatuses might be null/empty before normalizing
        clean_status_rows = df['lineStatuses'].dropna().tolist()
        
        if clean_status_rows:
            status_df = pd.json_normalize(clean_status_rows)
            
            # Combine back. We use reset_index to ensure the rows align perfectly
            final_df = pd.concat([
                df[['id', 'name', 'modeName', 'created']].reset_index(drop=True),
                status_df[['statusSeverity', 'statusSeverityDescription', 'reason']].reset_index(drop=True)
            ], axis=1)
        else:
            # Fallback if explode resulted in nothing
            return
    else:
        return

    # Force Types and fill missing reasons
    final_df['id'] = final_df['id'].astype(str)
    if 'reason' in final_df.columns:
        final_df['reason'] = final_df['reason'].fillna("No disruptions").astype(str)
    else:
        final_df['reason'] = "No disruptions"
    
    json_data = final_df.to_dict(orient='records')
    for row in json_data:
        obj = tflLineStatus(**row) # Uses tflLineStatus dataclass
        producer.send(topic_name2, value=obj)
    producer.flush()
    print(f"Sent {len(final_df)} line status updates to {topic_name2}")


def fetch_and_send_stoppoints():
    # 50 lines provides a massive, high-quality sample for a standard project
    target_lines = line_ids
    all_stops = []

    for lid in target_lines:
        url_stops = f"https://api.tfl.gov.uk/Line/{lid}/StopPoints"
        try:
            res = requests.get(url_stops, timeout=10)      # Added a 10-second timeout so one bad line doesn't hang the script
            if res.status_code == 200:
                all_stops.extend(res.json())
        except Exception as e:
            print(f" Skipping {lid} due to error: {e}")
            continue

    # Step C: Clean and Deduplicate
    if all_stops:
        df = pd.DataFrame(all_stops)
        
        # Select essential columns for your Dimension Table
        # We ensure types are strictly set for Parquet schema stability
        df_final = df[['naptanId', 'commonName', 'lat', 'lon', 'placeType']].copy()
        df_final['naptanId'] = df_final['naptanId'].astype(str)
        df_final['commonName'] = df_final['commonName'].astype(str)
        df_final['placeType'] = df_final['placeType'].astype(str)
        
        # Deduplicate: Lines share stops, we only want one row per NaptanID
        df_unique = df_final.drop_duplicates(subset=['naptanId'])
     
        
        # Step D: Convert and Send
        json_data = df_unique.to_dict(orient='records')
        for row in json_data:
            obj = tflStopPoint(**row) # Uses tflStopPoint dataclass
            producer.send(topic_name3, value=obj)
        producer.flush()
             
        print(f"SUCCESS: {len(df_unique)} stops flushed to Redpanda topic '{topic_name3}'.")
    else:
        print("ERROR: No stop data collected.")


def run_infinite_stream():
    print(f" Starting TfL Stream to Redpanda topics: {topic_name1}, {topic_name2}, {topic_name3}... ")
    
    # Fetch Static Metadata ONCE
    fetch_and_send_stoppoints()

    while True:
        try:
            start_time = time.time()
            
            print(f" Fetching data from TfL API...")
            fetch_and_send_tlf_arrivals()
            fetch_and_send_line_status()

            
            duration = time.time() - start_time
            print(f" Batch sent successfully in {duration:.2f} seconds.")
            
            # Wait 2 minutes before fetching again
            # Adjust this based on how 'real-time' you want the dashboard
            print("zzz Sleeping for 200 seconds...")
            time.sleep(200)
            
        except Exception as e:
            print(f" Error in stream: {e}")
            print("Waiting 30 seconds before retrying...")
            time.sleep(30)

if __name__ == "__main__":
    run_infinite_stream()