import os
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "devtoken123")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "aiops")
RAW_BUCKET = "raw_metrics"
NORMAL_BUCKET = "normal_metrics"
INCIDENT_BUCKET = "incident_logs"

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

def ensure_buckets():
    """Ensure normal and incident buckets exist"""
    # In a real scenario we'd use the Buckets API. Assuming they are pre-created via docker for MVP.
    pass

def process_windows():
    print("Running Data Gating (Training Separation)...")
    # Query last 10 minutes of raw metrics
    query = f'''
    from(bucket:"{RAW_BUCKET}")
    |> range(start: -10m)
    |> filter(fn: (r) => r._measurement == "system_metrics")
    '''
    
    try:
        tables = query_api.query(query, org=INFLUXDB_ORG)
        
        # Simplified Mock Logic: Normally we'd query an external API to see if there's an active incident
        # Here we just look if CPU > 85 to simulate "incident period"
        is_incident_window = False
        
        for table in tables:
            for record in table.records:
                if record.get_field() == "cpu_percent" and record.get_value() > 85:
                    is_incident_window = True
                    break
        
        # Write back to appropriate bucket
        bucket_dest = INCIDENT_BUCKET if is_incident_window else NORMAL_BUCKET
        
        # Note: True implementation would copy all records in this window to the dest bucket.
        print(f"Data Gate: Classified last 10 mins as {'INCIDENT' if is_incident_window else 'HEALTHY'}. Writing to {bucket_dest}.")
        
    except Exception as e:
        print(f"Error in data gate: {e}")

def main():
    while True:
        try:
            process_windows()
        except Exception as e:
            print(e)
        time.sleep(600) # run every 10 mins

if __name__ == "__main__":
    main()
