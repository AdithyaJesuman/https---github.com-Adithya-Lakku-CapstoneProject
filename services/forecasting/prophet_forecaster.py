import os
import time
import pandas as pd
from prophet import Prophet
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta

# Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "devtoken123")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "aiops")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "raw_metrics")
SERVICE_NAME = "payment-api"

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

def fetch_historical_data(metric_name, days=7):
    print(f"Fetching {days} days of data for {metric_name}...")
    query = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
    |> range(start: -{days}d)
    |> filter(fn: (r) => r._measurement == "system_metrics" and r.service_name == "{SERVICE_NAME}" and r._field == "{metric_name}")
    |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    '''
    
    try:
        tables = query_api.query(query, org=INFLUXDB_ORG)
        records = []
        for table in tables:
            for record in table.records:
                records.append({
                    "ds": record.get_time().replace(tzinfo=None),
                    "y": record.get_value()
                })
        
        return pd.DataFrame(records)
    except Exception as e:
        print(f"Error fetching data from InfluxDB: {e}")
        return pd.DataFrame()

def train_and_forecast(df, periods=240): # 240 minutes = 4 hours
    if df.empty or len(df) < 10:
        return None
        
    m = Prophet(yearly_seasonality=False, daily_seasonality=True, weekly_seasonality=True)
    m.fit(df)
    
    future = m.make_future_dataframe(periods=periods, freq='T')
    forecast = m.predict(future)
    
    return forecast

def predict_time_to_threshold_breach(forecast, threshold, metric_name):
    if forecast is None:
        return None
        
    now = datetime.utcnow()
    future_forecast = forecast[forecast['ds'] > now]
    
    breach = future_forecast[future_forecast['yhat'] >= threshold]
    if not breach.empty:
        breach_time = breach.iloc[0]['ds']
        minutes_to_breach = int((breach_time - now).total_seconds() / 60)
        return minutes_to_breach
    
    return None

def main():
    while True:
        try:
            print("--- Retraining Prophet Models ---")
            
            # Predict throughput threshold breach (e.g., max capacity 2000 RPS)
            df_rps = fetch_historical_data("throughput_rps")
            forecast_rps = train_and_forecast(df_rps)
            mins_to_cap = predict_time_to_threshold_breach(forecast_rps, 2000, "throughput_rps")
            
            if mins_to_cap is not None:
                print(f"[FORECAST] throughput_rps predicted to hit 2000 capacity limit in {mins_to_cap} minutes")
            else:
                print("[FORECAST] throughput_rps looks stable for the next 4 hours.")
                
            # Predict latency threshold breach (e.g., SLA 1000ms)
            df_latency = fetch_historical_data("response_time_ms")
            forecast_latency = train_and_forecast(df_latency)
            mins_to_lat = predict_time_to_threshold_breach(forecast_latency, 1000, "response_time_ms")
            
            if mins_to_lat is not None:
                print(f"[FORECAST] response_time_ms predicted to breach 1000ms SLA in {mins_to_lat} minutes")
            else:
                print("[FORECAST] response_time_ms looks stable for the next 4 hours.")
                
        except Exception as e:
            print(f"Forecasting error: {e}")
            
        print("Sleeping for 1 hour before next training cycle...")
        time.sleep(3600)

if __name__ == "__main__":
    main()
