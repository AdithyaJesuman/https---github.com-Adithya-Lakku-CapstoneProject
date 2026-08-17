import os
import json
import uuid
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'engineered-features',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def evaluate_signals(event):
    raw = event["raw_metrics"]
    derived = event["derived_features"]
    
    signals = []
    
    # 1. CPU climbing
    if raw["cpu_percent"] > 80 and raw["cpu_percent"] < 95:
        # Simplistic assumption for MVP, real would use rate of change
        signals.append({
            "type": "capacity_wall_approaching",
            "prob": 0.8,
            "eta": 15 * 60,
            "evidence": "CPU > 80% and climbing"
        })
        
    # 2. Pool exhaustion risk
    if raw["active_connections"] > 800: # Assuming 1000 is limit
        signals.append({
            "type": "db_connection_pool_exhaustion",
            "prob": 0.9,
            "eta": 5 * 60,
            "evidence": "active_connections > 80% capacity"
        })
        
    # 3. Memory leak risk
    if derived["memory_leak_slope"] > 0.1 and raw["throughput_rps"] < 2000:
        signals.append({
            "type": "memory_leak_oom",
            "prob": 0.85,
            "eta": 120 * 60,
            "evidence": "positive memory slope independent of traffic"
        })

    return signals

def main():
    print("Starting Multi-Signal Predictor...")
    for message in consumer:
        event = message.value
        try:
            signals = evaluate_signals(event)
            
            for sig in signals:
                if sig["prob"] > 0.7:
                    forecast = {
                        "forecast_id": f"FCST-{str(uuid.uuid4())[:8]}",
                        "service_name": event["service_name"],
                        "predicted_incident_type": sig["type"],
                        "time_to_incident_seconds": sig["eta"],
                        "confidence": sig["prob"],
                        "evidence": [sig["evidence"]]
                    }
                    producer.send("forecasts", value=forecast)
                    print(f"🔮 PREDICTION: {forecast['predicted_incident_type']} in {forecast['time_to_incident_seconds']}s")
                    
        except Exception as e:
            print(f"Error in signal predictor: {e}")

if __name__ == "__main__":
    main()
