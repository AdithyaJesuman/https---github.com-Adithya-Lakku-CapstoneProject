import os
import json
import time
from collections import deque
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'raw-metrics',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Rolling windows for stats
history = {
    "cpu_percent": deque(maxlen=60), # 10 mins (60 samples @ 10s)
    "memory_percent": deque(maxlen=360), # 1 hr (360 samples)
    "response_time_ms": deque(maxlen=60),
    "throughput_rps": deque(maxlen=60),
    "active_connections": deque(maxlen=60)
}

def compute_derived_features(raw):
    metrics = raw["metrics"]
    
    # Update history
    for k in history.keys():
        if k in metrics:
            history[k].append(metrics[k])
            
    # 1. CPU per request
    cpu_per_request = 0
    if metrics.get("throughput_rps", 0) > 0:
        cpu_per_request = metrics["cpu_percent"] / metrics["throughput_rps"]
        
    # 2. Little's Law Residual: active_connections - (rps * latency/1000)
    littles_residual = metrics["active_connections"] - (metrics["throughput_rps"] * (metrics["response_time_ms"] / 1000.0))
    
    # 3. Memory leak slope (linear regression over last hour)
    memory_slope = 0
    if len(history["memory_percent"]) > 10:
        y = np.array(history["memory_percent"])
        x = np.arange(len(y))
        slope, _ = np.polyfit(x, y, 1)
        memory_slope = slope
        
    # 4. Tail skew (Approximated: assume response_time_ms is P99, we need P50, but we just use P99 - Mean of history)
    tail_skew = 0
    if len(history["response_time_ms"]) > 5:
        mean_rt = np.mean(history["response_time_ms"])
        tail_skew = metrics["response_time_ms"] - mean_rt

    engineered = {
        "timestamp": raw["timestamp"],
        "service_name": raw["service_name"],
        "instance_id": raw["instance_id"],
        "region": raw["region"],
        "raw_metrics": metrics,
        "derived_features": {
            "cpu_per_request": round(cpu_per_request, 4),
            "littles_law_residual": round(littles_residual, 2),
            "memory_leak_slope": round(memory_slope, 4),
            "tail_skew": round(tail_skew, 2)
        }
    }
    return engineered

def main():
    print("Starting Feature Engineering Engine...")
    for message in consumer:
        raw_event = message.value
        try:
            engineered = compute_derived_features(raw_event)
            producer.send("engineered-features", value=engineered)
            print(f"[{raw_event['timestamp']}] Engineered features for {raw_event['service_name']}")
        except Exception as e:
            print(f"Error computing features: {e}")

if __name__ == "__main__":
    main()
