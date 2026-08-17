import os
import json
import uuid
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from sentence_transformers import SentenceTransformer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'raw-logs',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load lightweight embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Known bad patterns (fast path)
KNOWN_BAD = ["connection pool exhausted", "oom", "timeout", "deadlock", "out of memory"]

# Mock normal baseline cluster centers (in reality, compute from Normal DB)
# For MVP, we just check against known bad patterns and extreme outlier distances
normal_baseline = np.random.rand(10, 384) # 384 is dimension of MiniLM

def cosine_distance(u, v):
    return 1.0 - (np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v)))

def analyze_log(log_event):
    message = log_event.get("message", "").lower()
    
    # 1. Fast Path
    for bad in KNOWN_BAD:
        if bad in message:
            return {
                "log_anomaly_id": f"LOG-{str(uuid.uuid4())[:8]}",
                "service_name": log_event.get("service_name", "unknown"),
                "timestamp": log_event.get("timestamp"),
                "pattern": bad,
                "raw_log_line": message,
                "anomaly_score": 0.95
            }
            
    # 2. Embedding Path (simplified for MVP)
    embedding = model.encode(message)
    distances = [cosine_distance(embedding, center) for center in normal_baseline]
    min_dist = min(distances)
    
    if min_dist > 0.8: # Threshold for anomaly
        return {
            "log_anomaly_id": f"LOG-{str(uuid.uuid4())[:8]}",
            "service_name": log_event.get("service_name", "unknown"),
            "timestamp": log_event.get("timestamp"),
            "pattern": "unknown_semantic_anomaly",
            "raw_log_line": message,
            "anomaly_score": min(1.0, min_dist)
        }
        
    return None

def main():
    print("Starting Log Intelligence...")
    for message in consumer:
        log_event = message.value
        try:
            anomaly = analyze_log(log_event)
            if anomaly:
                producer.send("log-anomalies", value=anomaly)
                print(f"📝 LOG ANOMALY DETECTED: {anomaly['pattern']} | Score: {anomaly['anomaly_score']}")
        except Exception as e:
            print(f"Error in log analyzer: {e}")

if __name__ == "__main__":
    main()
