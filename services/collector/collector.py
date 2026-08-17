import os
import time
import json
import random
import uuid
import logging
from typing import Dict, Any, Optional
import psutil
from datetime import datetime, timezone
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("collector")

# Configuration
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
INFLUXDB_URL: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN: str = os.getenv("INFLUXDB_TOKEN", "devtoken123")
INFLUXDB_ORG: str = os.getenv("INFLUXDB_ORG", "aiops")
INFLUXDB_BUCKET: str = os.getenv("INFLUXDB_BUCKET", "raw_metrics")
SERVICE_NAME: str = "payment-api"
INSTANCE_ID: str = f"pod-{SERVICE_NAME}-{str(uuid.uuid4())[:4]}"
REGION: str = "us-east-1"
DEPLOYMENT_ID: str = "deploy-v1.4.2"

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Setup InfluxDB Client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Synthetic state for realistic random walks
state = {
    "response_time_ms": 300,
    "error_rate": 0.5,
    "throughput_rps": 1000,
    "db_query_time_ms": 100,
    "queue_depth": 10,
    "active_connections": 200,
    "anomaly_mode": None,
    "anomaly_ticks": 0
}

def simulate_metrics() -> Dict[str, Any]:
    """
    Simulates application metrics using a random walk and captures real system CPU/Memory.
    Periodically injects anomalies to generate synthetic incidents.
    
    Returns:
        Dict[str, Any]: A dictionary containing the simulated system and application metrics.
    """
    # Random walk
    state["throughput_rps"] = max(100, state["throughput_rps"] + random.randint(-50, 50))
    state["response_time_ms"] = max(50, state["response_time_ms"] + random.randint(-10, 10))
    state["error_rate"] = max(0.1, min(100.0, state["error_rate"] + random.uniform(-0.1, 0.1)))
    state["db_query_time_ms"] = max(20, state["db_query_time_ms"] + random.randint(-5, 5))
    state["queue_depth"] = max(0, state["queue_depth"] + random.randint(-2, 2))
    state["active_connections"] = max(50, state["active_connections"] + random.randint(-10, 10))

    # CPU/Mem from real system
    cpu_percent = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()
    memory_percent = mem.percent
    
    # Inject anomalies randomly
    if state["anomaly_mode"] is None and random.random() < 0.05:
        state["anomaly_mode"] = random.choice(["cpu_spike", "memory_leak", "db_pool_exhaustion"])
        state["anomaly_ticks"] = random.randint(10, 30)
        logger.warning(f"INJECTING ANOMALY: {state['anomaly_mode']}")
        
    if state["anomaly_mode"] is not None:
        if state["anomaly_mode"] == "cpu_spike":
            cpu_percent = min(100.0, cpu_percent + 80.0)
            state["response_time_ms"] += 500
            state["queue_depth"] += 100
        elif state["anomaly_mode"] == "memory_leak":
            memory_percent = min(100.0, memory_percent + (30 - state["anomaly_ticks"]) * 2)
        elif state["anomaly_mode"] == "db_pool_exhaustion":
            state["active_connections"] = min(1000, state["active_connections"] + 200)
            state["db_query_time_ms"] += 200
            state["error_rate"] += 10.0
            
        state["anomaly_ticks"] -= 1
        if state["anomaly_ticks"] <= 0:
            state["anomaly_mode"] = None
            logger.info("ANOMALY CLEARED")

    return {
        "cpu_percent": round(cpu_percent, 2),
        "memory_percent": round(memory_percent, 2),
        "response_time_ms": int(state["response_time_ms"]),
        "error_rate": round(state["error_rate"], 2),
        "throughput_rps": int(state["throughput_rps"]),
        "db_query_time_ms": int(state["db_query_time_ms"]),
        "queue_depth": int(state["queue_depth"]),
        "active_connections": int(state["active_connections"])
    }

def main() -> None:
    """
    Main loop for collecting metrics and publishing them to Kafka and InfluxDB.
    Runs indefinitely with a 10-second sleep interval.
    """
    logger.info("Starting Collector Service...")
    while True:
        try:
            timestamp: str = datetime.now(timezone.utc).isoformat()
            metrics: Dict[str, Any] = simulate_metrics()
            
            record: Dict[str, Any] = {
                "timestamp": timestamp,
                "service_name": SERVICE_NAME,
                "instance_id": INSTANCE_ID,
                "region": REGION,
                "metrics": metrics,
                "metadata": {
                    "deployment_id": DEPLOYMENT_ID,
                    "feature_flags_active": ["new_checkout_flow"]
                }
            }
            
            # Send to Kafka
            producer.send("raw-metrics", value=record)
            
            # Write to InfluxDB
            point = Point("system_metrics") \
                .tag("service_name", SERVICE_NAME) \
                .tag("instance_id", INSTANCE_ID) \
                .tag("region", REGION) \
                .tag("deployment_id", DEPLOYMENT_ID)
            
            for k, v in metrics.items():
                point.field(k, v)
                
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            
            logger.info(f"Published metrics: CPU {metrics['cpu_percent']}%, Mem {metrics['memory_percent']}%")
            
        except Exception as e:
            logger.error(f"Error publishing metrics: {e}", exc_info=True)
            
        time.sleep(10)

if __name__ == "__main__":
    psutil.cpu_percent(interval=None) # Initialize
    main()
