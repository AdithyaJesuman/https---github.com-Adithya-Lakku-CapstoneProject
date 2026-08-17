import os
import json
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sse_starlette.sse import EventSourceResponse
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

app = FastAPI(title="AIOps API Gateway")

# Configure CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In prod, restrict this to frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(os.path.join(os.path.dirname(__file__), "static"), exist_ok=True)
app.mount("/ui", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# We will create a producer for injecting simulated incidents
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Helper function to stream a specific Kafka topic
async def kafka_streamer(topic_name: str):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest" # Only want live data for UI
    )
    # Non-blocking yield loop
    while True:
        # poll for messages (timeout_ms is small so it doesn't block entirely, but we use sleep to yield to event loop)
        msgs = consumer.poll(timeout_ms=100)
        if msgs:
            for tp, messages in msgs.items():
                for message in messages:
                    yield {"event": "message", "data": json.dumps(message.value)}
        await asyncio.sleep(0.1)


@app.get("/stream/metrics")
async def stream_metrics():
    """Stream raw metrics for the dashboard ticker"""
    return EventSourceResponse(kafka_streamer("raw-metrics"))

@app.get("/stream/anomalies")
async def stream_anomalies():
    """Stream detected anomalies and agent diagnosis decisions"""
    return EventSourceResponse(kafka_streamer("incidents-diagnosed"))

@app.get("/stream/actions")
async def stream_actions():
    """Stream executed actions (post-mortems, etc)"""
    return EventSourceResponse(kafka_streamer("post-mortems"))

class InjectRequest(BaseModel):
    incident_type: str

@app.post("/inject")
async def inject_anomaly(request: InjectRequest):
    """
    Simulates a failure by pushing a highly anomalous metric event 
    directly to 'raw-metrics' to trigger the pipeline instantly.
    """
    import uuid
    import datetime
    
    mock_metric = {
        "event_id": f"EVT-{str(uuid.uuid4())[:8]}",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "service_name": "payment-api",
        "instance_id": "pod-demo-001",
        "region": "us-east",
        "cpu_percent": 30,
        "memory_percent": 40,
        "response_time_ms": 200,
        "throughput_rps": 500,
        "error_rate": 0,
        "active_connections": 200,
        "db_query_time_ms": 50,
        "queue_depth": 0
    }

    if request.incident_type == "db_connection_pool_exhaustion":
        mock_metric["active_connections"] = 999
        mock_metric["error_rate"] = 55.0
    elif request.incident_type == "cpu_saturation":
        mock_metric["cpu_percent"] = 99
        mock_metric["queue_depth"] = 250
    elif request.incident_type == "network_partition":
        mock_metric["error_rate"] = 90
        mock_metric["response_time_ms"] = 15000
    
    producer.send("raw-metrics", value=mock_metric)
    producer.flush()
    return {"status": "injected", "payload": mock_metric}

@app.get("/health")
def health():
    return {"status": "ok"}
