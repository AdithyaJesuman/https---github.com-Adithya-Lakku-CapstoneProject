import os
import json
import logging
from typing import Dict, Any, List
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("causal-engine")

KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'engineered-features',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class CausalDiscoveryEngine:
    """
    Analyzes streams of metrics to establish directional causality.
    Uses a simplified Granger Causality approximation for real-time streams.
    """
    def __init__(self, window_size: int = 100) -> None:
        self.window_size = window_size
        self.history: Dict[str, deque] = {
            "cpu_percent": deque(maxlen=window_size),
            "memory_percent": deque(maxlen=window_size),
            "response_time_ms": deque(maxlen=window_size),
            "active_connections": deque(maxlen=window_size),
            "error_rate": deque(maxlen=window_size)
        }
        
    def add_data(self, metrics: Dict[str, Any]) -> None:
        """Appends new metric values to the rolling history window."""
        for key in self.history.keys():
            if key in metrics:
                self.history[key].append(metrics[key])
                
    def check_causality(self, cause: str, effect: str) -> float:
        """
        Simplified Granger Causality approximation (Time-shifted cross-correlation).
        Checks if 'cause' occurring slightly earlier correlates with 'effect'.
        Returns a causality score between 0.0 and 1.0.
        """
        if len(self.history[cause]) < 50:
            return 0.0
            
        x = np.array(self.history[cause])
        y = np.array(self.history[effect])
        
        # Shift cause backward in time to see if it predicts effect
        # For simplicity, we just do a lag-1 correlation
        x_lag = x[:-1]
        y_curr = y[1:]
        
        if np.std(x_lag) == 0 or np.std(y_curr) == 0:
            return 0.0
            
        correlation = np.corrcoef(x_lag, y_curr)[0, 1]
        return max(0.0, float(correlation)) # Only care about positive causation for MVP

    def build_causal_graph(self) -> Dict[str, Any]:
        """Builds a Directed Acyclic Graph (DAG) of metric causality."""
        nodes = list(self.history.keys())
        edges = []
        
        for cause in nodes:
            for effect in nodes:
                if cause != effect:
                    score = self.check_causality(cause, effect)
                    if score > 0.6: # Threshold for causality
                        edges.append({
                            "from": cause,
                            "to": effect,
                            "weight": round(score, 2)
                        })
                        
        return {"nodes": nodes, "edges": edges}

def main() -> None:
    logger.info("Starting Causal Discovery Engine...")
    engine = CausalDiscoveryEngine()
    
    for message in consumer:
        event = message.value
        try:
            raw_metrics = event.get("raw_metrics", {})
            engine.add_data(raw_metrics)
            
            # Periodically compute and publish the causal graph
            if len(engine.history["cpu_percent"]) % 10 == 0 and len(engine.history["cpu_percent"]) > 50:
                graph = engine.build_causal_graph()
                if graph["edges"]:
                    logger.info(f"Causal Edges Detected: {graph['edges']}")
                    
                    producer.send("causal-graphs", value={
                        "timestamp": event["timestamp"],
                        "service_name": event["service_name"],
                        "graph": graph
                    })
        except Exception as e:
            logger.error(f"Error in causal engine: {e}", exc_info=True)

if __name__ == "__main__":
    main()
