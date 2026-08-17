"""
Improved Anomaly Detector
- Uses a rolling buffer with periodic refit for adaptive learning
- Adds baseline tracking per metric for realistic confidence scoring
- Adds error_rate and db_query_time detection
- Adds proper logging
"""
import os
import json
import uuid
import logging
import numpy as np
from typing import Dict, Any, Optional, List
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("anomaly-detector")

KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
WARMUP_SAMPLES: int = 100   # more samples = better baseline model
REFIT_INTERVAL: int = 500   # refit model every N samples

consumer = KafkaConsumer(
    "engineered-features",
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

iso_forest = IsolationForest(n_estimators=200, contamination=0.04, random_state=42)
data_buffer: List[List[float]] = []
sample_count: int = 0
model_fitted: bool = False

# Rolling baselines for adaptive 3-sigma
_METRIC_HISTORY: Dict[str, deque] = {
    k: deque(maxlen=300) for k in
    ["cpu_percent", "memory_percent", "response_time_ms", "error_rate",
     "throughput_rps", "queue_depth", "active_connections", "db_query_time_ms"]
}

# Hard critical thresholds (always fire regardless of model state)
_HARD_THRESHOLDS: Dict[str, float] = {
    "cpu_percent":         95.0,
    "memory_percent":      95.0,
    "error_rate":          40.0,
    "active_connections":  950.0,
    "queue_depth":         100.0,
    "response_time_ms":    3000.0,
    "db_query_time_ms":    5000.0,
}

# Soft thresholds for sigma-based detection (3σ above rolling mean)
_SIGMA_METRICS: List[str] = ["response_time_ms", "db_query_time_ms", "error_rate", "queue_depth"]


def _build_vector(raw: Dict[str, Any], derived: Dict[str, Any]) -> List[float]:
    return [
        raw.get("cpu_percent", 0),
        raw.get("memory_percent", 0),
        raw.get("response_time_ms", 0),
        raw.get("error_rate", 0),
        raw.get("throughput_rps", 0),
        raw.get("queue_depth", 0),
        raw.get("active_connections", 0),
        raw.get("db_query_time_ms", 0),
        derived.get("cpu_per_request", 0),
        derived.get("memory_leak_slope", 0),
        derived.get("tail_skew", 0),
        derived.get("littles_law_residual", 0),
    ]


def _update_baselines(raw: Dict[str, Any]) -> None:
    for k in _METRIC_HISTORY:
        if k in raw:
            _METRIC_HISTORY[k].append(raw[k])


def _sigma_violations(raw: Dict[str, Any]) -> List[str]:
    """Returns metrics breaching 3 standard deviations above their rolling mean."""
    violations = []
    for metric in _SIGMA_METRICS:
        hist = _METRIC_HISTORY.get(metric)
        if hist and len(hist) >= 30:
            arr = np.array(hist)
            mean, std = arr.mean(), arr.std()
            if std > 0 and raw.get(metric, 0) > mean + 3 * std:
                violations.append(metric)
    return violations


def _hard_threshold_triggers(raw: Dict[str, Any]) -> List[str]:
    return [k for k, thresh in _HARD_THRESHOLDS.items() if raw.get(k, 0) >= thresh]


def detect_anomaly(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    global sample_count, model_fitted

    raw: Dict[str, Any] = event.get("raw_metrics", {})
    derived: Dict[str, Any] = event.get("derived_features", {})

    _update_baselines(raw)
    vector = _build_vector(raw, derived)
    data_buffer.append(vector)
    sample_count += 1

    # Adaptive refit
    if sample_count == WARMUP_SAMPLES:
        iso_forest.fit(data_buffer)
        model_fitted = True
        logger.info("IsolationForest fitted on initial %d samples.", WARMUP_SAMPLES)
    elif model_fitted and sample_count % REFIT_INTERVAL == 0:
        iso_forest.fit(data_buffer[-WARMUP_SAMPLES:])
        logger.info("IsolationForest re-fitted at sample %d.", sample_count)

    # Trim buffer to 2000 samples max
    if len(data_buffer) > 2000:
        data_buffer.pop(0)

    # --- Detection ---
    hard_triggers = _hard_threshold_triggers(raw)
    sigma_triggers = _sigma_violations(raw)

    iso_is_anomaly = False
    iso_score = 0.0
    if model_fitted:
        pred = iso_forest.predict([vector])[0]
        iso_score = iso_forest.decision_function([vector])[0]
        iso_is_anomaly = pred == -1

    is_anomaly = iso_is_anomaly or bool(hard_triggers) or bool(sigma_triggers)

    if not is_anomaly:
        return None

    # Merge all triggering signals
    all_triggers = list(set(hard_triggers + sigma_triggers))
    if not all_triggers and iso_is_anomaly:
        # ISO forest flagged but no named trigger — add whichever metrics are elevated
        if raw.get("cpu_percent", 0) > 75:    all_triggers.append("cpu_percent")
        if raw.get("queue_depth", 0) > 30:    all_triggers.append("queue_depth")
        if raw.get("error_rate", 0) > 5:      all_triggers.append("error_rate")
        if raw.get("response_time_ms", 0) > 800: all_triggers.append("response_time_ms")

    # Confidence: base on hard threshold breach + sigma count + iso score
    confidence = 0.5
    confidence += 0.15 * min(len(hard_triggers), 3)
    confidence += 0.10 * min(len(sigma_triggers), 2)
    if model_fitted:
        confidence += max(0.0, min(0.2, abs(iso_score)))
    confidence = round(min(1.0, confidence), 2)

    severity = "critical" if confidence >= 0.90 else "high" if confidence >= 0.75 else "medium"

    anomaly = {
        "anomaly_id": f"ANOM-{str(uuid.uuid4())[:8]}",
        "timestamp":  event.get("timestamp"),
        "service_name": event.get("service_name"),
        "instance_id": event.get("instance_id"),
        "detector": "ensemble_iso_3sigma_adaptive",
        "confidence": confidence,
        "severity": severity,
        "triggering_metrics": all_triggers or ["unknown"],
        "raw_values": {k: raw[k] for k in all_triggers if k in raw},
        "baseline_values": {
            k: round(float(np.mean(_METRIC_HISTORY[k])), 2)
            for k in all_triggers if k in _METRIC_HISTORY and len(_METRIC_HISTORY[k]) > 0
        }
    }
    return anomaly


def main() -> None:
    logger.info("Anomaly Detector started (adaptive Isolation Forest + 3σ + hard thresholds).")
    for message in consumer:
        event = message.value
        try:
            anomaly = detect_anomaly(event)
            if anomaly:
                producer.send("anomalies-detected", value=anomaly)
                logger.warning(
                    "ANOMALY [%s] svc=%s conf=%.2f sev=%s triggers=%s",
                    anomaly["anomaly_id"], anomaly["service_name"],
                    anomaly["confidence"], anomaly["severity"],
                    anomaly["triggering_metrics"]
                )
        except Exception as e:
            logger.error("Error in detection: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
