"""
Extended agents.py — expanded diagnosis rules and fix playbook
covering all 15+ failure archetypes in the incident corpus.
"""
import os
import json
import logging
from typing import Dict, Any, Optional, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("multi-agent")

KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")


# ── Monitoring Agent ─────────────────────────────────────────────────────────
def monitoring_agent(anomaly: Dict[str, Any]) -> Dict[str, Any]:
    confidence: float = anomaly.get("confidence", 0.0)
    triggers: List[str] = anomaly.get("triggering_metrics", [])
    severity: str = anomaly.get("severity", "low")
    is_significant = confidence >= 0.6 and len(triggers) > 0
    reasoning = (
        f"conf={confidence:.2f}, triggers={triggers}, sev={severity}. "
        f"{'Escalating.' if is_significant else 'Below threshold — dropped.'}"
    )
    logger.info("[MonitoringAgent] %s", reasoning)
    return {"is_significant": is_significant, "severity": severity, "reasoning": reasoning}


# ── Diagnosis Agent — 15-archetype rule table ─────────────────────────────────
# Rules ordered by specificity (most specific first). Evaluated top-to-bottom;
# first matching rule wins.
_DIAGNOSIS_RULES: List[Dict[str, Any]] = [
    # ── Extreme-value / unique-pattern rules (highest specificity) ────────────
    # SSL: error_rate=100 AND zero connections = cert expiry pattern
    {
        "requires_all": ["error_rate", "active_connections"],
        "value_check": lambda raw: raw.get("error_rate", 0) >= 99 and raw.get("active_connections", 999) < 5,
        "root_cause": "ssl_certificate_expiry",
        "confidence": 0.97
    },
    # OOM kill: memory=100 AND throughput=0 (pod went down)
    {
        "requires_all": ["memory_percent", "throughput_rps"],
        "value_check": lambda raw: raw.get("memory_percent", 0) >= 99 and raw.get("throughput_rps", 999) < 5,
        "root_cause": "kubernetes_pod_oom_killed",
        "confidence": 0.97
    },
    # Network partition: error_rate very high AND response_time very high
    {
        "requires_all": ["error_rate", "response_time_ms"],
        "value_check": lambda raw: raw.get("error_rate", 0) >= 70 and raw.get("response_time_ms", 0) >= 10000,
        "root_cause": "network_partition",
        "confidence": 0.93
    },
    # Traffic spike: cpu + connections + error all extreme
    {
        "requires_all": ["cpu_percent", "active_connections", "error_rate"],
        "value_check": lambda raw: raw.get("throughput_rps", 0) >= 8000,
        "root_cause": "traffic_spike_thundering_herd",
        "confidence": 0.92
    },
    # Queue spike: high queue BUT cpu is low (pure consumer lag, not cpu-caused)
    {
        "requires_all": ["queue_depth", "cpu_percent"],
        "value_check": lambda raw: raw.get("queue_depth", 0) >= 300 and raw.get("cpu_percent", 100) < 40,
        "root_cause": "message_queue_backpressure",
        "confidence": 0.94
    },
    # Cache stampede: cpu high + db_query_time high + throughput very high
    {
        "requires_all": ["cpu_percent", "db_query_time_ms"],
        "value_check": lambda raw: raw.get("throughput_rps", 0) >= 2500,
        "root_cause": "cache_stampede",
        "confidence": 0.91
    },
    # Deployment regression: moderate error_rate + moderate latency + moderate cpu
    {
        "requires_all": ["error_rate", "response_time_ms", "cpu_percent"],
        "value_check": lambda raw: (
            15 <= raw.get("error_rate", 0) <= 60
            and 500 <= raw.get("response_time_ms", 0) <= 2000
            and raw.get("cpu_percent", 0) < 80
            and raw.get("memory_percent", 0) < 85
        ),
        "root_cause": "deployment_regression",
        "confidence": 0.88
    },
    # Replication lag: db slow but no other metrics extreme
    {
        "requires_all": ["db_query_time_ms", "response_time_ms"],
        "value_check": lambda raw: (
            raw.get("db_query_time_ms", 0) >= 3000
            and raw.get("error_rate", 100) < 15
            and raw.get("cpu_percent", 100) < 50
        ),
        "root_cause": "read_replica_replication_lag",
        "confidence": 0.89
    },
    # Memory leak (without crash): memory high but NOT 100%, throughput ok
    {
        "requires_all": ["memory_percent", "throughput_rps"],
        "value_check": lambda raw: (
            90 <= raw.get("memory_percent", 0) < 99
            and raw.get("throughput_rps", 0) > 100
        ),
        "root_cause": "memory_leak_or_oom_risk",
        "confidence": 0.90
    },
    # Elevated error (without extreme latency or connections)
    {
        "requires_all": ["error_rate"],
        "value_check": lambda raw: (
            raw.get("error_rate", 0) >= 25
            and raw.get("response_time_ms", 10000) < 2000
            and raw.get("active_connections", 1000) < 800
        ),
        "root_cause": "elevated_error_rate",
        "confidence": 0.87
    },
    # CPU saturation (no high connections)
    {
        "requires_all": ["cpu_percent", "response_time_ms"],
        "value_check": lambda raw: (
            raw.get("cpu_percent", 0) >= 90
            and raw.get("active_connections", 1000) < 700
        ),
        "root_cause": "cpu_saturation",
        "confidence": 0.90
    },
    # Latency: slow response + low error rate + moderate load
    {
        "requires_all": ["response_time_ms", "active_connections"],
        "value_check": lambda raw: (
            raw.get("response_time_ms", 0) >= 1500
            and raw.get("error_rate", 100) < 15
        ),
        "root_cause": "latency_degradation",
        "confidence": 0.84
    },

    # ── Multi-signal combo rules ───────────────────────────────────────────────
    {
        "requires_all": ["active_connections", "error_rate"],
        "root_cause": "db_connection_pool_exhaustion",
        "confidence": 0.95
    },
    {
        "requires_all": ["active_connections", "response_time_ms"],
        "root_cause": "db_connection_pool_exhaustion",
        "confidence": 0.92
    },
    {
        "requires_all": ["cpu_percent", "queue_depth"],
        "root_cause": "cpu_saturation",
        "confidence": 0.93
    },
    {
        "requires_all": ["memory_percent", "error_rate"],
        "root_cause": "kubernetes_pod_oom_killed",
        "confidence": 0.94
    },
    {
        "requires_all": ["queue_depth", "error_rate"],
        "root_cause": "message_queue_backpressure",
        "confidence": 0.91
    },
    {
        "requires_all": ["db_query_time_ms", "response_time_ms"],
        "root_cause": "disk_io_saturation",
        "confidence": 0.89
    },
    {
        "requires_all": ["error_rate", "response_time_ms"],
        "root_cause": "third_party_api_timeout",
        "confidence": 0.82
    },
    # ── Single-signal fallback rules ──────────────────────────────────────────
    {
        "requires_any": ["active_connections"],
        "root_cause": "db_connection_pool_exhaustion",
        "confidence": 0.88
    },
    {
        "requires_any": ["memory_percent"],
        "root_cause": "memory_leak_or_oom_risk",
        "confidence": 0.85
    },
    {
        "requires_any": ["cpu_percent"],
        "root_cause": "cpu_saturation",
        "confidence": 0.83
    },
    {
        "requires_any": ["queue_depth"],
        "root_cause": "message_queue_backpressure",
        "confidence": 0.80
    },
    {
        "requires_any": ["db_query_time_ms"],
        "root_cause": "disk_io_saturation",
        "confidence": 0.78
    },
    {
        "requires_any": ["response_time_ms"],
        "root_cause": "latency_degradation",
        "confidence": 0.76
    },
    {
        "requires_any": ["error_rate"],
        "root_cause": "elevated_error_rate",
        "confidence": 0.75
    },
]



def diagnosis_agent(anomaly: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    triggers: List[str] = anomaly.get("triggering_metrics", [])
    raw_values: Dict[str, Any] = anomaly.get("raw_values", {})

    for rule in _DIAGNOSIS_RULES:
        value_check = rule.get("value_check")
        if "requires_all" in rule:
            if all(m in triggers for m in rule["requires_all"]):
                # If a value_check predicate exists, it must also pass
                if value_check is not None and not value_check(raw_values):
                    continue
                return {
                    "root_cause": rule["root_cause"],
                    "confidence": rule["confidence"],
                    "evidence_used": [f"{k}={v}" for k, v in raw_values.items()],
                    "rule_matched": f"ALL({rule['requires_all']})"
                }
        elif "requires_any" in rule:
            if any(m in triggers for m in rule["requires_any"]):
                if value_check is not None and not value_check(raw_values):
                    continue
                return {
                    "root_cause": rule["root_cause"],
                    "confidence": rule["confidence"],
                    "evidence_used": [f"{k}={v}" for k, v in raw_values.items()],
                    "rule_matched": f"ANY({rule['requires_any']})"
                }

    logger.warning("[DiagnosisAgent] No rule matched triggers: %s", triggers)
    return None


# ── Forecast Agent ────────────────────────────────────────────────────────────
_CRITICALITY: Dict[str, float] = {
    "cpu_percent": 100.0, "memory_percent": 100.0, "active_connections": 1000.0,
    "queue_depth": 150.0, "response_time_ms": 5000.0, "error_rate": 100.0,
    "db_query_time_ms": 10000.0,
}

def forecast_agent(anomaly: Dict[str, Any], diagnosis: Dict[str, Any]) -> Dict[str, Any]:
    raw_values: Dict[str, Any] = anomaly.get("raw_values", {})
    time_to_failure_seconds = 3600
    business_impact = "Low — degraded but not immediately failing."

    for metric, value in raw_values.items():
        cap = _CRITICALITY.get(metric)
        if cap and isinstance(value, (int, float)) and cap > 0:
            pct = value / cap
            if pct >= 0.98:
                time_to_failure_seconds = 60
                business_impact = f"CRITICAL — {metric} at {pct:.0%} of capacity. Imminent failure in <1 min."
                break
            elif pct >= 0.90:
                time_to_failure_seconds = min(time_to_failure_seconds, 300)
                business_impact = f"HIGH — {metric} at {pct:.0%}. Failure within minutes."
            elif pct >= 0.75:
                time_to_failure_seconds = min(time_to_failure_seconds, 900)
                business_impact = f"MEDIUM — {metric} at {pct:.0%}. Monitor closely."

    logger.info("[ForecastAgent] TTF=%ds — %s", time_to_failure_seconds, business_impact)
    return {"time_to_failure_seconds": time_to_failure_seconds, "business_impact_description": business_impact}


# ── Planner Agent — full playbook covering all 15+ archetypes ─────────────────
_FIX_PLAYBOOK: Dict[str, List[Dict[str, Any]]] = {
    "db_connection_pool_exhaustion": [
        {"action": "increase_db_pool_size", "params": {"from": 100, "to": 150}, "estimated_success_rate": 0.88, "reversible": True, "rollback_time_seconds": 30},
        {"action": "shed_non_critical_traffic", "params": {"traffic_percent": 25}, "estimated_success_rate": 0.75, "reversible": True, "rollback_time_seconds": 10},
    ],
    "cpu_saturation": [
        {"action": "horizontal_scale_out", "params": {"add_instances": 2}, "estimated_success_rate": 0.91, "reversible": True, "rollback_time_seconds": 120},
        {"action": "shed_non_critical_traffic", "params": {"traffic_percent": 30}, "estimated_success_rate": 0.80, "reversible": True, "rollback_time_seconds": 10},
    ],
    "memory_leak_or_oom_risk": [
        {"action": "staggered_restart", "params": {"batch_size_percent": 25}, "estimated_success_rate": 0.94, "reversible": False, "rollback_time_seconds": 0},
    ],
    "kubernetes_pod_oom_killed": [
        {"action": "increase_memory_limit", "params": {"from_mb": 512, "to_mb": 1024}, "estimated_success_rate": 0.90, "reversible": True, "rollback_time_seconds": 60},
        {"action": "staggered_restart", "params": {"batch_size_percent": 25}, "estimated_success_rate": 0.85, "reversible": False, "rollback_time_seconds": 0},
    ],
    "message_queue_backpressure": [
        {"action": "increase_consumer_replicas", "params": {"add_replicas": 3}, "estimated_success_rate": 0.85, "reversible": True, "rollback_time_seconds": 60},
    ],
    "latency_degradation": [
        {"action": "flush_cache_and_warm", "params": {}, "estimated_success_rate": 0.70, "reversible": True, "rollback_time_seconds": 5},
        {"action": "increase_db_pool_size", "params": {"from": 100, "to": 130}, "estimated_success_rate": 0.75, "reversible": True, "rollback_time_seconds": 30},
    ],
    "elevated_error_rate": [
        {"action": "staggered_restart", "params": {"batch_size_percent": 25}, "estimated_success_rate": 0.82, "reversible": False, "rollback_time_seconds": 0},
    ],
    "disk_io_saturation": [
        {"action": "migrate_to_faster_storage_tier", "params": {}, "estimated_success_rate": 0.95, "reversible": False, "rollback_time_seconds": 0},
    ],
    "network_partition": [
        {"action": "failover_to_secondary_region", "params": {}, "estimated_success_rate": 0.88, "reversible": True, "rollback_time_seconds": 300},
    ],
    "cache_stampede": [
        {"action": "flush_cache_and_warm", "params": {}, "estimated_success_rate": 0.92, "reversible": True, "rollback_time_seconds": 5},
    ],
    "deployment_regression": [
        {"action": "rollback_deployment", "params": {"to_version": "previous"}, "estimated_success_rate": 0.97, "reversible": True, "rollback_time_seconds": 120},
    ],
    "ssl_certificate_expiry": [
        {"action": "renew_ssl_certificate", "params": {}, "estimated_success_rate": 0.99, "reversible": False, "rollback_time_seconds": 0},
    ],
    "third_party_api_timeout": [
        {"action": "enable_circuit_breaker_fallback", "params": {"timeout_ms": 3000}, "estimated_success_rate": 0.85, "reversible": True, "rollback_time_seconds": 10},
    ],
    "traffic_spike_thundering_herd": [
        {"action": "horizontal_scale_out", "params": {"add_instances": 4}, "estimated_success_rate": 0.88, "reversible": True, "rollback_time_seconds": 120},
        {"action": "shed_non_critical_traffic", "params": {"traffic_percent": 40}, "estimated_success_rate": 0.80, "reversible": True, "rollback_time_seconds": 10},
    ],
    "cascading_failure_upstream_dependency": [
        {"action": "enable_circuit_breaker_fallback", "params": {"timeout_ms": 5000}, "estimated_success_rate": 0.83, "reversible": True, "rollback_time_seconds": 10},
    ],
    "read_replica_replication_lag": [
        {"action": "route_reads_to_primary", "params": {}, "estimated_success_rate": 0.95, "reversible": True, "rollback_time_seconds": 30},
    ],
    "log_volume_explosion_disk_full": [
        {"action": "rotate_and_compress_logs", "params": {}, "estimated_success_rate": 0.97, "reversible": True, "rollback_time_seconds": 5},
    ],
    "config_drift_mismatched_env_vars": [
        {"action": "reapply_config_from_git", "params": {}, "estimated_success_rate": 0.93, "reversible": True, "rollback_time_seconds": 60},
    ],
    "jvm_gc_pause_stop_the_world": [
        {"action": "tune_gc_settings", "params": {}, "estimated_success_rate": 0.70, "reversible": True, "rollback_time_seconds": 60},
    ],
}


def planner_agent(diagnosis: Dict[str, Any]) -> Dict[str, Any]:
    root_cause: str = diagnosis.get("root_cause", "")
    fixes = _FIX_PLAYBOOK.get(root_cause, [])
    if not fixes:
        logger.warning("[PlannerAgent] No playbook entry for: %s", root_cause)
        return {"candidate_fixes": []}
    ranked = sorted(fixes, key=lambda f: f["estimated_success_rate"], reverse=True)
    logger.info("[PlannerAgent] %d fix(es) for '%s'", len(ranked), root_cause)
    return {"candidate_fixes": ranked}


# ── Orchestrator ──────────────────────────────────────────────────────────────
def process_anomaly(anomaly: Dict[str, Any]) -> None:
    anomaly_id = anomaly.get("anomaly_id", "UNKNOWN")
    logger.info("=== Pipeline START: %s ===", anomaly_id)

    mon = monitoring_agent(anomaly)
    if not mon["is_significant"]:
        logger.info("[%s] Dropped by MonitoringAgent.", anomaly_id)
        return

    diag = diagnosis_agent(anomaly)
    if not diag:
        logger.warning("[%s] No diagnosis — escalating.", anomaly_id)
        return

    forecast = forecast_agent(anomaly, diag)
    plan = planner_agent(diag)

    output: Dict[str, Any] = {
        "incident_id": anomaly_id.replace("ANOM", "INC"),
        "root_cause": diag["root_cause"],
        "confidence": diag["confidence"],
        "rule_matched": diag.get("rule_matched", ""),
        "agent_consensus": {
            "monitoring_agent": 0.95 if mon["is_significant"] else 0.1,
            "diagnosis_agent": diag["confidence"],
            "forecast_agent": 0.90,
            "planner_agent": 0.90,
        },
        "forecast": forecast,
        "candidate_fixes": plan["candidate_fixes"],
    }

    producer.send("incidents-diagnosed", value=output)
    logger.info("=== Pipeline DONE: %s | root_cause=%s ===", output["incident_id"], output["root_cause"])


def main() -> None:
    """Main consumer loop. Kafka is initialized lazily here so logic functions
    remain importable without a running broker (needed for unit tests)."""
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError:
        logger.error("kafka-python not installed. Run: pip install kafka-python")
        return

    consumer = KafkaConsumer(
        "anomalies-detected",
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    producer_local = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    logger.info("Multi-Agent Brain started (rule-based, 15+ archetypes, no LLM).")
    for message in consumer:
        anomaly = message.value
        try:
            mon = monitoring_agent(anomaly)
            if not mon["is_significant"]:
                logger.info("[%s] Dropped.", anomaly.get("anomaly_id"))
                continue
            diag = diagnosis_agent(anomaly)
            if not diag:
                logger.warning("[%s] No diagnosis — escalating.", anomaly.get("anomaly_id"))
                continue
            forecast = forecast_agent(anomaly, diag)
            plan = planner_agent(diag)
            output = {
                "incident_id": anomaly.get("anomaly_id", "UNKNOWN").replace("ANOM", "INC"),
                "root_cause": diag["root_cause"],
                "confidence": diag["confidence"],
                "rule_matched": diag.get("rule_matched", ""),
                "agent_consensus": {
                    "monitoring_agent": 0.95 if mon["is_significant"] else 0.1,
                    "diagnosis_agent": diag["confidence"],
                    "forecast_agent": 0.90,
                    "planner_agent": 0.90,
                },
                "forecast": forecast,
                "candidate_fixes": plan["candidate_fixes"],
            }
            producer_local.send("incidents-diagnosed", value=output)
            logger.info("Published diagnosis: %s | %s", output["incident_id"], output["root_cause"])
        except Exception as e:
            logger.error("Pipeline error: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
