# System Architecture — Full Spec

This is the contract. If your service follows the input/output schemas here, your AI-generated code will connect to everyone else's AI-generated code without manual glue work.

---

## 1. Full Layer Diagram

```
LAYER 0: DATA COLLECTION                     [Person 1]
├─ OpenTelemetry Collectors
├─ Kafka topic: raw-metrics
└─ InfluxDB (raw storage)

LAYER 1: FEATURE ENGINEERING                 [Person 1 + Person 2]
├─ Rolling stats, derivatives, EWMA
├─ Kafka topic: engineered-features
└─ Derived features (tail_skew, cpu_per_request, etc.)

LAYER 2: ANOMALY DETECTION                   [Person 1]
├─ Isolation Forest + RRCF ensemble
├─ Kafka topic: anomalies-detected
└─ Output: anomaly event JSON (schema below)

LAYER 3: FORECASTING                         [Person 2]
├─ Prophet + PatchTST
├─ Kafka topic: forecasts
└─ Output: "failure predicted in X min, confidence Y%"

LAYER 4: LOG + TRACE INTELLIGENCE            [Person 2]
├─ LogBERT / DeepLog embeddings
├─ Kafka topic: log-anomalies
└─ Output: log anomaly event JSON

LAYER 5: KNOWLEDGE GRAPH                     [Person 3]
├─ Neo4j: service dependency map
├─ Query API: blast radius, related incidents
└─ Output: dependency context JSON

LAYER 6: MULTI-AGENT REASONING               [Person 3]
├─ Ollama (local Llama 3 / Mistral) — 8 agents (Monitoring, Diagnosis, Forecast, Planner, Risk, Validator, Executor)
├─ Input: anomaly + forecast + log + graph context
└─ Output: incident diagnosis + ranked fix options JSON

LAYER 7: DIGITAL TWIN SIMULATION             [Person 3]
├─ Lightweight infra clone (Docker-based)
├─ Input: candidate fix from Layer 6
└─ Output: simulation result JSON (success/fail, side effects, confidence)

LAYER 8: POLICY + RISK ENGINE                [Person 4]
├─ Confidence thresholds, cooldowns, corroboration checks
├─ Input: simulation result
└─ Output: ALLOW_AUTO_HEAL / ESCALATE_TO_HUMAN

LAYER 9: SELF-HEALING EXECUTION              [Person 4]
├─ Executes approved actions (scale, restart, reroute)
└─ Output: execution result JSON

LAYER 10: INCIDENT MEMORY                    [Person 4]
├─ ChromaDB vector search
├─ Stores every resolved incident
└─ Retrieves similar past incidents for new ones

LAYER 11: TRAINING DATA MANAGEMENT           [Person 1 + Person 4]
├─ Normal Metrics DB (train on this ONLY)
├─ Incident Logs DB (never train on this)
└─ Gate: exclude incident window ± 30 min from training data

DASHBOARD (cross-cutting)                    [Person 4]
└─ Grafana: live metrics, active incidents, agent reasoning trail
```

---

## 2. Data Flow (End to End)

```
Real service running (e.g. a test Flask app)
   ↓
Person 1: Collector reads CPU/memory/latency every 10s
   ↓ Kafka: raw-metrics
Person 1/2: Feature engineering computes derived features
   ↓ Kafka: engineered-features
Person 1: Anomaly detector flags abnormal pattern
   ↓ Kafka: anomalies-detected
Person 2: Forecaster adds "will get worse in X min" context
   ↓ Kafka: forecasts
Person 2: Log intelligence adds relevant log anomalies
   ↓ Kafka: log-anomalies
Person 3: Knowledge graph adds "what depends on this service"
   ↓ (direct query, not Kafka)
Person 3: Multi-agent system reasons over ALL of the above
   ↓ produces ranked candidate fixes
Person 3: Digital twin simulates top fix candidates
   ↓ produces confidence + side-effect report
Person 4: Policy engine decides AUTO_HEAL or ESCALATE
   ↓
Person 4: Executor runs the fix (if approved) or alerts human
   ↓
Person 4: Incident stored in memory (ChromaDB)
   ↓
Person 4: Dashboard shows the whole trail
```

---

## 3. JSON Schemas — The Contract Between Everyone

Save these in `shared/schemas/`. Every service reads/writes exactly this shape. If you need a new field, add it here first.

### 3.1 `raw_metric.schema.json` (Person 1 produces, everyone can read)
```json
{
  "timestamp": "2026-08-08T14:32:00Z",
  "service_name": "payment-api",
  "instance_id": "pod-payment-api-7f8b",
  "region": "us-east-1",
  "metrics": {
    "cpu_percent": 87.3,
    "memory_percent": 62.1,
    "response_time_ms": 340,
    "error_rate": 2.1,
    "throughput_rps": 1450,
    "db_query_time_ms": 120,
    "queue_depth": 45,
    "active_connections": 312
  },
  "metadata": {
    "deployment_id": "deploy-2026-08-08-v1.4.2",
    "feature_flags_active": ["new_checkout_flow"]
  }
}
```

### 3.2 `anomaly_event.schema.json` (Person 1 produces → Person 3 consumes)
```json
{
  "anomaly_id": "ANOM-20260808-001",
  "timestamp": "2026-08-08T14:32:15Z",
  "service_name": "payment-api",
  "detector": "isolation_forest",
  "confidence": 0.94,
  "severity": "high",
  "triggering_metrics": ["cpu_percent", "queue_depth"],
  "raw_values": {"cpu_percent": 95.2, "queue_depth": 1200},
  "baseline_values": {"cpu_percent": 42.0, "queue_depth": 30}
}
```

### 3.3 `forecast_event.schema.json` (Person 2 produces → Person 3 consumes)
```json
{
  "forecast_id": "FCST-20260808-001",
  "service_name": "payment-api",
  "predicted_incident_type": "db_connection_pool_exhaustion",
  "time_to_incident_seconds": 300,
  "confidence": 0.88,
  "evidence": ["connection count climbing 5/min", "traffic forecast +30% in 3hr"]
}
```

### 3.4 `log_anomaly.schema.json` (Person 2 produces → Person 3 consumes)
```json
{
  "log_anomaly_id": "LOG-20260808-001",
  "service_name": "payment-api",
  "timestamp": "2026-08-08T14:32:10Z",
  "pattern": "connection pool exhausted",
  "raw_log_line": "ERROR: could not obtain connection from pool within 30000ms",
  "anomaly_score": 0.91
}
```

### 3.5 `graph_context.schema.json` (Person 3's knowledge graph → Person 3's agents)
```json
{
  "service_name": "payment-api",
  "depends_on": ["postgres-primary", "redis-cache"],
  "depended_on_by": ["checkout-service", "order-service"],
  "blast_radius": ["checkout-service", "order-service", "notification-service"],
  "similar_past_incidents": ["INC-2026-01-15-001"]
}
```

### 3.6 `agent_diagnosis.schema.json` (Person 3's multi-agent output → Person 4's policy engine)
```json
{
  "incident_id": "INC-20260808-001",
  "root_cause": "Database connection pool (size 100) exhausted under traffic spike",
  "confidence": 0.97,
  "agent_consensus": {
    "monitoring_agent": 0.99,
    "diagnosis_agent": 0.97,
    "forecast_agent": 0.96,
    "risk_agent": 0.95
  },
  "candidate_fixes": [
    {
      "action": "increase_db_pool_size",
      "params": {"from": 100, "to": 150},
      "estimated_success_rate": 0.98,
      "reversible": true,
      "rollback_time_seconds": 30
    },
    {
      "action": "scale_db_instances",
      "params": {"from": 2, "to": 3},
      "estimated_success_rate": 0.90,
      "reversible": true,
      "rollback_time_seconds": 300
    }
  ]
}
```

### 3.7 `twin_simulation_result.schema.json` (Person 3's digital twin → Person 4's policy engine)
```json
{
  "incident_id": "INC-20260808-001",
  "fix_tested": "increase_db_pool_size",
  "simulation_success": true,
  "predicted_error_rate_after": 0.1,
  "predicted_latency_after_ms": 300,
  "side_effects": [],
  "confidence": 0.96
}
```

### 3.8 `policy_decision.schema.json` (Person 4's policy engine output)
```json
{
  "incident_id": "INC-20260808-001",
  "decision": "AUTO_HEAL",
  "reasoning": "Confidence 0.96 > threshold 0.95, reversible, corroborated by 3 signals",
  "action_approved": "increase_db_pool_size",
  "requires_human": false
}
```

### 3.9 `incident_memory_record.schema.json` (Person 4's ChromaDB storage format)
```json
{
  "incident_id": "INC-20260808-001",
  "timestamp": "2026-08-08T14:32:00Z",
  "symptoms": {"error_rate": 15.2, "cpu_percent": 95, "queue_depth": 1200},
  "root_cause": "Database connection pool exhaustion",
  "fix_applied": "increase_db_pool_size",
  "outcome": "resolved",
  "time_to_resolution_seconds": 240,
  "embedding_text": "high CPU, high queue depth, DB pool exhaustion, traffic spike triggered"
}
```

---

## 4. Kafka Topics (the message bus everyone shares)

| Topic | Producer | Consumer |
|-------|----------|----------|
| `raw-metrics` | Person 1 (collector) | Person 1 (anomaly), Person 2 (forecast/features) |
| `engineered-features` | Person 1/2 | Person 1 (anomaly detector) |
| `anomalies-detected` | Person 1 | Person 3 (multi-agent) |
| `forecasts` | Person 2 | Person 3 (multi-agent) |
| `log-anomalies` | Person 2 | Person 3 (multi-agent) |
| `incidents-diagnosed` | Person 3 | Person 4 (policy engine) |
| `policy-decisions` | Person 4 | Person 4 (executor), Dashboard |
| `execution-results` | Person 4 | Person 4 (incident memory), Dashboard |

---

## 5. Training Data Management Rule (Critical — Everyone Must Follow)

```
Raw Metrics (everything, from Layer 0)
        ↓
   Was this a healthy period?
   (no active alert + no open incident + within 2σ of baseline + no deployment happening)
        ↓
   YES → Normal Metrics DB   → Person 1 retrains anomaly models on THIS ONLY
   NO  → Incident Logs DB    → Person 4 uses for RCA/memory, NEVER for training

Rule: exclude incident window + 30 min before + 30 min after from training data.

If you retrain on incident data, the model learns "98% CPU is normal" and
stops detecting real problems. This is the single most common mistake in AIOps.
```

---

## 6. The 6 Incident Archetypes (Test Your System Against These)

| Archetype | Chain | Correct Fix | Wrong Fix |
|-----------|-------|-------------|-----------|
| A. CPU Saturation | Traffic↑ → CPU↑ → Latency↑ → Errors↑ | Scale out early | Wait for error_rate confirmation |
| B. Memory Leak | Memory slope↑ → GC pauses → OOM → Restart | Staggered graceful restart on slope | Restart 100% fleet simultaneously |
| C. DB Pool Exhaustion | Slow queries → Pool fills → Errors | Kill long queries + widen pool | Scale app instances (adds contention) |
| D. Disk I/O Saturation | WAL writes → Disk queue → DB slow | Check disk before CPU/memory | Assume CPU/memory without checking disk |
| E. Healing Side Effect | Restart → cold cache → temp DB spike | Suppress alert during warm-up | Trigger second healing action |
| F. Retry Storm | Errors↑ → Retries → RPS↑ → More errors | Rate-limit at edge | Scale out (feeds the storm) |

Use these to write your test cases in Week 6-7.

---

## 7. Guardrails (Non-Negotiable for Person 4's Policy Engine)

- Debounce: require 3+ consecutive breaches, not 1 sample
- Cooldown: no repeat action on same target within 5 min
- Corroboration: require 2+ independent signals agreeing, not 1 metric alone
- Blast radius limiting: stagger fleet actions, never 100% at once
- Human escalation required for: schema changes, DB failover, anything touching data correctness
