# Person 1 — Data Collection & Anomaly Detection

**You own:** Architecture Layers 0, 1, 2
**Your folders:** `services/collector/`, `services/anomaly-detection/`
**You produce:** `raw-metrics` and `anomalies-detected` Kafka topics
**Read first:** `docs/ARCHITECTURE.md` sections 1, 3.1, 3.2, 5

---

## What You're Building

1. A metrics collector that reads real system stats every 10 seconds and pushes them to Kafka
2. A feature engineering module that computes derived features (tail skew, CPU-per-request, etc.)
3. An ensemble anomaly detector that flags abnormal patterns
4. The training data separation system (Normal DB vs Incident DB)

---

## Your Metrics List (What To Collect)

Core (must have):
`response_time_ms` (P50/P95/P99), `error_rate` (split 4xx/5xx), `throughput_rps`, `cpu_percent`, `memory_percent`, `queue_depth`, `db_query_time_ms`, `db_connections_active`, `cache_hit_ratio`, `active_connections`

OS-level (nice to have, add in Week 3+):
`load_average_1min`, `context_switches_per_sec`, `open_file_descriptors`, `tcp_connections`

Metadata (collect alongside every metric):
`timestamp`, `service_name`, `instance_id`, `region`, `deployment_id`, `feature_flags_active`

---

## Your Derived Features (Feature Engineering)

| Feature | Formula |
|---------|---------|
| `tail_skew` | P99 - P50 |
| `cpu_per_request` | cpu_percent / throughput_rps |
| `little_law_residual` | active_connections - (rps × latency_ms/1000) |
| `memory_leak_slope` | slope of memory_percent over 1hr, traffic-independent |
| `seasonal_zscore` | (current - baseline_same_hour) / MAD |
| `queue_wait_estimate` | queue_depth / processing_rate |

---

## Week-by-Week Plan

### Week 1-2: Collector + Storage
- [ ] Set up InfluxDB via Docker Compose (shared, but you own the schema)
- [ ] Write `collector.py` using `psutil` — reads CPU/memory/disk/network every 10s
- [ ] Add app-level metrics (response_time, error_rate, throughput) — hook into a test Flask app
- [ ] Push every reading to Kafka topic `raw-metrics` matching `shared/schemas/raw_metric.schema.json`
- [ ] Also write to InfluxDB for historical storage/dashboard use

### Week 3-4: Feature Engineering + Ensemble Detector
- [ ] Build rolling window stats (1min/5min/15min) on top of raw metrics
- [ ] Implement the 6 derived features above
- [ ] Train Isolation Forest on synthetic + real "normal" data
- [ ] Add a second detector (RRCF or simple 3-sigma) — ensemble the two, vote-based
- [ ] Publish anomaly events to Kafka topic `anomalies-detected` matching the schema

### Week 5-6: Training Data Separation (Critical)
- [ ] Build `Normal Metrics DB` — a filtered view/table containing only healthy-period data
- [ ] Build `Incident Logs DB` — stores anomaly periods + 30min before/after, separately
- [ ] Write the gating logic: a period is "healthy" if no active alert, no open incident, within 2σ of baseline, no deployment in progress
- [ ] Set up scheduled retraining (weekly) that pulls ONLY from Normal Metrics DB
- [ ] Test: inject a fake anomaly, confirm it does NOT end up in the retraining set

### Week 7-8: Streaming + Integration
- [ ] Wire everything through Kafka properly (not just batch scripts)
- [ ] Load test: simulate traffic spike, confirm detection within 30 seconds
- [ ] Confirm your `anomalies-detected` events are being correctly consumed by Person 3's agents
- [ ] Fix any schema mismatches found during integration testing

---

## Claude Code Prompts (Copy-Paste Ready)

**Prompt 1 — Collector:**
```
Build a Python metrics collector in services/collector/ that:
- Uses psutil to read cpu_percent, memory_percent, disk_io, network_io every 10 seconds
- Simulates app-level metrics (response_time_ms, error_rate, throughput_rps) with realistic
  random walk behavior, with occasional injected anomalies (CPU spike, memory leak) for testing
- Publishes each reading as JSON to a Kafka topic called "raw-metrics"
- The JSON shape must exactly match shared/schemas/raw_metric.schema.json
- Also writes each reading to InfluxDB using the influxdb-client library
- Include a Dockerfile and requirements.txt
```

**Prompt 2 — Feature engineering:**
```
Build a feature engineering module in services/collector/features.py that:
- Reads raw metrics from Kafka topic "raw-metrics"
- Computes rolling window stats (mean, std) over 1min/5min/15min windows
- Computes these derived features: tail_skew (P99-P50), cpu_per_request
  (cpu_percent/throughput_rps), little_law_residual (active_connections -
  throughput_rps*response_time_ms/1000), memory_leak_slope (linear regression
  slope of memory_percent over the last hour, independent of traffic changes),
  seasonal_zscore (compare current value to same-hour-of-week historical baseline
  using median absolute deviation)
- Publishes engineered features to Kafka topic "engineered-features"
- Include unit tests for each feature calculation with known input/output pairs
```

**Prompt 3 — Anomaly ensemble:**
```
Build an anomaly detection ensemble in services/anomaly-detection/ that:
- Consumes from Kafka topics "raw-metrics" and "engineered-features"
- Runs two detectors in parallel: sklearn IsolationForest and a simple
  3-sigma statistical detector on each core metric
- Combines them into an ensemble score (both must agree above their thresholds
  to fire, OR either one crosses a very high confidence threshold alone)
- When an anomaly fires, publishes to Kafka topic "anomalies-detected" with
  this exact JSON shape: [paste shared/schemas/anomaly_event.schema.json here]
- Include a train.py script that trains IsolationForest ONLY on data pulled
  from a "normal_metrics" InfluxDB bucket (never train on flagged anomaly periods)
- Include a retrain scheduler that runs weekly via cron or APScheduler
```

**Prompt 4 — Training data separation:**
```
Build a training data management system in services/anomaly-detection/data_gate.py that:
- Reads from the raw InfluxDB metrics bucket
- Classifies each 1-minute window as "healthy" or "incident" based on: no active
  alert during window, no open incident ticket, metric within 2 standard
  deviations of rolling baseline, no deployment_id change in the last 15 minutes
- Writes healthy windows to a "normal_metrics" InfluxDB bucket
- Writes incident windows (plus 30 min buffer before and after) to a separate
  "incident_logs" InfluxDB bucket
- Never allows incident_logs data to be queried by the training pipeline —
  add an assertion/guard that raises an error if train.py tries to read from
  incident_logs bucket
- Write a test that injects a synthetic anomaly and verifies it lands in
  incident_logs, NOT normal_metrics
```

---

## Definition of Done (Week 8 Checklist)

- [ ] Collector runs continuously, pushes to Kafka + InfluxDB
- [ ] At least 8 core metrics collected accurately
- [ ] 6 derived features computed correctly (verified with unit tests)
- [ ] Ensemble anomaly detector catches injected CPU spike within 30 seconds
- [ ] Ensemble anomaly detector catches injected memory leak (slope-based, before OOM)
- [ ] False positive rate under manual testing is acceptably low (<5% on stable traffic)
- [ ] Normal/Incident DB separation confirmed working via test
- [ ] Retraining only pulls from Normal DB — confirmed via code review
- [ ] All Kafka messages validate against the JSON schemas in `shared/schemas/`
