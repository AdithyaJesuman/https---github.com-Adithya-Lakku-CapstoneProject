# Person 2 — Forecasting & Log Intelligence

**You own:** Architecture Layers 3, 4
**Your folders:** `services/forecasting/`, `services/log-intelligence/`
**You produce:** `forecasts` and `log-anomalies` Kafka topics
**Read first:** `docs/ARCHITECTURE.md` sections 1, 3.3, 3.4

---

## What You're Building

1. A forecasting engine that predicts incidents 1-2 hours before they happen (not just detects them)
2. A log intelligence pipeline that finds anomalous log patterns and connects them to metric anomalies
3. Support for Person 1's feature engineering (you two will co-own some of the derived features)

---

## Your Forecasting Stack

| Horizon | Tool | Use Case |
|---------|------|----------|
| Short-term (minutes) | TimesFM or simple exponential smoothing | "Will breach threshold in next 10 min" |
| Medium-term (hours) | PatchTST (HuggingFace) | "Connection pool will exhaust in 3 hours" |
| Long-term (days) | Prophet | Capacity planning, seasonal trends |

**Start with Prophet — it's the easiest to get working in Week 1. Add PatchTST once Prophet is solid.**

---

## Incident Prediction Signals (What You're Looking For)

| Signal | Leads By | What It Predicts |
|--------|----------|-------------------|
| CPU climbing >5%/min sustained | 15-30 min | Capacity wall approaching |
| Queue depth building | 5-10 min | Backend slowness incoming |
| Error rate accelerating (2nd derivative) | 3-5 min | Cascading failure starting |
| Connection pool utilization >80% and rising | 5-10 min | Pool exhaustion |
| Memory slope positive & sustained, traffic flat | 1-4 hr | Memory leak → eventual OOM |
| P99 latency spiking while P50 flat | 2-5 min | Localized problem (one bad host/shard) |

---

## Week-by-Week Plan

### Week 1-2: Prophet Baseline
- [ ] Consume historical data from InfluxDB (Person 1's collector output)
- [ ] Train Prophet on `throughput_rps` and `response_time_ms` for capacity forecasting
- [ ] Build a simple linear regression for memory leak slope detection (traffic-independent)
- [ ] Get one working forecast: "at current trend, X will breach threshold in Y minutes"

### Week 3-4: Feature Engineering (Shared with Person 1)
- [ ] Coordinate with Person 1 — pick up remaining derived features from the master list not yet built
- [ ] Build: `db_contribution_ratio`, `time_since_last_deploy`, `error_budget_burn_rate`, `headroom`, `http_status_kl_divergence`
- [ ] Add these features to the `engineered-features` Kafka topic your and Person 1 both write to

### Week 5-6: PatchTST + Multi-Signal Prediction
- [ ] Integrate PatchTST from HuggingFace for medium-term forecasting
- [ ] Build the 6 prediction signals table above as actual code — each one checks its own condition and produces a probability + lead time
- [ ] Combine signals into a single forecast event, publish to Kafka topic `forecasts` matching the schema
- [ ] Test against the 6 incident archetypes in `ARCHITECTURE.md` §6 — can you predict at least Archetypes A, B, and C before they fully manifest?

### Week 7-8: Log Intelligence
- [ ] Set up a simple log ingestion pipeline (read from a test app's logs, or synthetic log generator)
- [ ] Build log embedding using a pretrained sentence transformer (simpler than full LogBERT for MVP — upgrade later if time allows)
- [ ] Detect anomalous log lines (e.g. "connection pool exhausted", "OOM killed") using embedding distance from a "normal logs" baseline
- [ ] Publish log anomalies to Kafka topic `log-anomalies` matching the schema
- [ ] Correlate log anomalies with metric anomalies by timestamp — this is what Person 3's agents will use for root cause evidence

---

## Claude Code Prompts (Copy-Paste Ready)

**Prompt 1 — Prophet baseline forecaster:**
```
Build a forecasting service in services/forecasting/prophet_forecaster.py that:
- Queries InfluxDB for the last 7 days of throughput_rps and response_time_ms
  for a given service_name
- Trains a Prophet model on each metric with hourly seasonality and weekly
  seasonality enabled
- Produces a forecast for the next 4 hours
- Exposes a function predict_time_to_threshold_breach(metric_name, threshold)
  that returns estimated minutes until the metric crosses the threshold, or
  None if not projected to breach within the forecast window
- Include a scheduled job that retrains the Prophet model daily
- Write a test using synthetic data with a known upward trend, verify the
  threshold breach prediction is roughly correct
```

**Prompt 2 — Multi-signal incident prediction:**
```
Build a multi-signal predictor in services/forecasting/signal_predictor.py that
implements these 6 detectors, each returning a probability (0-1) and estimated
lead time in seconds:
1. cpu_climbing: sustained CPU growth rate > 5%/min over last 10 minutes
2. queue_building: queue_depth trending up over last 5 minutes, rate of change positive
3. error_accelerating: second derivative of error_rate is positive and above threshold
4. pool_exhaustion_risk: db connection pool utilization > 80% and rising
5. memory_leak_risk: memory_percent has positive slope over 1 hour AND
   throughput_rps is flat/cyclical (not correlated with the memory rise)
6. localized_problem: P99 latency rising while P50 stays flat (ratio-based check)

Each detector consumes from Kafka topic "engineered-features". When any
detector fires above 0.7 probability, publish a forecast event to Kafka topic
"forecasts" matching this schema: [paste shared/schemas/forecast_event.schema.json]

Include unit tests for each detector using synthetic time series that should
and shouldn't trigger it.
```

**Prompt 3 — Log intelligence pipeline:**
```
Build a log anomaly detection service in services/log-intelligence/ that:
- Reads log lines from a Kafka topic "raw-logs" (JSON: {timestamp, service_name,
  level, message})
- Uses sentence-transformers (all-MiniLM-L6-v2) to embed each log message
- Maintains a rolling baseline of "normal" log embeddings from the last 24 hours
- Flags a log line as anomalous if its embedding distance from the nearest
  baseline cluster exceeds a threshold (use cosine distance)
- Also flags known bad patterns via simple string matching as a fast-path:
  "connection pool exhausted", "OOM", "timeout", "deadlock", "out of memory"
- Publishes anomalies to Kafka topic "log-anomalies" matching this schema:
  [paste shared/schemas/log_anomaly.schema.json]
- Include a synthetic log generator script for testing that produces normal
  logs plus occasional injected anomaly patterns
```

**Prompt 4 — Remaining derived features:**
```
Add these feature calculations to the shared feature engineering module
(coordinate location with Person 1, likely services/collector/features.py):
- db_contribution_ratio: db_query_time_ms / response_time_ms
- time_since_last_deploy: seconds since the most recent deployment_id change
- error_budget_burn_rate: error_rate / allowed_error_rate (assume allowed_error_rate
  = 1.0 unless configured per service)
- headroom: capacity_rps (configurable per service, default 5000) - throughput_rps
- http_status_kl_divergence: KL divergence between current 5-minute window's
  status code distribution and a rolling 1-hour baseline distribution
Include unit tests for each with known input/output pairs.
```

---

## Definition of Done (Week 8 Checklist)

- [ ] Prophet forecasts throughput and latency 4 hours ahead
- [ ] Threshold breach prediction gives a reasonable ETA (tested against synthetic trending data)
- [ ] All 6 multi-signal predictors implemented and unit tested
- [ ] At least one signal correctly predicts an injected incident 5+ minutes before it fully manifests
- [ ] Log anomaly detector catches injected "connection pool exhausted" pattern
- [ ] Log anomalies and metric anomalies can be correlated by timestamp (verified manually)
- [ ] All Kafka messages validate against `shared/schemas/`
- [ ] Coordinated with Person 1 — no duplicate feature engineering work
