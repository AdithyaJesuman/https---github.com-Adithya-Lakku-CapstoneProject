# Master Todo List — All 8 Weeks, All 4 People

Check off as you go. Daily sync: review this file, flag blockers.

---

## WEEK 0: Setup (Do This Before Week 1 Starts)

- [x] Create Git repo, invite all 4 people
- [x] Everyone installs: Docker, Docker Compose, Python 3.11+, Node.js (for Claude Code)
- [x] Everyone installs Ollama (ollama.com) and runs `ollama pull llama3.1:8b`
- [x] Confirm Ollama responds locally: `ollama run llama3.1:8b "hello"`
- [x] (Optional, personal choice) install an AI coding assistant like Claude Code or Cursor to help write code faster — not required, system itself runs at $0 either way
- [x] Clone repo, run `docker compose up` — confirm Kafka, InfluxDB, Neo4j, ChromaDB, Grafana all start
- [x] Everyone reads `README.md` and `docs/ARCHITECTURE.md` fully (30 min)
- [x] Everyone reads their own `PERSON_X.md` file fully
- [x] Set up shared Slack/Discord channel + daily 15-min standup time
- [x] Agree on branch strategy (recommend: each person works in `feature/person-N-*` branches, PR into `main`)

---

## WEEK 1-2: Foundations

**Person 1**
- [x] InfluxDB schema designed
- [x] Collector script running, pushing to Kafka `raw-metrics`
- [x] Metrics validated against `shared/schemas/raw_metric.schema.json`

**Person 2**
- [x] Prophet trained on historical throughput/latency
- [x] Basic threshold-breach prediction working
- [x] Memory leak slope detector (simple version) working

**Person 3**
- [x] Neo4j running, schema created
- [x] Test topology seeded (5-10 services)
- [x] `get_graph_context()` function returns blast radius correctly

**Person 4**
- [x] ChromaDB running
- [x] Basic store/retrieve incident functions working
- [x] 20 synthetic historical incidents seeded

**Integration checkpoint (end of Week 2):**
- [x] Everyone's raw output validates against their JSON schema
- [x] Kafka topics are all created and reachable by all services
- [x] `docker compose up` starts everything without errors

---

## WEEK 3-4: Core Intelligence

**Person 1**
- [x] 6 derived features implemented and unit tested
- [x] Isolation Forest ensemble detector working
- [x] Anomaly events publishing to `anomalies-detected`

**Person 2**
- [x] Remaining derived features implemented (coordinate with Person 1 — no duplicates)
- [x] PatchTST integrated for medium-term forecasting
- [x] 6 multi-signal predictors built and unit tested

**Person 3**
- [x] Ollama running locally and responding correctly
- [x] Monitoring + Diagnosis agents working end-to-end
- [x] Forecast + Planner agents added
- [x] Test: synthetic DB pool exhaustion scenario correctly diagnosed

**Person 4**
- [x] Policy engine core gates implemented (confidence, cooldown, corroboration)
- [x] Consuming from `incidents-diagnosed` topic
- [x] Unit tests for each policy gate

**Integration checkpoint (end of Week 4):**
- [x] Anomaly detected by Person 1 → successfully triggers Person 3's Diagnosis Agent
- [x] Schema mismatches identified and fixed
- [x] Everyone demos their piece in isolation to the team (15 min each)

---

## WEEK 5-6: Novel Features

**Person 1**
- [x] Normal DB / Incident DB separation built
- [x] Gating logic tested (inject anomaly, confirm it lands in Incident DB not Normal DB)
- [x] Weekly retraining scheduled, confirmed pulling only from Normal DB

**Person 2**
- [x] Log intelligence pipeline built (embedding-based anomaly detection)
- [x] Log anomalies publishing to `log-anomalies`
- [x] Metric + log anomaly correlation demonstrated

**Person 3**
- [x] Risk Agent added (uses graph blast radius)
- [x] Validator Agent added
- [x] Consensus checker built — forces escalation on agent disagreement
- [x] Digital twin simulator built (queueing theory model)
- [x] Twin correctly simulates pool-exhaustion fix

**Person 4**
- [x] Executor built with rollback capability
- [x] Staggered fleet action logic implemented
- [x] Full loop tested: diagnosis → decision → execution → memory storage

**Integration checkpoint (end of Week 6):**
- [x] Full pipeline runs end-to-end for at least ONE incident archetype (recommend: Archetype C, DB pool exhaustion — it's the most concretely defined)
- [x] All 4 people review each other's schema outputs for correctness
- [x] Write down every bug/mismatch found — fix before Week 7

---

## WEEK 7-8: Integration, Dashboard, Demo Prep

**Person 1**
- [x] Load test: confirm detection within 30 seconds of injected failure
- [x] Fix any integration bugs found in Week 6 checkpoint

**Person 2**
- [x] Test forecasting against injected incidents — confirm 5+ min early warning
- [x] Fix any integration bugs found in Week 6 checkpoint

**Person 3**
- [x] Full pipeline integration test: anomaly → diagnosis → twin → recommendation
- [x] Diagnosis latency per incident measured and acceptable for demo purposes
- [x] Fix any integration bugs found in Week 6 checkpoint

**Person 4**
- [x] Grafana dashboard built: live metrics, active incidents, reasoning trail, memory search
- [x] Full end-to-end demo scripted and tested
- [x] Fix any integration bugs found in Week 6 checkpoint

**All 4 — Final Integration Days (last 3-4 days of Week 8):**
- [x] Full system runs via single `docker compose up`
- [x] Test against all 6 incident archetypes — how many are correctly handled end-to-end?
- [x] At least 1 incident auto-resolved with zero human intervention
- [x] At least 1 incident correctly escalated to human (low confidence test case)
- [x] Incident memory correctly recalls a "repeat" incident and suggests the proven fix
- [x] Dashboard tells the full story visually for a demo audience
- [x] Record a demo video as backup in case live demo has issues
- [x] Write the final README update with "how to run the demo" instructions

---

## Success Criteria (Recap From README — Check All Before Calling It Done)

- [x] System collects real metrics from a live local service
- [x] Anomaly detected within 30 seconds of injected failure
- [x] Root cause correctly identified for at least 3 of the 6 incident archetypes
- [x] Digital twin simulates at least one fix before real execution
- [x] At least one incident auto-resolved end-to-end without human intervention
- [x] Incident stored in memory and successfully retrieved on a simulated repeat incident
- [x] Dashboard shows live metrics + active incidents + agent reasoning trail
- [x] Full demo runnable via `docker compose up` in under 5 minutes

---

## Ongoing / Every Week

- [x] Daily 15-min standup — what's blocked, what schema needs to change
- [x] Commit code daily, even WIP — don't let AI-generated code sit uncommitted
- [x] Review each other's Claude Code output before merging — AI writes fast but makes mistakes
- [x] Update `shared/schemas/` immediately if any field changes — message the team, don't silently drift
- [x] If Ollama inference feels slow during development, consider switching to a smaller model temporarily — no cost concern either way, only speed
