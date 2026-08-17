# Person 4 — Policy Engine, Incident Memory & Dashboard

**You own:** Architecture Layers 8, 9, 10, 11
**Your folders:** `services/policy-engine/`, `services/incident-memory/`, `services/dashboard/`
**You consume:** `incidents-diagnosed` topic from Person 3
**Read first:** `docs/ARCHITECTURE.md` sections 1, 3.6, 3.7, 3.8, 3.9, 5, 7

---

## What You're Building

1. The policy/risk engine — the safety gate that decides AUTO_HEAL vs ESCALATE_TO_HUMAN
2. The self-healing executor — actually runs approved fixes (with rollback capability)
3. Persistent incident memory using ChromaDB — vector search over past incidents
4. The Grafana dashboard — the thing everyone will see in the demo
5. Co-own the training data gating rule with Person 1

**You are the last line of defense before this system touches "production."** Be paranoid. A wrong auto-heal decision is worse than no automation at all.

---

## The Guardrails You Must Implement (Non-Negotiable)

1. **Confidence threshold**: no auto-heal below 95% overall confidence
2. **Debounce**: require 3+ consecutive confirming signals, not a single sample
3. **Cooldown**: no repeat action on the same target within 5 minutes
4. **Corroboration**: require 2+ independent signals agreeing (e.g. metric anomaly AND log anomaly), never act on one signal alone
5. **Blast radius limiting**: never apply an action to 100% of a fleet simultaneously — stagger
6. **Reversibility requirement**: every auto-heal action must have a defined rollback plan before it's allowed to execute
7. **Human escalation always required for**: schema changes, database failover, anything touching data correctness (e.g. routing reads to a replica with lag)

---

## Week-by-Week Plan

### Week 1-2: ChromaDB + Basic Storage
- [ ] Set up ChromaDB (Docker or local)
- [ ] Design the incident memory schema (see `agent_diagnosis` and `incident_memory_record` schemas)
- [ ] Build the storage function: takes a resolved incident, creates an embedding from symptoms + root cause text, stores it
- [ ] Build the retrieval function: given current symptoms, find the top-5 most similar past incidents
- [ ] Test with 10-20 synthetic historical incidents

### Week 3-4: Policy Engine Core
- [ ] Build the confidence threshold gate
- [ ] Build the debounce/cooldown state tracker (needs to persist state — use Redis or a simple DB table)
- [ ] Build the corroboration checker — requires input from both Person 1 (metric anomaly) and Person 2 (log anomaly / forecast) to count as corroborated
- [ ] Wire it to consume from `incidents-diagnosed` Kafka topic (Person 3's output)
- [ ] Output `policy_decision` events matching the schema

### Week 5-6: Executor + Rollback
- [ ] Build the executor — for MVP, this can act on your own digital twin / test environment rather than real infra (coordinate with Person 3)
- [ ] Every action must log a rollback plan before executing
- [ ] Build the staggering logic for fleet-wide actions (never restart 100% at once)
- [ ] Publish execution results to Kafka, feed successful resolutions into Incident Memory (Layer 10)
- [ ] Test the full loop: diagnosis → policy decision → execution → memory storage

### Week 7-8: Dashboard + Training Data Co-ownership
- [ ] Set up Grafana connected to InfluxDB (Person 1's data) and your policy decision logs
- [ ] Build panels: live metrics, active incidents list, agent reasoning trail (pull from Person 3's consensus logs), incident memory search UI
- [ ] Work with Person 1 to finalize the Normal DB / Incident DB gating — you're the consumer of Incident Logs DB for RCA display
- [ ] Final integration: run the whole system end-to-end, confirm dashboard shows the complete trail from anomaly to resolution

---

## Claude Code Prompts (Copy-Paste Ready)

**Prompt 1 — Incident memory with ChromaDB:**
```
Build an incident memory system in services/incident-memory/ using chromadb
(pip install chromadb) that:
- Creates a persistent ChromaDB collection called "incidents"
- Has a store_incident(incident_record) function that takes data matching
  shared/schemas/incident_memory_record.schema.json, builds an embedding_text
  string combining symptoms + root_cause + fix_applied, and stores it with
  all fields as metadata
- Has a find_similar_incidents(current_symptoms, top_k=5) function that
  embeds the current symptom description and returns the most similar past
  incidents with their similarity scores, root causes, and what fix worked
- Has an auto_apply_recommendation(current_symptoms) function that returns
  a recommendation ONLY if the top match has similarity > 0.9 AND that fix's
  historical success rate (computed from all past incidents with that fix) is
  > 0.95, otherwise returns a lower-confidence suggestion requiring approval
- Include a seed script that generates 20 synthetic historical incidents
  covering the 6 archetypes from ARCHITECTURE.md section 6, so there's data
  to test retrieval against
- Include a FastAPI endpoint POST /find-similar that takes symptoms and
  returns matches
```

**Prompt 2 — Policy and risk engine:**
```
Build a policy engine in services/policy-engine/ that:
- Consumes from Kafka topic "incidents-diagnosed" (schema:
  [paste shared/schemas/agent_diagnosis.schema.json])
- Also consumes twin simulation results from Kafka topic (coordinate exact
  topic name with Person 3) matching
  shared/schemas/twin_simulation_result.schema.json
- Implements these gates, ALL of which must pass for AUTO_HEAL:
  1. confidence >= 0.95 (from the agent diagnosis)
  2. twin simulation predicted success = true
  3. the candidate fix is marked reversible = true
  4. corroboration: at least 2 independent evidence sources contributed to
     the diagnosis (check agent_consensus fields are all present and above 0.7)
  5. cooldown check: no action taken on this service_name in the last 5 minutes
     (maintain this state in Redis or a simple SQLite table)
  6. NOT in the human-required category: block auto-heal if the fix action
     involves schema changes, database failover, or read-routing changes
- If all gates pass: decision = "AUTO_HEAL". Otherwise: decision = "ESCALATE_TO_HUMAN"
  with clear reasoning about which gate failed
- Publish decision matching shared/schemas/policy_decision.schema.json
- Include unit tests for each gate independently (should block on low confidence,
  should block on cooldown violation, should block on non-reversible fix, etc.)
```

**Prompt 3 — Executor with rollback:**
```
Build a self-healing executor in services/policy-engine/executor.py that:
- Consumes "policy_decisions" where decision == "AUTO_HEAL"
- Before executing, records a rollback snapshot: for a pool-size change, record
  the current pool size; for a scaling action, record current instance count
- Executes the action (for MVP, this modifies state in a mock/test environment
  — a simple JSON config file or in-memory service registry representing
  "current infrastructure state" is sufficient, does not need real Kubernetes)
- For fleet-wide actions (e.g. restart), stagger execution: apply to 25% of
  targets, wait 30 seconds, check health, then proceed to next 25% only if
  healthy
- After execution, waits a configurable verification window (default 60s),
  then checks whether the target metric improved (query InfluxDB)
- If not improved within the window, automatically triggers rollback using
  the saved snapshot, and marks the incident as "auto_heal_failed" requiring
  human review
- If improved, marks as "resolved" and publishes to Kafka topic
  "execution-results" for Person 4's incident memory to consume
- Include a test simulating both a successful heal and a failed heal requiring
  rollback
```

**Prompt 4 — Grafana dashboard setup:**
```
Set up a Grafana dashboard configuration in services/dashboard/ that:
- Connects to InfluxDB as a data source (metrics from Person 1's collector)
- Has a panel showing live cpu_percent, memory_percent, response_time_ms,
  error_rate for the last hour, per service_name
- Has a panel listing active incidents (query a Postgres/SQLite table you
  maintain that tracks incident status: detected, diagnosing, simulating,
  awaiting_approval, auto_healing, resolved, escalated)
- Has a panel showing the agent reasoning trail for the currently selected
  incident — pull from a logs table where Person 3's agent outputs are stored
  (coordinate with Person 3 on where they log this)
- Has a panel for incident memory: a simple search box (can be a separate
  small web page/API since Grafana panels are limited for this) that lets
  you type symptoms and see similar past incidents
- Export the dashboard as JSON so it can be version controlled and provisioned
  automatically via docker compose
- Include a docker compose service block for Grafana with the InfluxDB
  datasource and dashboard auto-provisioned on startup
```

---

## Definition of Done (Week 8 Checklist)

- [ ] ChromaDB stores and retrieves incidents by similarity correctly
- [ ] Policy engine correctly blocks auto-heal when confidence < 95%
- [ ] Policy engine correctly blocks auto-heal when cooldown is active
- [ ] Policy engine correctly requires human approval for schema/DB-failover actions
- [ ] Executor successfully runs a fix and verifies improvement
- [ ] Executor successfully rolls back a fix that didn't improve metrics
- [ ] Fleet actions are staggered, never applied to 100% simultaneously
- [ ] Dashboard shows live metrics, active incidents, and reasoning trail
- [ ] Full end-to-end demo runs via `docker compose up`: inject anomaly → watch it get diagnosed, simulated, decided, executed, and stored in memory — all visible on the dashboard
