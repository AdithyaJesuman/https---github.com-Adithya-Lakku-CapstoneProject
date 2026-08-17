# Person 3 — Knowledge Graph, Multi-Agent System & Digital Twin

**You own:** Architecture Layers 5, 6, 7 — this is the hardest and most novel part of the project
**Your folders:** `services/knowledge-graph/`, `services/multi-agent/`, `services/digital-twin/`
**You produce:** `incidents-diagnosed` Kafka topic
**Read first:** `docs/ARCHITECTURE.md` sections 1, 3.5, 3.6, 3.7

**Everything runs locally via Ollama — zero cost, no API keys, no internet dependency once the model is pulled.** Recommended model: `llama3.1:8b` (good balance of speed/quality on a normal laptop) or `mistral:7b` if you want something lighter. If your machine has a decent GPU, you can size up to `llama3.1:70b` for noticeably better reasoning quality.

---

## What You're Building

1. A Neo4j knowledge graph mapping service dependencies (who depends on what)
2. A multi-agent reasoning system — 8 specialized local LLM calls (via Ollama) that collaborate on diagnosis
3. A digital twin simulator that tests fixes before they touch production

This is the part that makes the whole project novel — no commercial tool (Datadog, Splunk, Dynatrace) does this. Take your time getting the agent orchestration right.

---

## The 8 Agents (What Each One Does)

| Agent | Input | Output |
|-------|-------|--------|
| Monitoring Agent | Raw anomaly event | "Is this genuinely abnormal? Severity?" |
| Diagnosis Agent | Anomaly + logs + graph context | Probable root cause + confidence |
| Forecast Agent | Diagnosis + forecast data | "What happens if we do nothing?" |
| Planner Agent | Root cause | 2-3 ranked candidate fixes |
| Risk Agent | Candidate fixes + graph blast radius | Risk score per fix |
| Validator Agent | Twin simulation results | Safe to execute? Yes/No + confidence |
| Executor Agent | Approved fix | Executes (or hands to Person 4's executor) |
| (Consensus check) | All agent outputs | Do they agree? If not, escalate |

**Important nuance from earlier discussion:** the LLM should NOT be the sole decision-maker. Each agent reasons over evidence produced by deterministic systems (Person 1 and 2's ML models), not raw vibes. The LLM explains and plans — the risk engine (Person 4) has final say on execution.

**Local model note:** an 8B local model is meaningfully weaker at structured JSON output and multi-step reasoning than a hosted frontier model. Compensate for this with: (1) keep each agent's prompt narrow and single-purpose rather than asking for everything at once, (2) always use `format="json"` mode, (3) validate every response against the JSON schema before trusting it, (4) if validation fails twice, escalate to human rather than guessing. This is actually good practice regardless of model choice — it's just more load-bearing here.

---

## Week-by-Week Plan

### Week 1-2: Knowledge Graph Foundation
- [ ] Set up Neo4j (via Docker Compose, shared infra)
- [ ] Design the schema: `(:Service)-[:DEPENDS_ON]->(:Service)`, `(:Service)-[:HAD_INCIDENT]->(:Incident)`
- [ ] Manually seed a test topology (5-10 services with realistic dependencies — e.g. payment-api → postgres, payment-api → redis, checkout-service → payment-api)
- [ ] Build a query function: given a service name, return `depends_on`, `depended_on_by`, and `blast_radius` (2-hop reachability)
- [ ] Expose this as a simple API matching `shared/schemas/graph_context.schema.json`

### Week 3-4: Agent Framework
- [ ] Install Ollama (ollama.com), pull a model: `ollama pull llama3.1:8b`
- [ ] Confirm it responds: `ollama run llama3.1:8b "say hello in JSON"`
- [ ] Build the Monitoring Agent and Diagnosis Agent first — get one working end-to-end before adding more
- [ ] Design your prompt templates carefully — each agent should get ONLY the evidence it needs, not a giant dump of everything
- [ ] Add the Forecast Agent and Planner Agent
- [ ] Test: feed it a synthetic "DB connection pool exhaustion" scenario, confirm the agents correctly diagnose it

### Week 5-6: Risk Agent, Validator, Consensus
- [ ] Add the Risk Agent — uses knowledge graph blast radius as input
- [ ] Add the Validator Agent — this one reads digital twin results (coordinate with your own Week 7 work, may need to stub twin results early)
- [ ] Build the consensus check: if agents disagree significantly, force escalation rather than picking one answer
- [ ] Publish final diagnosis to Kafka topic `incidents-diagnosed` matching the schema

### Week 7-8: Digital Twin Simulation
- [ ] Build a lightweight "twin" — this does NOT need to be a full infrastructure clone for MVP. A configurable simulator that models connection pools, queues, and basic queueing theory (Little's Law) is enough to demonstrate the concept
- [ ] Given a candidate fix (e.g. "increase pool 100→150"), simulate the effect on queue wait time and error rate using the formulas from the metrics guide
- [ ] Output simulation results matching `shared/schemas/twin_simulation_result.schema.json`
- [ ] Wire the twin into the Validator Agent's decision process
- [ ] Integration test: full pipeline from anomaly → diagnosis → twin simulation → final recommendation

---

## Claude Code Prompts (Copy-Paste Ready)

**Prompt 1 — Knowledge graph setup:**
```
Build a Neo4j-based service dependency graph in services/knowledge-graph/ that:
- Connects to Neo4j via the neo4j Python driver
- Has a schema.py that creates constraints for (:Service {name}) and
  (:Incident {id}) nodes, with relationships [:DEPENDS_ON] and [:HAD_INCIDENT]
- Has a seed_data.py that creates a realistic test topology: payment-api
  depends on postgres-primary and redis-cache; checkout-service depends on
  payment-api and inventory-service; order-service depends on payment-api
  and notification-service (add a few more to make it interesting)
- Exposes a function get_graph_context(service_name) that returns depends_on
  (direct dependencies), depended_on_by (direct dependents), and blast_radius
  (everything reachable within 2 hops), matching this JSON shape:
  [paste shared/schemas/graph_context.schema.json]
- Include a FastAPI endpoint GET /graph-context/{service_name} that returns this
```

**Prompt 2 — Multi-agent diagnosis system:**
```
Build a multi-agent incident diagnosis system in services/multi-agent/ using
Ollama's local REST API (pip install ollama, or plain requests calls to
http://localhost:11434/api/generate). Use model "llama3.1:8b". Build these
agents as separate functions, each making its own local Ollama call:

1. monitoring_agent(anomaly_event) -> returns severity assessment and whether
   this warrants further investigation, as JSON {is_significant, severity, reasoning}

2. diagnosis_agent(anomaly_event, log_anomalies, graph_context) -> given the
   raw evidence, identify the most likely root cause. Prompt should instruct
   Claude to reason ONLY from the evidence provided, not general knowledge,
   and to return JSON {root_cause, confidence, evidence_used}

3. forecast_agent(diagnosis, forecast_event) -> given the diagnosis and forecast
   data, explain what happens if no action is taken. Return JSON
   {time_to_failure_seconds, business_impact_description}

4. planner_agent(diagnosis, graph_context) -> generate 2-3 candidate fixes as
   JSON {candidate_fixes: [{action, params, estimated_success_rate, reversible,
   rollback_time_seconds}]}. Ground suggestions in the known incident archetypes
   (paste the archetype table from ARCHITECTURE.md section 6 into the system prompt)

5. risk_agent(candidate_fixes, graph_context) -> for each fix, assess risk using
   blast_radius from graph_context. Return JSON with risk_score (0-1) per fix

Each function should call Ollama with format="json" (Ollama supports forcing
JSON-mode output natively — use this instead of relying on prompt instructions
alone), a clear system prompt establishing the agent's single responsibility,
and a low temperature (0.1-0.2) for more consistent, less creative reasoning
on production infra decisions. Parse the JSON response, and include error
handling for malformed responses (retry once with a stricter prompt, then
fall back to escalate-to-human — local models are more prone to malformed
JSON than hosted frontier models, so this fallback path matters more here).

Example Ollama call pattern to use as the base for every agent:
import requests
def call_agent(system_prompt, user_content, model="llama3.1:8b"):
    response = requests.post("http://localhost:11434/api/generate", json={
        "model": model,
        "system": system_prompt,
        "prompt": user_content,
        "format": "json",
        "stream": False,
        "options": {"temperature": 0.15}
    })
    return response.json()["response"]

Then build an orchestrator function diagnose_incident(anomaly_event,
log_anomalies, forecast_event) that calls agents in sequence, aggregates their
outputs, computes an overall confidence (average of agent confidences), and
publishes the final result to Kafka topic "incidents-diagnosed" matching:
[paste shared/schemas/agent_diagnosis.schema.json]
```

**Prompt 3 — Consensus and disagreement handling:**
```
Add a consensus checker to services/multi-agent/consensus.py that:
- Takes the outputs of all agents from the diagnosis pipeline
- Computes an agreement score: how much do the agents' confidence levels align?
- If the standard deviation of agent confidences exceeds 0.15, OR if any single
  agent's confidence is below 0.6, flag the incident as "LOW_CONSENSUS" and
  force escalation regardless of the average confidence
- Otherwise mark as "HIGH_CONSENSUS" and allow the pipeline to proceed toward
  auto-heal evaluation
- Log the full reasoning trail (what each agent said) for dashboard display later
```

**Prompt 4 — Digital twin simulator:**
```
Build a lightweight digital twin simulator in services/digital-twin/ that
models infrastructure behavior using queueing theory rather than a full
infrastructure clone (this is sufficient for MVP). It should:

- Accept a current state snapshot: {db_pool_size, db_connections_active,
  avg_query_time_ms, arrival_rate_rps}
- Accept a candidate fix: {action: "increase_db_pool_size", params: {to: 150}}
  or {action: "scale_db_instances", params: {to: 3}}
- Simulate the effect using Little's Law (connections ≈ throughput × latency)
  and basic queueing theory (M/M/c queue approximation) to predict: new average
  wait time, new error rate (requests that would still time out), and whether
  the system reaches a stable state or continues to degrade
- Run the simulation for a configurable duration (default 300 simulated seconds)
  in discrete time steps
- Return results matching: [paste shared/schemas/twin_simulation_result.schema.json]
- Include a side-effect detector: if the fix requires >20% more memory or
  introduces replication lag (for the scale_db_instances case, simulate a
  simple replication lag model), flag it in the side_effects array

Include unit tests: a pool-exhaustion scenario where increasing pool size
should resolve it, and a case where the traffic is too high for any pool
size increase to help (should correctly report simulation_success: false)
```

---

## Definition of Done (Week 8 Checklist)

- [ ] Knowledge graph returns correct blast radius for test topology
- [ ] All 5 core agents (Monitoring, Diagnosis, Forecast, Planner, Risk) working end-to-end
- [ ] Consensus checker correctly forces escalation on a deliberately ambiguous test case
- [ ] Digital twin correctly simulates pool exhaustion fix and shows improvement
- [ ] Digital twin correctly identifies a fix that WON'T work (traffic too high)
- [ ] Full pipeline test: anomaly event in → final diagnosis + simulation result out
- [ ] Diagnosis published to Kafka matches schema exactly
- [ ] Diagnosis latency per incident is acceptable for a demo (local models are slower than hosted APIs — measure this early, consider a smaller/quantized model if it's too slow for a live demo)
