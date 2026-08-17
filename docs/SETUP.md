# Setup Guide

## 1. Prerequisites

- Docker + Docker Compose
- Python 3.11+
- Node.js 18+ (for Claude Code)
- Git

## 2. Install Claude Code (Everyone Does This)

```bash
npm install -g @anthropic-ai/claude-code
```

Verify: `claude --version`

## 3. Install Ollama (Powers Person 3's Multi-Agent System — Free, Local, No Keys)

1. Download and install from ollama.com (Mac/Linux/Windows all supported)
2. Pull the model everyone will use: `ollama pull llama3.1:8b`
3. Verify it works: `ollama run llama3.1:8b "say hello in JSON"`
4. Ollama runs a local server automatically on `http://localhost:11434` — no key, no account, no bill.

If your machine is slow running the 8B model, try `ollama pull mistral:7b` (lighter) or `ollama pull llama3.2:3b` (fastest, lower reasoning quality — fine for early testing).

**Note:** If you separately want to use Claude Code or Cursor to help *write* the project's code faster, that's a personal dev-tool choice and optional — it has nothing to do with the system itself, which runs at $0 using Ollama.

## 4. Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
# Ollama (local, no key needed — just confirm the URL matches your setup)
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=llama3.1:8b

# Infra (defaults work with docker compose, change only if you know why)
KAFKA_BROKER=localhost:9092
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=devtoken123
INFLUXDB_ORG=aiops
INFLUXDB_BUCKET=raw_metrics
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=devpassword123
CHROMADB_HOST=localhost
CHROMADB_PORT=8000
GRAFANA_URL=http://localhost:3000
```

**Never commit `.env` to git.** It's already in `.gitignore`.

## 5. Start Local Infrastructure

```bash
docker compose up -d
```

This starts:
| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Message bus between all services |
| Zookeeper | 2181 | Kafka dependency |
| InfluxDB | 8086 | Time series metrics storage |
| Neo4j | 7474 (UI), 7687 (bolt) | Knowledge graph |
| ChromaDB | 8000 | Vector DB for incident memory |
| Grafana | 3000 | Dashboard |
| Ollama | 11434 | Local LLM for multi-agent reasoning |

**Ollama tip:** running it natively on your host (rather than via Docker) is usually faster for local dev since it can access your GPU directly. Either way works — the Docker service is included for convenience/consistency across the team.

Verify everything is up: `docker compose ps` — all should show "Up" or "healthy".

Check UIs:
- InfluxDB: http://localhost:8086
- Neo4j browser: http://localhost:7474 (login: neo4j / devpassword123)
- Grafana: http://localhost:3000 (default login: admin/admin)

## 6. Each Person's Local Dev Loop

```bash
# Example for Person 1
cd services/collector
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt

# Open Claude Code in this folder
claude

# Paste your prompts from PERSON_1_DATA_DETECTION.md one at a time
```

Repeat the same pattern in your own service folder (`services/forecasting/`, `services/multi-agent/`, `services/policy-engine/`, etc).

## 7. Running Tests

```bash
# Unit tests per service
cd services/collector && python -m pytest

# Integration tests (once all 4 parts are connected — Week 6+)
cd tests/integration && python -m pytest
```

## 8. Common Issues

**Kafka connection refused:** Wait 30-60 seconds after `docker compose up` — Kafka takes longer to start than other services.

**Neo4j auth error:** Double check `NEO4J_PASSWORD` in `.env` matches `docker compose.yml`.

**Ollama responses slow or timing out:** local inference speed depends on your machine's CPU/GPU. If agent tests are too slow for comfortable iteration, switch to a smaller model (`llama3.2:3b`) during development and only test with the larger model before the final demo.

**Ollama "model not found":** run `ollama pull llama3.1:8b` again — the model download can be interrupted on slow connections.

**Schema validation failing:** Someone changed a field without updating `shared/schemas/`. Check git blame on the schema file, ping that person.

## 9. Shutting Down

```bash
docker compose down          # stop everything, keep data
docker compose down -v       # stop everything, WIPE all data (careful — use for a clean reset)
```
