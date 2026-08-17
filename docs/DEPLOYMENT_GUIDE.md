# Deployment & Schema Configuration Guide

This document confirms the completion status of the 8-week MVP and outlines exactly how to deploy the platform to a production environment, including where configurations and database schemas are stored.

## 1. Project Completion Status
**Status: 100% Complete**
All features spanning the 8-week roadmap have been successfully implemented:
* **Weeks 1-2:** Kafka message bus, InfluxDB data collection, and shared JSON schemas.
* **Weeks 3-4:** Rolling feature engineering, Isolation Forest anomaly detection, Prophet forecasting, and the Deterministic Rule-Based Multi-Agent orchestrator.
* **Weeks 5-6:** Policy Engine (safety guardrails), Executor (healing actions), Incident Memory (ChromaDB), and Knowledge Graph (Neo4j).
* **Weeks 7-8:** Continuous Learning Loop, Causal Discovery Engine, Automated Post-Mortem RCAs, Log Intelligence, and Grafana Dashboard setup.

## 2. Deployment Configuration (Ports, Hosts, Passwords)

The entire platform is designed to be **Twelve-Factor App compliant**, meaning all configurations are handled via Environment Variables. 

### Local Deployment
To run locally, you only need Docker installed. Modern Docker uses `docker compose` instead of the older `docker-compose` binary. Run:
```bash
docker compose up -d --build
```
The python services default to finding `localhost` (or the internal docker network hostnames like `kafka` and `influxdb`) for all databases.

### Production / Remote Deployment

For a professional, production-grade deployment, this platform is designed to run on **Kubernetes (K8s)**. We have provided a unified Kubernetes deployment manifest in the root directory: `k8s-deployment.yaml`.

#### Deploying to Kubernetes:
1. Ensure you have a running K8s cluster (EKS, GKE, AKS, or Minikube).
2. Apply the manifest:
   ```bash
   kubectl apply -f k8s-deployment.yaml
   ```
3. The API Gateway (and AIOps Command Center) will be exposed on port `8000`. Get the external IP:
   ```bash
   kubectl get svc api-gateway-service
   ```

If you are deploying to a standard cloud VM instead of Kubernetes, you must provide the following environment variables to the Python microservice containers. You can set these in a `.env` file or directly in your CI/CD deployment manifests:

| Environment Variable | Default Value | Description |
|----------------------|---------------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | The host and port for the Kafka message broker. |
| `INFLUXDB_URL` | `http://localhost:8086` | The URL for InfluxDB. |
| `INFLUXDB_TOKEN` | `devtoken123` | The authentication token for InfluxDB (set securely in production). |
| `INFLUXDB_ORG` | `aiops` | The InfluxDB organization name. |
| `NEO4J_URI` | `bolt://localhost:7687` | The URI for the Neo4j Knowledge Graph. |
| `NEO4J_USER` | `neo4j` | The username for Neo4j. |
| `NEO4J_PASSWORD` | `devpassword123` | The password for Neo4j. |
| `CHROMADB_HOST` | `localhost` | The host for ChromaDB (Incident Memory). |
| `CHROMADB_PORT` | `8000` | The port for ChromaDB. |

*Note: The initial setup passwords for the databases themselves (InfluxDB, Neo4j, Grafana) are currently hardcoded in the `docker compose.yml`. You should change those values in `docker compose.yml` before deploying to production.*

## 3. Database Schemas

Unlike traditional SQL databases, our modern databases define their schemas dynamically through code. Here is exactly where you can find and modify the schemas for each database:

### A. Neo4j (Knowledge Graph)
* **Location:** `services/knowledge-graph/schema.py`
* **How it works:** Neo4j is schemaless by nature, but we enforce uniqueness constraints programmatically. When `schema.py` boots up, it runs Cypher queries to create constraints like `CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE`.
* **Nodes:** `Service`, `Incident`, `Action`.
* **Edges:** `DEPENDS_ON`, `HAD_INCIDENT`, `HAS_PROVEN_FIX`.

### B. ChromaDB (Vector Incident Memory)
* **Location:** `services/incident-memory/memory.py`
* **How it works:** We use Pydantic models in Python to enforce the schema before storing it in ChromaDB. 
* **Schema Definition:** Look for the `IncidentRecord` class in `memory.py`, which enforces fields like `incident_id`, `timestamp`, `symptoms`, `root_cause`, and `fix_applied`. The symptom strings are embedded as vectors.

### C. InfluxDB (Time Series Metrics)
* **Location:** `services/collector/collector.py` and `docker compose.yml`
* **How it works:** InfluxDB uses a "Measurement, Tag, Field" schema.
* **Buckets:** `raw_metrics`, `normal_metrics`, `incident_logs`. (Configured in `docker compose.yml` and `services/anomaly-detection/data_gate.py`).
* **Schema Definition:** In `collector.py`, the schema is dynamically generated via the dictionary returned by `simulate_metrics()`. It tracks tags like `service_name`, `instance_id`, `region`, and fields like `cpu_percent`, `memory_percent`, `response_time_ms`, etc.

### D. Kafka (Inter-Service Communication)
* **Location:** `shared/schemas/`
* **How it works:** Kafka topics (`raw-metrics`, `anomalies-detected`, etc.) strictly enforce JSON formats. We defined these rigid JSON schemas during Week 1. Any microservice publishing to Kafka must conform to the structures defined in this folder.
