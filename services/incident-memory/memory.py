"""
Updated memory.py — seeds the full 50+ incident corpus from shared/incident_corpus.py
"""
import os
import sys
import chromadb
from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
from typing import Dict, Any, List

# Allow importing from shared/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../shared"))
from incident_corpus import INCIDENT_CORPUS

CHROMADB_HOST = os.getenv("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", "8000"))

chroma_client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)
collection = chroma_client.get_or_create_collection(name="incidents")

app = FastAPI(title="Incident Memory API")


class IncidentRecord(BaseModel):
    incident_id: str
    timestamp: str
    symptoms: Dict[str, Any]
    root_cause: str
    fix_applied: str
    outcome: str
    time_to_resolution_seconds: int


class SymptomsQuery(BaseModel):
    symptoms_text: str
    top_k: int = 5


@app.post("/store")
def store_incident(record: IncidentRecord):
    embedding_text = (
        f"Symptoms: {record.symptoms}. "
        f"Root cause: {record.root_cause}. "
        f"Fix applied: {record.fix_applied}. "
        f"Outcome: {record.outcome}."
    )
    collection.upsert(
        documents=[embedding_text],
        metadatas=[{
            "incident_id": record.incident_id,
            "timestamp": record.timestamp,
            "root_cause": record.root_cause,
            "fix_applied": record.fix_applied,
            "outcome": record.outcome,
            "time_to_resolution_seconds": record.time_to_resolution_seconds
        }],
        ids=[record.incident_id]
    )
    return {"status": "success", "incident_id": record.incident_id}


@app.post("/find-similar")
def find_similar_incidents(query: SymptomsQuery):
    results = collection.query(
        query_texts=[query.symptoms_text],
        n_results=min(query.top_k, collection.count() or 1)
    )
    matches = []
    if results["documents"] and results["documents"][0]:
        for i in range(len(results["documents"][0])):
            matches.append({
                "document": results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
                "distance": results["distances"][0][i] if results["distances"] else None
            })
    return {"matches": matches}


@app.get("/health")
def health():
    return {"status": "ok", "incident_count": collection.count()}


def seed_corpus():
    """Seeds the full 50+ incident corpus if collection is empty."""
    if collection.count() == 0:
        print(f"Seeding {len(INCIDENT_CORPUS)} incidents from corpus...")
        for inc in INCIDENT_CORPUS:
            record = IncidentRecord(
                incident_id=inc["incident_id"],
                timestamp=inc["timestamp"],
                symptoms=inc["symptoms"],
                root_cause=inc["root_cause"],
                fix_applied=inc["fix_applied"],
                outcome=inc["outcome"],
                time_to_resolution_seconds=inc["time_to_resolution_seconds"]
            )
            store_incident(record)
        print(f"Seed complete — {collection.count()} incidents in memory.")
    else:
        print(f"ChromaDB already has {collection.count()} incidents. Skipping seed.")


if __name__ == "__main__":
    try:
        seed_corpus()
    except Exception as e:
        print(f"Warning: Could not seed corpus: {e}")
    uvicorn.run(app, host="0.0.0.0", port=8002)
