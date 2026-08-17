import os
from neo4j import GraphDatabase
from fastapi import FastAPI, HTTPException
import uvicorn

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "devpassword123")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
app = FastAPI(title="Knowledge Graph API")

def create_constraints():
    with driver.session() as session:
        # Create constraint for Service names to be unique
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE")
        # Create constraint for Incident IDs to be unique
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Incident) REQUIRE i.id IS UNIQUE")
        print("Neo4j constraints created.")

def get_graph_context_query(tx, service_name):
    query = """
    MATCH (target:Service {name: $service_name})
    
    // Find what it depends on (1 hop outgoing)
    OPTIONAL MATCH (target)-[:DEPENDS_ON]->(dep:Service)
    WITH target, collect(DISTINCT dep.name) AS depends_on
    
    // Find what depends on it (1 hop incoming)
    OPTIONAL MATCH (target)<-[:DEPENDS_ON]-(depBy:Service)
    WITH target, depends_on, collect(DISTINCT depBy.name) AS depended_on_by
    
    // Blast radius: anything reachable within 2 hops incoming
    OPTIONAL MATCH (target)<-[:DEPENDS_ON*1..2]-(reach:Service)
    WITH target, depends_on, depended_on_by, collect(DISTINCT reach.name) AS blast_radius
    
    // Past incidents
    OPTIONAL MATCH (target)-[:HAD_INCIDENT]->(inc:Incident)
    WITH depends_on, depended_on_by, blast_radius, collect(DISTINCT inc.id) AS similar_past_incidents
    
    RETURN depends_on, depended_on_by, blast_radius, similar_past_incidents
    """
    result = tx.run(query, service_name=service_name)
    record = result.single()
    if not record:
        return None
        
    return {
        "service_name": service_name,
        "depends_on": record["depends_on"],
        "depended_on_by": record["depended_on_by"],
        "blast_radius": record["blast_radius"],
        "similar_past_incidents": record["similar_past_incidents"]
    }

@app.get("/graph-context/{service_name}")
def get_graph_context(service_name: str):
    with driver.session() as session:
        context = session.execute_read(get_graph_context_query, service_name)
        
    if not context:
        raise HTTPException(status_code=404, detail="Service not found in Knowledge Graph")
        
    return context

from pydantic import BaseModel

class LearnedFix(BaseModel):
    service_name: str
    action: str
    incident_id: str

@app.post("/learn-fix")
def learn_fix(fix: LearnedFix):
    """
    Continuous Learning Loop: Ingests a successful auto-heal action and links it to the service.
    This builds an automated "proven playbook" over time.
    """
    query = """
    MATCH (s:Service {name: $service_name})
    MERGE (a:Action {name: $action})
    MERGE (s)-[r:HAS_PROVEN_FIX]->(a)
    ON CREATE SET r.success_count = 1, r.last_incident = $incident_id
    ON MATCH SET r.success_count = r.success_count + 1, r.last_incident = $incident_id
    RETURN r.success_count AS count
    """
    with driver.session() as session:
        result = session.run(query, service_name=fix.service_name, action=fix.action, incident_id=fix.incident_id)
        record = result.single()
        count = record["count"] if record else 0
        
    print(f"🧠 [KNOWLEDGE GRAPH] Learned fix '{fix.action}' for '{fix.service_name}'. Success count: {count}")
    return {"status": "learned", "success_count": count}

if __name__ == "__main__":
    create_constraints()
    uvicorn.run(app, host="0.0.0.0", port=8001)
