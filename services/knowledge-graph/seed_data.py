from schema import driver, create_constraints

def seed_database():
    create_constraints()
    
    query = """
    // Clear existing data
    MATCH (n) DETACH DELETE n;
    
    // Create services
    CREATE (payment:Service {name: 'payment-api'})
    CREATE (postgres:Service {name: 'postgres-primary'})
    CREATE (redis:Service {name: 'redis-cache'})
    CREATE (checkout:Service {name: 'checkout-service'})
    CREATE (inventory:Service {name: 'inventory-service'})
    CREATE (order:Service {name: 'order-service'})
    CREATE (notification:Service {name: 'notification-service'})
    
    // Create relationships
    CREATE (payment)-[:DEPENDS_ON]->(postgres)
    CREATE (payment)-[:DEPENDS_ON]->(redis)
    CREATE (checkout)-[:DEPENDS_ON]->(payment)
    CREATE (checkout)-[:DEPENDS_ON]->(inventory)
    CREATE (order)-[:DEPENDS_ON]->(payment)
    CREATE (order)-[:DEPENDS_ON]->(notification)
    
    // Past Incident
    CREATE (inc1:Incident {id: 'INC-2026-01-15-001'})
    CREATE (payment)-[:HAD_INCIDENT]->(inc1)
    """
    
    with driver.session() as session:
        session.run(query)
        print("Knowledge Graph seeded with test topology.")

if __name__ == "__main__":
    seed_database()
