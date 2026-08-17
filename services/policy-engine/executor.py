import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'policy-decisions',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def execute_action(incident_id, action):
    print(f"🔧 EXECUTING AUTO-HEAL: {action} for {incident_id}")
    
    # 1. Record Rollback Snapshot
    rollback_plan = f"Revert {action}"
    print(f"   [Snapshot recorded: {rollback_plan}]")
    
    # 2. Execute Staggered Action
    if "restart" in action or "scale" in action:
        print("   -> Applying to 25% of fleet...")
        time.sleep(2) # simulate API call
        print("   -> Health check passed. Applying to next 25%...")
        time.sleep(2)
        print("   -> Fleet rollout complete.")
    else:
        print("   -> Applying configuration change immediately.")
        time.sleep(1)
        
    # 3. Verify Improvement (mock wait window)
    print("   -> Waiting 30s verification window to check metrics...")
    # (In reality, query InfluxDB here to check if metric improved)
    time.sleep(3) 
    
    # Assume success for MVP demo
    success = True
    
    if success:
        print(f"✅ AUTO-HEAL SUCCESSFUL. {incident_id} marked as resolved.")
        # Publish to execution-results so Incident Memory can store it
        producer.send("execution-results", value={
            "incident_id": incident_id,
            "status": "resolved",
            "action_executed": action
        })
    else:
        print(f"❌ AUTO-HEAL FAILED. Triggering rollback: {rollback_plan}")
        producer.send("execution-results", value={
            "incident_id": incident_id,
            "status": "auto_heal_failed",
            "action_executed": action
        })
        
    return success

def main():
    print("Starting Self-Healing Executor...")
    for message in consumer:
        decision_event = message.value
        try:
            if decision_event.get("decision") == "AUTO_HEAL":
                action = decision_event.get("action_approved")
                if action:
                    success = execute_action(decision_event["incident_id"], action)
                    
                    if success:
                        # CONTINUOUS LEARNING LOOP
                        # Publish a learned fix event that the Knowledge Graph will ingest
                        producer.send("learned-fixes", value={
                            "incident_id": decision_event["incident_id"],
                            "service_name": "payment-api", # mock
                            "successful_action": action,
                            "timestamp": time.time()
                        })
                        print(f"🎓 CONTINUOUS LEARNING: Published proven fix '{action}' for Knowledge Graph ingestion.")
            else:
                print(f"⚠️ ESCALATED TO HUMAN: {decision_event['incident_id']} - {decision_event['reasoning']}")
        except Exception as e:
            print(f"Error in executor: {e}")

if __name__ == "__main__":
    main()
