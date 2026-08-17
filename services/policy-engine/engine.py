import os
import json
import time
import logging
from collections import defaultdict
from typing import Dict, Any, Tuple
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("policy-engine")

KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")

consumer = KafkaConsumer(
    'incidents-diagnosed',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# In-memory cooldown state (MVP, real would use Redis)
cooldowns: Dict[str, float] = defaultdict(float)
COOLDOWN_SECONDS: int = 300 # 5 mins

def evaluate_policy(diagnosis: Dict[str, Any]) -> Tuple[str, str]:
    """
    Evaluates the proposed diagnosis and fix against strict safety guardrails.
    
    Returns:
        Tuple[str, str]: The decision (AUTO_HEAL or ESCALATE_TO_HUMAN) and the reasoning string.
    """
    incident_id = diagnosis.get("incident_id", "UNKNOWN")
    service_name = "payment-api" # For MVP we know it's always this service from our collector
    now = time.time()
    
    # 1. Cooldown Gate
    if now - cooldowns[service_name] < COOLDOWN_SECONDS:
        logger.warning(f"[{incident_id}] Cooldown active for {service_name}.")
        return "ESCALATE_TO_HUMAN", "Cooldown active. No repeat actions within 5 mins."
        
    # 2. Confidence Gate
    confidence = diagnosis.get("confidence", 0.0)
    if confidence < 0.95:
        logger.warning(f"[{incident_id}] Low confidence: {confidence}")
        return "ESCALATE_TO_HUMAN", f"Confidence {confidence} < 0.95 threshold."
        
    # 3. Corroboration Gate
    consensus = diagnosis.get("agent_consensus", {})
    if len(consensus) < 2 or any(v < 0.7 for v in consensus.values()):
        logger.warning(f"[{incident_id}] Low agent corroboration: {consensus}")
        return "ESCALATE_TO_HUMAN", "Agents lack corroboration or strong consensus."
        
    # 4. Check candidate fixes
    fixes = diagnosis.get("candidate_fixes", [])
    if not fixes:
        return "ESCALATE_TO_HUMAN", "No candidate fixes available."
        
    best_fix = fixes[0]
    
    # 5. Risk Category Gate (Escalate DB schema changes)
    if "schema" in best_fix.get("action", "").lower():
        logger.warning(f"[{incident_id}] Schema change requested. Escalating.")
        return "ESCALATE_TO_HUMAN", "Schema changes always require human approval."

    # Passed all gates
    cooldowns[service_name] = now
    return "AUTO_HEAL", f"Confidence {confidence} > 0.95, corroborated, safe action."

def main() -> None:
    """Main consumer loop for the Policy Engine."""
    logger.info("Starting Policy Engine...")
    for message in consumer:
        diagnosis = message.value
        try:
            decision, reasoning = evaluate_policy(diagnosis)
            
            policy_output = {
                "incident_id": diagnosis["incident_id"],
                "decision": decision,
                "reasoning": reasoning,
                "action_approved": diagnosis["candidate_fixes"][0]["action"] if decision == "AUTO_HEAL" and diagnosis.get("candidate_fixes") else None,
                "requires_human": decision == "ESCALATE_TO_HUMAN"
            }
            
            producer.send("policy-decisions", value=policy_output)
            logger.info(f"🛡️ POLICY DECISION for {diagnosis.get('incident_id')}: {decision} ({reasoning})")
            
        except Exception as e:
            logger.error(f"Error in policy engine: {e}", exc_info=True)

if __name__ == "__main__":
    main()
