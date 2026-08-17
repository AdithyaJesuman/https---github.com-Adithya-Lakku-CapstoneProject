import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("post-mortem")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
REPORTS_DIR = os.getenv("REPORTS_DIR", "./reports")

os.makedirs(REPORTS_DIR, exist_ok=True)

consumer = KafkaConsumer(
    'execution-results',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def generate_report(incident_id: str, status: str, action: str) -> str:
    """Generates a professional markdown post-mortem report."""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # In a real implementation, we would query ChromaDB and InfluxDB here 
    # to pull the exact graphs, agent confidence scores, and metrics that led up to this.
    
    report = f"""# Incident Post-Mortem: {incident_id}
**Date:** {timestamp}
**Final Status:** `{"RESOLVED" if status == "resolved" else "FAILED & ESCALATED"}`

## 1. Executive Summary
An anomaly was detected and processed through the AIOps pipeline. The Multi-Agent system diagnosed the issue and proposed `{action}` as the primary fix.

## 2. Action Taken
The Executor service attempted the action: **{action}**.
- **Outcome:** {status.upper()}
"""

    if status == "resolved":
        report += """
## 3. Continuous Learning
Because this action was successful, the Knowledge Graph has been dynamically updated. The `HAS_PROVEN_FIX` edge has been established so future occurrences of this pattern will bypass full diagnosis.
"""
    else:
        report += """
## 3. Failure Analysis
The executed action failed to bring the system metrics back to baseline during the verification window. 
**Action Taken:** Automatic rollback was triggered.
**Next Steps:** A human Site Reliability Engineer (SRE) must review the ChromaDB incident logs and the Knowledge Graph blast radius to manually deploy a fix.
"""
    
    filename = os.path.join(REPORTS_DIR, f"{incident_id}_post_mortem.md")
    with open(filename, "w") as f:
        f.write(report)
        
    logger.info(f"Generated Post-Mortem Report: {filename}")
    return filename

def main() -> None:
    logger.info("Starting Automated Post-Mortem Generator...")
    for message in consumer:
        event = message.value
        try:
            generate_report(
                incident_id=event.get("incident_id", "UNKNOWN"),
                status=event.get("status", "unknown"),
                action=event.get("action_executed", "none")
            )
        except Exception as e:
            logger.error(f"Error generating report: {e}", exc_info=True)

if __name__ == "__main__":
    main()
