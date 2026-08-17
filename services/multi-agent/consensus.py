import json
import numpy as np

def check_consensus(agent_outputs):
    """
    Evaluates agreement between agents.
    agent_outputs is expected to be a dict of agent_name -> confidence_score
    e.g. {"monitoring_agent": 0.95, "diagnosis_agent": 0.9, "planner_agent": 0.92}
    """
    confidences = list(agent_outputs.values())
    
    if not confidences:
        return "LOW_CONSENSUS"
        
    std_dev = np.std(confidences)
    min_conf = min(confidences)
    
    # If there's high disagreement (std_dev > 0.15) OR any agent is very unsure
    if std_dev > 0.15 or min_conf < 0.6:
        return "LOW_CONSENSUS"
        
    return "HIGH_CONSENSUS"

# In a full implementation, this would be integrated into the agents.py orchestrator 
# before publishing to incidents-diagnosed. For the sake of MVP structure, we place the 
# pure logic here.
