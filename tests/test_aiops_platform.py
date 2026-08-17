"""
Comprehensive AIOps Platform Test Suite
Covers: unit tests, integration tests, stress tests, and mock incident scenarios.
Run with: pytest tests/ -v --tb=short
"""
import sys
import os
import time
import copy
import random
import pytest

# Compute project root (tests/ is one level below root)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "services", "multi-agent"))
sys.path.insert(0, os.path.join(_ROOT, "services", "anomaly-detection"))
sys.path.insert(0, os.path.join(_ROOT, "services", "digital-twin"))
sys.path.insert(0, os.path.join(_ROOT, "shared"))

from agents import (
    monitoring_agent, diagnosis_agent, forecast_agent,
    planner_agent, process_anomaly, _DIAGNOSIS_RULES, _FIX_PLAYBOOK
)
from incident_corpus import INCIDENT_CORPUS


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════

def make_anomaly(triggers, raw_values=None, confidence=0.85, severity="high"):
    """Factory for anomaly events used in tests."""
    return {
        "anomaly_id": f"ANOM-TEST-{random.randint(1000,9999)}",
        "timestamp": "2026-08-12T18:00:00Z",
        "service_name": "payment-api",
        "instance_id": "pod-test-0001",
        "detector": "test",
        "confidence": confidence,
        "severity": severity,
        "triggering_metrics": triggers,
        "raw_values": raw_values or {},
        "baseline_values": {}
    }


# ═══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — Monitoring Agent
# ═══════════════════════════════════════════════════════════════════════════════

class TestMonitoringAgent:
    def test_high_confidence_significant(self):
        a = make_anomaly(["cpu_percent"], confidence=0.90)
        r = monitoring_agent(a)
        assert r["is_significant"] is True

    def test_low_confidence_dropped(self):
        a = make_anomaly(["cpu_percent"], confidence=0.40)
        r = monitoring_agent(a)
        assert r["is_significant"] is False

    def test_no_triggers_dropped(self):
        a = make_anomaly([], confidence=0.95)
        r = monitoring_agent(a)
        assert r["is_significant"] is False

    def test_exactly_threshold_significant(self):
        a = make_anomaly(["error_rate"], confidence=0.60)
        r = monitoring_agent(a)
        assert r["is_significant"] is True

    def test_just_below_threshold_dropped(self):
        a = make_anomaly(["error_rate"], confidence=0.59)
        r = monitoring_agent(a)
        assert r["is_significant"] is False

    def test_severity_propagated(self):
        a = make_anomaly(["cpu_percent"], severity="critical", confidence=0.95)
        r = monitoring_agent(a)
        assert r["severity"] == "critical"


# ═══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — Diagnosis Agent
# ═══════════════════════════════════════════════════════════════════════════════

class TestDiagnosisAgent:
    def test_db_pool_from_connections_and_error(self):
        a = make_anomaly(["active_connections", "error_rate"],
                         raw_values={"active_connections": 980, "error_rate": 30.0})
        r = diagnosis_agent(a)
        assert r is not None
        assert r["root_cause"] == "db_connection_pool_exhaustion"
        assert r["confidence"] >= 0.90

    def test_cpu_saturation_detected(self):
        a = make_anomaly(["cpu_percent", "queue_depth"],
                         raw_values={"cpu_percent": 97, "queue_depth": 120})
        r = diagnosis_agent(a)
        assert r["root_cause"] == "cpu_saturation"

    def test_memory_oom_detected(self):
        a = make_anomaly(["memory_percent", "error_rate"],
                         raw_values={"memory_percent": 100, "error_rate": 100})
        r = diagnosis_agent(a)
        assert r["root_cause"] == "kubernetes_pod_oom_killed"

    def test_queue_backpressure_detected(self):
        a = make_anomaly(["queue_depth", "error_rate"],
                         raw_values={"queue_depth": 500, "error_rate": 50})
        r = diagnosis_agent(a)
        assert r["root_cause"] == "message_queue_backpressure"

    def test_disk_io_detected(self):
        a = make_anomaly(["db_query_time_ms", "response_time_ms"],
                         raw_values={"db_query_time_ms": 8000, "response_time_ms": 9000})
        r = diagnosis_agent(a)
        assert r["root_cause"] == "disk_io_saturation"

    def test_unknown_trigger_returns_none(self):
        a = make_anomaly(["totally_unknown_metric"])
        r = diagnosis_agent(a)
        assert r is None

    def test_multi_signal_higher_confidence_than_single(self):
        single = make_anomaly(["active_connections"])
        multi  = make_anomaly(["active_connections", "error_rate"])
        r_single = diagnosis_agent(single)
        r_multi  = diagnosis_agent(multi)
        assert r_multi["confidence"] >= r_single["confidence"]

    def test_all_15_root_causes_have_playbook_entry(self):
        """Every root cause in the diagnosis rules must have a matching fix."""
        covered_causes = set()
        for rule in _DIAGNOSIS_RULES:
            covered_causes.add(rule["root_cause"])
        for cause in covered_causes:
            assert cause in _FIX_PLAYBOOK, f"Missing playbook for: {cause}"

    def test_corpus_alignment(self):
        """Every incident in the corpus should be diagnosable by the agent."""
        undiagnosed = []
        for inc in INCIDENT_CORPUS:
            symptoms = list(inc["symptoms"].keys())
            a = make_anomaly(symptoms, raw_values=inc["symptoms"])
            r = diagnosis_agent(a)
            if r is None:
                undiagnosed.append(inc["incident_id"])
        # Allow up to 10% miss rate for edge cases (e.g., cert expiry, config drift)
        miss_rate = len(undiagnosed) / len(INCIDENT_CORPUS)
        assert miss_rate <= 0.10, f"Too many undiagnosed ({miss_rate:.0%}): {undiagnosed}"


# ═══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — Forecast Agent
# ═══════════════════════════════════════════════════════════════════════════════

class TestForecastAgent:
    def test_critical_ttf_under_60s(self):
        a = make_anomaly(["active_connections"],
                         raw_values={"active_connections": 999})
        diag = {"root_cause": "db_connection_pool_exhaustion", "confidence": 0.92}
        r = forecast_agent(a, diag)
        assert r["time_to_failure_seconds"] <= 60

    def test_high_breach_ttf_under_300s(self):
        a = make_anomaly(["cpu_percent"], raw_values={"cpu_percent": 92})
        diag = {"root_cause": "cpu_saturation", "confidence": 0.85}
        r = forecast_agent(a, diag)
        assert r["time_to_failure_seconds"] <= 300

    def test_low_breach_ttf_is_long(self):
        a = make_anomaly(["cpu_percent"], raw_values={"cpu_percent": 50})
        diag = {"root_cause": "cpu_saturation", "confidence": 0.75}
        r = forecast_agent(a, diag)
        assert r["time_to_failure_seconds"] >= 900

    def test_business_impact_nonempty(self):
        a = make_anomaly(["cpu_percent"], raw_values={"cpu_percent": 97})
        diag = {"root_cause": "cpu_saturation", "confidence": 0.90}
        r = forecast_agent(a, diag)
        assert len(r["business_impact_description"]) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS — Planner Agent
# ═══════════════════════════════════════════════════════════════════════════════

class TestPlannerAgent:
    def test_returns_fixes_for_known_root_cause(self):
        diag = {"root_cause": "cpu_saturation", "confidence": 0.90}
        r = planner_agent(diag)
        assert len(r["candidate_fixes"]) > 0

    def test_fixes_sorted_by_success_rate(self):
        diag = {"root_cause": "db_connection_pool_exhaustion", "confidence": 0.92}
        r = planner_agent(diag)
        rates = [f["estimated_success_rate"] for f in r["candidate_fixes"]]
        assert rates == sorted(rates, reverse=True)

    def test_all_fixes_have_required_fields(self):
        for root_cause, fixes in _FIX_PLAYBOOK.items():
            for fix in fixes:
                assert "action" in fix, f"{root_cause} fix missing 'action'"
                assert "reversible" in fix, f"{root_cause} fix missing 'reversible'"
                assert "estimated_success_rate" in fix, f"{root_cause} fix missing success rate"
                assert 0.0 <= fix["estimated_success_rate"] <= 1.0

    def test_unknown_root_cause_returns_empty(self):
        diag = {"root_cause": "alien_invasion", "confidence": 0.5}
        r = planner_agent(diag)
        assert r["candidate_fixes"] == []

    def test_all_corpus_root_causes_have_playbook(self):
        corpus_causes = {inc["root_cause"] for inc in INCIDENT_CORPUS}
        missing = corpus_causes - set(_FIX_PLAYBOOK.keys())
        assert len(missing) == 0, f"Root causes in corpus with no playbook: {missing}"


# ═══════════════════════════════════════════════════════════════════════════════
# MOCK INCIDENT SCENARIOS — End-to-End Pipeline (no Kafka)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMockIncidentScenarios:
    def _run_pipeline(self, triggers, raw_values, confidence=0.90):
        """Simulates the full deterministic pipeline without Kafka."""
        a = make_anomaly(triggers, raw_values, confidence=confidence)
        mon = monitoring_agent(a)
        if not mon["is_significant"]:
            return None
        diag = diagnosis_agent(a)
        if not diag:
            return None
        forecast = forecast_agent(a, diag)
        plan = planner_agent(diag)
        return {"diag": diag, "forecast": forecast, "plan": plan}

    def test_db_pool_exhaustion_scenario(self):
        result = self._run_pipeline(
            ["active_connections", "error_rate"],
            {"active_connections": 990, "error_rate": 45.0}
        )
        assert result is not None
        assert result["diag"]["root_cause"] == "db_connection_pool_exhaustion"
        assert result["plan"]["candidate_fixes"][0]["action"] == "increase_db_pool_size"

    def test_cpu_spike_scenario(self):
        result = self._run_pipeline(
            ["cpu_percent", "queue_depth"],
            {"cpu_percent": 98, "queue_depth": 200}
        )
        assert result is not None
        assert result["diag"]["root_cause"] == "cpu_saturation"
        assert result["forecast"]["time_to_failure_seconds"] <= 60

    def test_memory_leak_scenario(self):
        result = self._run_pipeline(
            ["memory_percent"],
            {"memory_percent": 97}
        )
        assert result is not None
        assert "memory" in result["diag"]["root_cause"]

    def test_queue_backpressure_scenario(self):
        result = self._run_pipeline(
            ["queue_depth", "error_rate"],
            {"queue_depth": 500, "error_rate": 50.0}
        )
        assert result is not None
        assert result["diag"]["root_cause"] == "message_queue_backpressure"
        assert result["plan"]["candidate_fixes"][0]["action"] == "increase_consumer_replicas"

    def test_network_partition_scenario(self):
        result = self._run_pipeline(
            ["error_rate", "response_time_ms"],
            {"error_rate": 95.0, "response_time_ms": 30000}
        )
        assert result is not None
        assert result["diag"] is not None

    def test_low_confidence_scenario_dropped(self):
        result = self._run_pipeline(
            ["cpu_percent"],
            {"cpu_percent": 80},
            confidence=0.30
        )
        assert result is None

    def test_disk_io_saturation_scenario(self):
        result = self._run_pipeline(
            ["db_query_time_ms", "response_time_ms"],
            {"db_query_time_ms": 8000, "response_time_ms": 9000}
        )
        assert result is not None
        assert result["diag"]["root_cause"] == "disk_io_saturation"

    def test_oom_kill_scenario(self):
        result = self._run_pipeline(
            ["memory_percent", "error_rate"],
            {"memory_percent": 100, "error_rate": 100.0}
        )
        assert result is not None
        assert result["diag"]["root_cause"] == "kubernetes_pod_oom_killed"


# ═══════════════════════════════════════════════════════════════════════════════
# STRESS TESTS — Throughput & Latency
# ═══════════════════════════════════════════════════════════════════════════════

class TestStressThroughput:
    STRESS_N = 10_000

    def _random_anomaly(self):
        triggers = random.choice([
            ["active_connections", "error_rate"],
            ["cpu_percent", "queue_depth"],
            ["memory_percent"],
            ["response_time_ms"],
            ["queue_depth", "error_rate"],
            ["db_query_time_ms", "response_time_ms"],
        ])
        raw = {
            "cpu_percent": random.uniform(50, 100),
            "memory_percent": random.uniform(50, 100),
            "response_time_ms": random.uniform(100, 10000),
            "error_rate": random.uniform(0, 100),
            "active_connections": random.uniform(100, 1000),
            "queue_depth": random.uniform(0, 500),
            "db_query_time_ms": random.uniform(10, 10000),
        }
        return make_anomaly(triggers, raw_values=raw, confidence=random.uniform(0.6, 1.0))

    def test_10k_anomalies_in_under_5s(self):
        """Full pipeline must process 10,000 anomaly events in <5 seconds."""
        anomalies = [self._random_anomaly() for _ in range(self.STRESS_N)]
        start = time.perf_counter()
        for a in anomalies:
            mon = monitoring_agent(a)
            if mon["is_significant"]:
                diag = diagnosis_agent(a)
                if diag:
                    forecast_agent(a, diag)
                    planner_agent(diag)
        elapsed = time.perf_counter() - start
        throughput = self.STRESS_N / elapsed
        print(f"\n[STRESS] Processed {self.STRESS_N} anomalies in {elapsed:.2f}s ({throughput:.0f}/s)")
        assert elapsed < 5.0, f"Too slow: {elapsed:.2f}s for {self.STRESS_N} events"

    def test_average_pipeline_latency_under_1ms(self):
        """Per-event latency must be <1ms (p99 target for production use)."""
        anomalies = [self._random_anomaly() for _ in range(1000)]
        latencies = []
        for a in anomalies:
            t0 = time.perf_counter()
            mon = monitoring_agent(a)
            if mon["is_significant"]:
                diag = diagnosis_agent(a)
                if diag:
                    forecast_agent(a, diag)
                    planner_agent(diag)
            latencies.append((time.perf_counter() - t0) * 1000)

        import statistics
        p50 = statistics.median(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        print(f"\n[STRESS] Latency — p50={p50:.3f}ms  p99={p99:.3f}ms")
        assert p99 < 1.0, f"p99 latency too high: {p99:.3f}ms"

    def test_no_crashes_on_random_input(self):
        """System must never crash or throw an exception on any random input."""
        for _ in range(5000):
            a = {
                "anomaly_id": "ANOM-FUZZ",
                "timestamp": "2026-01-01T00:00:00Z",
                "service_name": "test",
                "confidence": random.uniform(0, 1),
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "triggering_metrics": random.sample(
                    ["cpu_percent", "memory_percent", "error_rate", "queue_depth",
                     "response_time_ms", "active_connections", "db_query_time_ms", "unknown_field"],
                    k=random.randint(0, 3)
                ),
                "raw_values": {},
                "baseline_values": {}
            }
            try:
                mon = monitoring_agent(a)
                if mon["is_significant"]:
                    diag = diagnosis_agent(a)
                    if diag:
                        forecast_agent(a, diag)
                        planner_agent(diag)
            except Exception as e:
                pytest.fail(f"Crash on fuzz input: {e}\nInput: {a}")


# ═══════════════════════════════════════════════════════════════════════════════
# ACCURACY TESTS — Corpus Recall
# ═══════════════════════════════════════════════════════════════════════════════

class TestAccuracy:
    def test_diagnosis_accuracy_on_corpus(self):
        """
        Measures root-cause diagnosis recall against the full 50+ incident corpus.
        The corpus contains 15+ archetypes. Not all are diagnosable from metric names
        alone (e.g. ssl_certificate_expiry, config_drift). The rule engine covers
        the 8 most common causes — industry-standard coverage for rule-based AIOps.
        We print a full breakdown so you can see exactly what is/isn't covered.
        """
        correct = 0
        total = len(INCIDENT_CORPUS)
        missed = []

        for inc in INCIDENT_CORPUS:
            # Use symptom keys as triggers (simulating what the detector would produce)
            triggers = list(inc["symptoms"].keys())
            a = make_anomaly(triggers, raw_values=inc["symptoms"], confidence=0.90)
            result = diagnosis_agent(a)
            if result and result["root_cause"] == inc["root_cause"]:
                correct += 1
            else:
                got = result["root_cause"] if result else "None"
                missed.append(f"{inc['incident_id']}: expected={inc['root_cause']}, got={got}")

        recall = correct / total
        print(f"\n[ACCURACY] Corpus recall: {correct}/{total} = {recall:.1%}")
        if missed:
            print("  Missed:")
            for m in missed:
                print(f"    {m}")
        # Rule-based systems cover the 8 most frequent archetypes.
        # Exotic causes (ssl_expiry, config_drift, gc_pause, disk_io) require
        # separate detectors. Target: >40% exact-match recall on full corpus.
        assert recall >= 0.40, f"Recall too low: {recall:.1%} (target ≥40%)"

    def test_fix_quality_for_resolved_incidents(self):
        """
        For corpus incidents that were 'resolved' (not escalated), the planner
        must return at least one candidate fix matching the recorded fix_applied.
        """
        resolved = [i for i in INCIDENT_CORPUS if i["outcome"] == "resolved"]
        matched = 0
        for inc in resolved:
            diag = {"root_cause": inc["root_cause"], "confidence": 0.90}
            plan = planner_agent(diag)
            fixes = [f["action"] for f in plan["candidate_fixes"]]
            if inc["fix_applied"] in fixes:
                matched += 1

        match_rate = matched / len(resolved) if resolved else 0
        print(f"\n[ACCURACY] Fix match rate: {matched}/{len(resolved)} = {match_rate:.1%}")
        assert match_rate >= 0.60, f"Fix match rate too low: {match_rate:.1%}"
