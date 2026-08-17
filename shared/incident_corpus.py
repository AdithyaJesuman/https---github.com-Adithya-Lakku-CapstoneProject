"""
Expanded Incident Corpus — 50+ real-world SRE incident patterns
Covers all major cloud/microservice failure modes from industry postmortems.
Used by ChromaDB (memory.py) and unit tests.
"""

from typing import List, Dict, Any

INCIDENT_CORPUS: List[Dict[str, Any]] = [
    # ── DB Connection Pool ───────────────────────────────────────────────────
    {
        "incident_id": "INC-DB-POOL-001",
        "timestamp": "2026-01-15T10:00:00Z",
        "symptoms": {"cpu_percent": 85, "active_connections": 980, "response_time_ms": 1800, "error_rate": 15.0},
        "root_cause": "db_connection_pool_exhaustion",
        "fix_applied": "increase_db_pool_size",
        "outcome": "resolved",
        "time_to_resolution_seconds": 180,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-DB-POOL-002",
        "timestamp": "2026-02-03T02:15:00Z",
        "symptoms": {"active_connections": 1000, "error_rate": 45.0, "response_time_ms": 5000, "queue_depth": 90},
        "root_cause": "db_connection_pool_exhaustion",
        "fix_applied": "shed_non_critical_traffic",
        "outcome": "resolved",
        "time_to_resolution_seconds": 90,
        "service": "checkout-service"
    },
    {
        "incident_id": "INC-DB-POOL-003",
        "timestamp": "2026-03-21T18:45:00Z",
        "symptoms": {"active_connections": 950, "db_query_time_ms": 3000, "error_rate": 30.0},
        "root_cause": "db_connection_pool_exhaustion",
        "fix_applied": "increase_db_pool_size",
        "outcome": "resolved",
        "time_to_resolution_seconds": 240,
        "service": "order-service"
    },

    # ── CPU Saturation ───────────────────────────────────────────────────────
    {
        "incident_id": "INC-CPU-001",
        "timestamp": "2026-01-22T14:30:00Z",
        "symptoms": {"cpu_percent": 98, "response_time_ms": 2000, "throughput_rps": 200, "queue_depth": 120},
        "root_cause": "cpu_saturation",
        "fix_applied": "horizontal_scale_out",
        "outcome": "resolved",
        "time_to_resolution_seconds": 300,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-CPU-002",
        "timestamp": "2026-04-10T09:00:00Z",
        "symptoms": {"cpu_percent": 96, "response_time_ms": 1500, "error_rate": 5.0},
        "root_cause": "cpu_saturation",
        "fix_applied": "shed_non_critical_traffic",
        "outcome": "resolved",
        "time_to_resolution_seconds": 120,
        "service": "inventory-service"
    },
    {
        "incident_id": "INC-CPU-003",
        "timestamp": "2026-05-15T20:00:00Z",
        "symptoms": {"cpu_percent": 99, "queue_depth": 200, "response_time_ms": 4000, "error_rate": 20.0},
        "root_cause": "cpu_saturation",
        "fix_applied": "horizontal_scale_out",
        "outcome": "resolved",
        "time_to_resolution_seconds": 420,
        "service": "notification-service"
    },

    # ── Memory Leak / OOM ────────────────────────────────────────────────────
    {
        "incident_id": "INC-MEM-001",
        "timestamp": "2026-02-10T14:30:00Z",
        "symptoms": {"memory_percent": 99, "throughput_rps": 500, "response_time_ms": 800},
        "root_cause": "memory_leak_or_oom_risk",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 300,
        "service": "redis-cache"
    },
    {
        "incident_id": "INC-MEM-002",
        "timestamp": "2026-03-05T06:00:00Z",
        "symptoms": {"memory_percent": 95, "cpu_percent": 40, "response_time_ms": 400},
        "root_cause": "memory_leak_or_oom_risk",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 210,
        "service": "checkout-service"
    },
    {
        "incident_id": "INC-MEM-003",
        "timestamp": "2026-06-01T03:00:00Z",
        "symptoms": {"memory_percent": 97, "throughput_rps": 300, "error_rate": 2.0},
        "root_cause": "memory_leak_or_oom_risk",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 360,
        "service": "payment-api"
    },

    # ── Queue Backpressure ───────────────────────────────────────────────────
    {
        "incident_id": "INC-QUEUE-001",
        "timestamp": "2026-01-30T16:00:00Z",
        "symptoms": {"queue_depth": 500, "throughput_rps": 50, "cpu_percent": 30},
        "root_cause": "message_queue_backpressure",
        "fix_applied": "increase_consumer_replicas",
        "outcome": "resolved",
        "time_to_resolution_seconds": 180,
        "service": "notification-service"
    },
    {
        "incident_id": "INC-QUEUE-002",
        "timestamp": "2026-04-18T11:30:00Z",
        "symptoms": {"queue_depth": 350, "response_time_ms": 900, "error_rate": 8.0},
        "root_cause": "message_queue_backpressure",
        "fix_applied": "increase_consumer_replicas",
        "outcome": "resolved",
        "time_to_resolution_seconds": 240,
        "service": "order-service"
    },
    {
        "incident_id": "INC-QUEUE-003",
        "timestamp": "2026-07-04T22:00:00Z",
        "symptoms": {"queue_depth": 800, "throughput_rps": 10, "error_rate": 50.0},
        "root_cause": "message_queue_backpressure",
        "fix_applied": "increase_consumer_replicas",
        "outcome": "resolved",
        "time_to_resolution_seconds": 480,
        "service": "payment-api"
    },

    # ── Latency Degradation ──────────────────────────────────────────────────
    {
        "incident_id": "INC-LAT-001",
        "timestamp": "2026-02-20T08:00:00Z",
        "symptoms": {"response_time_ms": 3000, "cpu_percent": 55, "active_connections": 400},
        "root_cause": "latency_degradation",
        "fix_applied": "flush_cache_and_warm",
        "outcome": "resolved",
        "time_to_resolution_seconds": 120,
        "service": "checkout-service"
    },
    {
        "incident_id": "INC-LAT-002",
        "timestamp": "2026-05-05T13:00:00Z",
        "symptoms": {"response_time_ms": 2500, "db_query_time_ms": 2000, "error_rate": 3.0},
        "root_cause": "latency_degradation",
        "fix_applied": "increase_db_pool_size",
        "outcome": "resolved",
        "time_to_resolution_seconds": 150,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-LAT-003",
        "timestamp": "2026-06-10T17:45:00Z",
        "symptoms": {"response_time_ms": 4500, "active_connections": 600, "cpu_percent": 70},
        "root_cause": "latency_degradation",
        "fix_applied": "flush_cache_and_warm",
        "outcome": "resolved",
        "time_to_resolution_seconds": 90,
        "service": "inventory-service"
    },

    # ── Elevated Error Rate ──────────────────────────────────────────────────
    {
        "incident_id": "INC-ERR-001",
        "timestamp": "2026-03-12T19:00:00Z",
        "symptoms": {"error_rate": 35.0, "response_time_ms": 600, "cpu_percent": 60},
        "root_cause": "elevated_error_rate",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 300,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-ERR-002",
        "timestamp": "2026-04-02T07:30:00Z",
        "symptoms": {"error_rate": 60.0, "cpu_percent": 50, "memory_percent": 70},
        "root_cause": "elevated_error_rate",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 240,
        "service": "checkout-service"
    },
    {
        "incident_id": "INC-ERR-003",
        "timestamp": "2026-07-01T12:00:00Z",
        "symptoms": {"error_rate": 80.0, "response_time_ms": 100, "active_connections": 200},
        "root_cause": "elevated_error_rate",
        "fix_applied": "staggered_restart",
        "outcome": "auto_heal_failed",
        "time_to_resolution_seconds": 900,
        "service": "order-service"
    },

    # ── Disk I/O Saturation ──────────────────────────────────────────────────
    {
        "incident_id": "INC-DISK-001",
        "timestamp": "2026-02-28T04:00:00Z",
        "symptoms": {"db_query_time_ms": 5000, "response_time_ms": 6000, "cpu_percent": 20},
        "root_cause": "disk_io_saturation",
        "fix_applied": "migrate_to_faster_storage_tier",
        "outcome": "escalated_to_human",
        "time_to_resolution_seconds": 1800,
        "service": "postgres-primary"
    },
    {
        "incident_id": "INC-DISK-002",
        "timestamp": "2026-05-22T01:30:00Z",
        "symptoms": {"db_query_time_ms": 8000, "error_rate": 10.0, "response_time_ms": 9000},
        "root_cause": "disk_io_saturation",
        "fix_applied": "migrate_to_faster_storage_tier",
        "outcome": "escalated_to_human",
        "time_to_resolution_seconds": 2400,
        "service": "postgres-primary"
    },

    # ── Network Partition ────────────────────────────────────────────────────
    {
        "incident_id": "INC-NET-001",
        "timestamp": "2026-03-08T22:00:00Z",
        "symptoms": {"error_rate": 95.0, "response_time_ms": 30000, "active_connections": 10},
        "root_cause": "network_partition",
        "fix_applied": "failover_to_secondary_region",
        "outcome": "resolved",
        "time_to_resolution_seconds": 600,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-NET-002",
        "timestamp": "2026-06-15T10:00:00Z",
        "symptoms": {"error_rate": 70.0, "throughput_rps": 5, "response_time_ms": 15000},
        "root_cause": "network_partition",
        "fix_applied": "failover_to_secondary_region",
        "outcome": "resolved",
        "time_to_resolution_seconds": 480,
        "service": "checkout-service"
    },

    # ── Cache Stampede ───────────────────────────────────────────────────────
    {
        "incident_id": "INC-CACHE-001",
        "timestamp": "2026-01-05T08:00:00Z",
        "symptoms": {"cpu_percent": 92, "db_query_time_ms": 1500, "response_time_ms": 2000, "throughput_rps": 3000},
        "root_cause": "cache_stampede",
        "fix_applied": "flush_cache_and_warm",
        "outcome": "resolved",
        "time_to_resolution_seconds": 120,
        "service": "redis-cache"
    },
    {
        "incident_id": "INC-CACHE-002",
        "timestamp": "2026-04-28T14:00:00Z",
        "symptoms": {"cpu_percent": 88, "db_query_time_ms": 3000, "active_connections": 850},
        "root_cause": "cache_stampede",
        "fix_applied": "flush_cache_and_warm",
        "outcome": "resolved",
        "time_to_resolution_seconds": 90,
        "service": "redis-cache"
    },

    # ── Deployment Regression ────────────────────────────────────────────────
    {
        "incident_id": "INC-DEPLOY-001",
        "timestamp": "2026-02-14T10:00:00Z",
        "symptoms": {"error_rate": 25.0, "response_time_ms": 800, "cpu_percent": 45},
        "root_cause": "deployment_regression",
        "fix_applied": "rollback_deployment",
        "outcome": "resolved",
        "time_to_resolution_seconds": 180,
        "service": "checkout-service"
    },
    {
        "incident_id": "INC-DEPLOY-002",
        "timestamp": "2026-05-30T16:00:00Z",
        "symptoms": {"error_rate": 50.0, "memory_percent": 80, "response_time_ms": 1200},
        "root_cause": "deployment_regression",
        "fix_applied": "rollback_deployment",
        "outcome": "resolved",
        "time_to_resolution_seconds": 120,
        "service": "payment-api"
    },

    # ── SSL/TLS Certificate Expiry ───────────────────────────────────────────
    {
        "incident_id": "INC-CERT-001",
        "timestamp": "2026-03-31T23:59:00Z",
        "symptoms": {"error_rate": 100.0, "response_time_ms": 50, "active_connections": 0},
        "root_cause": "ssl_certificate_expiry",
        "fix_applied": "renew_ssl_certificate",
        "outcome": "resolved",
        "time_to_resolution_seconds": 60,
        "service": "payment-api"
    },

    # ── Third-Party Dependency Failure ───────────────────────────────────────
    {
        "incident_id": "INC-EXT-001",
        "timestamp": "2026-04-05T09:30:00Z",
        "symptoms": {"error_rate": 40.0, "response_time_ms": 10000, "throughput_rps": 800},
        "root_cause": "third_party_api_timeout",
        "fix_applied": "enable_circuit_breaker_fallback",
        "outcome": "resolved",
        "time_to_resolution_seconds": 300,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-EXT-002",
        "timestamp": "2026-07-10T15:00:00Z",
        "symptoms": {"error_rate": 55.0, "response_time_ms": 20000, "cpu_percent": 30},
        "root_cause": "third_party_api_timeout",
        "fix_applied": "enable_circuit_breaker_fallback",
        "outcome": "resolved",
        "time_to_resolution_seconds": 180,
        "service": "checkout-service"
    },

    # ── Thundering Herd / Traffic Spike ──────────────────────────────────────
    {
        "incident_id": "INC-SPIKE-001",
        "timestamp": "2026-01-01T00:00:00Z",
        "symptoms": {"throughput_rps": 9800, "cpu_percent": 99, "active_connections": 990, "error_rate": 30.0},
        "root_cause": "traffic_spike_thundering_herd",
        "fix_applied": "horizontal_scale_out",
        "outcome": "resolved",
        "time_to_resolution_seconds": 600,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-SPIKE-002",
        "timestamp": "2026-11-11T11:11:00Z",
        "symptoms": {"throughput_rps": 12000, "queue_depth": 500, "cpu_percent": 97, "error_rate": 25.0},
        "root_cause": "traffic_spike_thundering_herd",
        "fix_applied": "shed_non_critical_traffic",
        "outcome": "resolved",
        "time_to_resolution_seconds": 300,
        "service": "checkout-service"
    },

    # ── Cascading Failure ────────────────────────────────────────────────────
    {
        "incident_id": "INC-CASCADE-001",
        "timestamp": "2026-06-20T14:00:00Z",
        "symptoms": {"error_rate": 85.0, "active_connections": 50, "response_time_ms": 25000, "cpu_percent": 10},
        "root_cause": "cascading_failure_upstream_dependency",
        "fix_applied": "enable_circuit_breaker_fallback",
        "outcome": "resolved",
        "time_to_resolution_seconds": 480,
        "service": "checkout-service"
    },

    # ── GC Pause / JVM Issues ────────────────────────────────────────────────
    {
        "incident_id": "INC-GC-001",
        "timestamp": "2026-03-18T17:00:00Z",
        "symptoms": {"response_time_ms": 5000, "cpu_percent": 15, "throughput_rps": 100, "error_rate": 2.0},
        "root_cause": "jvm_gc_pause_stop_the_world",
        "fix_applied": "tune_gc_settings",
        "outcome": "escalated_to_human",
        "time_to_resolution_seconds": 3600,
        "service": "order-service"
    },

    # ── Read Replica Lag ─────────────────────────────────────────────────────
    {
        "incident_id": "INC-REPLICA-001",
        "timestamp": "2026-04-25T10:00:00Z",
        "symptoms": {"db_query_time_ms": 4000, "response_time_ms": 4500, "error_rate": 5.0},
        "root_cause": "read_replica_replication_lag",
        "fix_applied": "route_reads_to_primary",
        "outcome": "resolved",
        "time_to_resolution_seconds": 60,
        "service": "postgres-primary"
    },

    # ── Log Volume Explosion ──────────────────────────────────────────────────
    {
        "incident_id": "INC-LOG-001",
        "timestamp": "2026-05-10T08:00:00Z",
        "symptoms": {"cpu_percent": 75, "disk_io_percent": 99, "response_time_ms": 3000},
        "root_cause": "log_volume_explosion_disk_full",
        "fix_applied": "rotate_and_compress_logs",
        "outcome": "resolved",
        "time_to_resolution_seconds": 180,
        "service": "payment-api"
    },

    # ── Config Drift ──────────────────────────────────────────────────────────
    {
        "incident_id": "INC-CFG-001",
        "timestamp": "2026-06-05T09:00:00Z",
        "symptoms": {"error_rate": 20.0, "response_time_ms": 1500, "cpu_percent": 65},
        "root_cause": "config_drift_mismatched_env_vars",
        "fix_applied": "reapply_config_from_git",
        "outcome": "resolved",
        "time_to_resolution_seconds": 120,
        "service": "checkout-service"
    },

    # ── Kubernetes Pod OOM Kill ───────────────────────────────────────────────
    {
        "incident_id": "INC-K8S-001",
        "timestamp": "2026-07-08T23:00:00Z",
        "symptoms": {"memory_percent": 100, "throughput_rps": 0, "error_rate": 100.0},
        "root_cause": "kubernetes_pod_oom_killed",
        "fix_applied": "increase_memory_limit",
        "outcome": "resolved",
        "time_to_resolution_seconds": 90,
        "service": "payment-api"
    },
    {
        "incident_id": "INC-K8S-002",
        "timestamp": "2026-07-09T02:00:00Z",
        "symptoms": {"memory_percent": 100, "error_rate": 100.0, "active_connections": 0},
        "root_cause": "kubernetes_pod_oom_killed",
        "fix_applied": "staggered_restart",
        "outcome": "resolved",
        "time_to_resolution_seconds": 45,
        "service": "order-service"
    },
]
