import os

# Pyro nameserver
NAMESERVER_HOST = os.getenv("NAMESERVER_HOST", "localhost")
NAMESERVER_PORT = int(os.getenv("NAMESERVER_PORT", "9090"))

# RAFT timing (milliseconds)
ELECTION_TIMEOUT_MIN = float(os.getenv("ELECTION_TIMEOUT_MIN", "300"))
ELECTION_TIMEOUT_MAX = float(os.getenv("ELECTION_TIMEOUT_MAX", "500"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "100"))

# Storage
DATA_DIR = os.getenv("DATA_DIR", "data")

# Node configurations (host configurable via env for Docker)
NODES = {
    "node1": {
        "host": os.getenv("NODE1_HOST", "localhost"),
        "port": int(os.getenv("NODE1_PORT", "9091")),
        "object_id": "node1",
    },
    "node2": {
        "host": os.getenv("NODE2_HOST", "localhost"),
        "port": int(os.getenv("NODE2_PORT", "9092")),
        "object_id": "node2",
    },
    "node3": {
        "host": os.getenv("NODE3_HOST", "localhost"),
        "port": int(os.getenv("NODE3_PORT", "9093")),
        "object_id": "node3",
    },
}

TOTAL_NODES = len(NODES)
