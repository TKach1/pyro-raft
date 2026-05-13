import os

# Pyro nameserver
NAMESERVER_HOST = os.getenv("NAMESERVER_HOST", "localhost")
NAMESERVER_PORT = int(os.getenv("NAMESERVER_PORT", "9090"))

# RAFT timing (milliseconds)
ELECTION_TIMEOUT_MIN = float(os.getenv("ELECTION_TIMEOUT_MIN", "150"))
ELECTION_TIMEOUT_MAX = float(os.getenv("ELECTION_TIMEOUT_MAX", "300"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "50"))

# Storage
DATA_DIR = os.getenv("DATA_DIR", "data")

# Node configurations for hardcoded URIs
NODES = {
    "node1": {"port": 9091, "object_id": "node1"},
    "node2": {"port": 9092, "object_id": "node2"},
    "node3": {"port": 9093, "object_id": "node3"},
    "node4": {"port": 9094, "object_id": "node4"},
}

# Total nodes
TOTAL_NODES = len(NODES)
