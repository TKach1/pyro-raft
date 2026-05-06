import os

# Pyro nameserver
NAMESERVER_HOST = os.getenv("NAMESERVER_HOST", "localhost")
NAMESERVER_PORT = int(os.getenv("NAMESERVER_PORT", "9090"))

# RAFT timing (milliseconds)
ELECTION_TIMEOUT_MIN = float(os.getenv("ELECTION_TIMEOUT_MIN", "150"))
ELECTION_TIMEOUT_MAX = float(os.getenv("ELECTION_TIMEOUT_MAX", "300"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "50"))

# Storage
DATA_DIR = os.getenv("DATA_DIR", "/data")
