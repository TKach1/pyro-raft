#!/usr/bin/env python3
import subprocess
import time
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import NODES

def start_nameserver():
    print("Starting nameserver...")
    return subprocess.Popen([sys.executable, 'src/nameserver.py'])

def start_node(node_id):
    print(f"Starting node {node_id}...")
    return subprocess.Popen([sys.executable, 'src/candidate.py', node_id])

def start_client():
    print("Starting client...")
    return subprocess.Popen([sys.executable, 'src/client.py'])

if __name__ == "__main__":
    # Start nameserver
    ns_proc = start_nameserver()
    time.sleep(2)  # Wait for nameserver to start

    # Start 4 nodes
    node_procs = []
    for node_id in NODES:
        proc = start_node(node_id)
        node_procs.append(proc)
        time.sleep(1)  # Stagger starts

    # Start client
    client_proc = start_client()

    try:
        # Wait for processes
        ns_proc.wait()
        for proc in node_procs:
            proc.wait()
        client_proc.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        ns_proc.terminate()
        for proc in node_procs:
            proc.terminate()
        client_proc.terminate()