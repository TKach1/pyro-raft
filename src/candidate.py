import Pyro5.api
import Pyro5.server
import threading
import time
import random
import json
import os

from config import DATA_DIR, NAMESERVER_HOST, NAMESERVER_PORT, NODES, TOTAL_NODES, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL

@Pyro5.api.expose
class CandidateNode:
    """RAFT candidate/follower/leader node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.log = []  # List of dicts: {"term": int, "command": str}
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {nid: len(self.log) + 1 for nid in NODES if nid != node_id}  # For leader
        self.match_index = {nid: 0 for nid in NODES if nid != node_id}  # For leader
        self.votes_received = 0
        self.data_file = f"{DATA_DIR}/{self.node_id}_data.json"
        self.state_file = f"{DATA_DIR}/{self.node_id}_state.json"
        self.election_timer = None
        self.heartbeat_timer = None
        self.other_nodes = {nid: f"PYRO:{NODES[nid]['object_id']}@{NAMESERVER_HOST}:{NODES[nid]['port']}" for nid in NODES if nid != node_id}
        self.load_state()

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.current_term = state.get("current_term", 0)
                self.voted_for = state.get("voted_for", None)
                self.log = state.get("log", [])
                self.commit_index = state.get("commit_index", 0)

    def save_state(self):
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
            "commit_index": self.commit_index
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f)

    def get_last_log_index(self):
        return len(self.log)

    def get_last_log_term(self):
        return self.log[-1]["term"] if self.log else 0

    def start_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(ELECTION_TIMEOUT_MIN / 1000, ELECTION_TIMEOUT_MAX / 1000)
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        if self.state == "leader":
            return
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        self.save_state()
        self.reset_election_timer()
        # Send RequestVote to others
        for uri in self.other_nodes.values():
            threading.Thread(target=self.send_request_vote, args=(uri,)).start()

    def send_request_vote(self, uri):
        try:
            proxy = Pyro5.api.Proxy(uri)
            response = proxy.request_vote(self.current_term, self.node_id, self.get_last_log_index(), self.get_last_log_term())
            if response["vote_granted"]:
                self.votes_received += 1
                if self.votes_received > TOTAL_NODES // 2 and self.state == "candidate":
                    self.become_leader()
        except Exception as e:
            print(f"Error sending vote to {uri}: {e}")

    def become_leader(self):
        self.state = "leader"
        self.leader_id = self.node_id
        # Register as leader
        ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        ns.register("leader", self.uri)
        self.start_heartbeat()

    def start_heartbeat(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL / 1000, self.send_heartbeats)
        self.heartbeat_timer.start()

    def send_heartbeats(self):
        if self.state != "leader":
            return
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self.send_append_entries, args=(nid, uri, [])).start()
        self.start_heartbeat()

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        self.start_election_timer()

    def request_vote(self, term: int, candidate_id: str, last_log_index: int = 0, last_log_term: int = 0) -> dict:
        """Handle RequestVote RPC."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"
            self.save_state()
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}
        if self.voted_for is not None and self.voted_for != candidate_id:
            return {"term": self.current_term, "vote_granted": False}
        if last_log_term < self.get_last_log_term() or (last_log_term == self.get_last_log_term() and last_log_index < self.get_last_log_index()):
            return {"term": self.current_term, "vote_granted": False}
        self.voted_for = candidate_id
        self.save_state()
        self.reset_election_timer()
        return {"term": self.current_term, "vote_granted": True}

    def append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int, entries: list, leader_commit: int) -> dict:
        """Handle AppendEntries RPC (heartbeat + replication)."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"
            self.save_state()
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        self.leader_id = leader_id
        self.state = "follower"
        self.reset_election_timer()
        # Check log consistency
        if prev_log_index > 0:
            if prev_log_index > len(self.log) or self.log[prev_log_index - 1]["term"] != prev_log_term:
                return {"term": self.current_term, "success": False}
        # Append new entries
        for i, entry in enumerate(entries):
            index = prev_log_index + i + 1
            if index > len(self.log):
                self.log.append(entry)
            elif self.log[index - 1]["term"] != entry["term"]:
                self.log = self.log[:index - 1]
                self.log.append(entry)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
        self.save_state()
        return {"term": self.current_term, "success": True}

    def send_append_entries(self, nid, uri, entries):
        try:
            proxy = Pyro5.api.Proxy(uri)
            prev_log_index = self.next_index[nid] - 1
            prev_log_term = self.log[prev_log_index - 1]["term"] if prev_log_index > 0 else 0
            response = proxy.append_entries(self.current_term, self.node_id, prev_log_index, prev_log_term, entries, self.commit_index)
            if response["success"]:
                self.next_index[nid] += len(entries)
                self.match_index[nid] = self.next_index[nid] - 1
                self.update_commit_index()
            else:
                self.next_index[nid] -= 1
        except Exception as e:
            print(f"Error sending append entries to {uri}: {e}")

    def update_commit_index(self):
        for i in range(self.commit_index + 1, len(self.log) + 1):
            count = sum(1 for mid in self.match_index.values() if mid >= i)
            if count >= TOTAL_NODES // 2:
                self.commit_index = i
                self.save_state()
                # Send commit to followers? Actually, next heartbeat will include it.

    def submit(self, client_id: str, data: str) -> dict:
        """Receive data from client (only leader accepts)."""
        if self.state != "leader":
            return {"success": False, "error": "Not the leader", "leader_uri": self.leader_id}
        entry = {"term": self.current_term, "command": data}
        self.log.append(entry)
        self.save_state()
        # Replicate
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self.send_append_entries, args=(nid, uri, [entry])).start()
        # Wait for majority, but for simplicity, assume immediate for now
        self.commit_index = len(self.log)
        self.save_data(data)
        return {"success": True, "message": f"Data received by node {self.node_id} from client {client_id}", "data": data}

    def save_data(self, data: str) -> None:
        """Save data to local storage (only leader accepts)."""
        with open(self.data_file, "w") as f:
            f.write(data)

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python candidate.py <node_id>")
        sys.exit(1)
    node_id = sys.argv[1]
    if node_id not in NODES:
        print(f"Invalid node_id: {node_id}")
        sys.exit(1)
    
    node = CandidateNode(node_id)
    port = NODES[node_id]["port"]
    object_id = NODES[node_id]["object_id"]
    
    daemon = Pyro5.server.Daemon(host=NAMESERVER_HOST, port=port)
    uri = daemon.register(node, object_id)
    node.uri = uri
    
    ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
    ns.register(f"node.{node_id}", uri)
    
    print(f"Node {node_id} registered with URI: {uri}")
    
    node.start_election_timer()
    
    daemon.requestLoop()


if __name__ == "__main__":
    main()
