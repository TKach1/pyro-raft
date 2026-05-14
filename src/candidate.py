import Pyro5.api
import Pyro5.server
import threading
import time
import random
import json
import os
import sys

from config import (
    DATA_DIR, NAMESERVER_HOST, NAMESERVER_PORT,
    NODES, TOTAL_NODES,
    ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL,
)


class CandidateNode:
    """RAFT node (follower/candidate/leader)."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.votes_received = 0
        self.data_file = os.path.join(DATA_DIR, f"{self.node_id}_data.json")
        self.state_file = os.path.join(DATA_DIR, f"{self.node_id}_state.json")
        self.election_timer = None
        self.heartbeat_timer = None
        self.lock = threading.Lock()
        self.uri = None  # Set after daemon registration

        # Direct PYRO URIs for peer nodes
        self.other_nodes = {
            nid: f"PYRO:{NODES[nid]['object_id']}@{NODES[nid]['host']}:{NODES[nid]['port']}"
            for nid in NODES if nid != node_id
        }
        self.next_index = {nid: 1 for nid in self.other_nodes}
        self.match_index = {nid: 0 for nid in self.other_nodes}

        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_state()

    # --- Persistence ---

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
            "commit_index": self.commit_index,
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f)

    def save_data(self, client_id: str, data: str):
        """Persist committed data to JSON file."""
        records = []
        if os.path.exists(self.data_file):
            with open(self.data_file, "r") as f:
                records = json.load(f)
        records.append({
            "client_id": client_id,
            "data": data,
            "timestamp": time.time(),
        })
        with open(self.data_file, "w") as f:
            json.dump(records, f, indent=2)

    # --- Log helpers ---

    def get_last_log_index(self):
        return len(self.log)

    def get_last_log_term(self):
        return self.log[-1]["term"] if self.log else 0

    # --- Election timer ---

    def start_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(ELECTION_TIMEOUT_MIN / 1000, ELECTION_TIMEOUT_MAX / 1000)
        self.election_timer = threading.Timer(timeout, self._start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

    def _reset_election_timer(self):
        self.start_election_timer()

    # --- Election ---

    def _start_election(self):
        with self.lock:
            if self.state == "leader":
                return
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1
            self.save_state()
            print(f"[{self.node_id}] Starting election for term {self.current_term}")

        self._reset_election_timer()
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self._send_request_vote, args=(nid, uri), daemon=True).start()

    def _send_request_vote(self, nid, uri):
        try:
            proxy = Pyro5.api.Proxy(uri)
            proxy._pyroTimeout = 2
            term = self.current_term
            response = proxy.request_vote(
                term, self.node_id,
                self.get_last_log_index(), self.get_last_log_term(),
            )
            should_become_leader = False
            with self.lock:
                if response["term"] > self.current_term:
                    self.current_term = response["term"]
                    self.state = "follower"
                    self.voted_for = None
                    self.save_state()
                    return
                if response["vote_granted"] and self.state == "candidate" and self.current_term == term:
                    self.votes_received += 1
                    if self.votes_received > TOTAL_NODES // 2:
                        should_become_leader = True
            if should_become_leader:
                self._become_leader()
        except Exception as e:
            print(f"[{self.node_id}] Vote request to {nid} failed: {e}")

    def _become_leader(self):
        with self.lock:
            if self.state != "candidate":
                return
            self.state = "leader"
            self.leader_id = self.node_id
            for nid in self.other_nodes:
                self.next_index[nid] = len(self.log) + 1
                self.match_index[nid] = 0
            if self.election_timer:
                self.election_timer.cancel()
            print(f"[{self.node_id}] Became LEADER for term {self.current_term}")

        # Register as leader in nameserver (outside lock - network call)
        try:
            ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            ns.register("leader", self.uri)
        except Exception as e:
            print(f"[{self.node_id}] Failed to register as leader in NS: {e}")

        # Send immediate heartbeats
        self._send_heartbeats_now()

    def _send_heartbeats_now(self):
        if self.state != "leader":
            return
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self._send_append_entries, args=(nid, uri), daemon=True).start()
        self._start_heartbeat_timer()

    # --- Heartbeat ---

    def _start_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL / 1000, self._heartbeat_tick)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def _heartbeat_tick(self):
        if self.state != "leader":
            return
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self._send_append_entries, args=(nid, uri), daemon=True).start()
        self._start_heartbeat_timer()

    # --- AppendEntries (send to follower) ---

    def _send_append_entries(self, nid, uri):
        try:
            proxy = Pyro5.api.Proxy(uri)
            proxy._pyroTimeout = 2
            prev_log_index = self.next_index[nid] - 1
            prev_log_term = (
                self.log[prev_log_index - 1]["term"]
                if 0 < prev_log_index <= len(self.log)
                else 0
            )
            entries = self.log[self.next_index[nid] - 1:]

            response = proxy.append_entries(
                self.current_term, self.node_id,
                prev_log_index, prev_log_term,
                entries, self.commit_index,
            )

            with self.lock:
                if response["term"] > self.current_term:
                    self.current_term = response["term"]
                    self.state = "follower"
                    self.voted_for = None
                    self.save_state()
                    return
                if response["success"]:
                    if entries:
                        self.next_index[nid] = prev_log_index + len(entries) + 1
                        self.match_index[nid] = self.next_index[nid] - 1
                        self._update_commit_index()
                else:
                    self.next_index[nid] = max(1, self.next_index[nid] - 1)
        except Exception as e:
            print(f"[{self.node_id}] AppendEntries to {nid} failed: {e}")

    # --- RPC handlers ---

    @Pyro5.api.expose
    def request_vote(self, term, candidate_id, last_log_index=0, last_log_term=0):
        """Handle RequestVote RPC."""
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "follower"
                self.save_state()

            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if self.voted_for is not None and self.voted_for != candidate_id:
                return {"term": self.current_term, "vote_granted": False}

            # Log up-to-date check
            if last_log_term < self.get_last_log_term():
                return {"term": self.current_term, "vote_granted": False}
            if last_log_term == self.get_last_log_term() and last_log_index < self.get_last_log_index():
                return {"term": self.current_term, "vote_granted": False}

            self.voted_for = candidate_id
            self.save_state()

        self._reset_election_timer()
        print(f"[{self.node_id}] Voted for {candidate_id} in term {term}")
        return {"term": self.current_term, "vote_granted": True}

    @Pyro5.api.expose
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        """Handle AppendEntries RPC (heartbeat + replication)."""
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "follower"
                self.save_state()

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            self.leader_id = leader_id
            self.state = "follower"

            # Log consistency check
            if prev_log_index > 0:
                if prev_log_index > len(self.log):
                    return {"term": self.current_term, "success": False}
                if self.log[prev_log_index - 1]["term"] != prev_log_term:
                    return {"term": self.current_term, "success": False}

            # Append / overwrite entries
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

        self._reset_election_timer()
        return {"term": self.current_term, "success": True}

    # --- Commit ---

    def _update_commit_index(self):
        """Must be called with self.lock held."""
        for i in range(self.commit_index + 1, len(self.log) + 1):
            # RAFT §5.4.2: only commit entries from current term by counting replicas
            if self.log[i - 1]["term"] != self.current_term:
                continue
            count = 1 + sum(1 for mid in self.match_index.values() if mid >= i)
            if count > TOTAL_NODES // 2:
                self.commit_index = i
                self.save_state()

    # --- Client interface ---

    @Pyro5.api.expose
    def submit(self, client_id, data):
        """Receive data from client (only leader accepts)."""
        with self.lock:
            if self.state != "leader":
                return {"success": False, "error": "Not the leader", "leader_id": self.leader_id}
            entry = {"term": self.current_term, "command": data}
            self.log.append(entry)
            self.save_state()
            entry_index = len(self.log)

        # Trigger immediate replication
        for nid, uri in self.other_nodes.items():
            threading.Thread(target=self._send_append_entries, args=(nid, uri), daemon=True).start()

        # Wait for majority commit
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.commit_index >= entry_index:
                self.save_data(client_id, data)
                return {"success": True, "message": f"Committed by leader {self.node_id}", "data": data}
            time.sleep(0.05)

        return {"success": False, "error": "Timeout waiting for majority commit"}

    # --- Status ---

    @Pyro5.api.expose
    def get_status(self):
        """Return node status."""
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
        }


def main(node_id: str):
    if node_id not in NODES:
        print(f"Unknown node_id: {node_id}. Valid: {list(NODES.keys())}")
        sys.exit(1)

    node_config = NODES[node_id]
    my_port = node_config["port"]

    node = CandidateNode(node_id)

    daemon = Pyro5.server.Daemon(host="0.0.0.0", port=my_port)
    daemon.register(node, objectId=node_id)
    node.uri = f"PYRO:{node_id}@{node_config['host']}:{my_port}"

    print(f"[{node_id}] Pyro daemon on port {my_port}, URI: {node.uri}")
    print(f"[{node_id}] Peers: {list(node.other_nodes.keys())}")

    node.start_election_timer()
    daemon.requestLoop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python candidate.py <node_id>")
        sys.exit(1)
    main(sys.argv[1])
