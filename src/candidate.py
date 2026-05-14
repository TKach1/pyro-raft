import random
import threading
import time
import uuid

import Pyro5.api
import Pyro5.server

from src.config import DATA_DIR, ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN, HEARTBEAT_INTERVAL


class CandidateNode:
    """RAFT candidate/follower/leader node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.ns = None
        # TODO: implement RAFT state (follower/candidate/leader)
        self.state = "follower"
        # TODO: implement election logic
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        # TODO: implement log replication
        self.log = []
        self.commit_index = 0
        # Heartbeat tracking
        self.last_heartbeat = time.time()
        # TODO: implement data persistence (JSON)
        self.data_file = f"{DATA_DIR}/{self.node_id}_data.json"

    def _update_metadata(self):
        if self.ns:
            self.ns.register(f"node.{self.node_id}", self.ns.lookup(f"node.{self.node_id}"), metadata={"node", self.state})

    @Pyro5.api.expose
    def request_vote(self, term: int, candidate_id: str) -> dict:
        """Handle RequestVote RPC."""
        # TODO: implement
        pass

    @Pyro5.api.expose
    def append_entries(self, term: int, leader_id: str, entries: list) -> dict:
        """Handle AppendEntries RPC (heartbeat + replication)."""
        # term do leader é menor -> rejeita
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        # reconhece leader válido -> volta pra follower
        self.current_term = term
        self.state = "follower"
        self.leader_id = leader_id
        self.voted_for = None
        self.last_heartbeat = time.time()

        # replica entradas no log
        if entries:
            self.log.extend(entries)
            self.commit_index = len(self.log)

        self._update_metadata()
        return {"term": self.current_term, "success": True}
    
    def _save_data(self, commit_index: int, data: str) -> None:
        """Save data to local storage (only leader accepts)."""
        with open(self.data_file, "w") as f:
            f.write(f"{commit_index}:{data}")
    
    def _read_data(self) -> str:
        """Read data from local storage."""
        try:
            with open(self.data_file, "r") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    @Pyro5.api.expose
    def submit(self, client_id: str, data: str) -> dict:
        """Receive data from client (only leader accepts)."""
        # TODO: implement - reject if not leader, redirect to leader
        if self.state != "leader":
            return {"success": False, "error": "Not the leader", "leader_uri": self.leader_id}
        # Save data on log and replicate to followers
        self.log.append(data)
        self.commit_index += 1
        return {"success": True, "message": f"Data received by node {self.node_id} from client {client_id}", "data": data}

    def _start_election(self, ns):
        """Inicia eleição quando election timeout expira."""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        print(f"[{self.node_id}] Election timeout! Starting election for term {self.current_term}")

        votes = 1  # vota em si mesmo
        peers = ns.yplookup(meta_any={"node"})
        total_nodes = len(peers)

        for name, uri in peers.items():
            if name != f"node.{self.node_id}":
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    result = proxy.request_vote(self.current_term, self.node_id)
                    if result and result.get("vote_granted"):
                        votes += 1
                except Exception:
                    pass

        # ganhou maioria -> vira leader
        if votes >= (total_nodes // 2) + 1:
            self.state = "leader"
            self.leader_id = self.node_id
            self._update_metadata()
            print(f"[{self.node_id}] Won election for term {self.current_term} ({votes}/{total_nodes} votes)")
        else:
            self.state = "follower"
            self._update_metadata()
            print(f"[{self.node_id}] Lost election for term {self.current_term} ({votes}/{total_nodes} votes)")

    def _heartbeat_loop(self, ns):
        while True:
            if self.state == "leader":
                # leader manda heartbeat (append_entries vazio) pra cada follower
                peers = ns.yplookup(meta_any={"node"})
                for name, uri in peers.items():
                    if name != f"node.{self.node_id}":
                        try:
                            proxy = Pyro5.api.Proxy(uri)
                            response = proxy.append_entries(self.current_term, self.node_id, self.log)
                            if response and response.get("success"):
                                for entry in self.log:
                                    self._save_data(self.commit_index, entry)  # salva última entrada replicada
                                self.commit_index += self._read_data().count(":")  # simula commit das entradas replicadas
                        except Exception:
                            pass
                time.sleep(HEARTBEAT_INTERVAL / 1000)
            else:
                # follower/candidate: checa se election timeout expirou
                elapsed = (time.time() - self.last_heartbeat) * 1000  # em ms
                if elapsed >= self.election_timeout:
                    self._start_election(ns)
                    self.last_heartbeat = time.time()
                time.sleep(HEARTBEAT_INTERVAL / 1000)
        
def main():
    """Start candidate node."""
    # TODO: register with Pyro nameserver, start event loop
    ns = Pyro5.api.locate_ns()
    node_id = f"node-{uuid.uuid4().hex[:8]}"
    candidate = CandidateNode(node_id=node_id)
    candidate.ns = ns
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(candidate)
    ns.register(f"node.{node_id}", uri, metadata={"node", candidate.state})
    
    print(f"Candidate node is ready. ID: {candidate.node_id}, URI: {uri}")
    
    t = threading.Thread(target=candidate._heartbeat_loop, args=(ns,), daemon=True)
    t.start()
    
    daemon.requestLoop()
        

if __name__ == "__main__":
    main()
