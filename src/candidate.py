import Pyro5.api
import Pyro5.server

from src.config import DATA_DIR
from src.config import NAMESERVER_HOST, NAMESERVER_PORT

@Pyro5.api.expose
class CandidateNode:
    """RAFT candidate/follower/leader node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        # TODO: implement RAFT state (follower/candidate/leader)
        self.state = "follower"
        # TODO: implement election logic
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        # TODO: implement log replication
        self.log = []
        self.commit_index = 0
        # TODO: implement heartbeat
        self.last_heartbeat = 0
        # TODO: implement data persistence (JSON)
        self.data_file = f"{DATA_DIR}/{self.node_id}_data.json"

    def request_vote(self, term: int, candidate_id: str, last_log_index: int = 0, last_log_term: int = 0) -> dict:
        """Handle RequestVote RPC."""
        # If the incoming term is greater, update local term and convert to follower.
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"

        # Reject if the candidate's term is stale.
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        # Determine whether this node has already voted in this term.
        if self.voted_for is not None and self.voted_for != candidate_id:
            return {"term": self.current_term, "vote_granted": False}
        
        # Grant vote if:
        # 1. Haven't voted in this term OR already voted for this candidate
        # 2. Candidate's log is up-to-date or more recent
        
        can_vote = (self.voted_for is None)
    
        if can_vote and last_log_term >= self.get_last_log_term():
            self.voted_for = candidate_id
            return {"term": self.current_term, "vote_granted": True}
    
        return {"term": self.current_term, "vote_granted": False}

    def append_entries(self, term: int, leader_id: str, entries: list) -> dict:
        """Handle AppendEntries RPC (heartbeat + replication)."""
        # TODO: implement
        pass
    
    def save_data(self, data: str) -> None:
        """Save data to local storage (only leader accepts)."""

        with open(self.data_file, "w") as f:
            f.write(data)

    def submit(self, client_id: str, data: str) -> dict:
        """Receive data from client (only leader accepts)."""
        # TODO: implement - reject if not leader, redirect to leader
        if self.state != "leader":
            return {"success": False, "error": "Not the leader", "leader_uri": self.leader_id}
        # Save data and replicate to followers
        self.append_entries(term=self.current_term, leader_id=self.node_id, entries=[data])
        self.save_data(data)
        self.commit_index += 1
        return {"success": True, "message": f"Data received by node {self.node_id} from client {client_id}", "data": data}

def main():
    # TODO: register with Pyro nameserver, start event loop
    
    import sys

    node_id = input("Digite o ID do nó: ")  
    
    node = CandidateNode(node_id)

    ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)

    daemon = Pyro5.server.Daemon(host = NAMESERVER_HOST)
    
    uri = daemon.register(node)
    
    ns.register(f"node.{node_id}", uri)

    print(f"Node {node_id} registered with URI: {uri}")

    daemon.requestLoop()
    
    
    pass


if __name__ == "__main__":
    main()
