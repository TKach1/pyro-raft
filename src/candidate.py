import Pyro5.api
import Pyro5.server

from src.config import DATA_DIR


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

    def request_vote(self, term: int, candidate_id: str) -> dict:
        """Handle RequestVote RPC."""
        # TODO: implement
        pass

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
    """Start candidate node."""
    # TODO: register with Pyro nameserver, start event loop
    pass


if __name__ == "__main__":
    main()
