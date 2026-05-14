import uuid

import Pyro5.api

from src.config import NAMESERVER_HOST, NAMESERVER_PORT


class ClientNode:
    """Client that submits data to the RAFT cluster leader."""

    def __init__(self, client_id: str):
        self.client_id = client_id
        # TODO: discover leader via nameserver
        self.leader_uri = None
        # TODO: handle leader redirect - não precisa

    def send(self, data: str) -> dict:
        """Send string data to leader."""
        # TODO: implement - find leader, submit data, handle redirect
        ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        self.leader_uri = ns.yplookup("leader")
        leader = Pyro5.api.Proxy(self.leader_uri)
        try:
            response = leader.submit(client_id=self.client_id, data=data)
            print(f"Response from leader: {response}")
            return {"success": True, "message": response}
        except Pyro5.errors.PyroError as e:
            print(f"Error sending data to leader: {e}")
            return {"success": False, "error": str(e)}

def main():
    """Start client node."""
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    client = ClientNode(client_id=client_id)
    
    print(f"Client node is ready. ID: {client.client_id}")
    
    # TODO: connect to nameserver, find leader, send data
    while True:
        data = input("Data to send: ")  # or generate automatically
        client.send(data)
        
if __name__ == "__main__":
    main()
