import uuid
import time
import os

import Pyro5.api

from config import NAMESERVER_HOST, NAMESERVER_PORT


class ClientNode:
    """Client that submits data to the RAFT cluster leader."""

    def __init__(self, client_id: str):
        self.client_id = client_id
        self.leader_uri = None

    def find_leader(self):
        ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        self.leader_uri = ns.lookup("leader")
        return self.leader_uri

    def send(self, data: str) -> dict:
        """Send string data to leader with retries."""
        for attempt in range(5):
            try:
                if not self.leader_uri:
                    self.find_leader()
                leader = Pyro5.api.Proxy(self.leader_uri)
                leader._pyroTimeout = 10
                response = leader.submit(client_id=self.client_id, data=data)
                if response.get("success"):
                    print(f"[{self.client_id}] OK: {response}")
                    return response
                if response.get("leader_id"):
                    # Not the leader, re-discover
                    self.leader_uri = None
                    continue
                print(f"[{self.client_id}] Failed: {response}")
                return response
            except Exception as e:
                print(f"[{self.client_id}] Attempt {attempt + 1} failed: {e}")
                self.leader_uri = None
                time.sleep(2)
        return {"success": False, "error": "Failed after retries"}


def main():
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    client = ClientNode(client_id=client_id)
    print(f"[{client_id}] Client ready")

    auto_interval = float(os.getenv("AUTO_SEND_INTERVAL", "0"))

    if auto_interval > 0:
        # Auto mode for Docker
        print(f"[{client_id}] Auto mode (interval={auto_interval}s), waiting for cluster...")
        time.sleep(5)
        counter = 0
        while True:
            counter += 1
            data = f"data-{counter}-from-{client_id}"
            client.send(data)
            time.sleep(auto_interval)
    else:
        # Interactive mode
        while True:
            try:
                data = input("Data to send: ")
                if data.strip():
                    client.send(data)
            except (EOFError, KeyboardInterrupt):
                print(f"\n[{client_id}] Exiting")
                break


if __name__ == "__main__":
    main()
