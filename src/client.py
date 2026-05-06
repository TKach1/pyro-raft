import Pyro5.api


class ClientNode:
    """Client that submits data to the RAFT cluster leader."""

    def __init__(self, client_id: str):
        self.client_id = client_id
        # TODO: discover leader via nameserver
        # TODO: handle leader redirect

    def send(self, data: str) -> dict:
        """Send string data to leader."""
        # TODO: implement - find leader, submit data, handle redirect
        pass


def main():
    """Start client node."""
    # TODO: connect to nameserver, find leader, send data
    pass


if __name__ == "__main__":
    main()
