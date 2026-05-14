import Pyro5.nameserver

from config import NAMESERVER_PORT


def main():
    print(f"Starting Pyro nameserver on port {NAMESERVER_PORT}...")
    Pyro5.nameserver.start_ns_loop(host="0.0.0.0", port=NAMESERVER_PORT)


if __name__ == "__main__":
    main()
