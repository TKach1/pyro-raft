import Pyro5.nameserver

from src.config import NAMESERVER_HOST, NAMESERVER_PORT

def main():
    Pyro5.nameserver.start_ns_loop(host=NAMESERVER_HOST, port=NAMESERVER_PORT)


if __name__ == "__main__":
    main()
