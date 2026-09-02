from distributed_system.client.client import SocketClient
from distributed_system.config import DEFAULT_HOST, DEFAULT_PORT


def main():
    print("Client node started...")
    client = SocketClient()
    client.connect(DEFAULT_HOST, DEFAULT_PORT)
    
    res: str = client.send("Hello World!")
    print(res)

    client.close()

