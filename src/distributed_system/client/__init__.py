from distributed_system.client.client import SocketClient
from distributed_system.config import get_address


def main():
    print("Client node started...")
    client = SocketClient()
    client.connect(*get_address("S1"))
    
    res: str = client.send("Hello World!")
    print(res)

    client.close()

