import socket

from distributed_system.config import BUFFER_SIZE, ENCODING


class SocketClient:
    def __init__(self, buf_size: int = BUFFER_SIZE):
        self.buf_size: int = buf_size
        self.s: socket.socket | None = None # declare socket placeholder

    def connect(self, host: str, port: int):
        # Actually initialize the socket here
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))

    def send(self, msg: str) -> str:
        # Check if socket is actually initialized from connect first
        if not self.s:
            raise RuntimeError("Client is not connected to a server! Call connect() first.")
        self.s.sendall(msg.encode(ENCODING)) # send message
        
        # TODO: Make separate receive function b/c distributed systems aren't always one-to-one request/response. Asynchronous heartbeats or other messages may come from server at any time
        data = self.s.recv(self.buf_size) # wait for response 
        print(f"Client: Received {data!r}")

        return data.decode(ENCODING) # return decoded message

        
    def close(self):
        # Gracefully tears down the network socket
        if self.s:
            self.s.close()
            self.s = None
            print("Connection closed cleanly.")
