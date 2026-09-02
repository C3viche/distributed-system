import socket

from distributed_system.config import BUFFER_SIZE, DEFAULT_HOST, DEFAULT_PORT, ENCODING


class SocketServer:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.host: str = host
        self.port: int = port
        self.s: socket.socket | None = None # placeholder for socket
        
    # TODO: We should make this more robust with a `accept_connections` function and `handle_client` function so that we can both separate concerns of 1. waiting for clients and 2. handling the data pipeline for every specific client that connects. It may also be useful to register client information in a dictionary or array
    def start(self):
        # Here we create our socket:
        #   AF_INET is the Internet address family for IPv4
        #   SOCK_STREAM is the socket type for TCP

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.s.bind((self.host, self.port)) # bind to a specific ip4 addr and port (b/c it's a server)
        self.s.listen() # listen for incoming connections
        conn, addr = self.s.accept() 

        with conn:
            print(f"Connected by {addr}")
            while True:
                # Receive data and send it back to clients
                data = conn.recv(BUFFER_SIZE) # recall that data is in bytes here. We don't need to decode since we're sending it back

                if not data:
                    print("Server: Receieved empty data")
                    break

                res = f"Server: {data.decode(ENCODING)}"
                conn.sendall(res.encode(ENCODING))

    def close(self):
        # Gracefully closes the primary server listener
        if self.s:
            self.s.close()
            print("Server port closed.")

