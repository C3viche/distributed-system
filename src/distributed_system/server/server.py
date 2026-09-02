import socket

from distributed_system.config import BUFFER_SIZE, DEFAULT_HOST, DEFAULT_PORT

# Here we create our socket:
#   AF_INET is the Internet address family for IPv4
#   SOCK_STREAM is the socket type for TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((DEFAULT_HOST, DEFAULT_PORT)) # bind to a specific ip4 addr and port (b/c it's a server)
    s.listen() # listen for incoming connections
    conn, addr = s.accept() 
    
    with conn:
        print(f"Connected by {addr}")
        while True:
            # Receive data and send it back to clients
            data = conn.recv(BUFFER_SIZE)
            if not data:
                break
            conn.sendall(data)
