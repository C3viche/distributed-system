import socket

from distributed_system.config import BUFFER_SIZE, DEFAULT_HOST, DEFAULT_PORT

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((DEFAULT_HOST, DEFAULT_PORT)) # connect to server
    s.sendall(b"Hello World") # send it a message
    data = s.recv(BUFFER_SIZE) # recieve the response

print(f"Received {data!r}")

