from distributed_system.server.server import SocketServer


def main():
    print("Server node started...")
    server = SocketServer()
    server.start()
    server.close()
