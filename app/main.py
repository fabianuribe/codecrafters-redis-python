import socket
import threading

def handle_client_connection(conn):
    try:
        data = conn.recv(1024).decode("utf-8")
        if "ping" in data:
            conn.send(b"+PONG\r\n")

    except ValueError as e:
        print(e)
        conn.close();

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()

    while True:
        conn = server_socket.accept() 
        client_thread = threading.Thread(
            target=handle_client_connection, args=(conn)
        )
        client_thread.start()


if __name__ == "__main__":
    main()
