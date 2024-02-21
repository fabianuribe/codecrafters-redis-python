import argparse
import socket
import threading
from datetime import datetime, timedelta

db = {}
replication = {
    "role": "master",
    "connected_slaves": 0,
    "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "master_repl_offset": 0
}

parser = argparse.ArgumentParser(description='Example script to take a port argument.')
parser.add_argument('--port', type=int, help='The port number to use.', default=6379)
parser.add_argument('--replicaof', type=str, nargs=2, help='The master host and master port for the replica.')

def connect_to_master(host: str, host_port: int, self_port: int):
    """stablishes connection with master"""
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect the socket to the server's port
        server_address = (host, host_port)
        print(f'Connecting to master on {host} port {host_port}')
        sock.connect(server_address)

        # Send PING
        print(f'Sending: PING')
        sock.sendall(encode_message(["PING"], "array"))

        # Look for the response (optional)
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')  # Decode bytes to string

        sock.sendall(encode_message(["REPLCONF", "listening-port", str(self_port)], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')  # Decode bytes to string

        sock.sendall(encode_message(["REPLCONF", "capa", "eof", "capa", "psync2"], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')  # Decode bytes to string

        sock.sendall(encode_message(["PSYNC", "?", "-1"], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')  # Decode bytes to string

    finally:
        # Clean up the connection
        sock.close()

def parse_command(command: str) -> list[str]:
    """Parses a Redis command and returns the components."""
    if command.startswith("*"):
        parts = command[1:].split("\r\n");
        return parts[2::2] # Skip every other element starting from the third

    return []

def encode_message(message: list[str], type: str) -> bytes:
    """Encodes a response based on the response type."""
    new_line = '\r\n'
    msg = ""
    if type == "simple":
        msg = f"+{message[0]}"
    elif type == "array":
        message_parts = []
        if len(message) == 0:
            message_parts.append(-1)
        else:
            for part in message:
                message_parts.append(f"${len(part)}")
                message_parts.append(part)

        msg = f"*{len(message)}{new_line}{new_line.join(str(part) for part in message_parts)}"
    else:
        message_parts = []
        if len(message) == 0:
            message_parts.append(-1)
        else:
            for part in message:
                message_parts.append(len(part))
                message_parts.append(part)

        msg = f"${new_line.join(str(part) for part in message_parts)}"

    return f"{msg}{new_line}".encode("utf-8")

def calculate_expiry(milliseconds: int) -> datetime:
    """Calculates the expiry time from now."""
    return datetime.now() + timedelta(milliseconds=milliseconds)

def is_expired(item: tuple) -> bool:
    """Checks if an item has expired."""
    return item[1] != None and datetime.now() > item[1]

def set_db_item(key: str, value: str, px: int = -1):
    """Sets an item in the database with optional expiry."""
    expiry = calculate_expiry(px) if px >= 0 else None
    db[key] = (value, expiry)

def get_db_item(key) -> tuple | None: 
    """Retrieves an item from the database if not expired."""
    return db[key] if key in db and not is_expired(db[key]) else None

def get_replication_info() -> str:
    """Retrieves the instances replication information."""
    return "\n".join(["# Replication"] +[f'{key}:{value}' for key, value in replication.items()])

def handle_client_connection(conn, address):
    """Handles a client connection, processing commands."""
    try:
        while True:
            data = conn.recv(1024).decode("utf-8")
            if not data:
                break

            command_parts = parse_command(data)
            if len(command_parts) == 0:
                break;

            command =  command_parts[0].lower()
            arguments = command_parts[1:]

            if command == "ping" :
                print(f"PING from {address}")
                conn.send(encode_message(["PONG"], "simple"))
            elif command == "replconf" :
                print(f"REPLCONF ({arguments[0]}) from {address}")
                conn.send(encode_message(["OK"], "simple"))
            elif command == "psync" :
                print(f"PSYNC from {address}")
                conn.send(encode_message(["FULLRESYNC <REPL_ID> 0"] , "simple"))
            elif command == "info" :
                print(f"INFO from {address}")
                if len(arguments) and arguments[0].lower() == "replication":
                    conn.send(encode_message([get_replication_info()], "bulk"))
            elif command == "echo" :
                print(f"ECHO from {address}")
                conn.send(encode_message(arguments, "bulk"))
            elif command == "set" :
                print(f"SET from {address}")
                px = int(arguments[3]) if (len(arguments) >= 4 and arguments[2].lower() == "px") else -1
                set_db_item(arguments[0], arguments[1], px)
                conn.send(encode_message(["OK"], "simple"))
            elif command == "get" :
                print(f"GET from {address}")
                value = get_db_item(arguments[0])
                conn.send(encode_message([value[0]] if value else [], "bulk"))
            else:
                raise ValueError(f"Unsupported command: {command}")

    except ValueError as e:
        print(f"Error handling client {address}: {e}")

def main():
    args = parser.parse_args()
    port = args.port or 6379

    if args.replicaof:
        master_host, master_port = args.replicaof
        replication["role"] = "slave"
        print(f"Configured as slave of: {master_host}:{master_port}")
        connect_to_master(master_host, int(master_port), int(port))

    with socket.create_server(("localhost", port), reuse_port=True) as server_socket:
        server_socket.listen()
        print(f"Server listening on localhost:{port}")
        while True:
            conn, address = server_socket.accept()
            threading.Thread(target=handle_client_connection, args=(conn, address)).start()

if __name__ == "__main__":
    main()
