import argparse
import base64
from collections import namedtuple
import socket
import threading
from datetime import datetime, timedelta
import select
import time

Replica = namedtuple('Replica', ['host', 'port', 'connection'])

db = {}
replicas = []
replication = {
    "role": "master",
    "connected_slaves": 0,
    "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "master_repl_offset": 0,
}

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

def encode_file(file: bytes) -> bytes:
    """Encodes file for transmission via the RESP protocol."""
    new_line = '\r\n'
    return f"${len(file)}{new_line}".encode("utf-8") + file

def construct_rdb() -> bytes:
    """Constructs a binary RDB reperesentation of the current state of the replica"""
    EMPTY_RDB_FILE = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    return base64.b64decode(EMPTY_RDB_FILE)

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

def register_replica(replica: Replica):
    """Registers a new replica"""
    replicas.append(replica)

def remove_replica(replica: Replica):
    """Removes a replica"""
    replica.connection.close()
    replicas.remove(replica)

def propagate_command(command):
    """Propagates the command to all the registered replicas."""
    if len(replicas):
        print(f"Propagating to {len(replicas)} replicas.")
        for replica in replicas:
            try:
                replica.connection.sendall(command.encode("utf-8"))
            except Exception as e:
                print(f"Error propagating to {replica.host}:{replica.port} : {e}")
                remove_replica(replica)

def handle_command(resp: str, conn, address):
    """Handles processing of RESP commands."""
    command_parts = parse_command(resp)
    if len(command_parts) == 0:
        return;

    command =  command_parts[0].lower()
    arguments = command_parts[1:]

    print(f"{command.upper()} from {address}")

    if command == "ping" :
        conn.send(encode_message(["PONG"], "simple"))
    elif command == "replconf" :
        if arguments[0].lower() == "listening-port":
            register_replica(Replica(host=address[0], port=int(arguments[1]), connection=conn))
        conn.send(encode_message(["OK"], "simple"))
    elif command == "psync" :
        conn.sendall(encode_message([f"FULLRESYNC {replication['master_replid']} 0"] , "simple"))
        conn.sendall(encode_file(construct_rdb()))
    elif command == "info" :
        if len(arguments) and arguments[0].lower() == "replication":
            conn.send(encode_message([get_replication_info()], "bulk"))
    elif command == "echo" :
        conn.send(encode_message(arguments, "bulk"))
    elif command == "set" :
        px = int(arguments[3]) if (len(arguments) >= 4 and arguments[2].lower() == "px") else -1
        set_db_item(arguments[0], arguments[1], px)
        conn.send(encode_message(["OK"], "simple"))
        propagate_command(resp)
    elif command == "get" :
        value = get_db_item(arguments[0])
        conn.send(encode_message([value[0]] if value else [], "bulk"))
    else:
        raise ValueError(f"Unsupported command: {command}")


def handle_client_connection(conn, address):
    """Handles a client connection, processing commands."""
    try:
        while True:
            resp = conn.recv(1024).decode("utf-8")
            if not resp:
                break
            handle_command(resp, conn, address)

    except ValueError as e:
        print(f"Error handling client {address}: {e}")

def read_until_newline(sock):
    """Reads from the socket until it encounters \r\n, returning the accumulated data."""
    data = b''
    while not data.endswith(b'\r\n'):
        part = sock.recv(1)  # Reading byte by byte might be inefficient, consider larger chunks
        if part == b'':
            raise Exception("Socket connection lost")
        data += part
    return data[:-2]  # Remove the trailing \r\n

def read_bulk_string(sock, length):
    """Reads a specific length of bytes from the socket, for bulk string handling."""
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if chunk == b'':
            raise Exception("Socket connection lost")
        data += chunk
    return data

def handle_psync_response(sock):
    """Handles the PSYNC response, ensuring the full resync message and RDB file are correctly processed."""
    # Handle the +FULLRESYNC part
    full_resync_response = read_until_newline(sock).decode()
    print(f"Received: {full_resync_response}")

    if not full_resync_response.startswith('+FULLRESYNC'):
        raise Exception("Unexpected response to PSYNC")

    # Now, expecting the bulk string with the RDB file
    bulk_string_header = read_until_newline(sock).decode()
    if not bulk_string_header.startswith('$'):
        raise Exception("Expected bulk string for RDB file")

    length_of_file = int(bulk_string_header[1:])
    rdb_file_contents = read_bulk_string(sock, length_of_file)
    #TODO: Implement DB sync

def connect_to_master(host: str, host_port: int, self_port: int):
    """stablishes connection with master"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect the socket to the server's port
        server_address = (host, host_port)
        print(f'Connecting to master on {host} port {host_port}')
        sock.connect(server_address)

        # Send PING
        print(f'Sending: PING')
        sock.sendall(encode_message(["PING"], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')

        # Send 1st REPLCONF
        sock.sendall(encode_message(["REPLCONF", "listening-port", str(self_port)], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')

        # Send 2nd REPLCONF
        sock.sendall(encode_message(["REPLCONF", "capa", "eof", "capa", "psync2"], "array"))
        response = sock.recv(4096)
        print(f'Received: {response.decode()}')

        # Send PSYNC
        sock.sendall(encode_message(["PSYNC", "?", "-1"], "array"))
        handle_psync_response(sock)

        return sock;

    except Exception as e:
        print(f"Error connecting to master: {e}")
        return None;

def start_replication(host: str, host_port: int, self_port: int):
    """Start connection with the master."""

    try:
        sock = connect_to_master(host, host_port, self_port)
        print(f"Replica ready...")
        while sock:
            ready_to_read, _, _ = select.select([sock], [], [], 5)
            if ready_to_read:
                response = sock.recv(4096)
                if not response:
                    break  # Connection closed by the master
                print(f'Received from Master: {response}')
                handle_command(response.decode("utf-8"), sock, (host, host_port))
            # Implement heartbeat here, if necessary
    except Exception as e:
        print(f"Replication Ended")
        print(f"Error: {e}")

def main():
    parser = argparse.ArgumentParser(description='Example script to take a port argument.')
    parser.add_argument('--port', type=int, help='The port number to use.', default=6379)
    parser.add_argument('--replicaof', type=str, nargs=2, help='The master host and master port for the replica.')

    args = parser.parse_args()
    port = args.port or 6379

    if args.replicaof:
        master_host, master_port = args.replicaof
        replication["role"] = "slave"
        print(f"Configured as slave of: {master_host}:{master_port}")
        threading.Thread(target=start_replication, args=(master_host, int(master_port), int(port))).start()
        # start_replication(master_host, int(master_port), int(port))

    with socket.create_server(("localhost", port), reuse_port=True) as server_socket:
        server_socket.listen()
        print(f"Server listening on localhost:{port}")
        while True:
            conn, address = server_socket.accept()
            threading.Thread(target=handle_client_connection, args=(conn, address)).start()

if __name__ == "__main__":
    main()
