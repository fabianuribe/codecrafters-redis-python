import socket
import threading
from datetime import datetime, timedelta

db = {}

def parse_command(command: str) -> list[str]:
    parsed_command = []
    command_type = command[0]
    parts = command[1:].split("\r\n");

    # Arrays
    if command_type == "*":
        parsed_command.extend(parts[2::2])

    return parsed_command

def encode_response(response: list[str], type: str) -> bytes:
    new_line = '\r\n'
    res = ""
    if type == "simple":
        res = f"+{response[0]}"
    else:
        response_parts = []
        if len(response) == 0:
            response_parts.append(-1)
        else:
            for part in response:
                response_parts.append(len(part))
                response_parts.append(part)

        res = f"${new_line.join(str(part) for part in response_parts)}"

    return f"{res}{new_line}".encode("utf-8")

def calculate_expiry(milliseconds: int):
    return datetime.now() + timedelta(milliseconds=milliseconds)

def is_expired(item: tuple):
    return item[1] != None and datetime.now() > item[1]

def set_db_item(key, value, px = -1):
    expiry = calculate_expiry(px) if px >= 0 else None
    db[key] = (value, expiry)

def get_db_item(key): 
    return db[key] if key in db and not is_expired(db[key]) else None

def handle_client_connection(conn, address):
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
                conn.send(encode_response(["PONG"], "simple"))

            elif command == "echo" :
                conn.send(encode_response(arguments, "bulk"))

            elif command == "set" :
                px = int(arguments[3]) if (len(arguments) >= 4 and arguments[2].lower() == "px") else -1
                set_db_item(arguments[0], arguments[1], px)
                conn.send(encode_response(["OK"], "simple"))

            elif command == "get" :
                value = get_db_item(arguments[0])
                conn.send(encode_response([value[0]] if value else [], "bulk"))

            else:
                raise ValueError(f"Unsupported command: {command}")

    except ValueError as e:
        print(e)
        conn.close()

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()

    while True:
        conn, address = server_socket.accept() 
        client_thread = threading.Thread(
            target=handle_client_connection, args=(conn, address)
        )
        client_thread.start()


if __name__ == "__main__":
    main()
