import socket
import threading

def parse_command(command: str) -> list[str]:
    parsed_command = []
    command_type = command[0]
    parts = command[1:].split("\r\n");

    # Arrays
    if command_type == "*":
        item_count = int(parts[0])
        slice_start = 2
        slice_end = slice_start + item_count + 1
        parsed_command.extend(parts[slice_start:slice_end:2])

    return parsed_command

def encode_response(response: list[str], type: str) -> bytes:
    new_line = '\r\n'
    res = ""
    if type == "simple":
        res = f"+{response[0]}"
    else:
        response_parts = []
        for part in response:
            response_parts.append(len(part))
            response_parts.append(part)
        res = f"${new_line.join(str(part) for part in response_parts)}"

    return f"{res}{new_line}".encode("utf-8")

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
