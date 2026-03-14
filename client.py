import argparse
import os
import socket
import ssl
import time
import uuid
HOST = os.getenv("SERVER_HOST", "127.0.0.1")
PORT = int(os.getenv("SERVER_PORT", "65432"))
VALID_ACTIONS = {"BOOK", "CANCEL", "QUERY", "LIST", "CLEAR"}
def parse_server_endpoint(server: str) -> tuple[str, int]:
    endpoint = server.strip()
    if not endpoint:
        raise ValueError("Server endpoint cannot be empty")
    if ":" not in endpoint:
        raise ValueError("Server endpoint must be in host:port format")
    host, port_text = endpoint.rsplit(":", 1)
    host = host.strip()
    if not host:
        raise ValueError("Server host cannot be empty")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("Server port must be a number") from exc
    if port <= 0 or port > 65535:
        raise ValueError("Server port must be between 1 and 65535")
    return host, port
def build_request(action: str, request_id: str, seat_id: str = "-", user_id: str = "-") -> str:
    client_ts = int(time.time() * 1000)
    action = action.upper()
    if action == "LIST":
        return f"LIST|{request_id}|{client_ts}\n"
    if action == "CLEAR":
        return f"CLEAR|{request_id}|{user_id}|{client_ts}\n"
    return f"{action}|{request_id}|{seat_id}|{user_id}|{client_ts}\n"
def send_request(
    action: str,
    seat_id: str = "-",
    user_id: str = "-",
    request_id: str | None = None,
    host: str = HOST,
    port: int = PORT,
) -> str:
    request_id = request_id or str(uuid.uuid4())
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    secure_client = context.wrap_socket(client_socket, server_hostname=host)
    try:
        secure_client.connect((host, port))
        request = build_request(action, request_id, seat_id, user_id)
        secure_client.sendall(request.encode("utf-8"))
        file = secure_client.makefile("rb")
        response = file.readline().decode("utf-8").strip()
        return response
    finally:
        secure_client.close()
def _parse_response(response: str) -> dict[str, str]:
    parts = response.split("|", 6)
    if len(parts) < 6:
        return {"raw": response}
    return {
        "raw": response,
        "kind": parts[0],
        "request_id": parts[1],
        "status": parts[2],
        "server_ts": parts[3],
        "code": parts[4],
        "message": parts[5],
        "payload": parts[6] if len(parts) > 6 else "",
    }
def _print_list_table(response: str) -> None:
    parsed = _parse_response(response)
    if "status" not in parsed:
        print(response)
        return
    status = parsed["status"]
    code = parsed["code"]
    message = parsed["message"]
    payload = parsed["payload"]
    print(f"Status: {status}  Code: {code}  Message: {message}")
    if status != "OK":
        return
    version = "-"
    seat_map_raw = payload
    if ";" in payload:
        version_part, seat_map_raw = payload.split(";", 1)
        if version_part.startswith("version="):
            version = version_part.split("=", 1)[1]
    rows: list[tuple[str, str]] = []
    for item in seat_map_raw.split(","):
        if not item or ":" not in item:
            continue
        seat, owner = item.split(":", 1)
        rows.append((seat.strip(), owner.strip() if owner.strip() != "-" else "AVAILABLE"))
    if not rows:
        print("No seat data available")
        return
    seat_width = max(len("Seat"), max(len(seat) for seat, _ in rows))
    owner_width = max(len("Booker"), max(len(owner) for _, owner in rows))
    border = f"+-{'-' * seat_width}-+-{'-' * owner_width}-+"
    print(f"Version: {version}")
    print(border)
    print(f"| {'Seat'.ljust(seat_width)} | {'Booker'.ljust(owner_width)} |")
    print(border)
    for seat, owner in sorted(rows):
        print(f"| {seat.ljust(seat_width)} | {owner.ljust(owner_width)} |")
    print(border)
def _parse_payload_map(payload: str) -> dict[str, str]:
    data = {}
    for item in payload.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        data[key.strip()] = value.strip()
    return data
def _print_book_table(response: str) -> None:
    parsed = _parse_response(response)
    if "status" not in parsed:
        print(response)
        return
    print(f"Status: {parsed['status']}  Code: {parsed['code']}  Message: {parsed['message']}")
    payload = _parse_payload_map(parsed.get("payload", ""))
    seat = payload.get("seat", "-")
    booker = payload.get("owner", "-")
    version = payload.get("version", "-")
    if booker == "-":
        booker = "AVAILABLE"
    seat_width = max(len("Seat"), len(seat))
    booker_width = max(len("Booker"), len(booker))
    version_width = max(len("Version"), len(version))
    border = f"+-{'-' * seat_width}-+-{'-' * booker_width}-+-{'-' * version_width}-+"
    print(border)
    print(
        f"| {'Seat'.ljust(seat_width)} | {'Booker'.ljust(booker_width)} | {'Version'.ljust(version_width)} |"
    )
    print(border)
    print(f"| {seat.ljust(seat_width)} | {booker.ljust(booker_width)} | {version.ljust(version_width)} |")
    print(border)
def _print_clear_table(response: str) -> None:
    parsed = _parse_response(response)
    if "status" not in parsed:
        print(response)
        return
    print(f"Status: {parsed['status']}  Code: {parsed['code']}  Message: {parsed['message']}")
    payload = _parse_payload_map(parsed.get("payload", ""))
    cleared_by = payload.get("cleared_by", "-")
    cleared_count = payload.get("cleared_count", "-")
    version = payload.get("version", "-")
    by_width = max(len("Cleared By"), len(cleared_by))
    count_width = max(len("Seats Cleared"), len(cleared_count))
    version_width = max(len("Version"), len(version))
    border = f"+-{'-' * by_width}-+-{'-' * count_width}-+-{'-' * version_width}-+"
    print(border)
    print(
        f"| {'Cleared By'.ljust(by_width)} | {'Seats Cleared'.ljust(count_width)} | {'Version'.ljust(version_width)} |"
    )
    print(border)
    print(f"| {cleared_by.ljust(by_width)} | {cleared_count.ljust(count_width)} | {version.ljust(version_width)} |")
    print(border)
def _execute_and_print(
    action: str,
    seat: str,
    user: str,
    request_id: str | None = None,
    host: str = HOST,
    port: int = PORT,
) -> None:
    response = send_request(action, seat_id=seat, user_id=user, request_id=request_id, host=host, port=port)
    action = action.upper()
    if action == "LIST":
        _print_list_table(response)
    elif action == "BOOK":
        _print_book_table(response)
    elif action == "CLEAR":
        _print_clear_table(response)
    else:
        print(response)
def _interactive_mode(host: str = HOST, port: int = PORT) -> None:
    print("Reservation Client - Interactive Mode")
    print("Commands: BOOK, CANCEL, QUERY, LIST, CLEAR, EXIT")
    while True:
        action = input("\nAction> ").strip().upper()
        if action in {"EXIT", "QUIT"}:
            print("Bye")
            break
        if action not in VALID_ACTIONS:
            print("Invalid action. Use BOOK/CANCEL/QUERY/LIST or EXIT.")
            continue
        seat = "SEAT_1"
        user = "USER_A"
        if action in {"BOOK", "CANCEL", "QUERY"}:
            entered_seat = input("Seat (default SEAT_1)> ").strip().upper()
            entered_user = input("User (default USER_A)> ").strip().upper()
            if entered_seat:
                seat = entered_seat
            if entered_user:
                user = entered_user
        elif action == "CLEAR":
            entered_user = input("Cleared by (default USER_A)> ").strip().upper()
            if entered_user:
                user = entered_user
        _execute_and_print(action, seat, user, host=host, port=port)
def main() -> None:
    parser = argparse.ArgumentParser(description="Reservation client")
    parser.add_argument("action", nargs="?", choices=["BOOK", "CANCEL", "QUERY", "LIST", "CLEAR"], help="Action to execute")
    parser.add_argument("--seat", default="SEAT_1", help="Seat id, e.g. SEAT_1")
    parser.add_argument("--user", default="USER_A", help="User id")
    parser.add_argument("--req", default=None, help="Optional request id for idempotent retries")
    parser.add_argument("--server", default=None, help="Server endpoint in host:port format")
    parser.add_argument("--host", default=HOST, help="Server host/IP")
    parser.add_argument("--port", default=PORT, type=int, help="Server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    if args.server:
        host, port = parse_server_endpoint(args.server)
    if not args.action:
        _interactive_mode(host=host, port=port)
        return
    _execute_and_print(args.action, args.seat, args.user, args.req, host=host, port=port)
if __name__ == "__main__":
    main()