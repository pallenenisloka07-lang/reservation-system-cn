import argparse
import json
import os
import signal
import socket
import ssl
import subprocess
import threading
import time
HOST = os.getenv("SERVER_HOST", "0.0.0.0")
PORT = int(os.getenv("SERVER_PORT", "65432"))
SEAT_COUNT = 5
CERT_FILE = "server.crt"
KEY_FILE = "server.key"
SNAPSHOT_FILE = "state_snapshot.json"
JOURNAL_FILE = "booking_journal.jsonl"
state_lock = threading.Lock()
state = {
    "version": 0,
    "seats": {f"SEAT_{i}": None for i in range(1, SEAT_COUNT + 1)}
}
request_cache = {}
shutdown_event = threading.Event()
def now_ms() -> int:
    return int(time.time() * 1000)
def save_snapshot() -> None:
    temp_file = f"{SNAPSHOT_FILE}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_file, SNAPSHOT_FILE)
def append_journal(entry: dict) -> None:
    with open(JOURNAL_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
        f.flush()
        os.fsync(f.fileno())
def apply_entry(entry: dict) -> None:
    action = entry["action"]
    seat_id = entry["seat_id"]
    user_id = entry["user_id"]
    if action == "BOOK":
        state["seats"][seat_id] = user_id
    elif action == "CANCEL":
        state["seats"][seat_id] = None
    elif action == "CLEAR":
        for seat in state["seats"]:
            state["seats"][seat] = None
    state["version"] = entry["version"]
def recover_state() -> None:
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            snapshot = json.load(f)
            state["version"] = int(snapshot.get("version", 0))
            loaded_seats = snapshot.get("seats", {})
            for seat_id in state["seats"]:
                state["seats"][seat_id] = loaded_seats.get(seat_id)
    if os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if int(entry.get("version", 0)) > state["version"]:
                    apply_entry(entry)
    save_snapshot()
    print(f"[RECOVERY] version={state['version']} seats={state['seats']}")
def ensure_certificates() -> None:
    if os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE):
        return
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            KEY_FILE,
            "-out",
            CERT_FILE,
            "-days",
            "365",
            "-subj",
            "/CN=localhost",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"[TLS] Generated self-signed certificate: {CERT_FILE}, {KEY_FILE}")
def encode_response(request_id: str, status: str, code: str, message: str, payload: str = "") -> str:
    return f"RESP|{request_id}|{status}|{now_ms()}|{code}|{message}|{payload}\n"
def list_payload() -> str:
    return ",".join(f"{seat}:{owner if owner else '-'}" for seat, owner in state["seats"].items())
def process_request(parts: list[str]) -> str:
    if len(parts) < 3:
        return encode_response("-", "ERROR", "BAD_FORMAT", "Expected ACTION|REQ_ID|...", "")
    action = parts[0].strip().upper()
    request_id = parts[1].strip()
    client_ts = parts[-1].strip()
    if not request_id:
        return encode_response("-", "ERROR", "BAD_REQUEST_ID", "request_id is required", "")
    with state_lock:
        if request_id in request_cache:
            return request_cache[request_id]
        if action in {"BOOK", "CANCEL", "QUERY"} and len(parts) != 5:
            response = encode_response(request_id, "ERROR", "BAD_FORMAT", "Expected 5 fields", "")
            request_cache[request_id] = response
            return response
        if action == "LIST" and len(parts) != 3:
            response = encode_response(request_id, "ERROR", "BAD_FORMAT", "Expected LIST|REQ_ID|CLIENT_TS", "")
            request_cache[request_id] = response
            return response
        if action == "CLEAR" and len(parts) != 4:
            response = encode_response(request_id, "ERROR", "BAD_FORMAT", "Expected CLEAR|REQ_ID|USER_ID|CLIENT_TS", "")
            request_cache[request_id] = response
            return response
        if action == "LIST":
            payload = f"version={state['version']};{list_payload()}"
            response = encode_response(request_id, "OK", "LIST_OK", "Current seat map", payload)
            request_cache[request_id] = response
            return response
        if action == "CLEAR":
            user_id = parts[2].strip() or "SYSTEM"
            cleared_count = sum(1 for owner in state["seats"].values() if owner is not None)
            new_version = state["version"] + 1
            entry = {
                "version": new_version,
                "server_ts": now_ms(),
                "client_ts": client_ts,
                "request_id": request_id,
                "action": "CLEAR",
                "seat_id": "*",
                "user_id": user_id,
            }
            append_journal(entry)
            apply_entry(entry)
            save_snapshot()
            response = encode_response(
                request_id,
                "OK",
                "CLEARED",
                "All seats cleared",
                f"cleared_by={user_id};cleared_count={cleared_count};version={state['version']}"
            )
            request_cache[request_id] = response
            return response
        seat_id = parts[2].strip().upper()
        user_id = parts[3].strip()
        if seat_id not in state["seats"]:
            response = encode_response(request_id, "FAIL", "INVALID_SEAT", "Invalid seat id", "")
            request_cache[request_id] = response
            return response
        if action == "QUERY":
            owner = state["seats"][seat_id]
            payload = f"seat={seat_id};owner={owner if owner else '-'};version={state['version']}"
            response = encode_response(request_id, "OK", "QUERY_OK", "Seat status", payload)
            request_cache[request_id] = response
            return response
        if action == "BOOK":
            current_owner = state["seats"][seat_id]
            if current_owner is not None:
                response = encode_response(
                    request_id,
                    "FAIL",
                    "ALREADY_BOOKED",
                    f"{seat_id} already booked",
                    f"seat={seat_id};owner={current_owner};version={state['version']}"
                )
                request_cache[request_id] = response
                return response
            new_version = state["version"] + 1
            entry = {
                "version": new_version,
                "server_ts": now_ms(),
                "client_ts": client_ts,
                "request_id": request_id,
                "action": "BOOK",
                "seat_id": seat_id,
                "user_id": user_id,
            }
            append_journal(entry)
            apply_entry(entry)
            save_snapshot()
            response = encode_response(
                request_id,
                "OK",
                "BOOKED",
                f"{seat_id} booked for {user_id}",
                f"seat={seat_id};owner={user_id};version={state['version']}"
            )
            request_cache[request_id] = response
            return response
        if action == "CANCEL":
            current_owner = state["seats"][seat_id]
            if current_owner is None:
                response = encode_response(
                    request_id,
                    "FAIL",
                    "NOT_BOOKED",
                    f"{seat_id} is not booked",
                    f"seat={seat_id};owner=-;version={state['version']}"
                )
                request_cache[request_id] = response
                return response
            if current_owner != user_id:
                response = encode_response(
                    request_id,
                    "FAIL",
                    "NOT_OWNER",
                    f"{seat_id} is owned by {current_owner}",
                    f"seat={seat_id};owner={current_owner};version={state['version']}"
                )
                request_cache[request_id] = response
                return response
            new_version = state["version"] + 1
            entry = {
                "version": new_version,
                "server_ts": now_ms(),
                "client_ts": client_ts,
                "request_id": request_id,
                "action": "CANCEL",
                "seat_id": seat_id,
                "user_id": user_id,
            }
            append_journal(entry)
            apply_entry(entry)
            save_snapshot()
            response = encode_response(
                request_id,
                "OK",
                "CANCELLED",
                f"{seat_id} cancelled by {user_id}",
                f"seat={seat_id};owner=-;version={state['version']}"
            )
            request_cache[request_id] = response
            return response
        response = encode_response(request_id, "ERROR", "UNKNOWN_ACTION", "Unsupported action", "")
        request_cache[request_id] = response
        return response
def handle_client(conn: ssl.SSLSocket, addr) -> None:
    print(f"[NEW CONNECTION] {addr} connected")
    conn.settimeout(10)
    try:
        file = conn.makefile("rwb")
        while not shutdown_event.is_set():
            raw = file.readline()
            if not raw:
                break
            request = raw.decode("utf-8").strip()
            if not request:
                continue
            parts = request.split("|")
            response = process_request(parts)
            file.write(response.encode("utf-8"))
            file.flush()
    except Exception as e:
        print(f"[ERROR] {addr} -> {e}")
    finally:
        conn.close()
        print(f"[DISCONNECTED] {addr} disconnected")
def _shutdown_handler(signum, frame):
    shutdown_event.set()
def start_server(host: str = HOST, port: int = PORT) -> None:
    recover_state()
    ensure_certificates()
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(50)
    server_socket.settimeout(1.0)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    secure_server = context.wrap_socket(server_socket, server_side=True)
    print(f"[STARTING] Secure reservation server listening on {host}:{port}")
    print("[PROTOCOL] BOOK|REQ_ID|SEAT_ID|USER_ID|CLIENT_TS")
    print("[PROTOCOL] CANCEL|REQ_ID|SEAT_ID|USER_ID|CLIENT_TS")
    print("[PROTOCOL] QUERY|REQ_ID|SEAT_ID|USER_ID|CLIENT_TS")
    print("[PROTOCOL] LIST|REQ_ID|CLIENT_TS")
    print("[PROTOCOL] CLEAR|REQ_ID|USER_ID|CLIENT_TS")
    try:
        while not shutdown_event.is_set():
            try:
                conn, addr = secure_server.accept()
            except socket.timeout:
                continue
            thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
    finally:
        save_snapshot()
        secure_server.close()
        print("[SHUTTING DOWN] Server stopped")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TLS reservation server")
    parser.add_argument("--host", default=HOST, help="Bind host/IP (use 0.0.0.0 for all interfaces)")
    parser.add_argument("--port", default=PORT, type=int, help="Bind port")
    args = parser.parse_args()
    start_server(host=args.host, port=args.port)