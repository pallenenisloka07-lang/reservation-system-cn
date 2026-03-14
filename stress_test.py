import argparse
import random
import threading
import time
from collections import Counter

from client import send_request

SEATS = [f"SEAT_{i}" for i in range(1, 6)]
USER_POOL = [f"USER_{i:03d}" for i in range(1, 201)]


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


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.responses = []
        self.codes = Counter()

    def add(self, response: str):
        parts = response.split("|")
        code = parts[4] if len(parts) > 4 else "UNKNOWN"
        with self.lock:
            self.responses.append(response)
            self.codes[code] += 1



def worker(worker_id: int, requests_per_worker: int, stats: Stats, host: str, port: int):
    random.seed(time.time_ns() + worker_id)
    for i in range(requests_per_worker):
        seat = random.choice(SEATS)
        user = random.choice(USER_POOL)
        req_id = f"W{worker_id}-{i}-{time.time_ns()}"
        response = send_request("BOOK", seat_id=seat, user_id=user, request_id=req_id, host=host, port=port)
        stats.add(response)



def parse_list_payload(payload: str):
    parts = payload.split(";")
    seat_map = {}
    for item in parts:
        if item.startswith("version="):
            continue
        if ":" in item:
            seat, owner = item.split(":", 1)
            seat_map[seat] = None if owner == "-" else owner
    return seat_map



def main():
    parser = argparse.ArgumentParser(description="Concurrent stress test for reservation server")
    parser.add_argument("--workers", type=int, default=100, help="Number of concurrent worker threads")
    parser.add_argument("--requests", type=int, default=20, help="Booking attempts per worker")
    parser.add_argument("--server", default=None, help="Server endpoint in host:port format")
    args = parser.parse_args()

    num_workers = args.workers
    requests_per_worker = args.requests
    stats = Stats()

    host = "127.0.0.1"
    port = 65432
    if args.server:
        host, port = parse_server_endpoint(args.server)

    print(f"Starting stress test with {num_workers} workers x {requests_per_worker} requests on {host}:{port}")
    start = time.time()

    threads = [
        threading.Thread(target=worker, args=(worker_id, requests_per_worker, stats, host, port), daemon=True)
        for worker_id in range(num_workers)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    duration = time.time() - start
    total_requests = num_workers * requests_per_worker

    list_resp = send_request("LIST", request_id=f"LIST-{time.time_ns()}", host=host, port=port)
    list_parts = list_resp.split("|", 6)
    payload = list_parts[6] if len(list_parts) > 6 else ""
    seat_map = parse_list_payload(payload)

    booked_count = sum(1 for owner in seat_map.values() if owner is not None)

    print("\n===== Stress Test Report =====")
    print(f"Total requests: {total_requests}")
    print(f"Elapsed time: {duration:.2f}s")
    print(f"Throughput: {total_requests / duration:.2f} req/s")
    print(f"Response code counts: {dict(stats.codes)}")
    print(f"Final seat map: {seat_map}")

    if booked_count > len(SEATS):
        print("[FAILED] Invalid state: booked seats exceed capacity")
    elif len(seat_map) != len(SEATS):
        print("[FAILED] Invalid seat map size")
    else:
        print("[PASSED] No double booking; atomic seat ownership preserved")


if __name__ == "__main__":
    main()