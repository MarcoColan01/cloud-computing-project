"""
Flight Flow - Dashboard backend.

Flask app that:
  - consumes flight.telemetry (live flight events) into in-memory state
  - consumes flight.stats (aggregated counters) into in-memory state
  - serves the HTML page and a Server-Sent Events stream

The Kafka consumers use a unique random group_id per dashboard instance so
that each replica receives ALL messages (broadcast pattern), not a partition
slice. This is the right semantics for a dashboard: every screen must see
every flight.

Adapted from Galliano's WeatherFlow dashboard backend.
"""

import os
import json
import threading
import queue
import uuid
from datetime import datetime, timezone

from flask import Flask, Response, render_template
from confluent_kafka import Consumer

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_TELEMETRY = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_STATS = os.getenv("TOPIC_STATS", "flight.stats")

GROUP_TELEMETRY = os.getenv(
    "DASHBOARD_TELEMETRY_GROUP", f"dashboard-telemetry-{uuid.uuid4()}"
)
GROUP_STATS = os.getenv(
    "DASHBOARD_STATS_GROUP", f"dashboard-stats-{uuid.uuid4()}"
)

# Airports we render on the board (codes match what pollers publish)
AIRPORTS = ["AMS", "HEL", "OSL"]
BOARD_SIZE = int(os.getenv("BOARD_SIZE", "20"))

webapp = Flask(__name__, static_folder="static", template_folder="template")

# ----------------------------------------------------------------------
# In-memory state (thread-safe)
# ----------------------------------------------------------------------
state_lock = threading.Lock()

# Per-airport state. flights is keyed by (flight_code, scheduled_departure)
# so that successive updates of the same flight overwrite the same entry.
state = {
    code: {
        "flights": {},        # (flight_code, sched) -> flight dict
        "stats": {
            "departed_today": 0,
            "avg_delay_minutes": None,
            "stats_day_utc": None,
        },
    }
    for code in AIRPORTS
}

# Output queue: SSE clients drain this. One queue per connected client.
sse_clients: list[queue.Queue] = []
sse_clients_lock = threading.Lock()

started = False


def kafka_ssl_base() -> dict:
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION", "/app/security/ca.crt"),
        "ssl.certificate.location": os.getenv(
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "/app/security/client-creds/kafka.client.certificate.pem",
        ),
        "ssl.key.location": os.getenv(
            "KAFKA_SSL_KEY_LOCATION",
            "/app/security/client-creds/kafka.client.key",
        ),
    }


# ----------------------------------------------------------------------
# State manipulation helpers
# ----------------------------------------------------------------------
def upsert_flight(event: dict) -> None:
    """Insert or update one flight in the per-airport board."""
    airport = event.get("airport")
    if airport not in state:
        return
    fc = event.get("flight_code")
    sd = event.get("scheduled_departure")
    if not fc or not sd:
        return
    key = f"{fc}|{sd}"

    with state_lock:
        # If poller signaled a deletion (e.g. flight has departed), remove it
        if event.get("event_type") == "DELETE":
            state[airport]["flights"].pop(key, None)
        else:
            state[airport]["flights"][key] = event


def update_stats(event: dict) -> None:
    airport = event.get("airport")
    if airport not in state:
        return
    with state_lock:
        state[airport]["stats"] = {
            "departed_today": event.get("departed_today", 0),
            "avg_delay_minutes": event.get("avg_delay_minutes"),
            "stats_day_utc": event.get("stats_day_utc"),
        }


def board_snapshot() -> dict:
    """
    Build the dashboard payload:
      - for each airport, take the BOARD_SIZE flights with the earliest
        scheduled_departure that are not in the past
    Returns the full state already shaped for the UI.
    """
    now_utc = datetime.now(timezone.utc)
    out = {"airports": {}}
    with state_lock:
        for airport, bucket in state.items():
            # Filter flights still relevant (scheduled in the future or just departed)
            flights = []
            for fl in bucket["flights"].values():
                sd = fl.get("scheduled_departure")
                if not sd:
                    continue
                try:
                    sd_dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
                except ValueError:
                    continue
                # Tolerate a small lookback so just-departed flights linger briefly
                if (sd_dt - now_utc).total_seconds() > -1800:
                    flights.append(fl)
            flights.sort(key=lambda f: f["scheduled_departure"])
            out["airports"][airport] = {
                "flights": flights[:BOARD_SIZE],
                "stats": bucket["stats"],
            }
    return out


def push_to_clients(payload: str) -> None:
    """Fan-out a JSON string to every connected SSE client."""
    with sse_clients_lock:
        clients = list(sse_clients)
    for q in clients:
        try:
            q.put_nowait(payload)
        except queue.Full:
            pass


def broadcast_state() -> None:
    """Serialize the current state and send it to all SSE clients."""
    payload = json.dumps(board_snapshot())
    push_to_clients(payload)


# ----------------------------------------------------------------------
# Background Kafka consumers (one thread each)
# ----------------------------------------------------------------------
def telemetry_loop() -> None:
    cfg = {
        **kafka_ssl_base(),
        "group.id": GROUP_TELEMETRY,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": "flight-dashboard-telemetry",
    }
    consumer = Consumer(cfg)
    consumer.subscribe([TOPIC_TELEMETRY])
    print(f"[dashboard] telemetry consumer started, group={GROUP_TELEMETRY}", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[dashboard] telemetry kafka error: {msg.error()}", flush=True)
            continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[dashboard] malformed telemetry: {e}", flush=True)
            continue
        upsert_flight(event)
        broadcast_state()


def stats_loop() -> None:
    cfg = {
        **kafka_ssl_base(),
        "group.id": GROUP_STATS,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": "flight-dashboard-stats",
    }
    consumer = Consumer(cfg)
    consumer.subscribe([TOPIC_STATS])
    print(f"[dashboard] stats consumer started, group={GROUP_STATS}", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[dashboard] stats kafka error: {msg.error()}", flush=True)
            continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[dashboard] malformed stats: {e}", flush=True)
            continue
        update_stats(event)
        broadcast_state()


def start_kafka_threads() -> None:
    global started
    if started:
        return
    threading.Thread(target=telemetry_loop, daemon=True).start()
    threading.Thread(target=stats_loop, daemon=True).start()
    started = True


@webapp.before_request
def before():
    start_kafka_threads()


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------
@webapp.get("/healthcheck")
def health():
    return {
        "ok": True,
        "bootstrap": BOOTSTRAP,
        "topics": {"telemetry": TOPIC_TELEMETRY, "stats": TOPIC_STATS},
        "groups": {"telemetry": GROUP_TELEMETRY, "stats": GROUP_STATS},
        "airports": AIRPORTS,
    }


@webapp.get("/")
def index():
    return render_template("index.html", airports=AIRPORTS)


@webapp.get("/snapshot")
def snapshot():
    """One-shot REST endpoint: useful for the very first paint of the page."""
    return board_snapshot()


@webapp.get("/stream")
def stream():
    """Server-Sent Events: pushes a fresh snapshot whenever Kafka updates state."""
    client_q: queue.Queue = queue.Queue(maxsize=64)
    with sse_clients_lock:
        sse_clients.append(client_q)

    def event_stream():
        try:
            # Send the current snapshot immediately on connect
            yield f"data: {json.dumps(board_snapshot())}\n\n"
            while True:
                payload = client_q.get()
                yield f"data: {payload}\n\n"
        finally:
            with sse_clients_lock:
                if client_q in sse_clients:
                    sse_clients.remove(client_q)

    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    webapp.run(host="0.0.0.0", port=8500, debug=False)