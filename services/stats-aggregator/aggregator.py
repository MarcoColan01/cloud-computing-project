import os
import json
import time
import logging
import signal
import threading
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, Producer, KafkaException


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_IN = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_OUT = os.getenv("TOPIC_STATS", "flight.stats")
GROUP_ID = os.getenv("GROUP_ID", "stats-aggregator-group")
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL_SEC", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("stats-aggregator")



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


class StatsState:
    """
    Per-airport rolling counters for the current UTC day.

    Structure:
        {
            "AMS": {
                "departed_keys": {(flight_code, scheduled_departure), ...},
                "delay_sum": int,
                "delay_count": int,
            },
            ...
        }
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._airports: dict = {}
        self._current_day: Optional[str] = None  

    def _airport_bucket(self, airport: str) -> dict:
        return self._airports.setdefault(airport, {
            "departed_keys": set(),
            "delay_sum": 0,
            "delay_count": 0,
        })

    def _maybe_rollover(self, today_utc: str) -> None:
        """Reset all counters if the UTC day has changed since last update."""
        if self._current_day is None:
            self._current_day = today_utc
            return
        if today_utc != self._current_day:
            log.info("UTC day rollover %s -> %s, resetting counters",
                     self._current_day, today_utc)
            self._airports.clear()
            self._current_day = today_utc

    def update(self, event: dict) -> None:
        """Incorporate one telemetry event into the running stats."""
        airport = event.get("airport")
        status = event.get("status")
        if not airport or status != "DEPARTED":
            return

        with self._lock:
            today_utc = datetime.now(timezone.utc).date().isoformat()
            self._maybe_rollover(today_utc)

            bucket = self._airport_bucket(airport)
            key = (event.get("flight_code"), event.get("scheduled_departure"))
            if key in bucket["departed_keys"]:
                return
            bucket["departed_keys"].add(key)

            delay = event.get("delay_minutes")
            if isinstance(delay, (int, float)):
                bucket["delay_sum"] += int(delay)
                bucket["delay_count"] += 1

    def snapshot(self) -> list[dict]:
        """Return a list of stat messages, one per airport currently tracked."""
        out = []
        with self._lock:
            today_utc = datetime.now(timezone.utc).date().isoformat()
            self._maybe_rollover(today_utc)

            for airport, bucket in self._airports.items():
                count = len(bucket["departed_keys"])
                avg_delay = (
                    round(bucket["delay_sum"] / bucket["delay_count"], 1)
                    if bucket["delay_count"] > 0 else None
                )
                out.append({
                    "airport": airport,
                    "stats_day_utc": today_utc,
                    "departed_today": count,
                    "avg_delay_minutes": avg_delay,
                    "ts": time.time(),
                })
        return out


def publish_loop(state: StatsState, producer: Producer, stop: threading.Event) -> None:
    """Background loop: every PUBLISH_INTERVAL seconds emit a stats snapshot."""
    log.info("Publisher thread started, interval=%ds", PUBLISH_INTERVAL)
    while not stop.is_set():
        if stop.wait(PUBLISH_INTERVAL):
            break

        snapshots = state.snapshot()
        if not snapshots:
            log.debug("No airports tracked yet, skipping publish")
            continue

        for snap in snapshots:
            try:
                producer.produce(
                    TOPIC_OUT,
                    key=snap["airport"].encode("utf-8"),
                    value=json.dumps(snap).encode("utf-8"),
                )
            except BufferError:
                log.warning("Producer queue full while publishing %s stats", snap["airport"])
            except KafkaException as e:
                log.error("Failed to publish stats for %s: %s", snap["airport"], e)
        producer.poll(0)
        log.info("Published stats for %d airport(s): %s",
                 len(snapshots),
                 ", ".join(f"{s['airport']}=dep:{s['departed_today']},"
                           f"avg:{s['avg_delay_minutes']}" for s in snapshots))


def start_stats_aggregator():
    log.info("Starting stats-aggregator")
    log.info("In: %s   Out: %s   GroupID: %s", TOPIC_IN, TOPIC_OUT, GROUP_ID)

    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "flight-stats-aggregator-consumer",
    })
    consumer.subscribe([TOPIC_IN])

    producer = Producer({
        **kafka_ssl_base(),
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 1,
        "retries": 2147483647,
        "compression.type": "gzip",
        "client.id": "flight-stats-aggregator-producer",
    })

    state = StatsState()
    stop_event = threading.Event()

    publisher = threading.Thread(
        target=publish_loop, args=(state, producer, stop_event), daemon=True
    )
    publisher.start()

    def shutdown(*_):
        log.info("Shutdown signal received")
        stop_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error("Malformed message at offset %d: %s, skipping", msg.offset(), e)
                consumer.commit(message=msg, asynchronous=False)
                continue

            state.update(event)
            consumer.commit(message=msg, asynchronous=False)

    except KafkaException as e:
        log.error("Fatal Kafka error: %s", e)
    finally:
        log.info("Closing consumer and flushing producer")
        stop_event.set()
        publisher.join(timeout=5)
        producer.flush(timeout=10)
        consumer.close()


if __name__ == "__main__":
    start_stats_aggregator()