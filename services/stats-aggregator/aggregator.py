import os
import json
import time
import logging
import signal
import threading
from datetime import datetime, timezone, time as dtime
from typing import Optional

from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_IN = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_OUT = os.getenv("TOPIC_STATS", "flight.stats")
GROUP_ID = os.getenv("GROUP_ID", "stats-aggregator-group")
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL_SEC", "10"))

if not GROUP_ID or not GROUP_ID.strip():
    raise RuntimeError(
        "GROUP_ID env var is empty or not set. "
        "Check docker-compose.yml environment section."
    )

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


def seek_to_start_of_today_utc(consumer: Consumer, topic: str, timeout: float = 15.0) -> None:
    """
    Force the consumer to (re)read from 00:00 UTC of the current day.

    Why: stats are per-day. If the service restarts mid-day, we want to
    rebuild the in-memory counters from the same point so that the
    published stats stay consistent. We override whatever offset was
    previously committed for this group.
    """
    log.info("Waiting for partition assignment on %s ...", topic)
    deadline = time.time() + timeout
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(0.5)

    parts = consumer.assignment()
    if not parts:
        log.warning("No partitions assigned within %.1fs; skipping seek", timeout)
        return

    # 00:00 UTC of today in milliseconds since epoch
    today = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(today, dtime.min, tzinfo=timezone.utc)
    timestamp_ms = int(start_of_day.timestamp() * 1000)

    # Ask Kafka for the offset corresponding to that timestamp on each partition
    query = [TopicPartition(p.topic, p.partition, timestamp_ms) for p in parts]
    resolved = consumer.offsets_for_times(query, timeout=timeout)

    for tp in resolved:
        if tp.offset < 0:
            # No messages at or after midnight — seek to end (nothing to replay)
            log.info("No messages since %s on %s[%d] — seeking to end",
                     start_of_day.isoformat(), tp.topic, tp.partition)
            continue
        consumer.seek(tp)
        log.info("Seeked %s[%d] to offset %d (%s UTC)",
                 tp.topic, tp.partition, tp.offset, start_of_day.isoformat())


class StatsState:
    """
    Per-airport rolling counters for the current UTC day.

    Internal structure per airport:
        {
            "departed_keys": set of (flight_code, scheduled_departure),
            "delay_sum":     int   (sum of delay_minutes for departed flights),
            "delay_count":   int   (flights with a numeric delay_minutes),
        }
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._airports: dict = {}
        self._current_day: Optional[str] = None  # ISO date, e.g. "2026-05-02"

    def _airport_bucket(self, airport: str) -> dict:
        return self._airports.setdefault(airport, {
            "departed_keys": set(),
            "delay_sum": 0,
            "delay_count": 0,
        })

    def _maybe_rollover(self, today_utc: str) -> None:
        """Reset all counters when the UTC day changes."""
        if self._current_day is None:
            self._current_day = today_utc
            return
        if today_utc != self._current_day:
            log.info("UTC day rollover %s → %s, resetting counters",
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

            # Idempotency: same flight seen twice does not double-count
            if key in bucket["departed_keys"]:
                return
            bucket["departed_keys"].add(key)

            delay = event.get("delay_minutes")
            if isinstance(delay, (int, float)):
                bucket["delay_sum"] += int(delay)
                bucket["delay_count"] += 1

    def snapshot(self) -> list[dict]:
        """Return one stats dict per airport currently tracked."""
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
        # Wait first, then publish — avoids an empty burst right at boot
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
        log.info(
            "Published stats for %d airport(s): %s",
            len(snapshots),
            ", ".join(
                f"{s['airport']}=dep:{s['departed_today']},avg:{s['avg_delay_minutes']}"
                for s in snapshots
            ),
        )

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

    # Seek to 00:00 UTC today so daily stats are always rebuilt from a
    # consistent baseline, even after a mid-day restart.
    seek_to_start_of_today_utc(consumer, TOPIC_IN)

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
                log.error("Malformed message at offset %d: %s — skipping", msg.offset(), e)
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


    