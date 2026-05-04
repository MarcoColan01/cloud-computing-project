import os
import json 
import logging 
import signal
import time
from datetime import datetime, timezone, time as dtime
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_IN = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_OUT = os.getenv("TOPIC_ALERTS", "flight.alerts")
GROUP_ID = os.getenv("GROUP_ID", "notifier-group")

if not GROUP_ID or not GROUP_ID.strip():
    raise RuntimeError("GROUP_ID env var is empty or not set.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("notifier")

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
    log.info("Waiting for partition assignment on %s ...", topic)
    deadline = time.time() + timeout
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(0.5)
    
    parts = consumer.assignment()
    if not parts:
        log.warning("No partitions assigned within %.1fs; skipping seek", timeout)
        return 
    
    today = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(today, dtime.min, tzinfo=timezone.utc)
    timestamp_ms = int(start_of_day.timestamp() * 1000)

    query = [TopicPartition(p.topic, p.partition, timestamp_ms) for p in parts]
    resolved = consumer.offsets_for_times(query, timeout=timeout)

    for tp in resolved:
        if tp.offset < 0:
            log.info("No messages since %s on %s[%d] - seeking to end",
            start_of_day.isoformat(), tp.topic, tp.partition)
            continue
        consumer.seek(tp)
        log.info("Seeked %s[%d] to offset %d (%s UTC)",
        tp.topic, tp.partition, tp.offset, start_of_day.isoformat())

def resolve_actual_departure_iso(event: dict) -> str | None:
    for field in ("actual_departure", "estimated_departure", "observed_at_utc"):
        val = event.get(field)
        if val:
            return val 
    return event.get("scheduled_departure")

def build_notification(event: dict) -> dict | None:
    flight_code = event.get("flight_code")
    if not flight_code:
        return None 
    
    sched = event.get("scheduled_departure")
    actual = resolve_actual_departure_iso(event)
    if not sched or not actual:
        return None
    
    destination = (
        event.get("destination_name")
        or event.get("destination_city")
        or event.get("destination_iata")
        or "—"
    )

    text = (
        f"Flight {flight_code} to {destination}: "
        f"departed at {actual} (scheduled: {sched})"
    )

    return {
        "airport": event.get("airport"),
        "flight_code": flight_code,
        "destination_iata": event.get("destination_iata"),
        "destination_name": destination,
        "scheduled_departure": sched,
        "actual_departure": actual,
        "delay_minutes": event.get("delay_minutes"),
        "text": text,
        # Day key for client-side filtering of yesterday's notifications
        "alert_day_utc": datetime.now(timezone.utc).date().isoformat(),
        "ts": time.time(),
    }

    #RESTART FROM start_notifier()

