import json 
import sys 
import time 
from pathlib import Path

from confluent_kafka import Producer, KafkaException

BOOTSTRAP_EXTERNAL = "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"

PAYLOAD = json.dumps({
    "airport": "AMS",
    "flight_code": "ROGUE001",
    "airline_iata": "XX",
    "airline_name": "Rogue",
    "scheduled_departure": "2026-12-31T23:59:00Z",
    "destination_iata": "XXX",
    "destination_name": "Nowhere",
    "status": "SCHEDULED",
}).encode("utf-8")

def attempt(label:str, config:dict) -> None:
    print(f"\n{'='*60}")
    print(f"Attempt: {label}")
    print(f"{'='*60}")
    try:
        p = Producer(config)
        p.produce("flight.telemetry", value=PAYLOAD,
        on_delivery=lambda err, msg: print(f"   delivery: err ={err}"))
        for _ in range(20):
            p.poll(0.5)
        p.flush(timeout=5)
        print(" -> ATTEMPT FINISHED")
    except KafkaException as e:
        print(f"    -> KafkaException: {e}")
    except Exception as e:
        print(f"    -> Exception: {e}")


attempt("plain TCP (no TLS)", {
    "bootstrap.servers": BOOTSTRAP_EXTERNAL,
    "security.protocol": "PLAINTEXT",
    "message.timeout.ms": 5000,
    "client.id": "rogue-no-cert",
})

attempt("TLS with bogus client certificate", {
    "bootstrap.servers": BOOTSTRAP_EXTERNAL,
    "security.protocol": "SSL",
    "ssl.ca.location": "/dev/null",
    "ssl.certificate.location": "/dev/null",
    "ssl.key.location": "/dev/null",
    "message.timeout.ms": 5000,
    "client.id": "rogue-bogus",
})

print("\nAll attempts done.")