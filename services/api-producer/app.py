"   HTTP -> Kafka gateway. Receives normalized FlightEvent payloads via
"   POST /flight and publishes them to the flight.telemetry Kafka topic.
"

import os 
import time 
import logging
from datetime import datetime 
from typing import Optional, Literal 

from fastapi import FastAPI, HTTPException 
from pydantic import BaseModel, Field
from confluent_kafka import Producer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC = os.getenv("TOPIC_EVENTS", "flight.telemetry")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-producer")

app = FastAPI(title="Flight Flow API Producer")

Airport = Literal["AMS", "LHR", "MUC"]
FlightStatus = Literal[
    "SCHEDULED", "BOARDING", "LAST_CALL", "GATE_OPEN", "GATE_CLOSED",
    "DEPARTED", "DELAYED", "CANCELLED", "DIVERTED", "UNKNOWN",
]
ServiceType = Literal["J", "C", "G", "H"]

class FlightEvent(BaseModel):
    eventId: str = Field(default_factory=lambda: f"evt-{int(time.time()*1000)}")
    ts: float = Field(default_factory=time.time)

    
    airport: Airport
    flight_code: str
    airline_iata: str
    airline_name: str

    scheduled_departure: datetime
    estimated_departure: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    delay_minutes: Optional[int] = None

    gate: Optional[str] = None
    terminal: Optional[str] = None
    destination_iata: str = ""
    destination_name: str = ""

    status: FlightStatus = "SCHEDULED"

    aircraft_type: Optional[str] = None

    is_codeshare: bool = False
    is_cargo: bool = False
    service_type: ServiceType = "J"

def kafka_ssl_base() -> dict:
    cfg = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": os.get
    }