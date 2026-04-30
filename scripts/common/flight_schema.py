"""
Canonical flight event schema, shared by all pollers.
"""

from dataclasses import dataclass, asdict, field
from typing import Any, Optional, Literal

Airport = Literal["AMS", "LHR", "MUC", "HEL", "OSL"]
EventType = Literal["UPSERT", "DELETE"]

FlightStatus = Literal[
    "SCHEDULED",
    "BOARDING",
    "LAST_CALL",
    "GATE_OPEN",
    "GATE_CLOSED",
    "DEPARTED",
    "DELAYED",
    "CANCELLED",
    "DIVERTED",
    "UNKNOWN",
]

"""
J = Scheduled passenger flight
C = Cargo flight
G = General aviation flight
H = Charter flight / other
"""
ServiceType = Literal["J", "C", "G", "H"]


@dataclass
class FlightEvent:
    airport: Airport
    flight_code: str
    airline_iata: str
    airline_name: str
    scheduled_departure: str

    event_type: EventType = "UPSERT"
    changed_fields: dict[str, dict[str, Any]] = field(default_factory=dict)

    estimated_departure: Optional[str] = None
    actual_departure: Optional[str] = None
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

    def to_dict(self) -> dict:
        return asdict(self)
