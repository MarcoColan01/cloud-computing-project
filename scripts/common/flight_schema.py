"""
Canonical flight event schema, shared by all pollers.

This dataclass is the contract between the airport-specific pollers
(Schiphol, Heathrow, Munich, ...) and the API-producer. Each poller is
responsible for translating its airport's proprietary API response into
this normalized shape before POSTing to /flight.

The same shape is mirrored on the API-producer side as a Pydantic model
(see services/API-producer/app.py). 
"""

from dataclasses import dataclass, asdict, field
from typing import Optional, Literal

Airport = Literal["AMS", "LHR", "MUC"]
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
    estimated_departure: Optional[str] = None
    actual_departure: Optional[str] = None
    delay_minutes: Optional[int] = None  # negative = early, positive = late

    gate: Optional[str] = None
    terminal: Optional[str] = None
    destination_iata: str = " "
    destination_name: str = " "

    status: FlightStatus = "SCHEDULED"

    aircraft_type: Optional[str] = None

    is_codeshare: bool = False
    is_cargo: bool = False 
    service_type: ServiceType = "J"

    def to_dict(self) -> dict:
        return asdict(self)