import os
import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.flight_schema import FlightEvent 

PROJECT_ROOT = HERE.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SCHIPHOL_APP_ID = os.getenv("SCHIPHOL_APP_ID")
SCHIPHOL_APP_KEY = os.getenv("SCHIPHOL_APP_KEY")
if not SCHIPHOL_APP_ID or not SCHIPHOL_APP_KEY:
    print("ERROR: SCHIPHOL_APP_KEY or SCHIPHOL_APP_ID missing in .env",
    file=sys.stderr)
    sys.exit(1)

SCHIPHOL_BASE = "https://api.schiphol.nl/public-flights/flights"
RESOURCE_VERSION = "v4"

API_PRODUCER_URL = os.getenv("API_PRODUCER_URL", "http://127.0.0.1:8000/flight")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))  #in seconds
LOOK_AHEAD_HOURS = int(os.getenv("LOOK_AHEAD_HOURS", "8"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "10")) 

SCHIPHOL_STATUS_MAP = {
    "SCH": "SCHEDULED",
    "AIR": "DEPARTED",
    "DEP": "DEPARTED",
    "DEL": "DELAYED",
    "WIL": "BOARDING",        # "Wait in Lounge"
    "BRD": "BOARDING",
    "GCH": "GATE_OPEN",
    "GTO": "GATE_OPEN",
    "GCL": "GATE_CLOSED",
    "GTD": "GATE_CLOSED",
    "TOM": "SCHEDULED",       # "Tomorrow"
    "CNX": "CANCELLED",
    "DIV": "DIVERTED",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s %(message)s]")
log = logging.getLogger("schiphol-poller")

def fetch_page(page:int, schedule_date:str) -> Optional[dict]:
    headers = {
        "Accept": "application/json",
        "ResourceVersion": RESOURCE_VERSION,
        "app_id": SCHIPHOL_APP_ID,
        "app_key": SCHIPHOL_APP_KEY,
    }
    params = {
        "flightDirection": "D",
        "scheduleDate": schedule_date,
        "includedelays": "true",
        "sort": "+scheduleTime",
        "page": page,
    }
    try:
        r = requests.get(SCHIPHOL_BASE, headers = headers, params=params, timeout=15)
    except requests.RequestException as e:
        log.error("Schiphol request failed: %s", e)
        return None
    
    if r.status_code == 429:
        log.warning("SChiphol rate limit hit")
        time.sleep(30)
        return None
    
    if r.status_code == 400:
        log.error("SChiphol API error %d: %s", r.status_code, r.text[:200])
        return None 
    
    try:
        return r.json()
    except ValueError as e:
        log.error("Schiphol response not JSON: %s", e)
        return None
    
def is_codeshare(raw: dict) -> bool:
    return raw.get("mainFlight") and raw.get("mainFlight") != raw.get("flightName")
    
def is_filtered_service(raw: dict) -> bool:
    st = (raw.get("serviceType") or "").upper()
    return st in ("C", "G", "H")

def map_status(raw: dict) -> str:
    states = (raw.get("publicFlightState") or {}).get("flightStates") or []
    if not states:
        return "UNKNOWN"
    for code in reversed(states):
        mapped = SCHIPHOL_STATUS_MAP.get(code)
        if mapped:
            return mapped
    return "UNKNOWN"

def compute_delay_minutes(raw:dict) -> Optional[int]:
    sched = raw.get("scheduleDateTime")
    if not sched:
        return None
    
    est = raw.get("actualOffBlockTime") or raw.get("publicEstimatedOffBlockTime")
    if not est:
        return None
    
    try:
        sched_dt = datetime.fromisoformat(sched.replace("Z", "+00:00"))
        est_dt = datetime.fromisoformat(est.replace("Z", "+00:00"))
        return int((est_dt-sched_dt).total_seconds() / 60)
    except(ValueError, AttributeError):
        return None

def normalize(raw:dict) -> Optional[FlightEvent]:
    try:
        flight_code = raw.get("flightName")
        airline_iata = raw.get("prefixIATA") or ""
        scheduled = raw.get("scheduleDateTime")
        if not flight_code or not scheduled:
            return None
        
        destinations = (raw.get("route") or {}).get("destinations") or []
        dest_iata = destinations[0] if destinations else ""

        aircraft = (raw.get("aircraftType") or {}).get("iataSub") \
                   or (raw.get("aircraftType") or {}).get("iataMain")

        sched_dt = datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
        sched_iso = sched_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        est = raw.get("publicEstimatedOffBlockTime") or raw.get("expectedTimeBoarding")
        est_iso = None 
        if est:
            try:
                est_dt = datetime.fromisoformat(est.replace("Z", "+00:00"))
                est_iso = est_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                pass

        actual = raw.get("actualOffBlockTime")
        actual_iso = None
        if actual:
            try:
                actual_dt = datetime.fromisoformat(actual.replace("Z", "+00:00"))
                actual_iso = actual_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
        
        return FlightEvent(
            airport="AMS",
            flight_code= flight_code.strip(),
            airline_iata=airline_iata,
            airline_name=airline_iata,
            scheduled_departure=sched_iso,
            estimated_departure=est_iso,
            actual_departure=actual_iso,
            delay_minutes=compute_delay_minutes(raw),
            gate=raw.get("gate"),
            terminal=str(raw.get("terminal")) if raw.get("terminal") else None,
            destination_iata=dest_iata,
            destination_name="",  
            status=map_status(raw),
            aircraft_type=aircraft,
            is_codeshare=False,         
            is_cargo=False,             
            service_type=(raw.get("serviceType") or "J"),
        )
    except Exception as e:
        log.warning("Failed to normalize flight %s: %s", raw.get("flightName"), e)
        return None

def forward(event: FlightEvent) -> bool:
    try:
        r = requests.post(API_PRODUCER_URL, json=event.to_dict(), timeout=5)
        if r.status_code == 200:
            return True
        log.warning("API-producer rejected %s: HTTP %d %s",
                    event.flight_code, r.status_code, r.text[:120])
        return False
    except requests.RequestException as e:
        log.error("API-producer unreachable: %s", e)
        return False

def fetch_window():
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=LOOK_AHEAD_HOURS)
    days_to_fetch = sorted({now.date(), horizon.date()})

    for day in days_to_fetch:
        date_str = day.strftime("%Y-%m-%d")
        for page in range(MAX_PAGES):
            data = fetch_page(page, date_str)
            if not data:
                break
            flights = data.get("flights") or [] 
            if not flights:
                break

            log.info("Page %d for %s: %d raw flights", page, date_str, len(flights))
            yield from flights

            if len(flights) < 20:
                break

def run():
    log.info("Starting Schiphol poller")
    log.info("API-producer URL: %s", API_PRODUCER_URL)
    log.info("Poll interval: %ds   Look-ahead: %dh", POLL_INTERVAL, LOOK_AHEAD_HOURS)

    while True:
        cycle_start = time.time()
        seen, kept, sent_ok, sent_fail = 0, 0, 0, 0
        skip_codeshare, skip_cargo, skip_past = 0, 0, 0
        now_utc = datetime.now(timezone.utc)

        try:
            for raw in fetch_window():
                seen += 1
                if is_codeshare(raw):
                    skip_codeshare += 1
                    continue
                if is_filtered_service(raw):
                    skip_cargo += 1
                    continue

                event = normalize(raw)
                if not event:
                    continue

                # Drop flights already in the past (we want forward-looking dashboard)
                try:
                    sched_dt = datetime.fromisoformat(event.scheduled_departure.replace("Z", "+00:00"))
                    # Allow a 30-minute lookback so flights "just departed" are still visible
                    if sched_dt < now_utc - timedelta(minutes=30):
                        skip_past += 1
                        continue
                except ValueError:
                    pass

                kept += 1
                if forward(event):
                    sent_ok += 1
                else:
                    sent_fail += 1

        except Exception as e:
            log.exception("Unexpected error in poll cycle: %s", e)

        elapsed = time.time() - cycle_start
        log.info(
            "Cycle done in %.1fs | seen=%d kept=%d sent=%d failed=%d | "
            "skipped: codeshare=%d cargo/GA=%d past=%d",
            elapsed, seen, kept, sent_ok, sent_fail, skip_codeshare, skip_cargo, skip_past
        )

        sleep_for = max(5, POLL_INTERVAL - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Poller stopped by user")
