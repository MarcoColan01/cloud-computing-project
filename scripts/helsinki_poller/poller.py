#HELSINKI POLLER

import logging, os, sys, time
from dataclasses import fields, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv

try: from zoneinfo import ZoneInfo
except ImportError: from datetime import timezone as ZoneInfo


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.flight_schema import FlightEvent

load_dotenv(HERE.parent.parent / ".env")
ENV = lambda k, d=None: os.getenv(k,d)

if not (APP_KEY := ENV("FINAVIA_APP_KEY")):
    sys.exit("ERROR: FINAVIA_APP_KEY missing")

API_URL, POLL_INT = ENV("API_PRODUCER_URL", "http://127.0.0.1:8000/flight"), int(ENV("POLL_INTERVAL", "30"))
LOOK_AHEAD, BOARD_SIZE = int(ENV("LOOK_AHEAD_HOURS", "8")), int(ENV("BOARD_SIZE", "20"))
GRACE_MIN = int(ENV("MISSING_GRACE_MINUTES", "10"))
TZ = ZoneInfo("Europe/Helsinki") if hasattr(ZoneInfo, "__call__") else ZoneInfo(timedelta(hours=3))

BASE_URL = "https://apigw.finavia.fi/flights/public/v0"

NS = "http://www.finavia.fi/FlightsService.xsd"

STATUS_MAP = {
    "BOR": "BOARDING",
    "BRD": "BOARDING",
    "LCL": "LAST_CALL",
    "LAS": "LAST_CALL",
    "GTC": "GATE_CLOSED",
    "GCL": "GATE_CLOSED",
    "GTO": "GATE_OPEN",
    "DEP": "DEPARTED",
    "AIR": "DEPARTED",
    "CNX": "CANCELLED",
    "CAN": "CANCELLED",
    "DLY": "DELAYED",
    "DEL": "DELAYED",
    "DIV": "DIVERTED",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("helsinki-poller")

def ns_tag(tag:str) -> str:
    return f"{{{NS}}}{tag}"

def get_text(el, tag: str) -> str:
    """Read text of a child tag, accounting for namespace. Empty string if missing."""
    child = el.find(ns_tag(tag))
    return child.text.strip() if child is not None and child.text else ""

def dt_parse(val):
    """Parse Finavia ISO timestamp ('Z' suffix = UTC), return aware datetime in TZ_HEL."""
    if not val:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(val, fmt)
            return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(TZ)
        except ValueError:
            continue
    
    return None

def now():
    return datetime.now(TZ).replace(microsecond=0)

def fetch_flights():
    """One GET retrieves all departures of the day from HEL. Filtering is client-side."""
    url = f"{BASE_URL}/flights/dep/HEL"
    headers = {"app_key": APP_KEY, "Accept": "application/xml"}
    try:
        res = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException as e:
        log.error("Finavia request failed: %s", e)
        return []

    if res.status_code == 429:
        log.warning("Finavia rate limit hit")
        time.sleep(30)
        return []
    if res.status_code >= 400:
        log.error("Finavia API error %d: %s", res.status_code, res.text[:200])
        return []
    
    try:
        root = ET.fromstring(res.content)
    except ET.ParseError as e:
        log.error("Finavia response not valid XML: %s", e)
        return []
    
    dep_el = root.find(ns_tag("dep"))
    body_el = dep_el.find(ns_tag("body")) if dep_el is not None else None
    if body_el is None:
        log.error("Finavia response has no <dep><body> structure")
        return []
    
    return body_el.findall(ns_tag("flight"))

def parse_flight(raw):
    """Convert one Finavia <flight> element into a FlightEvent. Returns None on filtering."""
    code = get_text(raw, "fltnr")
    main_flight = get_text(raw, "mfltnr")

    if not code or main_flight:
        return None
    
    sched = dt_parse(get_text(raw, "sdt"))
    if not sched:
        return None
    
    prt = get_text(raw, "prt")
    actual = dt_parse(get_text(raw, "act_d"))
    estimated = dt_parse(get_text(raw, "pest_d")) or dt_parse(get_text(raw, "est_d"))
    best_est = actual or estimated

    status = STATUS_MAP.get(prt) if prt else None
    if status is None:
        if actual:
            status = "DEPARTED"
        elif best_est and best_est > sched:
            status = "DELAYED"
        else:
            status = "SCHEDULED"
    
    airline_iata = code[:2].upper() if len(code) >= 2 and code[:2].isalpha() else ""
    dest_iata = get_text(raw, "route_1")
    dest_name = get_text(raw, "route_n_1")

    aircraft = get_text(raw, "actype") or None
    gate = get_text(raw, "gate") or None
    terminal = None

    delay = int((best_est - sched).total_seconds() /60) if best_est else None

    kw = dict(
        airport="HEL",
        flight_code=code,
        airline_iata=airline_iata,
        airline_name=airline_iata,  
        scheduled_departure=sched.isoformat("T", "seconds"),
        estimated_departure=best_est.isoformat("T", "seconds") if best_est else None,
        actual_departure=actual.isoformat("T", "seconds") if actual else None,
        delay_minutes=delay,
        gate=gate,
        terminal=terminal,
        destination_iata=dest_iata,
        destination_name=dest_name,
        service_type="J",
        status=status,
        aircraft_type=aircraft,
        is_codeshare=False,
        is_cargo=False,
    )

    ev = FlightEvent(**{k: v for k, v in kw.items() if k in {f.name for f in fields(FlightEvent)}} if is_dataclass(FlightEvent) else kw)
    for k, v in kw.items(): 
        setattr(ev, k, v)
    return ev

def run():
    log.info(f"Starting Helsinki poller. API: {API_URL}")
    tracked = {}

    while True:
        t0, ref_now = time.time(), now()
        horizon = ref_now + timedelta(hours=LOOK_AHEAD)
        metrics = dict(sent=0, fail=0, deleted=0, updated=0, added=0)

        cands = {}
        for raw in fetch_flights():
            ev = parse_flight(raw)
            if not ev:
                continue
            sched_dt = dt_parse(ev.scheduled_departure)
            if sched_dt is None or sched_dt < ref_now or sched_dt > horizon:
                continue
            cands[(ev.flight_code, ev.scheduled_departure)] = ev

        consider = []
        for k, prev in list(tracked.items()):
            curr = cands.get(k)
            if not curr and dt_parse(prev.scheduled_departure) < ref_now - timedelta(minutes=GRACE_MIN):
                prev.status, prev.event_type = "DEPARTED", "DELETE"
                prev.observed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                consider.append(prev); del tracked[k]; metrics["deleted"] += 1
            elif curr:
                curr_status = getattr(curr, "status", None)
                departed = curr_status == "DEPARTED" or (
                    curr.actual_departure and dt_parse(curr.actual_departure) <= ref_now
                )
                if departed:
                    curr.status, curr.event_type = "DEPARTED", "DELETE"
                    curr.observed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    consider.append(curr); del tracked[k]; metrics["deleted"] += 1
                elif curr_status == "CANCELLED":
                    
                    curr.event_type = "DELETE"
                    curr.observed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    consider.append(curr); del tracked[k]; metrics["deleted"] += 1
                else:
                    curr.event_type = "UPSERT"; tracked[k] = curr; consider.append(curr); metrics["updated"] += 1

        for k, ev in sorted(cands.items(), key=lambda x: dt_parse(x[1].scheduled_departure)):
            if len(tracked) >= BOARD_SIZE: break
            if k not in tracked and dt_parse(ev.scheduled_departure) >= ref_now \
               and getattr(ev, "status", None) not in ("DEPARTED", "CANCELLED"):
                ev.event_type = "UPSERT"; tracked[k] = ev; consider.append(ev); metrics["added"] += 1

        for ev in consider:
            pld = ev.to_dict()
            pld["event_type"] = getattr(ev, "event_type", "UPSERT")
            if hasattr(ev, "observed_at_utc"):
                pld["observed_at_utc"] = ev.observed_at_utc
            try:
                if requests.post(API_URL, json=pld, timeout=5).status_code == 200:
                    metrics["sent"] += 1
                else:
                    metrics["fail"] += 1
            except requests.RequestException:
                metrics["fail"] += 1

        el = time.time() - t0
        log.info(f"Cycle {el:.1f}s | trk:{len(tracked)} " + " ".join(f"{k}:{v}" for k, v in metrics.items()))
        time.sleep(max(5, POLL_INT - int(el)))


if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: log.info("Stopped by user")


