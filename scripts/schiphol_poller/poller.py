import json, logging, os, sys, time
from dataclasses import fields, is_dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

try: from zoneinfo import ZoneInfo
except ImportError: from datetime import timezone as ZoneInfo

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.flight_schema import FlightEvent

load_dotenv(HERE.parent.parent / ".env")
ENV = lambda k, d=None: os.getenv(k, d)

if not (APP_ID := ENV("SCHIPHOL_APP_ID")) or not (APP_KEY := ENV("SCHIPHOL_APP_KEY")):
    sys.exit("ERROR: SCHIPHOL_APP_KEY/ID missing")

API_URL, POLL_INT = ENV("API_PRODUCER_URL", "http://127.0.0.1:8000/flight"), int(ENV("POLL_INTERVAL", "60"))
LOOK_AHEAD, MAX_PAGES, BOARD_SIZE = int(ENV("LOOK_AHEAD_HOURS", "8")), int(ENV("MAX_PAGES", "10")), int(ENV("BOARD_SIZE", "20"))
GRACE_MIN, RETENTION_H = int(ENV("MISSING_GRACE_MINUTES", "10")), int(ENV("POLLER_STATE_RETENTION_HOURS", "36"))
STATE_FILE = Path(ENV("POLLER_STATE_FILE", HERE.parent.parent / ".poller_state" / "schiphol_ams_state.json"))
TZ = ZoneInfo("Europe/Amsterdam") if hasattr(ZoneInfo, "__call__") else ZoneInfo(timedelta(hours=2))

STATUS_MAP = {"SCH": "SCHEDULED", "AIR": "DEPARTED", "DEP": "DEPARTED", "DEL": "DELAYED", "WIL": "BOARDING", "BRD": "BOARDING", "GCH": "GATE_OPEN", "GTO": "GATE_OPEN", "GCL": "GATE_CLOSED", "GTD": "GATE_CLOSED", "TOM": "SCHEDULED", "CNX": "CANCELLED", "DIV": "DIVERTED"}
MONITORED = ("airport", "flight_code", "airline_iata", "airline_name", "scheduled_departure", "estimated_departure", "actual_departure", "delay_minutes", "gate", "terminal", "destination_iata", "destination_name", "status", "aircraft_type", "is_codeshare", "is_cargo", "service_type")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("schiphol-poller")

def dt_parse(val): return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(TZ) if val else None
def dt_fmt(dt): return dt.astimezone(TZ).replace(tzinfo=None).isoformat(timespec="seconds")
def now(): return datetime.now(TZ).replace(microsecond=0)

def load_state():
    try: return json.loads(STATE_FILE.read_text("utf-8")) if STATE_FILE.exists() else {"version": 1, "last_sent": {}}
    except Exception: return {"version": 1, "last_sent": {}}

def fetch_flights(start, end):
    res_list = []
    for p in range(MAX_PAGES):
        res = requests.get("https://api.schiphol.nl/public-flights/flights", timeout=15,
            headers={"Accept": "application/json", "ResourceVersion": ENV("SCHIPHOL_RESOURCE_VERSION", "v4"), "app_id": APP_ID, "app_key": APP_KEY},
            params={"flightDirection": "D", "fromDateTime": dt_fmt(start), "toDateTime": dt_fmt(end), "searchDateTimeField": "scheduleDateTime", "includedelays": "true", "sort": "+scheduleTime", "page": p})
        if res.status_code == 429: time.sleep(30); break
        if res.status_code >= 400 or "flights" not in (data := res.json() if res.text else {}): break
        res_list.extend(data["flights"])
        if len(data["flights"]) < 20: break
    return res_list

def parse_flight(r):
    code, main_flight, service = str(r.get("flightName", "")).strip(), r.get("mainFlight"), str(r.get("serviceType", "J")).upper()
    if not code or r.get("scheduleDateTime") is None or (main_flight and main_flight != code) or service != "J": return None
    
    sched, act, est = dt_parse(r.get("scheduleDateTime")), dt_parse(r.get("actualOffBlockTime")), dt_parse(r.get("publicEstimatedOffBlockTime"))
    est = act or est
    
    kw = dict(airport="AMS", flight_code=code, airline_iata=(r.get("prefixIATA") or "").strip(), airline_name=(r.get("prefixIATA") or "").strip(),
        scheduled_departure=sched.isoformat("T", "seconds"), estimated_departure=est.isoformat("T", "seconds") if est else None,
        actual_departure=act.isoformat("T", "seconds") if act else None, delay_minutes=int((est - sched).total_seconds() / 60) if sched and est else None,
        gate=r.get("gate"), terminal=str(r.get("terminal")) if r.get("terminal") is not None else None,
        destination_iata=(r.get("route", {}).get("destinations") or [""])[0], destination_name="", service_type=service,
        status="DEPARTED" if act else next((STATUS_MAP[s] for s in reversed((r.get("publicFlightState") or {}).get("flightStates", [])) if s in STATUS_MAP), "UNKNOWN"),
        aircraft_type=r.get("aircraftType", {}).get("iataSub") or r.get("aircraftType", {}).get("iataMain"), is_codeshare=False, is_cargo=False)
    
    ev = FlightEvent(**{k: v for k, v in kw.items() if k in {f.name for f in fields(FlightEvent)}} if is_dataclass(FlightEvent) else kw)
    for k, v in kw.items(): setattr(ev, k, v)
    return ev

def run():
    log.info(f"Starting Schiphol poller. API: {API_URL}")
    state, tracked = load_state(), {}

    while True:
        t0, ref_now, ls = time.time(), now(), state.setdefault("last_sent", {})
        horizon = ref_now + timedelta(hours=LOOK_AHEAD)
        metrics = dict(sent=0, fail=0, skip=0, deleted=0, updated=0, added=0)

        for k in list(ls):  # Prune old state
            if not isinstance(ls[k], dict) or dt_parse(ls[k].get("scheduled_departure")) < ref_now - timedelta(hours=RETENTION_H): del ls[k]

        fetch_start = min([dt_parse(e.scheduled_departure) for e in tracked.values()] + [ref_now]) - timedelta(minutes=5) if tracked else ref_now
        cands = { (ev.flight_code, ev.scheduled_departure): ev for r in fetch_flights(fetch_start, horizon) if (ev := parse_flight(r)) }

        consider = []
        for k, prev in list(tracked.items()):
            curr = cands.get(k)
            if not curr and dt_parse(prev.scheduled_departure) < ref_now - timedelta(minutes=GRACE_MIN):
                prev.status, prev.event_type = "DEPARTED", "DELETE"
                consider.append(prev); del tracked[k]; metrics["deleted"] += 1
            elif curr:
                if getattr(curr, "status", None) == "DEPARTED" or (curr.actual_departure and dt_parse(curr.actual_departure) <= ref_now):
                    curr.status, curr.event_type = "DEPARTED", "DELETE"
                    consider.append(curr); del tracked[k]; metrics["deleted"] += 1
                else:
                    curr.event_type = "UPSERT"; tracked[k] = curr; consider.append(curr); metrics["updated"] += 1

        for k, ev in sorted(cands.items(), key=lambda x: dt_parse(x[1].scheduled_departure)):
            if len(tracked) >= BOARD_SIZE: break
            if k not in tracked and dt_parse(ev.scheduled_departure) >= ref_now and getattr(ev, "status", None) != "DEPARTED":
                ev.event_type = "UPSERT"; tracked[k] = ev; consider.append(ev); metrics["added"] += 1

        for ev in consider:
            pld = ev.to_dict(); pld["event_type"] = getattr(ev, "event_type", "UPSERT")
            curr_cmp = {f: pld.get(f) for f in MONITORED} | {"event_type": pld["event_type"]}
            key = f"{pld['airport']}:{pld['flight_code']}:{pld['scheduled_departure']}"
            prev_cmp = ls.get(key) if isinstance(ls.get(key), dict) else None

            diff = {"_created": {"old": None, "new": True}} if not prev_cmp else {f: {"old": prev_cmp.get(f), "new": curr_cmp.get(f)} for f in curr_cmp.keys() | prev_cmp.keys() if prev_cmp.get(f) != curr_cmp.get(f)}
            if not diff: metrics["skip"] += 1; continue

            pld["changed_fields"] = diff
            try:
                if requests.post(API_URL, json=pld, timeout=5).status_code == 200:
                    ls[key], metrics["sent"] = curr_cmp, metrics["sent"] + 1
                else: metrics["fail"] += 1
            except requests.RequestException: metrics["fail"] += 1

        if metrics["sent"]:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), "utf-8")

        el = time.time() - t0
        log.info(f"Cycle {el:.1f}s | trk:{len(tracked)} " + " ".join(f"{k}:{v}" for k, v in metrics.items()))
        time.sleep(max(5, POLL_INT - int(el)))

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: log.info("Stopped by user")