

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9, non dovrebbe servire se usi Python 3.11
    ZoneInfo = None  # type: ignore[assignment]

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.flight_schema import FlightEvent  # noqa: E402

PROJECT_ROOT = HERE.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SCHIPHOL_APP_ID = os.getenv("SCHIPHOL_APP_ID")
SCHIPHOL_APP_KEY = os.getenv("SCHIPHOL_APP_KEY")

if not SCHIPHOL_APP_ID or not SCHIPHOL_APP_KEY:
    print("ERROR: SCHIPHOL_APP_KEY or SCHIPHOL_APP_ID missing in .env", file=sys.stderr)
    sys.exit(1)

SCHIPHOL_BASE = "https://api.schiphol.nl/public-flights/flights"
RESOURCE_VERSION = os.getenv("SCHIPHOL_RESOURCE_VERSION", "v4")

API_PRODUCER_URL = os.getenv("API_PRODUCER_URL", "http://127.0.0.1:8000/flight")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
LOOK_AHEAD_HOURS = int(os.getenv("LOOK_AHEAD_HOURS", "8"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
BOARD_SIZE = int(os.getenv("BOARD_SIZE", "20"))

# Quanto tollerare un volo tracciato che non compare piu' nelle API dopo il
# suo scheduled time prima di inviare DELETE. Serve a non cancellare troppo
# aggressivamente in caso di risposta incompleta o temporaneo buco API.
MISSING_GRACE_MINUTES = int(os.getenv("MISSING_GRACE_MINUTES", "10"))

# File di stato per evitare duplicati dopo un riavvio del poller.
# Puoi cambiarlo da .env con POLLER_STATE_FILE=/path/to/file.json.
STATE_FILE = Path(
    os.getenv(
        "POLLER_STATE_FILE",
        str(PROJECT_ROOT / ".poller_state" / "schiphol_ams_state.json"),
    )
)

# Dopo quanto tempo eliminare dallo state file voli vecchi. Serve a non far
# crescere indefinitamente il JSON. Deve essere abbastanza alto da evitare che
# un riavvio immediato ripubblichi eventi vecchi.
STATE_RETENTION_HOURS = int(os.getenv("POLLER_STATE_RETENTION_HOURS", "36"))

if ZoneInfo is not None:
    TZ_AMS = ZoneInfo("Europe/Amsterdam")
else:
    # Fallback statico: va bene in CEST, ma ZoneInfo e' preferibile per DST.
    from datetime import timezone

    TZ_AMS = timezone(timedelta(hours=2))

SCHIPHOL_STATUS_MAP = {
    "SCH": "SCHEDULED",
    "AIR": "DEPARTED",
    "DEP": "DEPARTED",
    "DEL": "DELAYED",
    "WIL": "BOARDING",  # Wait in Lounge
    "BRD": "BOARDING",
    "GCH": "GATE_OPEN",
    "GTO": "GATE_OPEN",
    "GCL": "GATE_CLOSED",
    "GTD": "GATE_CLOSED",
    "TOM": "SCHEDULED",  # Tomorrow
    "CNX": "CANCELLED",
    "DIV": "DIVERTED",
}

# Campi confrontati per decidere se pubblicare un nuovo evento.
# Campi volatili come eventId e ts sono aggiunti dall'API-producer, quindi qui
# non vengono usati per il confronto. Anche changed_fields non viene confrontato.
MONITORED_FIELDS = (
    "airport",
    "flight_code",
    "airline_iata",
    "airline_name",
    "scheduled_departure",
    "estimated_departure",
    "actual_departure",
    "delay_minutes",
    "gate",
    "terminal",
    "destination_iata",
    "destination_name",
    "status",
    "aircraft_type",
    "is_codeshare",
    "is_cargo",
    "service_type",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("schiphol-poller")


def as_schiphol_param(dt: datetime) -> str:
    """Formato richiesto dalle API Schiphol per fromDateTime/toDateTime.

    Nel tuo test iniziale veniva usato un datetime locale senza offset, ad es.
    2026-04-22T10:00:00. Manteniamo lo stesso formato, ma costruito a partire
    da Europe/Amsterdam.
    """

    return dt.astimezone(TZ_AMS).replace(tzinfo=None).isoformat(timespec="seconds")


def parse_api_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parsa un datetime Schiphol e lo restituisce in Europe/Amsterdam."""

    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_AMS)

    return dt.astimezone(TZ_AMS)


def to_ams_iso(value: Optional[str]) -> Optional[str]:
    """Datetime ISO-8601 in Europe/Amsterdam, es. 2026-04-22T15:34:00+02:00."""

    dt = parse_api_datetime(value)
    if not dt:
        return None
    return dt.isoformat(timespec="seconds")


def now_ams() -> datetime:
    return datetime.now(TZ_AMS).replace(microsecond=0)


def load_state() -> dict[str, Any]:
    """Carica l'ultimo payload inviato per ogni volo.

    Struttura:
    {
      "version": 1,
      "last_sent": {
        "AMS:HV6119:2026-04-30T16:15:00+02:00": {payload confrontabile}
      }
    }
    """

    if not STATE_FILE.exists():
        return {"version": 1, "last_sent": {}}

    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Cannot read poller state file %s: %s. Starting with empty state.", STATE_FILE, e)
        return {"version": 1, "last_sent": {}}

    if not isinstance(data, dict):
        return {"version": 1, "last_sent": {}}

    data.setdefault("version", 1)
    data.setdefault("last_sent", {})
    return data


def save_state(state: dict[str, Any]) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.replace(STATE_FILE)
    except OSError as e:
        log.warning("Cannot write poller state file %s: %s", STATE_FILE, e)


def prune_state(state: dict[str, Any], reference_now: datetime) -> int:
    """Rimuove dallo state file i voli troppo vecchi."""

    last_sent = state.get("last_sent", {})
    if not isinstance(last_sent, dict):
        state["last_sent"] = {}
        return 0

    cutoff = reference_now - timedelta(hours=STATE_RETENTION_HOURS)
    removed = 0

    for key, payload in list(last_sent.items()):
        if not isinstance(payload, dict):
            last_sent.pop(key, None)
            removed += 1
            continue

        scheduled_dt = parse_api_datetime(payload.get("scheduled_departure"))
        if scheduled_dt and scheduled_dt < cutoff:
            last_sent.pop(key, None)
            removed += 1

    return removed


def fetch_page(page: int, from_dt: datetime, to_dt: datetime) -> Optional[dict[str, Any]]:
    headers = {
        "Accept": "application/json",
        "ResourceVersion": RESOURCE_VERSION,
        "app_id": SCHIPHOL_APP_ID,
        "app_key": SCHIPHOL_APP_KEY,
    }

    params = {
        "flightDirection": "D",
        "fromDateTime": as_schiphol_param(from_dt),
        "toDateTime": as_schiphol_param(to_dt),
        "searchDateTimeField": "scheduleDateTime",
        "includedelays": "true",
        "sort": "+scheduleTime",
        "page": page,
    }

    try:
        response = requests.get(SCHIPHOL_BASE, headers=headers, params=params, timeout=15)
    except requests.RequestException as e:
        log.error("Schiphol request failed: %s", e)
        return None

    if response.status_code == 429:
        reset = response.headers.get("X-RateLimit-Reset")
        log.warning("Schiphol rate limit hit%s", f"; reset={reset}" if reset else "")
        time.sleep(30)
        return None

    if response.status_code >= 400:
        log.error("Schiphol API error %d: %s", response.status_code, response.text[:300])
        return None

    try:
        return response.json()
    except ValueError as e:
        log.error("Schiphol response not JSON: %s", e)
        return None


def fetch_window(from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    """Scarica le pagine Schiphol in una finestra temporale locale."""

    all_flights: list[dict[str, Any]] = []

    for page in range(MAX_PAGES):
        data = fetch_page(page, from_dt, to_dt)
        if not data:
            break

        flights = data.get("flights")
        if flights is None:
            log.error("Schiphol response has no 'flights' key. Top-level keys: %s", list(data.keys()))
            break

        if not flights:
            break

        log.info(
            "Fetched page=%d raw=%d window=%s -> %s",
            page,
            len(flights),
            as_schiphol_param(from_dt),
            as_schiphol_param(to_dt),
        )
        all_flights.extend(flights)

        # Le API restituiscono normalmente 20 risultati per pagina. Se ne arrivano
        # meno, siamo arrivati all'ultima pagina utile.
        if len(flights) < 20:
            break

    return all_flights


def is_codeshare(raw: dict[str, Any]) -> bool:
    """Tiene solo il volo operativo, scartando i codeshare."""

    flight_name = raw.get("flightName")
    main_flight = raw.get("mainFlight")
    return bool(main_flight and flight_name and main_flight != flight_name)


def is_filtered_service(raw: dict[str, Any]) -> bool:
    """Tiene solo i voli passeggeri schedulati: serviceType J."""

    service_type = (raw.get("serviceType") or "J").upper()
    return service_type != "J"


def map_status(raw: dict[str, Any]) -> str:
    if raw.get("actualOffBlockTime"):
        return "DEPARTED"

    states = (raw.get("publicFlightState") or {}).get("flightStates") or []
    if not states:
        return "UNKNOWN"

    # L'ultimo stato e' spesso il piu' informativo.
    for code in reversed(states):
        mapped = SCHIPHOL_STATUS_MAP.get(code)
        if mapped:
            return mapped

    return "UNKNOWN"


def compute_delay_minutes(raw: dict[str, Any]) -> Optional[int]:
    scheduled = parse_api_datetime(raw.get("scheduleDateTime"))
    effective = parse_api_datetime(raw.get("actualOffBlockTime") or raw.get("publicEstimatedOffBlockTime"))

    if not scheduled or not effective:
        return None

    return int((effective - scheduled).total_seconds() / 60)


def build_flight_event(**kwargs: Any) -> FlightEvent:
    """Costruisce FlightEvent restando compatibile col tuo schema attuale.

    Se aggiungi `event_type` e `changed_fields` alla dataclass in
    scripts/common/flight_schema.py, verranno passati al costruttore. Se non li
    aggiungi, il poller continuera' a funzionare e questi campi verranno aggiunti
    direttamente al JSON in event_to_payload().
    """

    if is_dataclass(FlightEvent):
        accepted_fields = {field.name for field in fields(FlightEvent)}
        constructor_kwargs = {key: value for key, value in kwargs.items() if key in accepted_fields}
    else:
        constructor_kwargs = kwargs

    event = FlightEvent(**constructor_kwargs)

    # Attributi dinamici: non entrano in asdict(), ma event_to_payload() li recupera.
    for key, value in kwargs.items():
        if not hasattr(event, key):
            setattr(event, key, value)

    return event


def normalize(raw: dict[str, Any]) -> Optional[FlightEvent]:
    try:
        flight_code = (raw.get("flightName") or "").strip()
        airline_iata = (raw.get("prefixIATA") or "").strip()
        scheduled_raw = raw.get("scheduleDateTime")

        if not flight_code or not scheduled_raw:
            return None

        scheduled_iso = to_ams_iso(scheduled_raw)
        if not scheduled_iso:
            return None

        destinations = (raw.get("route") or {}).get("destinations") or []
        destination_iata = destinations[0] if destinations else ""

        aircraft = (raw.get("aircraftType") or {}).get("iataSub") or (raw.get("aircraftType") or {}).get("iataMain")

        # Per un tabellone partenze, la stima migliore e' publicEstimatedOffBlockTime.
        # Se il volo e' gia' partito, usiamo actualOffBlockTime anche come estimated
        # per mostrare comunque l'orario effettivo.
        actual_raw = raw.get("actualOffBlockTime")
        estimated_raw = raw.get("publicEstimatedOffBlockTime") or actual_raw

        return build_flight_event(
            airport="AMS",
            flight_code=flight_code,
            airline_iata=airline_iata,
            airline_name=airline_iata,
            scheduled_departure=scheduled_iso,
            event_type="UPSERT",
            changed_fields={},
            estimated_departure=to_ams_iso(estimated_raw),
            actual_departure=to_ams_iso(actual_raw),
            delay_minutes=compute_delay_minutes(raw),
            gate=raw.get("gate"),
            terminal=str(raw.get("terminal")) if raw.get("terminal") is not None else None,
            destination_iata=destination_iata,
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


def flight_key(event: FlightEvent) -> tuple[str, str]:
    return event.flight_code, event.scheduled_departure


def state_key_from_payload(payload: dict[str, Any]) -> str:
    return f"{payload.get('airport')}:{payload.get('flight_code')}:{payload.get('scheduled_departure')}"


def state_key(event: FlightEvent) -> str:
    return state_key_from_payload(event_to_payload(event, include_changed_fields=False))


def has_departed(event: FlightEvent, reference_now: datetime) -> bool:
    if getattr(event, "status", None) == "DEPARTED":
        return True

    actual_dt = parse_api_datetime(getattr(event, "actual_departure", None))
    return bool(actual_dt and actual_dt <= reference_now)


def comparable_payload(payload: dict[str, Any]) -> dict[str, Any]:
    comparable = {field: payload.get(field) for field in MONITORED_FIELDS}
    comparable["event_type"] = payload.get("event_type", "UPSERT")
    return comparable


def diff_payloads(previous: Optional[dict[str, Any]], current: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Restituisce i campi cambiati tra previous e current.

    Per il primo invio usa il marcatore speciale _created. Per un update reale
    ritorna solo i campi che hanno cambiato valore, ad esempio:
    {
      "estimated_departure": {"old": null, "new": "2026-04-30T16:36:00+02:00"},
      "delay_minutes": {"old": null, "new": 21}
    }
    """

    current_cmp = comparable_payload(current)

    if previous is None:
        return {"_created": {"old": None, "new": True}}

    previous_cmp = comparable_payload(previous)
    changed: dict[str, dict[str, Any]] = {}

    for field in sorted(current_cmp.keys() | previous_cmp.keys()):
        old_value = previous_cmp.get(field)
        new_value = current_cmp.get(field)
        if old_value != new_value:
            changed[field] = {"old": old_value, "new": new_value}

    return changed


def event_to_payload(
    event: FlightEvent,
    event_type: Optional[str] = None,
    changed_fields: Optional[dict[str, dict[str, Any]]] = None,
    include_changed_fields: bool = True,
) -> dict[str, Any]:
    payload = event.to_dict()
    payload["event_type"] = event_type or getattr(event, "event_type", "UPSERT")

    dynamic_changed_fields = getattr(event, "changed_fields", None)
    if include_changed_fields:
        payload["changed_fields"] = changed_fields if changed_fields is not None else dynamic_changed_fields or {}

    return payload


def make_delete_event(event: FlightEvent) -> FlightEvent:
    setattr(event, "event_type", "DELETE")
    event.status = "DEPARTED"
    return event


def forward_payload(payload: dict[str, Any]) -> bool:
    try:
        response = requests.post(API_PRODUCER_URL, json=payload, timeout=5)
    except requests.RequestException as e:
        log.error("API-producer unreachable: %s", e)
        return False

    if response.status_code == 200:
        return True

    log.warning(
        "API-producer rejected %s event_type=%s: HTTP %d %s",
        payload.get("flight_code"),
        payload.get("event_type"),
        response.status_code,
        response.text[:200],
    )
    return False


def forward_if_changed(event: FlightEvent, state: dict[str, Any]) -> tuple[bool, bool, dict[str, dict[str, Any]]]:
    """Invia il volo solo se il payload e' cambiato rispetto all'ultimo invio.

    Ritorna:
    - attempted: True se ha provato a inviare un evento a API-producer
    - ok: True se l'invio e' riuscito; False se fallito o non tentato
    - changed_fields: diff calcolato
    """

    payload_without_diff = event_to_payload(event, include_changed_fields=False)
    key = state_key_from_payload(payload_without_diff)

    last_sent = state.setdefault("last_sent", {})
    previous_payload = last_sent.get(key)
    if previous_payload is not None and not isinstance(previous_payload, dict):
        previous_payload = None

    changed_fields = diff_payloads(previous_payload, payload_without_diff)

    if not changed_fields:
        return False, False, {}

    payload = event_to_payload(event, changed_fields=changed_fields)

    if not forward_payload(payload):
        return True, False, changed_fields

    # Salviamo solo dopo successo: se l'API-producer e' down, il ciclo successivo
    # ritentera' lo stesso update.
    last_sent[key] = comparable_payload(payload_without_diff)
    return True, True, changed_fields


def collect_candidates(from_dt: datetime, to_dt: datetime) -> list[FlightEvent]:
    candidates: list[FlightEvent] = []
    seen_keys: set[tuple[str, str]] = set()

    for raw in fetch_window(from_dt, to_dt):
        if is_codeshare(raw):
            continue

        if is_filtered_service(raw):
            continue

        event = normalize(raw)
        if not event:
            continue

        key = flight_key(event)
        if key in seen_keys:
            continue

        seen_keys.add(key)
        candidates.append(event)

    candidates.sort(
        key=lambda event: parse_api_datetime(event.scheduled_departure)
        or datetime.max.replace(tzinfo=TZ_AMS)
    )
    return candidates


def oldest_tracked_datetime(tracked: dict[tuple[str, str], FlightEvent]) -> Optional[datetime]:
    datetimes = [parse_api_datetime(event.scheduled_departure) for event in tracked.values()]
    datetimes = [dt for dt in datetimes if dt is not None]
    return min(datetimes) if datetimes else None


def run() -> None:
    log.info("Starting Schiphol poller")
    log.info("API-producer URL: %s", API_PRODUCER_URL)
    log.info("State file: %s", STATE_FILE)
    log.info(
        "Poll interval=%ds look_ahead=%dh board_size=%d timezone=%s",
        POLL_INTERVAL,
        LOOK_AHEAD_HOURS,
        BOARD_SIZE,
        TZ_AMS,
    )

    state = load_state()
    tracked: dict[tuple[str, str], FlightEvent] = {}

    while True:
        cycle_start = time.time()
        reference_now = now_ams()
        horizon = reference_now + timedelta(hours=LOOK_AHEAD_HOURS)

        sent_ok = 0
        sent_fail = 0
        skipped_unchanged = 0
        deleted = 0
        updated = 0
        added = 0
        fields_changed_counter: dict[str, int] = {}

        try:
            pruned = prune_state(state, reference_now)
            if pruned:
                log.info("Pruned %d old entries from state", pruned)

            # Query principale: prossimi voli da ora in poi. E' quella che
            # garantisce il requisito "prossimi 20 voli schedulati da adesso".
            future_candidates = collect_candidates(reference_now, horizon)

            # Query di aggiornamento: se abbiamo voli gia' tracciati con scheduled
            # time nel passato ma non ancora partiti, dobbiamo continuare a
            # recuperarli usando scheduleDateTime, quindi allarghiamo la finestra
            # fino al piu' vecchio volo tracciato.
            if tracked:
                oldest = oldest_tracked_datetime(tracked) or reference_now
                update_from = min(reference_now, oldest) - timedelta(minutes=5)
                update_candidates = collect_candidates(update_from, horizon)
            else:
                update_candidates = future_candidates

            candidate_map = {flight_key(event): event for event in update_candidates}
            events_to_consider: list[FlightEvent] = []

            # 1. Aggiorna i voli gia' sul tabellone, oppure cancellali se partiti.
            for key, previous_event in list(tracked.items()):
                current_event = candidate_map.get(key)

                if current_event is None:
                    scheduled_dt = parse_api_datetime(previous_event.scheduled_departure)

                    if scheduled_dt and scheduled_dt < reference_now - timedelta(minutes=MISSING_GRACE_MINUTES):
                        delete_event = make_delete_event(previous_event)
                        events_to_consider.append(delete_event)
                        tracked.pop(key, None)
                        deleted += 1

                    # Se non e' ancora oltre la grace window, lo teniamo in memoria.
                    continue

                if has_departed(current_event, reference_now):
                    delete_event = make_delete_event(current_event)
                    events_to_consider.append(delete_event)
                    tracked.pop(key, None)
                    deleted += 1
                    continue

                setattr(current_event, "event_type", "UPSERT")
                tracked[key] = current_event
                events_to_consider.append(current_event)
                updated += 1

            # 2. Riempi gli slot liberi con i prossimi voli schedulati da ora.
            for event in future_candidates:
                if len(tracked) >= BOARD_SIZE:
                    break

                key = flight_key(event)
                if key in tracked:
                    continue

                scheduled_dt = parse_api_datetime(event.scheduled_departure)
                if scheduled_dt and scheduled_dt < reference_now:
                    continue

                if has_departed(event, reference_now):
                    continue

                setattr(event, "event_type", "UPSERT")
                tracked[key] = event
                events_to_consider.append(event)
                added += 1

            # 3. Invia solo eventi nuovi o realmente modificati.
            for event in events_to_consider:
                attempted, ok, changed_fields = forward_if_changed(event, state)

                if not attempted:
                    skipped_unchanged += 1
                    continue

                if ok:
                    sent_ok += 1
                    for field in changed_fields:
                        fields_changed_counter[field] = fields_changed_counter.get(field, 0) + 1

                    log.info(
                        "Forwarded %s:%s event_type=%s changed=%s",
                        event.airport,
                        event.flight_code,
                        getattr(event, "event_type", "UPSERT"),
                        ",".join(changed_fields.keys()),
                    )
                else:
                    sent_fail += 1

            if sent_ok or pruned:
                save_state(state)

        except Exception as e:
            log.exception("Unexpected error in poll cycle: %s", e)

        elapsed = time.time() - cycle_start
        log.info(
            "Cycle done in %.1fs | local_now=%s | tracked=%d updated=%d added=%d deleted=%d "
            "sent=%d failed=%d skipped_unchanged=%d changed_fields=%s",
            elapsed,
            reference_now.isoformat(timespec="seconds"),
            len(tracked),
            updated,
            added,
            deleted,
            sent_ok,
            sent_fail,
            skipped_unchanged,
            fields_changed_counter or {},
        )

        sleep_for = max(5, POLL_INTERVAL - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Poller stopped by user")
