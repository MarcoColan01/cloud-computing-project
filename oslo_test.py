#!/usr/bin/env python3
"""
Avinor Public XML Feed — Oslo Gardermoen (OSL) departures.

Struttura XML reale (confermata da ispezione):
  <flight uniqueID="2012734448">
    <airline>SK</airline>
    <flight_id>SK455</flight_id>
    <dom_int>S</dom_int>
    <schedule_time>2026-04-28T08:00:00Z</schedule_time>
    <arr_dep>D</arr_dep>
    <airport>CPH</airport>       ← destinazione
    <via_airport/>
    <check_in>C</check_in>
    <gate>B21</gate>
    <status code="D" time="2026-04-28T08:03:00Z"/>
    <codeshare>...</codeshare>   ← solo se codeshare=Y
  </flight>

  pip install requests
  python avinor_departures_test.py
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL    = "https://asrv.avinor.no/XmlFeed/v1.0"
AIRPORT     = "OSL"
DIRECTION   = "D"
WINDOW_FROM = (16, 0)   # ora locale Oslo
WINDOW_TO   = (17, 0)
TIME_FROM   = 12        # ore indietro (relativo a ora corrente)
TIME_TO     = 12        # ore avanti

# ---------------------------------------------------------------------------
# Fuso orario Oslo
# ---------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo
    TZ_OSL = ZoneInfo("Europe/Oslo")
except ImportError:
    try:
        import pytz
        TZ_OSL = pytz.timezone("Europe/Oslo")
    except ImportError:
        TZ_OSL = timezone(timedelta(hours=2))

def make_local(h, m):
    d = date.today()
    dt = datetime(d.year, d.month, d.day, h, m)
    if hasattr(TZ_OSL, 'localize'):
        return TZ_OSL.localize(dt)
    return dt.replace(tzinfo=TZ_OSL)

window_from = make_local(*WINDOW_FROM)
window_to   = make_local(*WINDOW_TO)
print(f"Finestra: {window_from.strftime('%H:%M')}–{window_to.strftime('%H:%M')} Oslo  ({date.today()})")

# ---------------------------------------------------------------------------
# Helpers — definiti PRIMA di essere usati
# ---------------------------------------------------------------------------
def txt(el, tag, default=""):
    """Testo di un tag figlio. '' se assente o vuoto."""
    child = el.find(tag)
    return child.text.strip() if child is not None and child.text else default

def parse_dt(s):
    """Parsa stringa datetime → aware UTC. None se non parsabile."""
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",      # con Z (standard Avinor)
        "%Y-%m-%dT%H:%M:%S%z",     # con offset
        "%Y-%m-%dT%H:%M:%S.%f%z",  # con microsecondi
        "%Y-%m-%dT%H:%M:%S",       # senza timezone → trattato UTC
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def to_osl(dt):
    return dt.astimezone(TZ_OSL) if dt else None

def fmt_time(s):
    dt = to_osl(parse_dt(s))
    return dt.strftime("%H:%M") if dt else "-"

STATUS_LABELS = {
    "":  "Scheduled",
    "N": "New info",
    "E": "Delayed",
    "D": "Departed",
    "A": "Arrived",
    "C": "Cancelled",
}

# ---------------------------------------------------------------------------
# Chiamata API
# ---------------------------------------------------------------------------
params = {
    "airport":   AIRPORT,
    "direction": DIRECTION,
    "TimeFrom":  TIME_FROM,
    "TimeTo":    TIME_TO,
    "codeshare": "Y",
}

print(f"\n→ GET {BASE_URL}")
print(f"  params: {params}\n")

resp = requests.get(BASE_URL, params=params, timeout=20)

for h in ("Content-Type", "X-RateLimit-Limit", "X-RateLimit-Remaining"):
    if h in resp.headers:
        print(f"{h}: {resp.headers[h]}")

if resp.status_code != 200:
    print(f"HTTP {resp.status_code}\n{resp.text[:800]}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Parsing XML — usiamo resp.content per rispettare l'encoding dichiarato
# (iso-8859-1 nell'header HTTP, ma ElementTree legge quello nell'XML header)
# ---------------------------------------------------------------------------
root = ET.fromstring(resp.content)

def strip_ns(tag):
    return tag.split('}')[-1] if '}' in tag else tag

all_flights = [el for el in root.iter() if strip_ns(el.tag) == 'flight']
print(f"Voli grezzi (fascia ±{TIME_FROM}h): {len(all_flights)}")

# ---------------------------------------------------------------------------
# Filtro 1: finestra 10:00–14:00 Oslo
# schedule_time è un tag figlio con testo in formato UTC ISO-8601
# ---------------------------------------------------------------------------
def in_window(f):
    sdt_osl = to_osl(parse_dt(txt(f, "schedule_time")))
    return sdt_osl is not None and window_from <= sdt_osl <= window_to

window_flights = [f for f in all_flights if in_window(f)]
print(f"Nella fascia 10:00–14:00 Oslo: {len(window_flights)}")

# ---------------------------------------------------------------------------
# Filtro 2: solo voli operativi
# Avinor restituisce già solo il volo operante — nessuna deduplicazione
# necessaria. La conferma: "airline" = OperatingAirlineIata per spec.
# ---------------------------------------------------------------------------
operational = window_flights
print(f"Voli operativi: {len(operational)}\n")

# ---------------------------------------------------------------------------
# Calcolo ritardo
# <status code="E" time="EOBT"> → stima, code="D" time="AOBT" → effettivo
# "delayed" non è un tag in questo feed — lo deriviamo dai timestamp
# ---------------------------------------------------------------------------
def get_status(f):
    """Ritorna (code, time_str, delay_min)."""
    sel = f.find("status")
    if sel is None:
        return "", "", None
    code     = sel.get("code", "")
    time_str = sel.get("time", "")
    sched    = parse_dt(txt(f, "schedule_time"))
    actual   = parse_dt(time_str) if time_str else None
    delay    = None
    if sched and actual and code in ("E", "D"):
        delay = round((actual - sched).total_seconds() / 60)
    return code, time_str, delay

# ---------------------------------------------------------------------------
# Output tabellare
# ---------------------------------------------------------------------------
print(f"{'Volo':<10} {'AL':>2}  {'Sched':>5}  {'Dest':>4}  {'Via':>4}  "
      f"{'Gate':>4}  {'Chk':>3}  {'DI':>2}  {'Status':<18}  {'Delay':>6}")
print("─" * 82)

for f in operational:
    flt_id  = txt(f, "flight_id")
    airline = txt(f, "airline")
    sdt_osl = to_osl(parse_dt(txt(f, "schedule_time")))
    sched   = sdt_osl.strftime("%H:%M") if sdt_osl else "-"
    dest    = txt(f, "airport")
    via     = txt(f, "via_airport") or "-"
    gate    = txt(f, "gate") or "-"
    checkin = txt(f, "check_in") or "-"
    dom_int = txt(f, "dom_int")
    code, _, delay = get_status(f)
    status  = STATUS_LABELS.get(code, code or "Scheduled")
    delay_s = f"{delay:+d}m" if delay is not None else "-"

    print(f"{flt_id:<10} {airline:>2}  {sched:>5}  {dest:>4}  {via:>4}  "
          f"{gate:>4}  {checkin:>3}  {dom_int:>2}  {status:<18}  {delay_s:>6}")

# ---------------------------------------------------------------------------
# Dump del primo volo operativo
# ---------------------------------------------------------------------------
if operational:
    print("\n=== CAMPI del primo volo operativo ===")
    print(f"  uniqueID : {operational[0].get('uniqueID')}")
    for child in operational[0]:
        tag = strip_ns(child.tag)
        if child.attrib:
            print(f"  {tag:<15} text={child.text!r}  attribs={child.attrib}")
        else:
            print(f"  {tag:<15} = {child.text!r}")
