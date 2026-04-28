#!/usr/bin/env python3
"""
Finavia Public Flights API — Helsinki (HEL) departures.

Scarica tutte le partenze da HEL oggi, filtra 10:00-14:00 ora Helsinki,
rimuove i codeshare mostrando solo i voli operativi.

  export FINAVIA_APP_KEY="la-tua-subscription-key"
  pip install requests
  python finavia_departures_test.py
"""

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date

import requests

# ---------------------------------------------------------------------------
APP_KEY = "cfd46048700a44cd99e1eb87a3c9fd4f"
if not APP_KEY:
    sys.exit("ERRORE: esporta FINAVIA_APP_KEY")

BASE_URL    = "https://apigw.finavia.fi/flights/public/v0"
AIRPORT     = "HEL"        # path param — senza questo ritorna TUTTI gli aeroporti Finavia
FLIGHT_TYPE = "dep"        # dep | arr | all
WINDOW_FROM = (15, 0)
WINDOW_TO   = (17, 0)

# Namespace dichiarato nell'XML di risposta
NS = "http://www.finavia.fi/FlightsService.xsd"

# ---------------------------------------------------------------------------
# Fuso orario Helsinki
# ---------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo
    TZ_HEL = ZoneInfo("Europe/Helsinki")
except ImportError:
    try:
        import pytz
        TZ_HEL = pytz.timezone("Europe/Helsinki")
    except ImportError:
        TZ_HEL = timezone(timedelta(hours=3))

def make_local(h, m):
    d = date.today()
    dt = datetime(d.year, d.month, d.day, h, m)
    if hasattr(TZ_HEL, 'localize'):
        return TZ_HEL.localize(dt)
    return dt.replace(tzinfo=TZ_HEL)

window_from = make_local(*WINDOW_FROM)
window_to   = make_local(*WINDOW_TO)
print(f"Finestra: {window_from.strftime('%H:%M')}–{window_to.strftime('%H:%M')} Helsinki  ({date.today()})")

# ---------------------------------------------------------------------------
# Chiamata API — airport è PATH PARAM, non query param
# /flights/dep        → tutti gli aeroporti Finavia (21 aeroporti finlandesi)
# /flights/dep/HEL    → solo Helsinki ← quello che vogliamo
# ---------------------------------------------------------------------------
url = f"{BASE_URL}/flights/{FLIGHT_TYPE}/{AIRPORT}"
headers = {"app_key": APP_KEY, "Accept": "application/xml"}

print(f"\n→ GET {url}\n")
resp = requests.get(url, headers=headers, timeout=20)

for h in ("Content-Type", "X-RateLimit-Limit", "X-RateLimit-Remaining"):
    if h in resp.headers:
        print(f"{h}: {resp.headers[h]}")

if resp.status_code != 200:
    print(f"HTTP {resp.status_code}\n{resp.text[:800]}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Namespace handling
# La risposta ha xmlns="http://www.finavia.fi/FlightsService.xsd"
# ElementTree antepone {NS} a ogni tag → usiamo il prefisso esplicitamente
# oppure cerchiamo con la sintassi {NS}tag.
# ---------------------------------------------------------------------------
root = ET.fromstring(resp.text)

def ns(tag):
    """Costruisce il tag qualificato con namespace."""
    return f"{{{NS}}}{tag}"

dep_el   = root.find(ns("dep"))
body_el  = dep_el.find(ns("body")) if dep_el is not None else None

if body_el is None:
    # Diagnostica: mostra i tag figli effettivi così si capisce la struttura
    print("Struttura XML ricevuta (tag di primo livello):")
    for child in root:
        print(f"  {child.tag}")
        for sub in child:
            print(f"    {sub.tag}")
            break
    sys.exit(1)

raw_flights = body_el.findall(ns("flight"))
print(f"Voli grezzi da HEL (tutti gli orari): {len(raw_flights)}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get(el, tag):
    """Testo del tag figlio, tenendo conto del namespace. '' se assente/vuoto."""
    child = el.find(ns(tag))
    return child.text.strip() if child is not None and child.text else ""

def parse_dt(s):
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def to_hel(dt):
    return dt.astimezone(TZ_HEL) if dt else None

# ---------------------------------------------------------------------------
# Filtro 1: finestra 10:00–14:00 Helsinki
# sdt è UTC → convertiamo prima di confrontare
# ---------------------------------------------------------------------------
def in_window(f):
    sdt_loc = to_hel(parse_dt(get(f, "sdt")))
    return sdt_loc is not None and window_from <= sdt_loc <= window_to

window_flights = [f for f in raw_flights if in_window(f)]
print(f"Nella fascia 10:00–14:00: {len(window_flights)}")

# ---------------------------------------------------------------------------
# Filtro 2: solo voli operativi (escludi codeshare)
# mfltnr vuoto  → questo fltnr È il master (volo operante)
# mfltnr pieno  → questo fltnr è un codeshare; il volo reale è mfltnr
# <mfltnr/> in XML → text=None → get() ritorna "" → is_operating=True ✓
# ---------------------------------------------------------------------------
def is_operating(f):
    return get(f, "mfltnr") == ""

operational = [f for f in window_flights if     is_operating(f)]
codeshared  = [f for f in window_flights if not is_operating(f)]
print(f"Operativi: {len(operational)}   Codeshare filtrati: {len(codeshared)}\n")

# ---------------------------------------------------------------------------
# Ritardo in minuti
# ---------------------------------------------------------------------------
def delay_min(f):
    sched  = parse_dt(get(f, "sdt"))
    actual = parse_dt(get(f, "act_d")) or parse_dt(get(f, "pest_d"))
    if sched and actual:
        return round((actual - sched).total_seconds() / 60)
    return None

def fmt_time(s):
    dt = to_hel(parse_dt(s))
    return dt.strftime("%H:%M") if dt else "  -  "

# ---------------------------------------------------------------------------
# Output tabellare
# ---------------------------------------------------------------------------
print(f"{'Volo':<10} {'Sched':>5}  {'Dest':>4}  {'Destinazione':<22}"
      f"  {'Gate':>4}  {'Status':<14}  {'Delay':>6}  {'Board':>5}  {'Final':>5}")
print("─" * 92)

for f in operational:
    fltnr   = get(f, "fltnr")
    sdt_loc = to_hel(parse_dt(get(f, "sdt")))
    sched   = sdt_loc.strftime("%H:%M") if sdt_loc else "-"
    dest    = get(f, "route_1")
    dest_n  = (get(f, "route_n_1") or dest)[:22]
    gate    = get(f, "gate") or "-"
    status  = get(f, "prt") or "-"
    delay   = delay_min(f)
    delay_s = f"{delay:+d}m" if delay is not None else "-"
    board   = fmt_time(get(f, "calls_2"))
    final   = fmt_time(get(f, "calls_3"))

    print(f"{fltnr:<10} {sched:>5}  {dest:>4}  {dest_n:<22}"
          f"  {gate:>4}  {status:<14}  {delay_s:>6}  {board:>5}  {final:>5}")

# ---------------------------------------------------------------------------
# Dump campi del primo volo operativo
# ---------------------------------------------------------------------------
if operational:
    print("\n=== CAMPI del primo volo operativo ===")
    for child in operational[0]:
        # Rimuovi il prefisso namespace per leggibilità
        tag = child.tag.replace(f"{{{NS}}}", "")
        print(f"  {tag:<14} = {child.text!r}")

if codeshared:
    print(f"\n=== CONFRONTO: primo codeshare filtrato (mfltnr = {get(codeshared[0], 'mfltnr')!r}) ===")
    for child in codeshared[0]:
        tag = child.tag.replace(f"{{{NS}}}", "")
        print(f"  {tag:<14} = {child.text!r}")
