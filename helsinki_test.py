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


"""
ESEMPIO DI OUTPUT

Finestra: 15:00–17:00 Helsinki  (2026-04-30)

→ GET https://apigw.finavia.fi/flights/public/v0/flights/dep/HEL

Content-Type: application/xml
Voli grezzi da HEL (tutti gli orari): 304
Nella fascia 10:00–14:00: 51
Operativi: 51   Codeshare filtrati: 0

Volo       Sched  Dest  Destinazione            Gate  Status           Delay  Board  Final
────────────────────────────────────────────────────────────────────────────────────────────
FI343      15:00   KEF  Reykjavik                 18  -                    -    -      -  
AY1023     15:15   TLL  Tallinn                   11  -                    -    -      -  
AY315      15:30   VAA  Vaasa                     10  -                    -    -      -  
AY1763     15:35   FCO  Rome                      17  -                    -    -      -  
AY959      15:35   CPH  Copenhagen                19  -                    -    -      -  
AY1847     15:35   TIA  Tirana                   51B  -                    -    -      -  
AY593      15:40   KTT  Kittilä                    9  -                    -    -      -  
BT304      15:40   RIX  Riga                      15  -                    -    -      -  
D82802     15:45   NCE  Nice                     31E  -                    -    -      -  
PC1312     15:45   SAW  Istanbul Sabiha Gökçen    49  -                    -    -      -  
AY1863     15:45   RHO  Rhodos                    28  -                    -    -      -  
AY1019     15:45   TLL  Tallinn                  31C  -                    -    -      -  
FR2345     15:50   STN  London Stansted           47  -                    -    -      -  
AY1859     15:50   CHQ  Chania                   31D  -                    -    -      -  
AY441      15:55   OUL  Oulu                      26  -                    -    -      -  
AY1603     15:55   NCE  Nice                      30  -                    -    -      -  
AY813      15:55   ARN  Stockholm Arlanda         24  -                    -    -      -  
AY535      16:00   RVN  Rovaniemi                23A  -                    -    -      -  
AY1337     16:00   LHR  London Heathrow           42  -                    -    -      -  
SK1713     16:00   CPH  Copenhagen                14  -                    -    -      -  
AY1365     16:05   MAN  Manchester               51D  -                    -    -      -  
AY001      16:05   LAX  Los Angeles               44  -                    -    -      -  
D82894     16:05   PMI  Palma de Mallorca         16  -                    -    -      -  
AY1533     16:05   GVA  Geneva                    34  -                    -    -      -  
AY1075     16:05   RIX  Riga                     20A  -                    -    -      -  
AY915      16:10   OSL  Oslo                     34A  -                    -    -      -  
AY1577     16:10   CDG  Paris Charles de Gaull    29  -                    -    -      -  
AY369      16:15   KUO  Kuopio                   31C  -                    -    -      -  
AY1405     16:15   MUC  Munich                   31E  -                    -    -      -  
AY1105     16:15   VNO  Vilnius                  31B  -                    -    -      -  
AY865      16:15   GOT  Gothenburg                36  -                    -    -      -  
AY1803     16:20   VRN  Verona                    18  -                    -    -      -  
AY1255     16:20   BUD  Budapest                 23B  -                    -    -      -  
AY1435     16:20   BER  Berlin                    25  -                    -    -      -  
AY7055     16:20   LIN  Milan Linate              20  -                    -    -      -  
AY1021     16:25   TLL  Tallinn                   11  -                    -    -      -  
AY009      16:25   ORD  Chicago                   54  -                    -    -      -  
SK711      16:30   ARN  Stockholm Arlanda        19A  -                    -    -      -  
AY1783     16:30   VCE  Venice                    21  -                    -    -      -  
AY1425     16:30   HAM  Hamburg                  31D  -                    -    -      -  
AY1513     16:30   ZRH  Zurich                   36A  -                    -    -      -  
AY1395     16:35   DUS  Dusseldorf               23A  -                    -    -      -  
AY1305     16:40   AMS  Amsterdam                 35  -                    -    -      -  
AY1661     16:40   MAD  Madrid                    13  -                    -    -      -  
HO1608     16:45   PVG  Shanghai                  43  -                    -    -      -  
RP655      16:45   SVL  Savonlinna               17A  -                    -    -      -  
AY1545     16:45   BRU  Brussels Zaventem        31E  -                    -    -      -  
AY443      16:50   OUL  Oulu                      23  -                    -    -      -  
AY815      16:55   ARN  Stockholm Arlanda         27  -                    -    -      -  
AY1475     16:55   VIE  Vienna                    22  -                    -    -      -  
AY1415     16:55   FRA  Frankfurt                 19  -                    -    -      -  

=== CAMPI del primo volo operativo ===
  h_apt          = 'HEL'
  fltnr          = 'FI343'
  sdt            = '2026-04-30T12:00:00Z'
  sdate          = '20260430'
  acreg          = 'TFICC'
  actype         = '7M9'
  mfltnr         = None
  cflight_1      = 'AY6817'
  cflight_2      = None
  cflight_3      = None
  cflight_4      = None
  cflight_5      = None
  cflight_6      = None
  route_1        = 'KEF'
  route_2        = None
  route_3        = None
  route_4        = None
  route_n_1      = 'Reykjavik'
  route_n_2      = None
  route_n_3      = None
  route_n_4      = None
  route_n_fi_1   = 'Reykjavik'
  route_n_fi_2   = None
  route_n_fi_3   = None
  route_n_fi_4   = None
  chkarea        = '300'
  chkdsk_1       = '306'
  chkdsk_2       = '307'
  calls_1        = None
  calls_2        = None
  calls_3        = None
  calls_4        = None
  park           = '18'
  park_prv       = None
  gate           = '18'
  gate_prv       = None
  prm            = None
  prt            = None
  prt_f          = None
  prt_s          = None
  est_d          = None
  pest_d         = None
  act_d          = None
  ablk_d         = None
  callsign       = 'ICE343'
"""