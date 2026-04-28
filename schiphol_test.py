#!/usr/bin/env python3
"""
Schiphol Public Flight API — test script.

Scarica le PARTENZE da Schiphol (AMS) di OGGI tra le 10:00 e le 14:00 (ora locale).

Prerequisiti:
  1. Registrarsi su https://developer.schiphol.nl/signup e creare una Application
     per ottenere app_id e app_key.
  2. Esportare le credenziali come variabili d'ambiente:
        export SCHIPHOL_APP_ID="..."
        export SCHIPHOL_APP_KEY="..."
  3. pip install requests
  4. python schiphol_departures_test.py
"""

import json
import os
import sys
from datetime import date

import requests

API_URL = "https://api.schiphol.nl/public-flights/flights"
RESOURCE_VERSION = "v4"   # versione attualmente pubblica (Public Flight V4 API)

# --- Credenziali ------------------------------------------------------------
APP_ID = "16a1d0e9"
APP_KEY = "6e909dec5307824655e44fa75007f938"
if not APP_ID or not APP_KEY:
    sys.exit("ERRORE: esporta le variabili SCHIPHOL_APP_ID e SCHIPHOL_APP_KEY")

# --- Query ------------------------------------------------------------------
today = date.today().isoformat()   # es. "2026-04-22"

headers = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "ResourceVersion": RESOURCE_VERSION,
    "Accept": "application/json",
}

params = {
    "flightDirection": "D",                       # D = Departures, A = Arrivals
    "fromDateTime": f"{today}T10:00:00",
    "toDateTime":   f"{today}T10:30:00",
    "searchDateTimeField": "scheduleDateTime",    # filtra sullo scheduled time
    "sort": "+scheduleTime",                      # ordina dal più vicino al più lontano
    "includedelays": "true",
    "page": 0,
}

# --- Chiamata con paginazione (il server risponde con header Link: next) ----
all_flights = []
url = API_URL
first_request = True

while url:
    resp = requests.get(
        url,
        headers=headers,
        params=params if first_request else None,   # sulle pagine successive non serve
        timeout=15,
    )
    first_request = False

    # Alcune info utili su quota/ratelimit, se il server le espone
    for h in ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"):
        if h in resp.headers:
            print(f"{h}: {resp.headers[h]}")

    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} — {resp.text[:400]}")
        break

    page_flights = resp.json().get("flights", [])
    all_flights.extend(page_flights)
    print(f"Pagina scaricata: {len(page_flights)} voli (totale finora: {len(all_flights)})")

    # requests parsa automaticamente l'header Link in un dict
    next_link = resp.links.get("next")
    url = next_link["url"] if next_link else None

print(f"\n=== TOTALE: {len(all_flights)} partenze tra le 10:00 e le 14:00 di oggi ===\n")

# --- Vista rapida dei campi principali --------------------------------------
for f in all_flights[:10]:
    dest   = ",".join(f.get("route", {}).get("destinations", []))
    states = f.get("publicFlightState", {}).get("flightStates", [])
    print(
        f"{f.get('flightName',''):8} | "
        f"sched {f.get('scheduleTime','-'):5} -> {dest:4} | "
        f"gate={f.get('gate','-'):>4} | "
        f"term={f.get('terminal','-')!s:>4} | "
        f"state={states}"
    )

# --- Dump grezzo del primo volo per ispezione ------------------------------
if all_flights:
    print("\n=== JSON GREZZO del primo volo ===")
    print(json.dumps(all_flights[0], indent=2, ensure_ascii=False))

operational = [f for f in all_flights if f.get("flightName") == f.get("mainFlight")]
print(f"Voli operativi reali: {len(operational)}")
