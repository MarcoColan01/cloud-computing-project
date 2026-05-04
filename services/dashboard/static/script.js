const previousFlightState = {};
const flashTimers = {};
const flashingFlights = new Set();

const FLASH_DURATION_MS = 5000;
const WATCH_FIELDS = ["gate", "estimated_departure", "status"];

const AIRPORT_META = {
    AMS: { full: "Amsterdam Schiphol", city: "Amsterdam", tz: "Europe/Amsterdam" },
    HEL: { full: "Helsinki-Vantaa",    city: "Helsinki",  tz: "Europe/Helsinki"  },
    OSL: { full: "Oslo Gardermoen",    city: "Oslo",      tz: "Europe/Oslo"      }
};

fetch("/snapshot")
    .then(r => r.json())
    .then(payload => {
        // First paint: just record state, do not flash
        seedInitialState(payload);
        renderAll(payload);
    })
    .catch(err => console.error("snapshot fetch failed:", err));

const es = new EventSource("/stream");
let firstSseSkipped = false;
es.onmessage = (evt) => {
    try {
        const payload = JSON.parse(evt.data);
        // The SSE stream sends a full snapshot on connect; skip the first one
        // because /snapshot already seeded the state.
        if (!firstSseSkipped) {
            firstSseSkipped = true;
            return;
        }
        detectChanges(payload);
        renderAll(payload);
    } catch (e) {
        console.error("bad SSE payload", e);
    }
};
es.onerror = () => console.warn("SSE connection lost, will retry…");

function seedInitialState(payload) {
    const airports = (payload && payload.airports) || {};
    Object.values(airports).forEach(data => {
        (data.flights || []).forEach(f => {
            const fkey = makeKey(f);
            previousFlightState[fkey] = snapshotFields(f);
        });
    });
}

function detectChanges(payload) {
    const airports = (payload && payload.airports) || {};
    Object.values(airports).forEach(data => {
        (data.flights || []).forEach(f => {
            const fkey = makeKey(f);
            const prev = previousFlightState[fkey];
            if (!prev) {
                previousFlightState[fkey] = snapshotFields(f);
                return;
            }
            let changed = false;
            for (const field of WATCH_FIELDS) {
                if (prev[field] !== f[field]) {
                    changed = true;
                    break;
                }
            }
            if (changed) {
                triggerFlash(fkey);
                previousFlightState[fkey] = snapshotFields(f);
            }
        });
    });
}

function triggerFlash(fkey) {
    flashingFlights.add(fkey);
    if (flashTimers[fkey]) clearTimeout(flashTimers[fkey]);
    flashTimers[fkey] = setTimeout(() => {
        flashingFlights.delete(fkey);
        delete flashTimers[fkey];
        // Re-render to actually remove the emoji from the visible row
        renderAllFromCache();
    }, FLASH_DURATION_MS);
}

let lastPayload = null;
function renderAllFromCache() {
    if (lastPayload) renderAll(lastPayload);
}

function makeKey(f) {
    return `${f.airport}|${f.flight_code}|${f.scheduled_departure}`;
}

function snapshotFields(f) {
    const o = {};
    for (const field of WATCH_FIELDS) o[field] = f[field];
    return o;
}

function renderAll(payload) {
    lastPayload = payload;
    const airports = (payload && payload.airports) || {};
    Object.keys(AIRPORT_META).forEach(code => {
        const data = airports[code] || { flights: [], stats: {} };
        renderAirport(code, data);
    });
}

function renderAirport(code, data) {
    const meta = AIRPORT_META[code];

    setText(`full-${code}`, meta.full.toUpperCase());
    setText(`meta-city-${code}`, meta.city);

    const dot = document.getElementById(`dot-${code}`);
    const haveData = data.flights && data.flights.length > 0;
    if (dot) dot.className = "status-dot " + (haveData ? "ok" : "wait");

    const stats = data.stats || {};
    setText(`stat-departed-${code}`, stats.departed_today != null
        ? String(stats.departed_today).padStart(4, "0")
        : "—");
    setText(`stat-delay-${code}`, stats.avg_delay_minutes != null
        ? String(Math.round(stats.avg_delay_minutes))
        : "—");

    const tbody = document.getElementById(`flights-${code}`);
    if (!tbody) return;
    if (!haveData) {
        tbody.innerHTML = `<tr class="empty"><td colspan="8">Awaiting data…</td></tr>`;
        return;
    }
    tbody.innerHTML = data.flights.map(f => renderFlightRow(f, meta.tz)).join("");
}

function renderFlightRow(f, tz) {
    const sched = formatTime(f.scheduled_departure, tz);
    const estIso = f.estimated_departure || f.actual_departure || f.scheduled_departure;
    const est = formatTime(estIso, tz);
    const gate = f.gate || "—";

    const dest = f.destination_city || f.destination_iata || "—";
    const airlineLabel = f.airline_name_full || f.airline_iata || "—";
    const flight = f.flight_code || "—";
    const { label, cls } = statusBadge(f);

    const delta = computeDelta(f);
    const deltaHtml = renderDelta(delta);
    const estClass = (delta != null && delta >= 1) ? "est late" : "est";

    const fkey = makeKey(f);
    const warningCell = flashingFlights.has(fkey)
        ? `<td class="warn-cell"><span class="warn-flash">CHANGES!!!</span></td>`
        : `<td class="warn-cell"></td>`;

    return `
        <tr>
            <td class="airline" title="${escapeHtml(airlineLabel)}">${escapeHtml(airlineLabel)}</td>
            <td class="flight">${escapeHtml(flight)}</td>
            <td class="dest" title="${escapeHtml(f.destination_name_full || dest)}">${escapeHtml(dest)}</td>
            <td class="time">${sched}</td>
            <td class="${estClass}">${est}${deltaHtml}</td>
            <td class="gate">${escapeHtml(gate)}</td>
            <td class="info ${cls}">${label}</td>
            ${warningCell}
        </tr>
    `;
}

function computeDelta(f) {
    if (typeof f.delay_minutes === "number") {
        return f.delay_minutes;
    }
    if (!f.scheduled_departure) return null;
    const estIso = f.estimated_departure || f.actual_departure;
    if (!estIso) return null;
    try {
        const sched = new Date(f.scheduled_departure).getTime();
        const est = new Date(estIso).getTime();
        return Math.round((est - sched) / 60000);
    } catch {
        return null;
    }
}

function renderDelta(delta) {
    if (delta == null || delta === 0) return "";
    const cls = delta > 0 ? "delta-late" : "delta-early";
    const sign = delta > 0 ? "+" : "−";
    return ` <span class="${cls}">(${sign}${Math.abs(delta)})</span>`;
}

function statusBadge(f) {
    const s = f.status || "UNKNOWN";
    switch (s) {
        case "BOARDING":     return { label: "BOARDING",   cls: "boarding boarding-flash" };
        case "LAST_CALL":    return { label: "LAST CALL",  cls: "boarding boarding-flash" };
        case "GATE_OPEN":    return { label: "GATE OPEN",  cls: "boarding" };
        case "GATE_CLOSED":  return { label: "GATE CLOSE", cls: "gate-closed" };
        case "DELAYED":      return { label: "DELAYED",    cls: "late" };
        case "CANCELLED":    return { label: "CANCELLED",  cls: "cancel" };
        case "DIVERTED":     return { label: "DIVERTED",   cls: "late" };
        case "DEPARTED":     return { label: "DEPARTED",   cls: "departed" };
        default:
            if (typeof f.delay_minutes === "number" && f.delay_minutes >= 5) {
                return { label: "DELAYED", cls: "late" };
            }
            return { label: "ON TIME", cls: "ontime" };
    }
}

function formatTime(iso, tz) {
    if (!iso) return "—";
    try {
        const d = new Date(iso);
        return d.toLocaleTimeString("en-GB", {
            timeZone: tz, hour: "2-digit", minute: "2-digit", hour12: false
        });
    } catch {
        return "—";
    }
}

function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, ch => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[ch]));
}

function tickClock() {
    const now = new Date();
    const date = now.toLocaleDateString("en-GB", {
        timeZone: "UTC", weekday: "long", month: "short", day: "2-digit", year: "numeric"
    }).toUpperCase();
    const time = now.toLocaleTimeString("en-GB", {
        timeZone: "UTC", hour12: false
    }) + " UTC";
    setText("clock-date", date);
    setText("clock-time", time);

    Object.keys(AIRPORT_META).forEach(code => {
        const t = now.toLocaleTimeString("en-GB", {
            timeZone: AIRPORT_META[code].tz, hour12: false
        });
        setText(`time-${code}`, t);
    });
}
tickClock();
setInterval(tickClock, 1000);