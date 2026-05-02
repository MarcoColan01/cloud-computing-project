const AIRPORT_META = {
    AMS: { full: "Amsterdam Schiphol", city: "Amsterdam", tz: "Europe/Amsterdam"},
    HEL: { full: "Helsinki-Vantaa", city: "Helsinki", tz: "Europe/Helsinki"},
    AMS: { full: "Oslo Gardermoen", city: "Oslo", tz: "Europe/Oslo"},
};

fetch("/snapshot")
    .then(r => r.json())
    .then(renderAll)
    .catch(err => console.error("snapshot fetch failed:", err));

const es = new EventSource("/stream");
es.onmessage = (evt) => {
    try{
        const payload = JSON.parse(evt.data);
        renderAll(payload);
    }catch (e){
        console.error("bad SSE payload", e);
    }
};

es.onerror = () => console.warn("SSE connection lost, will retry...");

function renderAll(payload){
    const airports = (payload && payload.airports) || {};
    Object.keys(AIRPORT_META).forEach(code => {
        const data = airports[code] || { flights: [], stats: {} };
        renderAirport(code, data);
    });
}

function renderAirport(code, data){
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
            tbody.innerHTML = `<tr class="empty"><td colspan="7">Awaiting data…</td></tr>`;
            return;
        }
        tbody.innerHTML = data.flights.map(f => renderFlightRow(f, meta.tz)).join("");
}

function renderFlightRow(f, tz) {
    const sched = formatTime(f.scheduled_departure, tz);
    const est = formatTime(f.estimated_departure || f.actual_departure || f.scheduled_departure, tz);
    const gate = f.gate || "—";
    const dest = (f.destination_iata || "").toUpperCase();
    const airlineLabel = f.airline_name || f.airline_iata || "—";
    const flight = f.flight_code || "—";
    const { label, cls } = statusBadge(f);

    // Highlight EST in amber when delayed, white when on time
    const estDelayed = f.delay_minutes != null && f.delay_minutes >= 5;
    const estClass = estDelayed ? "est late" : "est";

    return `
        <tr>
            <td class="airline">${escapeHtml(airlineLabel)}</td>
            <td class="flight">${escapeHtml(flight)}</td>
            <td class="dest">${escapeHtml(dest)}</td>
            <td class="time">${sched}</td>
            <td class="${estClass}">${est}</td>
            <td class="gate">${escapeHtml(gate)}</td>
            <td class="info ${cls}">${label}</td>
        </tr>
    `;
}

function statusBadge(f) {
    const s = f.status || "UNKNOWN";
    // Map server-side status to compact UI label + CSS class
    switch (s) {
        case "BOARDING":     return { label: "BOARDING",  cls: "boarding" };
        case "LAST_CALL":    return { label: "LAST CALL", cls: "boarding" };
        case "GATE_OPEN":    return { label: "GATE OPEN", cls: "boarding" };
        case "GATE_CLOSED":  return { label: "GATE CLSD", cls: "late" };
        case "DELAYED":      return { label: "DELAYED",   cls: "late" };
        case "CANCELLED":    return { label: "CANCELLED", cls: "cancel" };
        case "DIVERTED":     return { label: "DIVERTED",  cls: "late" };
        case "DEPARTED":     return { label: "DEPARTED",  cls: "departed" };
        default:
            if (f.delay_minutes != null && f.delay_minutes >= 5) {
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

    // Per-board clocks: each shows local time of that airport
    Object.keys(AIRPORT_META).forEach(code => {
        const t = now.toLocaleTimeString("en-GB", {
            timeZone: AIRPORT_META[code].tz, hour12: false
        });
        setText(`time-${code}`, t);
    });
}
tickClock();
setInterval(tickClock, 1000);