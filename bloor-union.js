import unzipper from "unzipper";

const GTFS_URL =
  process.env.GTFS_URL ||
  "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip";

// In-memory cache (survives warm invocations)
let cache = {
  fetchedAt: 0,
  ttlMs: 6 * 60 * 60 * 1000, // 6 hours
  stops: null,              // { BLOOR_ID, UNION_ID }
  todayServices: null,      // Set(service_id)
  trips: null,              // Map(trip_id -> { service_id, trip_headsign, route_id })
  routes: null,             // Map(route_id -> { short, long })
  stopTimesIndex: null      // Map(trip_id -> { bloor:{dep,seq}, union:{arr,seq} })
};

function torontoNow() {
  // Simple TZ handling: rely on Intl with America/Toronto
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Toronto",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false
  }).formatToParts(new Date());

  const get = (t) => parts.find(p => p.type === t)?.value;
  const yyyy = get("year");
  const mm = get("month");
  const dd = get("day");
  const HH = get("hour");
  const MM = get("minute");
  const SS = get("second");

  return {
    yyyymmdd: `${yyyy}${mm}${dd}`,
    hhmm: `${HH}:${MM}`,
    nowSec: (Number(HH) * 3600) + (Number(MM) * 60) + Number(SS),
    // weekdayIndex0Mon: 0..6 where 0=Mon
    weekdayIndex0Mon: ((new Date(new Date().toLocaleString("en-US", { timeZone: "America/Toronto" })).getDay() + 6) % 7)
  };
}

function timeToSeconds(hhmmss) {
  const [h, m, s] = hhmmss.split(":").map(Number);
  return h * 3600 + m * 60 + s;
}

// Very small CSV parser that handles commas + quotes adequately for GTFS
function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const header = splitCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (cols.length !== header.length) continue;
    const obj = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = cols[j];
    rows.push(obj);
  }
  return rows;
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      // handle escaped quotes ""
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (ch === "," && !inQ) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

async function downloadAndReadZip() {
  const res = await fetch(GTFS_URL);
  if (!res.ok) throw new Error(`GTFS download failed: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const directory = await unzipper.Open.buffer(buf);
  const getFileText = async (name) => {
    const f = directory.files.find(x => x.path === name);
    if (!f) return null;
    const b = await f.buffer();
    return b.toString("utf8");
  };
  return { getFileText };
}

function buildTodayServices({ calendarTxt, calendarDatesTxt, yyyymmdd, weekdayIndex0Mon }) {
  const today = new Set();

  const cal = calendarTxt ? parseCsv(calendarTxt) : [];
  const cd = calendarDatesTxt ? parseCsv(calendarDatesTxt) : [];

  // calendar base
  for (const r of cal) {
    if (r.start_date > yyyymmdd || r.end_date < yyyymmdd) continue;
    const flags = [r.monday, r.tuesday, r.wednesday, r.thursday, r.friday, r.saturday, r.sunday].map(Number);
    if (flags[weekdayIndex0Mon] === 1) today.add(r.service_id);
  }

  // calendar_dates overrides (exception_type 1=add, 2=remove)
  for (const r of cd) {
    if (r.date !== yyyymmdd) continue;
    const ex = Number(r.exception_type);
    if (ex === 1) today.add(r.service_id);
    if (ex === 2) today.delete(r.service_id);
  }

  return today;
}

async function refreshCacheIfNeeded() {
  const now = Date.now();
  if (cache.stops && (now - cache.fetchedAt) < cache.ttlMs) return;

  const { yyyymmdd, weekdayIndex0Mon } = torontoNow();
  const zip = await downloadAndReadZip();

  const [
    stopsTxt,
    routesTxt,
    tripsTxt,
    stopTimesTxt,
    calendarTxt,
    calendarDatesTxt
  ] = await Promise.all([
    zip.getFileText("stops.txt"),
    zip.getFileText("routes.txt"),
    zip.getFileText("trips.txt"),
    zip.getFileText("stop_times.txt"),
    zip.getFileText("calendar.txt"),
    zip.getFileText("calendar_dates.txt")
  ]);

  if (!stopsTxt || !tripsTxt || !stopTimesTxt) {
    throw new Error("Missing required GTFS files in ZIP (stops/trips/stop_times).");
  }

  // Find stop_ids
  const stops = parseCsv(stopsTxt);
  const bloor = stops.find(s => (s.stop_name || "").toLowerCase().includes("bloor"));
  const union = stops.find(s => (s.stop_name || "").toLowerCase().includes("union station"));

  if (!bloor?.stop_id || !union?.stop_id) {
    throw new Error("Could not find stop_id for Bloor or Union Station in GTFS stops.txt");
  }

  // Routes map
  const routes = new Map();
  if (routesTxt) {
    for (const r of parseCsv(routesTxt)) {
      routes.set(r.route_id, { short: r.route_short_name, long: r.route_long_name });
    }
  }

  // Trips map
  const trips = new Map();
  for (const t of parseCsv(tripsTxt)) {
    trips.set(t.trip_id, {
      service_id: t.service_id,
      trip_headsign: t.trip_headsign,
      route_id: t.route_id
    });
  }

  // Today services
  const todayServices = buildTodayServices({
    calendarTxt,
    calendarDatesTxt,
    yyyymmdd,
    weekdayIndex0Mon
  });

  // Stop-times index (ONLY for the two stops, to avoid huge memory)
  // Map trip_id -> { bloor:{dep,seq}, union:{arr,seq} }
  const idx = new Map();

  // Stream-ish parse: iterate lines, avoid building full objects
  const lines = stopTimesTxt.split(/\r?\n/);
  const header = splitCsvLine(lines[0]);
  const col = (name) => header.indexOf(name);
  const iTrip = col("trip_id");
  const iArr = col("arrival_time");
  const iDep = col("departure_time");
  const iStop = col("stop_id");
  const iSeq = col("stop_sequence");

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = splitCsvLine(line);
    const stop_id = cols[iStop];
    if (stop_id !== bloor.stop_id && stop_id !== union.stop_id) continue;

    const trip_id = cols[iTrip];
    const seq = Number(cols[iSeq]);
    let entry = idx.get(trip_id);
    if (!entry) { entry = {}; idx.set(trip_id, entry); }

    if (stop_id === bloor.stop_id) entry.bloor = { dep: cols[iDep], seq };
    if (stop_id === union.stop_id) entry.union = { arr: cols[iArr], seq };
  }

  cache = {
    ...cache,
    fetchedAt: Date.now(),
    stops: { BLOOR_ID: bloor.stop_id, UNION_ID: union.stop_id },
    todayServices,
    trips,
    routes,
    stopTimesIndex: idx
  };
}

export default async function handler(req, res) {
  try {
    await refreshCacheIfNeeded();
    const { nowSec, hhmm } = torontoNow();

    const results = [];

    for (const [trip_id, st] of cache.stopTimesIndex.entries()) {
      if (!st.bloor || !st.union) continue;
      // Bloor -> Union only
      if (!(st.bloor.seq < st.union.seq)) continue;

      const trip = cache.trips.get(trip_id);
      if (!trip) continue;
      if (!cache.todayServices.has(trip.service_id)) continue;

      const depSec = timeToSeconds(st.bloor.dep);
      if (depSec < nowSec) continue;

      const route = cache.routes.get(trip.route_id) || {};
      const lineName = route.short || route.long || "GO";

      results.push({
        dep: st.bloor.dep.slice(0, 5),
        arr: st.union.arr.slice(0, 5),
        line: lineName,
        headsign: trip.trip_headsign || "Union Station",
        depSec
      });
    }

    results.sort((a, b) => a.depSec - b.depSec);

    const top3 = results.slice(0, 3).map(({ dep, arr, line, headsign }) => ({
      dep, arr, line, headsign
    }));

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    // Cache hint (TRMNL can still poll often; this reduces Vercel load)
    res.setHeader("Cache-Control", "s-maxage=60, stale-while-revalidate=300");

    res.status(200).send({
      title: "GO Train — Bloor → Union",
      updated: hhmm,
      departures: top3
    });
  } catch (e) {
    res.status(500).json({
      title: "GO Train — Bloor → Union",
      updated: null,
      departures: [],
      error: String(e?.message || e)
    });
  }
}
