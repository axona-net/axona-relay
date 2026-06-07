// geolocate.js — best-effort auto-detection of the relay's location, used only
// when RELAY_REGION=auto (opt-in; the default makes no network call).
//
// Layered, decreasing accuracy:
//   1. IP geolocation — one short HTTPS GET to a public geo API → lat/lng
//      (city-level; far finer than the ~2000km region cells). Discloses our IP
//      to that service. Tries providers in order; each with a hard timeout.
//   2. Timezone — Intl resolved timezone mapped to a representative coordinate.
//      Fully local, no network, no deps. Coarser but usually lands the cell.
//   3. (caller falls back to the configured default if both return null.)

const GEO_PROVIDERS = [
  { url: 'https://ipinfo.io/json',
    parse: (j) => { const [la, lo] = String(j.loc || '').split(','); return { lat: +la, lng: +lo, place: j.city }; } },
  { url: 'https://ipapi.co/json/',
    parse: (j) => ({ lat: +j.latitude, lng: +j.longitude, place: j.city }) },
  { url: 'http://ip-api.com/json/',
    parse: (j) => (j.status === 'success' ? { lat: +j.lat, lng: +j.lon, place: j.city } : {}) },
];

async function ipGeo({ timeoutMs = 4000, log = () => {} } = {}) {
  for (const p of GEO_PROVIDERS) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), timeoutMs);
      const res = await fetch(p.url, { signal: ctrl.signal, headers: { accept: 'application/json' } });
      clearTimeout(t);
      if (!res.ok) { log(`ip-geo ${p.url} → HTTP ${res.status}`); continue; }
      const { lat, lng, place } = p.parse(await res.json()) || {};
      if (Number.isFinite(lat) && Number.isFinite(lng)) return { lat, lng, place };
    } catch (e) { log(`ip-geo ${p.url} failed: ${e.message}`); }
  }
  return null;
}

// Representative coordinates for common IANA timezones.
const TZ_COORDS = {
  'America/New_York': [40.7, -74.0], 'America/Chicago': [41.9, -87.6],
  'America/Denver': [39.7, -105.0], 'America/Los_Angeles': [34.0, -118.2],
  'America/Toronto': [43.7, -79.4], 'America/Vancouver': [49.3, -123.1],
  'America/Mexico_City': [19.4, -99.1], 'America/Sao_Paulo': [-23.5, -46.6],
  'America/Bogota': [4.6, -74.1], 'America/Argentina/Buenos_Aires': [-34.6, -58.4],
  'America/Anchorage': [61.2, -149.9], 'America/Halifax': [44.6, -63.6],
  'Europe/London': [51.5, -0.1], 'Europe/Dublin': [53.3, -6.3],
  'Europe/Paris': [48.9, 2.3], 'Europe/Berlin': [52.5, 13.4],
  'Europe/Madrid': [40.4, -3.7], 'Europe/Rome': [41.9, 12.5],
  'Europe/Amsterdam': [52.4, 4.9], 'Europe/Stockholm': [59.3, 18.1],
  'Europe/Warsaw': [52.2, 21.0], 'Europe/Moscow': [55.8, 37.6],
  'Europe/Istanbul': [41.0, 28.9], 'Europe/Athens': [38.0, 23.7],
  'Africa/Cairo': [30.0, 31.2], 'Africa/Lagos': [6.5, 3.4],
  'Africa/Johannesburg': [-26.2, 28.0], 'Africa/Nairobi': [-1.3, 36.8],
  'Asia/Dubai': [25.2, 55.3], 'Asia/Karachi': [24.9, 67.0],
  'Asia/Kolkata': [22.0, 78.0], 'Asia/Shanghai': [31.2, 121.5],
  'Asia/Tokyo': [35.7, 139.7], 'Asia/Seoul': [37.6, 127.0],
  'Asia/Singapore': [1.3, 103.8], 'Asia/Bangkok': [13.8, 100.5],
  'Asia/Jakarta': [-6.2, 106.8], 'Asia/Hong_Kong': [22.3, 114.2],
  'Asia/Tehran': [35.7, 51.4], 'Asia/Jerusalem': [31.8, 35.2],
  'Australia/Sydney': [-33.9, 151.2], 'Australia/Perth': [-31.95, 115.9],
  'Pacific/Auckland': [-36.8, 174.8], 'Pacific/Honolulu': [21.3, -157.8],
};
// Continent-level fallback for any zone not in the table.
const CONTINENT_COORDS = {
  America: [39, -98], Europe: [50, 10], Asia: [34, 100], Africa: [2, 20],
  Australia: [-25, 134], Pacific: [0, -160], Indian: [-20, 80],
  Atlantic: [35, -30], Antarctica: [-75, 0],
};

function timezoneRegion() {
  let tz = '';
  try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || ''; } catch { /* */ }
  if (TZ_COORDS[tz]) return { lat: TZ_COORDS[tz][0], lng: TZ_COORDS[tz][1], tz };
  const cont = CONTINENT_COORDS[tz.split('/')[0]];
  if (cont) return { lat: cont[0], lng: cont[1], tz };
  return null;
}

/**
 * Auto-detect the relay's location. Returns { lat, lng, source, detail } or
 * null if both IP-geo and timezone are unavailable (caller uses its default).
 *   source: 'ip-geo' | 'timezone'
 */
export async function autoDetectRegion({ log = () => {} } = {}) {
  const ip = await ipGeo({ log });
  if (ip) return { lat: ip.lat, lng: ip.lng, source: 'ip-geo', detail: ip.place || null };
  const tz = timezoneRegion();
  if (tz) return { lat: tz.lat, lng: tz.lng, source: 'timezone', detail: tz.tz };
  return null;
}
