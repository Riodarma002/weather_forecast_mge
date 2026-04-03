from http.server import BaseHTTPRequestHandler
import json
import math
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import requests
import os

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

PITS = {
    "north": {"lat": 1.157896, "lon": 117.674641, "name": "Pit Utara"},
    "south": {"lat": 1.033785, "lon": 117.675995, "name": "Pit Selatan"}
}

BMKG_API_BASE = "https://api.bmkg.go.id/publik/prakiraan-cuaca"

BMKG_PRIMARY = "64.08.16.2003"  # Pengadan — hardcoded, dikonfirmasi user

# Kandidat fallback — Dikurangi agar tidak timeout di Vercel (maks 4)
BMKG_CANDIDATE_ADM4 = [
    "64.08.16.2003",  # ✅ UTAMA: Pengadan
    "64.08.16.2001",  # Karangan
    "64.08.15.2001",  # Kecamatan terdekat
    "64.02.09.2001",  # Tenggarong Seberang (fallback terjauh)
]

MAX_BMKG_DISTANCE_KM = 50.0

# ─── HAVERSINE ────────────────────────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def degrees_to_compass(deg):
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[int((float(deg) + 11.25) / 22.5) % 16]

# ─── RISK CALCULATOR ──────────────────────────────────────────────────────────
def calculate_risk(tp, ws_kmh, vs_km, tcc, hour=12, tp_3h=0.0, tp_showers=0.0):
    ws_ms = ws_kmh / 3.6  # km/h → m/s
    night = (hour >= 22 or hour < 5)

    if tp > 10 or ws_ms > 11.1 or vs_km < 0.5:
        return "critical"
    if tp_3h > 20:
        return "critical"
    if tp > 5 and ws_ms > 6.9:
        return "critical"
    if tp_showers > 7:
        return "critical"

    if tp > 5 or ws_ms > 8.3 or vs_km < 1.0:
        return "high"
    if night and vs_km < 2.0:
        return "high"
    if tp_3h > 10:
        return "high"
    if tp_showers > 3:
        return "high"

    if tp > 2 or ws_ms > 5.6 or vs_km < 3.0 or tcc > 90:
        return "medium"
    if night and tcc > 80:
        return "medium"
    if tp_showers > 1:
        return "medium"

    return "low"

# ─── BMKG FETCHER ─────────────────────────────────────────────────────────────
def fetch_bmkg_nearest(pit_lat, pit_lon, cache_session):
    best_adm4     = None
    best_distance = float('inf')
    best_data     = None

    for adm4 in BMKG_CANDIDATE_ADM4:
        try:
            url = f"{BMKG_API_BASE}?adm4={adm4}"
            r = cache_session.get(url, timeout=3)  # Ketat 3 detik agar tidak timeout Vercel
            if r.status_code != 200:
                continue
            j = r.json()

            loc  = j.get("lokasi") or {}
            blat = loc.get("lat")
            blon = loc.get("lon")
            if blat is None and j.get("data"):
                loc2 = j["data"][0].get("lokasi", {})
                blat = loc2.get("lat")
                blon = loc2.get("lon")
            if blat is None:
                continue

            dist = haversine(pit_lat, pit_lon, float(blat), float(blon))

            if dist < best_distance:
                best_distance = dist
                best_adm4     = adm4
                best_data     = j

                if dist < 5.0: # Break early if found within 5km to save API calls
                    break

        except Exception as e:
            continue

    if best_distance > MAX_BMKG_DISTANCE_KM or not best_data:
        return {}, None, None

    loc = best_data.get("lokasi") or (best_data.get("data", [{}])[0].get("lokasi", {}))
    lookup = {}
    cuaca_data = best_data.get("data", [{}])[0].get("cuaca", [])

    for day_group in cuaca_data:
        if not isinstance(day_group, list):
            continue
        for item in day_group:
            local_dt = item.get("local_datetime", "")
            if not local_dt:
                continue
            try:
                dt  = datetime.strptime(local_dt, "%Y-%m-%d %H:%M:%S")
                key = dt.strftime("%Y-%m-%d %H")
            except Exception:
                continue

            vs_text = item.get("vs_text", "> 10 km")
            try:
                vs_km = float(vs_text.replace(">","").replace("<","").replace("km","").strip())
            except Exception:
                vs_km = 10.0

            lookup[key] = {
                "t_bmkg":          item.get("t"),
                "hu":              item.get("hu"),
                "tcc":             item.get("tcc"),
                "ws_bmkg":         item.get("ws"),
                "wd":              item.get("wd", "?"),
                "weather_desc":    item.get("weather_desc", ""),
                "weather_desc_en": item.get("weather_desc_en", ""),
                "weather_code_bmkg": int(item.get("weather", 0) or 0),
                "vs_km":           vs_km,
                "vs_text":         vs_text,
            }

    area_info = {
        "desa":       loc.get("desa", ""),
        "kecamatan":  loc.get("kecamatan", ""),
        "kotkab":     loc.get("kotkab", ""),
        "provinsi":   loc.get("provinsi", ""),
        "distance_km": round(best_distance, 1),
        "adm4":       best_adm4,
    }
    return lookup, area_info, best_adm4

# ─── OPEN-METEO FETCHER ───────────────────────────────────────────────────────
def fetch_openmeteo(lat, lon, om_client):
    params = {
        "latitude":  lat,
        "longitude": lon,
        "hourly": [
            "temperature_2m",       # 0  °C
            "precipitation",        # 1  mm/h  — TOTAL
            "wind_speed_10m",       # 2  km/h
            "visibility",           # 3  m
            "weather_code",         # 4  WMO
            "relative_humidity_2m", # 5  %
            "cloud_cover",          # 6  %
            "wind_direction_10m",   # 7  °
            "rain",                 # 8  mm/h  — stratiform
            "showers",              # 9  mm/h  — konvektif
        ],
        "timezone":     "Asia/Makassar",
        "forecast_days": 7
    }
    responses = om_client.weather_api(
        "https://api.open-meteo.com/v1/forecast", params=params
    )
    return responses[0]

# ─── MAIN ENGINE RUNNER ───────────────────────────────────────────────────────
def generate_weather_data():
    # Gunakan memory backend agar kompatibel dengan Vercel (filesystem read-only)
    cache_session  = requests_cache.CachedSession(backend='memory', expire_after=3600)
    retry_session  = retry(cache_session, retries=1, backoff_factor=0.1)
    om_client      = openmeteo_requests.Client(session=retry_session)

    # Timezone WITA manual offset (UTC+8) agar tidak bergantung pada ZoneInfo eksternal
    now_wita = datetime.utcnow() + timedelta(hours=8)
    today    = now_wita.replace(hour=0, minute=0, second=0, microsecond=0)

    final_data = {}

    for pit_key, coords in PITS.items():
        try:
            om_resp = fetch_openmeteo(coords['lat'], coords['lon'], om_client)
            h_om    = om_resp.Hourly()

            arr_t       = h_om.Variables(0).ValuesAsNumpy()
            arr_tp      = h_om.Variables(1).ValuesAsNumpy()
            arr_ws      = h_om.Variables(2).ValuesAsNumpy()
            arr_vs      = h_om.Variables(3).ValuesAsNumpy()
            arr_wc      = h_om.Variables(4).ValuesAsNumpy()
            arr_hu      = h_om.Variables(5).ValuesAsNumpy()
            arr_tcc     = h_om.Variables(6).ValuesAsNumpy()
            arr_wd      = h_om.Variables(7).ValuesAsNumpy()
            arr_rain    = h_om.Variables(8).ValuesAsNumpy()
            arr_showers = h_om.Variables(9).ValuesAsNumpy()
        except Exception as e:
            final_data[pit_key] = {"hours": [], "allDays": []}
            continue

        bmkg_lookup, area_info, bmkg_adm4 = fetch_bmkg_nearest(
            coords['lat'], coords['lon'], cache_session
        )
        has_bmkg = len(bmkg_lookup) > 0

        out_all_days = []

        for day_idx in range(7):
            day_hours  = []
            tp_window  = []

            for h in range(24):
                idx = day_idx * 24 + h
                if idx >= len(arr_t):
                    break

                def safe(v, default=0):
                    return float(v) if v == v else default

                t_om       = round(safe(arr_t[idx],      28))
                tp         = round(safe(arr_tp[idx],     0.0), 1)
                ws_om      = round(safe(arr_ws[idx],     0.0), 1)
                vs_m       = safe(arr_vs[idx], 10000.0)
                wc         = int(safe(arr_wc[idx],       0))
                hu_om      = round(safe(arr_hu[idx],     75))
                tcc_om     = round(safe(arr_tcc[idx],    50))
                wd_deg     = safe(arr_wd[idx],            0.0)
                tp_rain    = round(safe(arr_rain[idx],   0.0), 1)
                tp_showers = round(safe(arr_showers[idx],0.0), 1)

                vs_km_om   = round(vs_m / 1000.0, 1)
                wd_str_om  = degrees_to_compass(wd_deg)

                tp_window.append(tp)
                if len(tp_window) > 3:
                    tp_window.pop(0)
                tp_3h = round(sum(tp_window), 1)

                bmkg = None
                bmkg_offset_used = None
                if has_bmkg:
                    # Cari slot BMKG terdekat — BMKG hanya ada setiap 3 jam (0,3,6,9,12,15,18,21)
                    # Prioritaskan offset terkecil dulu agar jam ganjil (cth: 17) menemukan 15 atau 18
                    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7, 8, -8, 9, -9]:
                        candidate_dt = today + timedelta(days=day_idx, hours=h + offset)
                        key = candidate_dt.strftime("%Y-%m-%d %H")
                        if key in bmkg_lookup:
                            bmkg = bmkg_lookup[key]
                            bmkg_offset_used = abs(offset)
                            break

                t_final = t_om
                hu_final = hu_om
                ws_final = ws_om
                wd_final = wd_str_om
                tcc_final = tcc_om
                vs_km_final = vs_km_om
                vs_text_final = f"{vs_km_final} km"
                wd_desc = ""
                wd_desc_en = ""
                wc_final = wc
                tp_final = tp

                if bmkg:
                    bwc = bmkg.get("weather_code_bmkg", 0)
                    wc_final = bwc

                    # Override curah hujan: jika BMKG bilang tidak hujan DAN slot BMKG dekat (<=2 jam)
                    # Jika jauh (>2 jam), percayakan Open-Meteo agar tidak salah membatalkan hujan nyata
                    bmkg_is_close = (bmkg_offset_used is not None and bmkg_offset_used <= 2)
                    if bwc in [0, 1, 2, 3, 4, 5, 10, 45] and bmkg_is_close:
                        tp_final = 0.0
                        if len(tp_window) > 0: tp_window[-1] = 0.0
                        tp_3h = round(sum(tp_window), 1)

                    if bmkg.get("t_bmkg") is not None: t_final = int(bmkg["t_bmkg"])
                    if bmkg.get("hu") is not None: hu_final = int(bmkg["hu"])
                    if bmkg.get("wd") not in [None, "?"]: wd_final = bmkg["wd"]
                    if bmkg.get("tcc") is not None: tcc_final = int(bmkg["tcc"])
                    if bmkg.get("vs_km") is not None: vs_km_final = bmkg["vs_km"]
                    if bmkg.get("vs_text"): vs_text_final = bmkg["vs_text"]
                    if bmkg.get("weather_desc"): wd_desc = bmkg["weather_desc"]
                    if bmkg.get("weather_desc_en"): wd_desc_en = bmkg["weather_desc_en"]

                risk = calculate_risk(tp_final, ws_final, vs_km_final, tcc_final, h, tp_3h, tp_showers)

                if bmkg:
                    if bwc in [95, 97]:
                        risk = "critical"
                    elif bwc in [63, 65] and risk in ["low", "medium"]:
                        risk = "high"

                day_hours.append({
                    "h":             h,
                    "timeStr":       f"{h:02d}:00",
                    "wc":            wc_final,
                    "t":             t_final,
                    "tp":            tp_final,
                    "tp_rain":        tp_rain,
                    "tp_showers":     tp_showers,
                    "tp_3h":          tp_3h,
                    "ws":            ws_final,
                    "wd":            wd_final,
                    "hu":            hu_final,
                    "tcc":           tcc_final,
                    "vs":            vs_km_final,
                    "vs_text":       vs_text_final,
                    "weather_desc":  wd_desc,
                    "weather_desc_en": wd_desc_en,
                    "risk":          risk,
                    "source":        "bmkg+openmeteo" if bmkg else "openmeteo",
                })

            out_all_days.append({"hours": day_hours})

        final_data[pit_key] = {
            "hours":   out_all_days[0]["hours"],
            "allDays": out_all_days,
            "meta": {
                "lat":          coords['lat'],
                "lon":          coords['lon'],
                "name":         coords['name'],
                "bmkg_area":    area_info,
                "bmkg_adm4":    bmkg_adm4,
                "has_bmkg":     has_bmkg,
                "updated_at":   now_wita.strftime("%Y-%m-%d %H:%M WITA"),
            }
        }

    return final_data


# ─── VERCEL SERVERLESS HANDLER ────────────────────────────────────────────────
class handler(BaseHTTPRequestHandler):

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        """Handle CORS preflight request"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        try:
            # Generate the weather payload
            payload = generate_weather_data()
            response_json = json.dumps(payload, ensure_ascii=False)
            
            # Send HTTP Response
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self._send_cors_headers()
            # Cache di Vercel Edge selama 1 jam, stale-while-revalidate 10 menit
            self.send_header('Cache-Control', 's-maxage=3600, stale-while-revalidate=600')
            self.end_headers()
            
            # Write payload format UTF-8
            self.wfile.write(response_json.encode('utf-8'))
        except Exception as e:
            # Handle Errors Contextually for the Client
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            import traceback
            error_msg = json.dumps({"error": str(e), "detail": traceback.format_exc()})
            self.wfile.write(error_msg.encode('utf-8'))
        return
