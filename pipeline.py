"""
Hood Brief — Scanner Pipeline
Memphis, TN — MPD Only
─────────────────────────────────────────────────────────────────
Features:
  • 10-code translation (Memphis codes)
  • CAD street name corrections (Shelby County 911 NG911 GDB)
  • Geocoding chain:
      0. Landmark lookup table
      1. Shelby County 911 address database (Supabase)
      2. Google Places API (named locations)
      3. Google Geocoding API (street addresses)
      4. Geocodio (US address fallback)
      5. Nominatim (free fallback)
      6. City center (last resort)
  • Intersection handling (numbered + pure intersections)
  • MPD station detection (coordinate bounding boxes)
  • Gang hotspot detection
  • Dispatcher call detection (WP units)
  • Daily heatmap refresh from Memphis Open Data Hub
  • P1, P2, and Medical incidents only
─────────────────────────────────────────────────────────────────
"""

import os
import re
import time
import json
import tempfile
import threading
import requests
from datetime import datetime, timezone
from openai import OpenAI

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SUPABASE_URL   = os.environ.get("SUPABASE_URL",   "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY",   "")

CITIES = {
    "memphis": {
        "label":      "Memphis, TN",
        "stream_url": os.environ.get("MEMPHIS_STREAM_URL", ""),
        "center":     (35.1495, -90.0490),
    },
}

CHUNK_SECONDS            = 30
MAX_RETRIES              = 3
HEATMAP_REFRESH_INTERVAL = 86400   # 24 hours

client = OpenAI(api_key=OPENAI_API_KEY)


# ══════════════════════════════════════════════════════════════════
#  CAD STREET NAME CORRECTIONS
#  Source: Shelby County 911 NG911 GeoDatabase — ROAD layer
#  Maps dispatcher/scanner shorthand to official street names
#  Applied before geocoding to improve address matching
# ══════════════════════════════════════════════════════════════════

CAD_CORRECTIONS = {
    "240":             "Interstate 240",
    "40":              "Interstate 40",
    "55":              "Interstate 55",
    "385":             "State Route 385",
    "64":              "US Highway 64",
    "51":              "US Highway 51",
    "mlk jr":          "Doctor Martin Luther King Junior",
    "mlk":             "Doctor Martin Luther King Junior",
    "old highway 78":  "Old US Highway 78",
    "horn lake":       "Old Horn Lake",
    "hernando":        "Rufus Thomas",
    "maryland ave":    "Charles E Blake Sr",
    "le conte":        "Leconte",
    "holsten":         "Holston",
    "new castle":      "Newcastle",
    "south center":    "Center",
    "green creek":     "Green",
    "glenn rogers sr": "Glenn Rogers Senior",
    "channel 3":       "Channel Three",
}

def apply_cad_corrections(location_text):
    """
    Normalize scanner/CAD shorthand to official street names before geocoding.
    E.g. 'on 51' -> 'US Highway 51', 'MLK Jr' -> 'Doctor Martin Luther King Junior'
    """
    if not location_text:
        return location_text
    result = location_text
    for cad, official in CAD_CORRECTIONS.items():
        pattern = rf'\b{re.escape(cad)}\b'
        result = re.sub(pattern, official, result, flags=re.IGNORECASE)
    return result


# ══════════════════════════════════════════════════════════════════
#  HEATMAP REFRESH
#  Fetches P1 violent crime data from Memphis Open Data Hub daily
#  and writes to Supabase heatmap_points table
# ══════════════════════════════════════════════════════════════════

MPD_INCIDENTS_API = "https://data.memphistn.gov/resource/puh4-eea4.json"

P1_CATEGORIES = {
    "HOMICIDE",
    "ROBBERY",
    "AGGRAVATED ASSAULT",
    "WEAPON LAW VIOLATION",
    "KIDNAPPING/ABDUCTION",
}

def refresh_heatmap():
    print("[Heatmap] Starting daily refresh from Memphis Open Data Hub...")
    try:
        params = {
            "$limit": 5000,
            "$where": f"ucr_category in ({','.join(repr(c) for c in P1_CATEGORIES)})",
            "$select": "latitude,longitude,ucr_category,offense_datetime",
        }
        r = requests.get(MPD_INCIDENTS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        print(f"[Heatmap] Fetched {len(data)} P1 incidents from Memphis Open Data")

        points = []
        for row in data:
            try:
                lat = float(row.get("latitude", 0))
                lng = float(row.get("longitude", 0))
                if not lat or not lng:
                    continue
                if 34.9 <= lat <= 35.5 and -90.4 <= lng <= -89.6:
                    points.append({
                        "lat":      lat,
                        "lng":      lng,
                        "category": row.get("ucr_category", ""),
                    })
            except (ValueError, TypeError):
                continue

        print(f"[Heatmap] {len(points)} valid coordinate points extracted")
        if not points:
            print("[Heatmap] No valid points — skipping update")
            return

        headers = {
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "return=minimal",
        }

        # Clear existing points
        del_r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/heatmap_points"
            "?id=neq.00000000-0000-0000-0000-000000000000",
            headers=headers, timeout=15,
        )
        del_r.raise_for_status()

        # Insert new points in batches of 500
        inserted = 0
        for i in range(0, len(points), 500):
            batch = points[i:i+500]
            ins_r = requests.post(
                f"{SUPABASE_URL}/rest/v1/heatmap_points",
                json=batch, headers=headers, timeout=30,
            )
            ins_r.raise_for_status()
            inserted += len(batch)

        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print(f"[Heatmap] ✅ Inserted {inserted} points — updated {ts}")

    except requests.exceptions.RequestException as e:
        print(f"[Heatmap] Network error: {e}")
    except Exception as e:
        print(f"[Heatmap] Error: {e}")


def heatmap_refresh_loop():
    while True:
        refresh_heatmap()
        time.sleep(HEATMAP_REFRESH_INTERVAL)


# ══════════════════════════════════════════════════════════════════
#  MPD STATION DETECTION
#  Coordinate-based bounding boxes from Memphis Open Data GeoJSON
# ══════════════════════════════════════════════════════════════════

STATION_BOUNDS = {
    "Austin Peay Station":   (35.188, 35.264, -90.060, -89.877),
    "Raines Station":        (34.994, 35.085, -90.185, -89.986),
    "Mt. Moriah Station":    (35.050, 35.108, -89.990, -89.830),
    "Crump Station":         (35.074, 35.193, -90.185, -89.957),
    "Tillman Station":       (35.106, 35.193, -89.988, -89.888),
    "North Main Station":    (35.124, 35.194, -90.086, -90.024),
    "Airways Station":       (35.073, 35.116, -90.099, -89.946),
    "Appling Farms Station": (35.117, 35.206, -89.888, -89.720),
    "Ridgeway Station":      (34.994, 35.083, -89.990, -89.781),
}

def detect_station(lat, lng):
    if not lat or not lng:
        return None
    for station, (min_lat, max_lat, min_lng, max_lng) in STATION_BOUNDS.items():
        if min_lat <= lat <= max_lat and min_lng <= lng <= max_lng:
            return station
    return None


# ══════════════════════════════════════════════════════════════════
#  DISPATCHER DETECTION
#  Memphis PD dispatchers use WP prefix in their call signs
#  e.g. WP-12, WP12, Whiskey Papa
# ══════════════════════════════════════════════════════════════════

def is_dispatcher_call(unit_text, transcript=""):
    search_text = f"{unit_text or ''} {transcript or ''}".lower()
    patterns = [
        r'\bwp[-\s]?\d+\b',
        r'\bwp\b',
        r'\bwhiskey\s+papa\b',
        r'\bdispatch\b',
    ]
    for pattern in patterns:
        if re.search(pattern, search_text, re.IGNORECASE):
            return True
    return False


# ══════════════════════════════════════════════════════════════════
#  GANG HOTSPOT ZONES — MEMPHIS
# ══════════════════════════════════════════════════════════════════

GANG_ZONES = {
    "memphis": [
        {
            "zone": "Orange Mound — High Gang Activity",
            "keywords": [
                "park ave", "park avenue", "deadrick", "spottswood",
                "semmes", "lamar ave", "kimball ave", "airways blvd",
                "southern ave", "goodwyn", "alston", "macon rd",
                "macon road", "given ave", "given avenue",
            ],
        },
        {
            "zone": "Frayser — High Gang Activity",
            "keywords": [
                "frayser blvd", "frayser boulevard", "n watkins",
                "north watkins", "thomas st", "thomas street",
                "hwy 51", "highway 51", "vollintine", "harvell",
                "overton crossing", "rangeline", "range line",
                "rugby", "snowden", "josephine", "dellwood",
                "frayser", "hawkins mill",
            ],
        },
        {
            "zone": "South Memphis — High Gang Activity",
            "keywords": [
                "e mclemore", "w mclemore", "mclemore ave",
                "elvis presley blvd", "s third st", "south third",
                "horn lake rd", "horn lake road", "s parkway",
                "south parkway", "florida st", "florida street",
                "trigg ave", "trigg avenue", "person ave",
                "mississippi blvd", "castalia st",
            ],
        },
        {
            "zone": "Hickory Hill — High Gang Activity",
            "keywords": [
                "hickory hill", "hickory ridge",
                "knight arnold rd", "knight arnold road",
                "shelby dr", "shelby drive", "germantown rd",
                "germantown road", "ridgeway rd", "ridgeway road",
                "mendenhall", "e shelby dr",
            ],
        },
        {
            "zone": "Binghampton — High Gang Activity",
            "keywords": [
                "binghampton", "lester st", "lester street",
                "tillman st", "tillman street", "broad ave",
                "broad avenue", "n graham", "graham st",
                "trezevant", "a w willis", "aw willis",
            ],
        },
        {
            "zone": "North Memphis — High Gang Activity",
            "keywords": [
                "smokey city", "klondike", "hyde park",
                "hollywood", "n hollywood", "north hollywood",
                "jackson ave", "jackson avenue", "n main",
                "auction ave", "brinkley", "manassas",
                "n second", "n 2nd", "joseph", "chelsea ave",
            ],
        },
        {
            "zone": "Whitehaven — High Gang Activity",
            "keywords": [
                "whitehaven", "white haven",
                "brooks rd", "brooks road",
                "american way", "kerr ave", "kerr avenue",
                "swinnea", "tchulahoma", "get well rd",
            ],
        },
        {
            "zone": "Tate & Boyd Area — AOB Gang Hub",
            "keywords": [
                "tate ave", "tate street", "boyd st",
                "boyd street", "boyd ave", "n tate",
            ],
        },
        {
            "zone": "Parkway Village — High Gang Activity",
            "keywords": [
                "parkway village", "e shelby dr",
                "knight arnold", "millbranch",
            ],
        },
        {
            "zone": "Westwood — High Gang Activity",
            "keywords": [
                "westwood", "walker homes", "person ave",
                "s westwood", "westhaven", "s parkway west",
            ],
        },
    ],
}

def check_gang_hotspot(location, title, city):
    if not location:
        return False, None
    zones = GANG_ZONES.get(city, [])
    search_text = f"{location} {title or ''}".lower()
    for zone in zones:
        for keyword in zone["keywords"]:
            if keyword.lower() in search_text:
                return True, zone["zone"]
    return False, None


# ══════════════════════════════════════════════════════════════════
#  10-CODE DICTIONARY — MEMPHIS
# ══════════════════════════════════════════════════════════════════

CODES_MEMPHIS = {
    "10-0":   "use caution",
    "10-1":   "poor radio signal",
    "10-2":   "good radio signal",
    "10-3":   "stop transmitting",
    "10-4":   "acknowledged",
    "10-5":   "relay message",
    "10-6":   "busy stand by",
    "10-7":   "out of service",
    "10-8":   "in service available",
    "10-9":   "repeat transmission",
    "10-10":  "off duty",
    "10-11":  "animal complaint",
    "10-12":  "standby",
    "10-13":  "weather and road conditions",
    "10-14":  "civilian escort",
    "10-15":  "subject in custody",
    "10-16":  "domestic disturbance",
    "10-17":  "pick up documents",
    "10-18":  "complete assignment quickly",
    "10-19":  "return to station",
    "10-20":  "location",
    "10-21":  "call by telephone",
    "10-22":  "disregard cancel",
    "10-23":  "arrived at scene",
    "10-24":  "assignment completed",
    "10-25":  "meet officer",
    "10-26":  "estimated time of arrival",
    "10-27":  "drivers license check",
    "10-28":  "vehicle registration check",
    "10-29":  "check for warrants",
    "10-30":  "unauthorized use of radio",
    "10-31":  "crime in progress",
    "10-32":  "person with a gun",
    "10-33":  "emergency all units stand by",
    "10-34":  "open door or window",
    "10-35":  "alarm",
    "10-36":  "correct time",
    "10-37":  "suspicious vehicle",
    "10-38":  "traffic stop",
    "10-39":  "proceed with lights and siren",
    "10-40":  "silent run no lights or siren",
    "10-41":  "beginning tour of duty",
    "10-42":  "ending tour of duty",
    "10-43":  "information",
    "10-44":  "request permission to leave patrol",
    "10-45":  "dead animal",
    "10-46":  "assist motorist",
    "10-47":  "emergency road repairs needed",
    "10-48":  "accident property damage only",
    "10-49":  "accident personal injury",
    "10-50":  "accident fatality",
    "10-51":  "request tow truck",
    "10-52":  "ambulance request",
    "10-53":  "dead on arrival",
    "10-54":  "livestock on road",
    "10-55":  "drunk driver",
    "10-56":  "intoxicated pedestrian",
    "10-57":  "hit and run accident",
    "10-58":  "direct traffic",
    "10-59":  "suspicious person",
    "10-60":  "squad in vicinity",
    "10-61":  "personnel in area",
    "10-62":  "reply to message",
    "10-63":  "prepare to copy",
    "10-64":  "message for local delivery",
    "10-65":  "net message assignment",
    "10-66":  "suspicious package",
    "10-67":  "person calling for help",
    "10-68":  "dispatch information",
    "10-69":  "message received",
    "10-70":  "prowler",
    "10-71":  "shooting",
    "10-72":  "stabbing",
    "10-73":  "smoke report",
    "10-74":  "negative",
    "10-75":  "in contact with",
    "10-76":  "en route",
    "10-77":  "estimated time of arrival",
    "10-78":  "need assistance",
    "10-79":  "notify investigator",
    "10-80":  "pursuit in progress",
    "10-81":  "breathalyzer report",
    "10-82":  "reserve lodging",
    "10-83":  "school crossing detail",
    "10-84":  "advise estimated time of arrival",
    "10-85":  "delayed",
    "10-86":  "officer on duty",
    "10-87":  "pick up checks",
    "10-88":  "present phone number of officer",
    "10-89":  "bomb threat",
    "10-90":  "bank alarm",
    "10-91":  "pick up prisoner",
    "10-91A": "vicious animal",
    "10-91B": "stray animal",
    "10-91C": "injured animal",
    "10-91D": "dead animal",
    "10-91E": "animal bite",
    "10-92":  "improperly parked vehicle",
    "10-93":  "blockade",
    "10-94":  "drag racing",
    "10-95":  "subject in custody",
    "10-96":  "mental health subject",
    "10-97":  "arrived at scene",
    "10-98":  "escaped prisoner",
    "10-99":  "officer needs help emergency",
    "10-100": "bathroom break",
    "10-200": "police needed at this location",
}

def translate_ten_codes(transcript, city):
    translated = transcript
    sorted_codes = sorted(CODES_MEMPHIS.items(), key=lambda x: len(x[0]), reverse=True)
    for code, meaning in sorted_codes:
        num = code.replace("10-", "").replace("10 ", "")
        patterns = [
            rf"\b10[-\s]?{re.escape(num)}\b",
            rf"\bten[-\s]{re.escape(num)}\b",
        ]
        for pattern in patterns:
            translated = re.sub(pattern, meaning, translated, flags=re.IGNORECASE)
    return re.sub(r" {2,}", " ", translated).strip()


# ══════════════════════════════════════════════════════════════════
#  MEMPHIS LANDMARK LOOKUP TABLE
#  Named locations that geocoding APIs struggle to find
# ══════════════════════════════════════════════════════════════════

MEMPHIS_LANDMARKS = {
    "super low":             (35.1281, -90.0372),
    "autozone park":         (35.1467, -90.0490),
    "liberty park":          (35.1189, -90.0528),
    "shelby farms":          (35.1503, -89.8724),
    "overton park":          (35.1489, -89.9841),
    "mud island":            (35.1584, -90.0565),
    "beale street":          (35.1396, -90.0502),
    "graceland":             (35.0472, -90.0232),
    "wolfchase":             (35.2018, -89.8241),
    "eastgate":              (35.1186, -89.8812),
    "hickory ridge mall":    (35.0556, -89.9268),
    "oak court":             (35.1180, -89.9506),
    "poplar plaza":          (35.1279, -89.9503),
    "southland mall":        (35.0281, -90.0187),
    "highland strip":        (35.1283, -89.9387),
    "cooper young":          (35.1175, -89.9837),
    "broad avenue":          (35.1503, -89.9641),
    "downtown memphis":      (35.1495, -90.0490),
    "medical district":      (35.1389, -90.0367),
    "methodist hospital":    (35.1389, -90.0367),
    "regional medical":      (35.1389, -90.0367),
    "the med":               (35.1389, -90.0367),
    "lebonheur":             (35.1503, -90.0367),
    "st jude":               (35.1516, -90.0412),
    "u of m":                (35.1189, -89.9387),
    "university of memphis": (35.1189, -89.9387),
    "memphis international": (35.0424, -89.9768),
    "memphis airport":       (35.0424, -89.9768),
    "fedex forum":           (35.1382, -90.0504),
    "pink palace":           (35.1189, -89.9503),
    "stax museum":           (35.1083, -90.0187),
    "national civil rights": (35.1346, -90.0587),
}

def check_landmark(location_text):
    if not location_text:
        return None
    text = location_text.lower()
    for keyword, coords in MEMPHIS_LANDMARKS.items():
        if keyword in text:
            print(f"  Landmark match: '{keyword}' -> {coords}")
            return coords
    return None


# ══════════════════════════════════════════════════════════════════
#  GEOCODING
#  Chain: Landmark -> 911 DB -> Google Places -> Google Geocoding
#         -> Geocodio -> Nominatim -> City center
# ══════════════════════════════════════════════════════════════════

def geocode_location(location_text, city):
    if not location_text:
        return CITIES[city]["center"]

    # Apply CAD name corrections first
    location_text = apply_cad_corrections(location_text)

    # Step 0: Landmark lookup
    landmark = check_landmark(location_text)
    if landmark:
        return landmark

    city_info    = CITIES[city]
    city_label   = city_info["label"]
    google_key   = os.environ.get("GOOGLE_MAPS_KEY", "")
    geocodio_key = os.environ.get("GEOCODIO_KEY", "")

    # Build query list for intersection handling
    location_queries = [location_text]

    cross_match = re.match(
        r'^(\d+\s+)([^,]+?)\s+and\s+(.+)$',
        location_text.strip(), re.IGNORECASE
    )
    intersection_match = re.match(
        r'^([^,\d][^,]+?)\s+and\s+([^,]+)$',
        location_text.strip(), re.IGNORECASE
    ) if not cross_match else None

    if cross_match:
        number  = cross_match.group(1).strip()
        street1 = cross_match.group(2).strip()
        street2 = cross_match.group(3).strip()
        location_queries = [f"{street1} and {street2}", f"{number} {street1}", street1]
        print(f"  Numbered intersection — trying: {location_queries}")
    elif intersection_match:
        street1 = intersection_match.group(1).strip()
        street2 = intersection_match.group(2).strip()
        location_queries = [f"{street1} and {street2}", street1, street2]
        print(f"  Pure intersection — trying: {location_queries}")

    def in_city(lat, lng):
        clat, clng = city_info["center"]
        return abs(lat - clat) + abs(lng - clng) < 2.0

    # Step 1: Shelby County 911 address database
    # Most accurate source for Memphis street addresses
    try:
        normalized = location_text.strip().upper()
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/memphis_addresses",
            params={"address": f"eq.{normalized}", "select": "lat,lng", "limit": 1},
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            timeout=5,
        )
        rows = resp.json()
        if rows and isinstance(rows, list) and len(rows) > 0:
            lat, lng = float(rows[0]['lat']), float(rows[0]['lng'])
            if in_city(lat, lng):
                print(f"  Geocoded (911 DB): {normalized} -> {lat}, {lng}")
                return lat, lng
    except Exception as e:
        print(f"  911 DB error: {e}")

    # Step 2: Google Places API — best for named locations
    if google_key:
        try:
            r = requests.get(
                "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
                params={
                    "input":        f"{location_text}, {city_label}",
                    "inputtype":    "textquery",
                    "fields":       "geometry,name,formatted_address",
                    "locationbias": f"circle:30000@{city_info['center'][0]},{city_info['center'][1]}",
                    "key":          google_key,
                },
                timeout=10,
            )
            data = r.json()
            if data.get("status") == "OK" and data.get("candidates"):
                loc = data["candidates"][0]["geometry"]["location"]
                lat, lng = float(loc["lat"]), float(loc["lng"])
                if in_city(lat, lng):
                    print(f"  Geocoded (Places): {location_text} -> {lat}, {lng}")
                    return lat, lng
            else:
                print(f"  Places API: {data.get('status')} for: {location_text}")
        except Exception as e:
            print(f"  Places API error: {e}")

    # Step 3: Google Geocoding API
    if google_key:
        for query in location_queries:
            try:
                r = requests.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"address": f"{query}, {city_label}", "key": google_key},
                    timeout=10,
                )
                data = r.json()
                if data.get("status") == "OK":
                    loc = data["results"][0]["geometry"]["location"]
                    lat, lng = float(loc["lat"]), float(loc["lng"])
                    if in_city(lat, lng):
                        print(f"  Geocoded (Google): {query} -> {lat}, {lng}")
                        return lat, lng
                else:
                    print(f"  Google geocode: {data.get('status')} for: {query}")
            except Exception as e:
                print(f"  Google geocoding error: {e}")

    # Step 4: Geocodio — good US address coverage, 2500 free/day
    if geocodio_key:
        for query in location_queries:
            try:
                r = requests.get(
                    "https://api.geocod.io/v1.7/geocode",
                    params={
                        "q":       f"{query}, {city_label}",
                        "api_key": geocodio_key,
                        "limit":   1,
                    },
                    timeout=10,
                )
                data    = r.json()
                results = data.get("results", [])
                if results:
                    loc = results[0]["location"]
                    lat, lng = float(loc["lat"]), float(loc["lng"])
                    if in_city(lat, lng):
                        print(f"  Geocoded (Geocodio): {query} -> {lat}, {lng}")
                        return lat, lng
                else:
                    print(f"  Geocodio: no results for: {query}")
            except Exception as e:
                print(f"  Geocodio error: {e}")

    # Step 5: Nominatim — free fallback
    for query in location_queries:
        try:
            time.sleep(1)
            r = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": f"{query}, {city_label}", "format": "json", "limit": 1},
                headers={
                    "User-Agent": "HoodBrief/1.0 (hoodbrief@proton.me)",
                    "Accept":     "application/json",
                },
                timeout=10,
            )
            results = r.json()
            if results:
                lat, lng = float(results[0]["lat"]), float(results[0]["lon"])
                if in_city(lat, lng):
                    print(f"  Geocoded (Nominatim): {query} -> {lat}, {lng}")
                    return lat, lng
            else:
                print(f"  Nominatim: no results for: {query}")
        except Exception as e:
            print(f"  Nominatim error: {e}")

    # Step 6: City center fallback
    print(f"  Falling back to city center for: {location_text}")
    return CITIES[city]["center"]


# ══════════════════════════════════════════════════════════════════
#  AUDIO CAPTURE
# ══════════════════════════════════════════════════════════════════

def capture_chunk(stream_url, duration=CHUNK_SECONDS):
    response = requests.get(stream_url, stream=True, timeout=15)
    response.raise_for_status()
    audio_data = b""
    start = time.time()
    for chunk in response.iter_content(chunk_size=4096):
        audio_data += chunk
        if time.time() - start >= duration:
            break
    return audio_data


# ══════════════════════════════════════════════════════════════════
#  WHISPER TRANSCRIPTION
# ══════════════════════════════════════════════════════════════════

def transcribe(audio_bytes):
    for attempt in range(MAX_RETRIES):
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                f.write(audio_bytes)
                tmp_path = f.name
            with open(tmp_path, "rb") as audio_file:
                result = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    language="en",
                    prompt=(
                        "Police scanner radio dispatch Memphis Tennessee MPD. "
                        "May include codes like 10-4, 10-20, 10-33, 10-99, "
                        "unit numbers like WP-12, 174, 156, "
                        "and Memphis street addresses and intersections."
                    )
                )
            return result.text.strip()
        except Exception as e:
            print(f"  Whisper attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return ""


# ══════════════════════════════════════════════════════════════════
#  GPT INCIDENT PARSER
# ══════════════════════════════════════════════════════════════════

def parse_incident(transcript_translated, city):
    city_info              = CITIES[city]
    city_label             = city_info["label"]
    center_lat, center_lng = city_info["center"]

    system_prompt = f"""You are a police incident parser for {city_label}.
You receive transcripts of radio dispatch audio where 10-codes have already
been translated to plain English. Extract structured incident data.

Return ONLY a valid JSON object — no markdown, no explanation.

If the transcript contains no dispatched incident return:
{{"incident": false}}

Otherwise return:
{{
  "incident": true,
  "title": "<6 words max incident description>",
  "location": "<street address or intersection>",
  "priority": "<one of: p1, p2, p3, medical, fire>",
  "unit": "<unit numbers or designations mentioned, including WP units>",
  "lat": <estimated latitude float>,
  "lng": <estimated longitude float>
}}

Priority guide:
  p1      = violent crime in progress, weapons, pursuit, officer needs help
  p2      = serious but not immediate: accidents with injuries, burglary, domestic
  p3      = low priority: noise complaints, minor traffic, suspicious person
  medical = any EMS or medical emergency
  fire    = any fire, smoke, explosion, hazmat

IMPORTANT: Scanner audio is often transcribed imperfectly by speech recognition.
Street names may be misheared or misspelled. Use your knowledge of real street names
in {city_label} to correct likely transcription errors in addresses before returning
them. If you see a street name that does not exist in {city_label} but sounds similar
to one that does, use the correct real street name instead. Always return the most
likely correct real street address based on context clues in the transcript.

WP units (e.g. WP-12, WP12) are dispatcher units — include them in the unit field.

For lat/lng use city center as fallback: {center_lat}, {center_lng}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": transcript_translated},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    return json.loads(response.choices[0].message.content)


# ══════════════════════════════════════════════════════════════════
#  SUPABASE WRITER
# ══════════════════════════════════════════════════════════════════

def save_incident(incident, city, transcript_original, transcript_translated,
                  gang_hotspot, gang_zone, station, is_dispatch):
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    payload = {
        "city":           city,
        "title":          incident.get("title"),
        "location":       incident.get("location"),
        "lat":            incident.get("lat"),
        "lng":            incident.get("lng"),
        "unit":           incident.get("unit"),
        "priority":       incident.get("priority"),
        "transcript":     transcript_translated,
        "transcript_raw": transcript_original,
        "gang_hotspot":   gang_hotspot,
        "gang_zone":      gang_zone,
        "station":        station,
        "is_dispatch":    is_dispatch,
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/incidents",
        json=payload, headers=headers, timeout=10,
    )
    r.raise_for_status()


# ══════════════════════════════════════════════════════════════════
#  MAIN CITY LOOP
# ══════════════════════════════════════════════════════════════════

def run_city(city):
    info       = CITIES[city]
    stream_url = info["stream_url"]
    label      = info["label"]

    print(f"[{label}] Pipeline started — capturing {CHUNK_SECONDS}s chunks...")

    while True:
        try:
            # Step 1: Capture audio
            audio = capture_chunk(stream_url, CHUNK_SECONDS)
            if len(audio) < 1000:
                print(f"[{label}] Audio chunk too small — skipping")
                time.sleep(5)
                continue

            # Step 2: Transcribe with Whisper
            transcript_raw = transcribe(audio)
            if not transcript_raw or len(transcript_raw.strip()) < 8:
                print(f"[{label}] No speech detected — skipping")
                continue

            print(f"[{label}] Raw: {transcript_raw[:120]}...")

            # Step 3: Translate 10-codes
            transcript_translated = translate_ten_codes(transcript_raw, city)
            if transcript_raw != transcript_translated:
                print(f"[{label}] Translated: {transcript_translated[:120]}...")

            # Step 4: Parse with GPT
            parsed = parse_incident(transcript_translated, city)
            if not parsed.get("incident"):
                print(f"[{label}] No incident detected — skipping")
                continue

            # Step 5: Filter by priority — P1, P2, Medical only
            priority = parsed.get("priority", "")
            if priority not in ("p1", "p2", "medical"):
                print(f"[{label}] Skipping {priority.upper()} — below threshold")
                continue

            # Step 6: Geocode
            location = parsed.get("location")
            lat, lng = geocode_location(location, city)
            parsed["lat"] = lat
            parsed["lng"] = lng

            # Step 7: Detect MPD station from coordinates
            station = detect_station(lat, lng)
            if station:
                print(f"  Station: {station}")

            # Step 8: Detect dispatcher call (WP units)
            unit        = parsed.get("unit", "")
            is_dispatch = is_dispatcher_call(unit, transcript_translated)
            if is_dispatch:
                print(f"  📡 Dispatcher call: {unit}")

            # Step 9: Check gang hotspot zones
            gang_hotspot, gang_zone = check_gang_hotspot(
                location, parsed.get("title"), city
            )
            if gang_hotspot:
                print(f"  ⚠ Gang hotspot: {gang_zone}")

            # Step 10: Save to Supabase
            save_incident(
                parsed, city,
                transcript_raw, transcript_translated,
                gang_hotspot, gang_zone, station, is_dispatch,
            )

            tags = []
            if is_dispatch:   tags.append("📡 DISPATCH")
            if station:       tags.append(station)
            if gang_hotspot:  tags.append(f"⚠ {gang_zone}")
            tag_str = f" [{', '.join(tags)}]" if tags else ""
            print(
                f"[{label}] ✅ Saved: [{priority.upper()}] "
                f"{parsed.get('title','?')} @ {parsed.get('location','?')}"
                f"{tag_str}"
            )

        except requests.exceptions.ConnectionError:
            print(f"[{label}] Stream connection lost — retrying in 10s")
            time.sleep(10)
        except requests.exceptions.HTTPError as e:
            print(f"[{label}] HTTP error: {e} — retrying in 15s")
            time.sleep(15)
        except json.JSONDecodeError:
            print(f"[{label}] GPT returned invalid JSON — skipping")
        except Exception as e:
            print(f"[{label}] Error: {e} — retrying in 5s")
            time.sleep(5)


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief — Pipeline Starting          ║")
    print("║  Memphis, TN — MPD Only                  ║")
    print("╚══════════════════════════════════════════╝")

    errors = []
    if not OPENAI_API_KEY: errors.append("OPENAI_API_KEY not set")
    if not SUPABASE_URL:   errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY:   errors.append("SUPABASE_KEY not set")
    for city, info in CITIES.items():
        if not info["stream_url"]:
            errors.append(f"{city.upper()}_STREAM_URL not set")

    if errors:
        print("\nMissing configuration:")
        for e in errors: print(f"  ✗ {e}")
        exit(1)

    threads = []

    # Heatmap refresh thread — runs immediately then every 24 hours
    hm = threading.Thread(target=heatmap_refresh_loop, daemon=True, name="heatmap")
    hm.start()
    threads.append(hm)
    print("  ✓ Started: Heatmap refresh (daily)")

    # Scanner thread for Memphis
    for city in CITIES:
        t = threading.Thread(target=run_city, args=(city,), daemon=True, name=city)
        t.start()
        threads.append(t)
        print(f"  ✓ Started: {CITIES[city]['label']}")

    print("\nAll threads running. Monitoring...\n")

    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            dead  = [t.name for t in threads if not t.is_alive()]
            status = f"[Heartbeat] Active: {', '.join(alive)}"
            if dead: status += f" | DEAD: {', '.join(dead)}"
            print(status)
    except KeyboardInterrupt:
        print("\nShutting down.")
