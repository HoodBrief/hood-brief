"""
Hood Brief — Scanner Pipeline
Memphis, TN + Baltimore, MD
Includes: 10-code translation, geocoding, gang hotspot detection
"""

import os
import re
import time
import json
import tempfile
import threading
import requests
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
    "baltimore": {
        "label":      "Baltimore, MD",
        "stream_url": os.environ.get("BALTIMORE_STREAM_URL", ""),
        "center":     (39.2904, -76.6122),
    },
}

CHUNK_SECONDS = 30
MAX_RETRIES   = 3

client = OpenAI(api_key=OPENAI_API_KEY)

# ══════════════════════════════════════════════════════════════════
#  GANG HOTSPOT ZONES
#
#  Each entry is a zone name paired with a list of keywords.
#  If ANY keyword is found in the incident location, the incident
#  is tagged with that zone name and gang_hotspot = true.
#  Keywords are matched case-insensitively.
# ══════════════════════════════════════════════════════════════════

GANG_ZONES = {
    "memphis": [
        {
            "zone": "Tate & Boyd Area — AOB Gang Hub",
            "keywords": ["tate", "boyd", "all off the blade", "aob"],
        },
        {
            "zone": "Orange Mound — High Gang Activity",
            "keywords": ["orange mound", "mound"],
        },
        {
            "zone": "South Memphis — High Gang Activity",
            "keywords": ["south memphis", "s memphis", "mississippi river", "e mclemore",
                         "w mclemore", "horn lake", "elvis presley blvd", "south third"],
        },
        {
            "zone": "Frayser — High Gang Activity",
            "keywords": ["frayser", "watkins", "north watkins", "n watkins",
                         "vollintine", "harvell", "overton crossing"],
        },
        {
            "zone": "Hickory Hill — High Gang Activity",
            "keywords": ["hickory hill", "hickory ridge", "knight arnold",
                         "shelby dr", "shelby drive", "lamar"],
        },
        {
            "zone": "Third Street Corridor — Gang Activity",
            "keywords": ["third street", "3rd street", "parkway village",
                         "n third", "n 3rd"],
        },
    ],
    "baltimore": [
        {
            "zone": "Sandtown-Winchester — High Gang Activity",
            "keywords": ["sandtown", "winchester", "north ave", "north avenue",
                         "baker street", "stricker", "gilmor"],
        },
        {
            "zone": "Cherry Hill — High Gang Activity",
            "keywords": ["cherry hill", "cherry hill rd", "cherry hill road",
                         "seabury", "benson", "round road"],
        },
        {
            "zone": "Greenmount East — High Gang Activity",
            "keywords": ["greenmount", "broadway east", "federal st", "federal street",
                         "chase street", "hoffman street", "aisquith"],
        },
        {
            "zone": "Broadway East — High Gang Activity",
            "keywords": ["broadway east", "broadway", "lafayette", "biddle",
                         "eager street", "mcelderry"],
        },
        {
            "zone": "Upton & Druid Heights — High Gang Activity",
            "keywords": ["upton", "druid heights", "druid hill", "madison ave",
                         "madison avenue", "mcculloh", "dolphin street"],
        },
        {
            "zone": "Southwest Baltimore — Operation Tornado Alley",
            "keywords": ["pratt street", "lemon street", "millington",
                         "edmondson", "sw baltimore", "southwest baltimore"],
        },
        {
            "zone": "Inner Harbor — Law Enforcement Focus Area",
            "keywords": ["inner harbor", "harbor", "pratt st", "light street",
                         "light st", "calvert street"],
        },
        {
            "zone": "Fells Point — Law Enforcement Focus Area",
            "keywords": ["fells point", "fell's point", "thames street",
                         "broadway pier", "upper fells"],
        },
        {
            "zone": "Canton Square — Law Enforcement Focus Area",
            "keywords": ["canton", "canton square", "o'donnell", "odonnell",
                         "boston street"],
        },
        {
            "zone": "Federal Hill — Law Enforcement Focus Area",
            "keywords": ["federal hill", "cross street", "light st", "covington",
                         "warren avenue"],
        },
        {
            "zone": "Patterson Park / Highlandtown — MS-13 Activity",
            "keywords": ["patterson park", "highlandtown", "eastern ave",
                         "eastern avenue", "conkling", "linwood"],
        },
    ],
}


# ══════════════════════════════════════════════════════════════════
#  GANG HOTSPOT DETECTOR
# ══════════════════════════════════════════════════════════════════

def check_gang_hotspot(location, title, city):
    """
    Checks whether an incident location matches any known gang
    hotspot zone for the given city.

    Searches both the location string and the incident title
    to maximise detection accuracy.

    Returns (is_hotspot: bool, zone_name: str or None)
    """
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
#  10-CODE DICTIONARIES
# ══════════════════════════════════════════════════════════════════

CODES_COMMON = {
    "10-0":  "use caution",
    "10-1":  "poor radio signal",
    "10-2":  "good radio signal",
    "10-3":  "stop transmitting",
    "10-4":  "acknowledged",
    "10-5":  "relay message",
    "10-6":  "busy stand by",
    "10-7":  "out of service",
    "10-8":  "in service available",
    "10-9":  "repeat transmission",
    "10-10": "off duty",
    "10-11": "animal complaint",
    "10-12": "standby",
    "10-13": "weather and road conditions",
    "10-14": "civilian escort",
    "10-15": "prisoner in custody",
    "10-16": "pick up prisoner",
    "10-17": "pick up documents",
    "10-18": "complete assignment quickly",
    "10-19": "return to station",
    "10-20": "location",
    "10-21": "call by telephone",
    "10-22": "disregard cancel",
    "10-23": "arrived at scene",
    "10-24": "assignment completed",
    "10-25": "meet officer",
    "10-26": "estimated time of arrival",
    "10-27": "drivers license check",
    "10-28": "vehicle registration check",
    "10-29": "check for warrants",
    "10-30": "unauthorized use of radio",
    "10-31": "crime in progress",
    "10-32": "person with a gun",
    "10-33": "emergency all units stand by",
    "10-34": "riot",
    "10-35": "major crime alert",
    "10-36": "correct time",
    "10-37": "suspicious vehicle",
    "10-38": "traffic stop",
    "10-39": "proceed with lights and siren",
    "10-40": "silent run no lights or siren",
    "10-41": "beginning tour of duty",
    "10-42": "ending tour of duty",
    "10-43": "information",
    "10-44": "request permission to leave patrol",
    "10-45": "dead animal",
    "10-46": "assist motorist",
    "10-47": "emergency road repairs needed",
    "10-48": "traffic standard needs repair",
    "10-49": "traffic light out",
    "10-50": "vehicle accident",
    "10-51": "request tow truck",
    "10-52": "request ambulance",
    "10-53": "road blocked",
    "10-54": "livestock on road",
    "10-55": "intoxicated driver",
    "10-56": "intoxicated pedestrian",
    "10-57": "hit and run accident",
    "10-58": "direct traffic",
    "10-59": "escort",
    "10-60": "squad in vicinity",
    "10-61": "personnel in area",
    "10-62": "reply to message",
    "10-63": "prepare to copy",
    "10-64": "message for local delivery",
    "10-65": "net message assignment",
    "10-66": "message cancellation",
    "10-67": "person calling for help",
    "10-68": "dispatch information",
    "10-69": "message received",
    "10-70": "fire alarm",
    "10-71": "shooting",
    "10-72": "stabbing",
    "10-73": "smoke report",
    "10-74": "negative",
    "10-75": "in contact with",
    "10-76": "en route",
    "10-77": "estimated time of arrival",
    "10-78": "need assistance",
    "10-79": "notify coroner",
    "10-80": "pursuit in progress",
    "10-81": "breathalyzer report",
    "10-82": "reserve lodging",
    "10-83": "school crossing detail",
    "10-84": "advise estimated time of arrival",
    "10-85": "delayed",
    "10-86": "officer on duty",
    "10-87": "pick up checks",
    "10-88": "present phone number of officer",
    "10-89": "bomb threat",
    "10-90": "bank alarm",
    "10-91": "pick up prisoner",
    "10-92": "improperly parked vehicle",
    "10-93": "blockade",
    "10-94": "drag racing",
    "10-95": "subject in custody",
    "10-96": "mental health subject",
    "10-97": "arrived at scene",
    "10-98": "escaped prisoner",
    "10-99": "officer needs help emergency",
    "10-100": "bathroom break",
    "10-200": "police needed at this location",
}

CODES_MEMPHIS = {
    **CODES_COMMON,
    "10-16":  "domestic disturbance",
    "10-34":  "open door or window",
    "10-35":  "alarm",
    "10-48":  "accident property damage only",
    "10-49":  "accident personal injury",
    "10-50":  "accident fatality",
    "10-52":  "ambulance request",
    "10-53":  "dead on arrival",
    "10-55":  "drunk driver",
    "10-59":  "suspicious person",
    "10-66":  "suspicious package",
    "10-70":  "prowler",
    "10-71":  "shooting",
    "10-72":  "stabbing",
    "10-79":  "notify investigator",
    "10-91A": "vicious animal",
    "10-91B": "stray animal",
    "10-91C": "injured animal",
    "10-91D": "dead animal",
    "10-91E": "animal bite",
}

CODES_BALTIMORE = {
    **CODES_COMMON,
    "10-1":    "unable to copy change location",
    "10-4":    "message received acknowledged",
    "10-7B":   "out of service personal",
    "10-15":   "enroute to hospital with patient",
    "10-19":   "return to",
    "10-25":   "do you have contact with",
    "10-33":   "emergency officer needs help",
    "10-38":   "stop suspicious vehicle",
    "10-40":   "respond without lights and siren",
    "10-50PI": "accident with personal injury",
    "10-50PD": "accident property damage only",
    "10-57":   "hit and run",
    "10-71":   "shooting",
    "10-79":   "bomb threat",
    "10-99":   "officer in danger immediate assistance needed",
}


# ══════════════════════════════════════════════════════════════════
#  10-CODE TRANSLATOR
# ══════════════════════════════════════════════════════════════════

def translate_ten_codes(transcript, city):
    codes = CODES_MEMPHIS if city == "memphis" else CODES_BALTIMORE
    translated = transcript
    sorted_codes = sorted(codes.items(), key=lambda x: len(x[0]), reverse=True)
    for code, meaning in sorted_codes:
        num = code.replace("10-", "").replace("10 ", "")
        patterns = [
            rf"\b10[-\s]?{re.escape(num)}\b",
            rf"\bten[-\s]{re.escape(num)}\b",
        ]
        for pattern in patterns:
            translated = re.sub(pattern, meaning, translated, flags=re.IGNORECASE)
    translated = re.sub(r" {2,}", " ", translated).strip()
    return translated


# ══════════════════════════════════════════════════════════════════
#  GEOCODING
# ══════════════════════════════════════════════════════════════════

def geocode_location(location_text, city):
    if not location_text:
        return CITIES[city]["center"]
    city_info  = CITIES[city]
    city_label = city_info["label"]

    # Try with full city name first
    queries = [
        f"{location_text}, {city_label}",
        f"{location_text}, {city_label.split(',')[0]}",
        location_text,
    ]

    for query in queries:
        try:
            # Nominatim requires a delay between requests
            time.sleep(1)
            url     = "https://nominatim.openstreetmap.org/search"
            params  = {
                "q":              query,
                "format":         "json",
                "limit":          1,
                "addressdetails": 1,
            }
            headers = {
                "User-Agent":    "HoodBrief/1.0 (hoodbrief@proton.me)",
                "Accept":        "application/json",
                "Referer":       "https://hoodbrief.netlify.app",
            }
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code != 200:
                print(f"  Nominatim returned {r.status_code} for: {query}")
                continue
            results = r.json()
            if results:
                lat = float(results[0]["lat"])
                lng = float(results[0]["lon"])
                # Sanity check — make sure coordinates are near the city
                center_lat, center_lng = city_info["center"]
                dist_from_center = abs(lat - center_lat) + abs(lng - center_lng)
                if dist_from_center > 2.0:
                    print(f"  Geocode result too far from city, skipping: {lat}, {lng}")
                    continue
                print(f"  Geocoded: {location_text} -> {lat}, {lng}")
                return lat, lng
            else:
                print(f"  No results for: {query}")
        except Exception as e:
            print(f"  Geocoding error: {e}")

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
                        "Police scanner radio dispatch. May contain codes like "
                        "10-4, 10-20, 10-33, 10-99, unit numbers, and street addresses."
                    )
                )
            return result.text.strip()
        except Exception as e:
            print(f"  Whisper attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return ""


# ══════════════════════════════════════════════════════════════════
#  GPT PARSER
# ══════════════════════════════════════════════════════════════════

def parse_incident(transcript_translated, city):
    city_info          = CITIES[city]
    city_label         = city_info["label"]
    center_lat, center_lng = city_info["center"]

    system_prompt = f"""You are a police incident parser for {city_label}.
You receive transcripts of radio dispatch audio where 10-codes have already
been translated to plain English. Extract structured incident data.

Return ONLY a valid JSON object, no markdown, no explanation.

If the transcript contains no dispatched incident return:
{{"incident": false}}

Otherwise return:
{{
  "incident": true,
  "title": "<6 words max incident description>",
  "location": "<street address or intersection>",
  "priority": "<one of: p1, p2, p3, medical, fire>",
  "unit": "<unit numbers mentioned>",
  "lat": <estimated latitude float>,
  "lng": <estimated longitude float>
}}

Priority guide:
  p1      = violent crime in progress, weapons, pursuit, officer needs help
  p2      = serious but not immediate: accidents with injuries, burglary, domestic
  p3      = low priority: noise complaints, minor traffic, suspicious person
  medical = any EMS or medical emergency
  fire    = any fire, smoke, explosion, hazmat

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

def save_incident(incident, city, transcript_original, transcript_translated, gang_hotspot, gang_zone):
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
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/incidents",
        json=payload,
        headers=headers,
        timeout=10,
    )
    r.raise_for_status()


# ══════════════════════════════════════════════════════════════════
#  MAIN CITY LOOP
# ══════════════════════════════════════════════════════════════════

def run_city(city):
    info       = CITIES[city]
    stream_url = info["stream_url"]
    label      = info["label"]

    print(f"[{label}] Pipeline started. Capturing {CHUNK_SECONDS}s chunks...")

    while True:
        try:
            # Step 1: Capture audio
            audio = capture_chunk(stream_url, CHUNK_SECONDS)
            if len(audio) < 1000:
                print(f"[{label}] Audio chunk too small - skipping")
                time.sleep(5)
                continue

            # Step 2: Transcribe
            transcript_raw = transcribe(audio)
            if not transcript_raw or len(transcript_raw.strip()) < 8:
                print(f"[{label}] No speech detected - skipping")
                continue

            print(f"[{label}] Raw: {transcript_raw[:100]}...")

            # Step 3: Translate 10-codes
            transcript_translated = translate_ten_codes(transcript_raw, city)
            if transcript_raw != transcript_translated:
                print(f"[{label}] Translated: {transcript_translated[:100]}...")

            # Step 4: Parse with GPT
            parsed = parse_incident(transcript_translated, city)
            if not parsed.get("incident"):
                print(f"[{label}] No incident detected - skipping")
                continue

            # Step 5: Geocode the location
            location = parsed.get("location")
            lat, lng = geocode_location(location, city)
            parsed["lat"] = lat
            parsed["lng"] = lng

            # Step 6: Check gang hotspot
            gang_hotspot, gang_zone = check_gang_hotspot(
                location, parsed.get("title"), city
            )
            if gang_hotspot:
                print(f"  ⚠ Gang hotspot detected: {gang_zone}")

            # Step 7: Save to Supabase
            save_incident(
                parsed, city,
                transcript_raw, transcript_translated,
                gang_hotspot, gang_zone
            )
            hotspot_tag = f" ⚠ {gang_zone}" if gang_hotspot else ""
            print(
                f"[{label}] Saved: [{parsed.get('priority','?').upper()}] "
                f"{parsed.get('title','?')} @ {parsed.get('location','?')}"
                f"{hotspot_tag}"
            )

        except requests.exceptions.ConnectionError:
            print(f"[{label}] Stream connection lost - retrying in 10s")
            time.sleep(10)
        except requests.exceptions.HTTPError as e:
            print(f"[{label}] HTTP error: {e} - retrying in 15s")
            time.sleep(15)
        except json.JSONDecodeError:
            print(f"[{label}] GPT returned invalid JSON - skipping")
        except Exception as e:
            print(f"[{label}] Error: {e} - retrying in 5s")
            time.sleep(5)


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════╗")
    print("║  Hood Brief - Pipeline Starting      ║")
    print("╚══════════════════════════════════════╝")

    errors = []
    if not OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY not set")
    if not SUPABASE_URL:
        errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY:
        errors.append("SUPABASE_KEY not set")
    for city, info in CITIES.items():
        if not info["stream_url"]:
            errors.append(f"{city.upper()}_STREAM_URL not set")

    if errors:
        print("\nMissing configuration:")
        for e in errors:
            print(f"  - {e}")
        exit(1)

    threads = []
    for city in CITIES:
        t = threading.Thread(target=run_city, args=(city,), daemon=True, name=city)
        t.start()
        threads.append(t)
        print(f"  Started: {CITIES[city]['label']}")

    print("\nBoth city pipelines running.\n")

    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            print(f"[Heartbeat] Active: {', '.join(alive)}")
    except KeyboardInterrupt:
        print("\nShutting down.")
