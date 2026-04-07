"""
╔══════════════════════════════════════════════════════════════════╗
║  HOOD BRIEF — Scanner Pipeline                                   ║
║  Memphis, TN  +  Washington, DC                                  ║
║                                                                  ║
║  SETUP — paste your values in the CONFIG block below             ║
║  then commit this file to your GitHub repo.                      ║
╚══════════════════════════════════════════════════════════════════╝

WHAT THIS SCRIPT DOES (in order):
  1. Captures 30-second audio chunks from each city's Broadcastify stream
  2. Sends audio to OpenAI Whisper for transcription
  3. Translates ALL 10-codes into plain English (city-specific dictionaries)
  4. Sends the translated transcript to GPT-4o-mini to extract structured data
     (title, location, priority, unit, lat/lng)
  5. Saves the structured incident to your Supabase database
  6. Your Hood Brief PWA picks it up in real time via WebSocket

Both cities run in parallel threads simultaneously.
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
#  CONFIG — fill these in, or set them as Railway env variables
# ══════════════════════════════════════════════════════════════════
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "YOUR_OPENAI_KEY_HERE")
SUPABASE_URL   = os.environ.get("SUPABASE_URL",   "YOUR_SUPABASE_URL_HERE")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY",   "YOUR_SUPABASE_ANON_KEY_HERE")

CITIES = {
    "memphis": {
        "label":      "Memphis, TN",
        "stream_url": os.environ.get("MEMPHIS_STREAM_URL", "YOUR_MEMPHIS_BROADCASTIFY_URL_HERE"),
        "center":     (35.1495, -90.0490),
    },
    "dc": {
        "label":      "Washington, DC",
        "stream_url": os.environ.get("DC_STREAM_URL", "YOUR_DC_BROADCASTIFY_URL_HERE"),
        "center":     (38.9072, -77.0369),
    },
}

CHUNK_SECONDS = 30   # how many seconds of audio to capture per chunk
MAX_RETRIES   = 3    # Whisper retry attempts on failure

client = OpenAI(api_key=OPENAI_API_KEY)

# ══════════════════════════════════════════════════════════════════
#  10-CODE DICTIONARIES
#
#  These cover the most common codes heard on Memphis PD and DC MPD
#  scanners. Both cities predominantly use standard APCO 10-codes
#  but with some local variations — those are noted below.
#
#  To add a code: "10-XX": "plain english meaning"
#  Codes are matched case-insensitively and with optional spaces
#  e.g. "10-4", "10 4", "104" all match "10-4"
# ══════════════════════════════════════════════════════════════════

# Codes shared by both Memphis and DC (standard APCO)
CODES_COMMON = {
    "10-0":  "use caution",
    "10-1":  "poor radio signal",
    "10-2":  "good radio signal",
    "10-3":  "stop transmitting",
    "10-4":  "acknowledged / understood",
    "10-5":  "relay message",
    "10-6":  "busy — stand by",
    "10-7":  "out of service",
    "10-8":  "in service / available",
    "10-9":  "repeat transmission",
    "10-10": "off duty",
    "10-11": "dog case / animal complaint",
    "10-12": "standby / visitors present",
    "10-13": "weather and road conditions",
    "10-14": "civilian escort",
    "10-15": "prisoner in custody",
    "10-16": "pick up prisoner",
    "10-17": "pick up papers / documents",
    "10-18": "complete assignment quickly",
    "10-19": "return to station",
    "10-20": "location / what is your location",
    "10-21": "call by telephone",
    "10-22": "disregard / cancel",
    "10-23": "arrived at scene",
    "10-24": "assignment completed",
    "10-25": "report to / meet officer",
    "10-26": "estimated time of arrival",
    "10-27": "driver's license check",
    "10-28": "vehicle registration check",
    "10-29": "check for warrants",
    "10-30": "unauthorized use of radio",
    "10-31": "crime in progress",
    "10-32": "person with a gun",
    "10-33": "emergency — all units stand by",
    "10-34": "riot",
    "10-35": "major crime alert",
    "10-36": "correct time",
    "10-37": "suspicious vehicle",
    "10-38": "stopping a vehicle",
    "10-39": "proceed with lights and siren",
    "10-40": "silent run — no lights or siren",
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
    "10-51": "request wrecker / tow truck",
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
    "10-63": "prepare to copy / make written copy",
    "10-64": "message for local delivery",
    "10-65": "net message assignment",
    "10-66": "message cancellation",
    "10-67": "clear for net message",
    "10-68": "dispatch information",
    "10-69": "message received",
    "10-70": "fire alarm",
    "10-71": "shooting",
    "10-72": "gun pulled",
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
    "10-84": "if meeting, advise ETA",
    "10-85": "delayed due to",
    "10-86": "officer on duty",
    "10-87": "pick up checks for distribution",
    "10-88": "present phone number of officer",
    "10-89": "bomb threat",
    "10-90": "bank alarm",
    "10-91": "pick up prisoner",
    "10-92": "improperly parked vehicle",
    "10-93": "blockade",
    "10-94": "drag racing",
    "10-95": "prisoner in custody / subject in custody",
    "10-96": "mental health subject",
    "10-97": "arrived at scene",
    "10-98": "prison break / escaped prisoner",
    "10-99": "officer needs help — emergency",
    "10-100": "bathroom break",
    "10-200": "police needed at this location",
}

# Memphis-specific codes and local variations
CODES_MEMPHIS = {
    **CODES_COMMON,
    "10-4":   "acknowledged",             # Memphis dispatchers say this constantly
    "10-15":  "subject in custody",
    "10-16":  "domestic disturbance",     # Memphis local: domestic call
    "10-34":  "open door or window",      # Memphis local usage
    "10-35":  "alarm",                    # Memphis local: security alarm
    "10-38":  "traffic stop",
    "10-48":  "accident — property damage only",
    "10-49":  "accident — personal injury",
    "10-50":  "accident — fatality",
    "10-52":  "ambulance request",
    "10-53":  "dead on arrival",
    "10-55":  "drunk driver",
    "10-59":  "suspicious person",
    "10-66":  "suspicious package",
    "10-67":  "person calling for help",
    "10-70":  "prowler",
    "10-71":  "shooting",
    "10-72":  "knifing / stabbing",
    "10-73":  "how do you copy",
    "10-79":  "notify investigator",
    "10-91A": "vicious animal",
    "10-91B": "stray animal",
    "10-91C": "injured animal",
    "10-91D": "dead animal",
    "10-91E": "animal bite",
}

# DC MPD-specific codes and local variations
CODES_DC = {
    **CODES_COMMON,
    "10-1":   "unable to copy — change location",
    "10-4":   "message received / acknowledged",
    "10-7":   "out of service",
    "10-7B":  "out of service — personal",
    "10-7OD": "out of service — off duty",
    "10-8":   "in service",
    "10-9":   "say again",
    "10-15":  "enroute to hospital with patient",
    "10-16":  "pick up prisoner",
    "10-19":  "return to",
    "10-20":  "location",
    "10-25":  "do you have contact with",
    "10-27":  "check driver's license",
    "10-28":  "check vehicle registration",
    "10-29":  "check for wanted",
    "10-33":  "emergency — officer needs help",
    "10-38":  "stop suspicious vehicle",
    "10-40":  "respond without lights and siren",
    "10-49":  "proceed to location",
    "10-50":  "accident",
    "10-50PI":"accident with personal injury",
    "10-50PD":"accident — property damage only",
    "10-55":  "intoxicated driver",
    "10-57":  "hit and run",
    "10-59":  "escort needed",
    "10-71":  "shooting",
    "10-79":  "bomb threat",
    "10-99":  "officer in danger — immediate assistance needed",
}

# ══════════════════════════════════════════════════════════════════
#  10-CODE TRANSLATOR
# ══════════════════════════════════════════════════════════════════

def translate_ten_codes(transcript: str, city: str) -> str:
    """
    Scans a transcript for any 10-codes and replaces them with
    plain English. Handles all common formatting variants:
      - "10-4"   → with hyphen
      - "10 4"   → with space
      - "104"    → no separator
      - "ten-4"  → written out
      - "ten four" → fully written out

    Returns the translated transcript.
    """
    codes = CODES_MEMPHIS if city == "memphis" else CODES_DC
    translated = transcript

    # Sort by code length descending so longer codes (10-99) match
    # before shorter prefixes (10-9) — prevents partial matches
    sorted_codes = sorted(codes.items(), key=lambda x: len(x[0]), reverse=True)

    for code, meaning in sorted_codes:
        # Extract the numeric part: "10-32" → "32"
        num = code.replace("10-", "").replace("10 ", "")

        # Build pattern matching all formatting variants
        # e.g. for 10-32: matches "10-32", "10 32", "1032", "ten-32", "ten 32"
        patterns = [
            rf"\b10[-\s]?{re.escape(num)}\b",          # 10-32 / 10 32 / 1032
            rf"\bten[-\s]{re.escape(num)}\b",           # ten-32 / ten 32
        ]

        # For single-digit codes also match written-out number words
        word_map = {
            "0":"zero","1":"one","2":"two","3":"three","4":"four",
            "5":"five","6":"six","7":"seven","8":"eight","9":"nine",
            "10":"ten","11":"eleven","12":"twelve","13":"thirteen",
            "14":"fourteen","15":"fifteen","16":"sixteen","17":"seventeen",
            "18":"eighteen","19":"nineteen","20":"twenty","21":"twenty one",
            "22":"twenty two","23":"twenty three","24":"twenty four",
            "25":"twenty five","26":"twenty six","27":"twenty seven",
            "28":"twenty eight","29":"twenty nine","30":"thirty",
            "33":"thirty three","38":"thirty eight","50":"fifty",
            "52":"fifty two","55":"fifty five","70":"seventy",
            "71":"seventy one","80":"eighty","95":"ninety five",
            "96":"ninety six","99":"ninety nine",
        }
        if num in word_map:
            patterns.append(rf"\bten[-\s]{word_map[num]}\b")

        for pattern in patterns:
            translated = re.sub(
                pattern,
                meaning,
                translated,
                flags=re.IGNORECASE
            )

    # Clean up any double spaces left behind
    translated = re.sub(r" {2,}", " ", translated).strip()

    return translated


def log_translation(original: str, translated: str, city: str):
    """Print a diff if any codes were translated — useful for debugging."""
    if original != translated:
        print(f"  [{city.upper()}] 10-code translation:")
        print(f"    BEFORE: {original[:120]}")
        print(f"    AFTER:  {translated[:120]}")


# ══════════════════════════════════════════════════════════════════
#  AUDIO CAPTURE
# ══════════════════════════════════════════════════════════════════

def capture_chunk(stream_url: str, duration: int = CHUNK_SECONDS) -> bytes:
    """
    Streams audio from Broadcastify for `duration` seconds
    and returns the raw bytes. Streams in small chunks to avoid
    loading the whole file into memory at once.
    """
    response = requests.get(stream_url, stream=True, timeout=15)
    response.raise_for_status()

    audio_data = b""
    start      = time.time()

    for chunk in response.iter_content(chunk_size=4096):
        audio_data += chunk
        if time.time() - start >= duration:
            break

    return audio_data


# ══════════════════════════════════════════════════════════════════
#  WHISPER TRANSCRIPTION
# ══════════════════════════════════════════════════════════════════

def transcribe(audio_bytes: bytes) -> str:
    """
    Sends raw audio bytes to OpenAI Whisper (whisper-1) and
    returns the transcript text. Retries up to MAX_RETRIES on failure.
    Whisper handles scanner audio well but may still mishear some codes —
    the translate_ten_codes step catches most of those.
    """
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
#  GPT-4o-mini INCIDENT PARSER
# ══════════════════════════════════════════════════════════════════

def parse_incident(transcript_translated: str, city: str) -> dict:
    """
    Sends the already-translated transcript to GPT-4o-mini.
    Returns a structured dict with incident fields, or
    {"incident": False} if no real incident was dispatched.

    We pass the TRANSLATED transcript so GPT sees plain English,
    not raw codes — this significantly improves parsing accuracy.
    """
    city_info  = CITIES[city]
    city_label = city_info["label"]
    center_lat, center_lng = city_info["center"]

    system_prompt = f"""You are a police incident parser for {city_label}.
You receive transcripts of radio dispatch audio where 10-codes have already 
been translated to plain English. Extract structured incident data.

Return ONLY a valid JSON object — no markdown, no explanation, nothing else.

If the transcript contains no dispatched incident (e.g. just silence, 
radio chatter, status updates, or test transmissions), return:
{{"incident": false}}

Otherwise return:
{{
  "incident": true,
  "title": "<6 words max — concise incident description>",
  "location": "<street address or intersection>",
  "priority": "<one of: p1, p2, p3, medical, fire>",
  "unit": "<unit numbers or designations mentioned>",
  "lat": <float — estimated latitude within {city_label}>,
  "lng": <float — estimated longitude within {city_label}>
}}

Priority guide:
  p1      = violent crime in progress, weapons, pursuit, officer needs help
  p2      = serious but not immediate: accidents with injuries, burglary, domestic
  p3      = low priority: noise complaints, minor traffic, suspicious person
  medical = any EMS / ambulance / medical emergency call
  fire    = any fire, smoke, explosion, hazmat

For lat/lng: estimate based on the street name within {city_label}.
If you cannot determine a specific location, use the city center:
lat {center_lat}, lng {center_lng}."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": transcript_translated},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    raw = response.choices[0].message.content
    return json.loads(raw)


# ══════════════════════════════════════════════════════════════════
#  SUPABASE WRITER
# ══════════════════════════════════════════════════════════════════

def save_incident(incident: dict, city: str, transcript_original: str, transcript_translated: str):
    """
    Writes a parsed incident row to the Supabase `incidents` table.
    Stores both the original (raw 10-code) and translated transcripts
    so you can audit the translations later if needed.
    """
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

    payload = {
        "city":                city,
        "title":               incident.get("title"),
        "location":            incident.get("location"),
        "lat":                 incident.get("lat"),
        "lng":                 incident.get("lng"),
        "unit":                incident.get("unit"),
        "priority":            incident.get("priority"),
        "transcript":          transcript_translated,   # plain English version shown in PWA
        "transcript_raw":      transcript_original,     # original with 10-codes (for auditing)
    }

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/incidents",
        json=payload,
        headers=headers,
        timeout=10,
    )
    r.raise_for_status()


# ══════════════════════════════════════════════════════════════════
#  SUPABASE SCHEMA NOTE
#
#  Add this column to your Supabase incidents table to store the
#  raw transcript alongside the translated one. Run this SQL in
#  the Supabase SQL Editor:
#
#  ALTER TABLE incidents ADD COLUMN IF NOT EXISTS transcript_raw text;
#
# ══════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════
#  MAIN CITY LOOP
# ══════════════════════════════════════════════════════════════════

def run_city(city: str):
    """
    Infinite loop for one city:
      1. Capture audio chunk
      2. Transcribe with Whisper
      3. Translate 10-codes to plain English
      4. Parse incident with GPT-4o-mini
      5. Save to Supabase if a real incident was found
    """
    info       = CITIES[city]
    stream_url = info["stream_url"]
    label      = info["label"]

    print(f"[{label}] Pipeline started. Capturing {CHUNK_SECONDS}s chunks...")

    while True:
        try:
            # ── Step 1: Capture audio ──────────────────────────
            audio = capture_chunk(stream_url, CHUNK_SECONDS)
            if len(audio) < 1000:
                print(f"[{label}] Audio chunk too small — skipping")
                time.sleep(5)
                continue

            # ── Step 2: Whisper transcription ──────────────────
            transcript_raw = transcribe(audio)
            if not transcript_raw or len(transcript_raw.strip()) < 8:
                print(f"[{label}] No speech detected — skipping")
                continue

            print(f"[{label}] Raw transcript: {transcript_raw[:100]}...")

            # ── Step 3: Translate 10-codes ─────────────────────
            transcript_translated = translate_ten_codes(transcript_raw, city)
            log_translation(transcript_raw, transcript_translated, city)

            # ── Step 4: Parse incident with GPT ───────────────
            parsed = parse_incident(transcript_translated, city)

            if not parsed.get("incident"):
                print(f"[{label}] No incident detected — skipping")
                continue

            # ── Step 5: Save to Supabase ───────────────────────
            save_incident(parsed, city, transcript_raw, transcript_translated)
            print(
                f"[{label}] ✓ Saved: [{parsed.get('priority','?').upper()}] "
                f"{parsed.get('title','?')} @ {parsed.get('location','?')}"
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
            print(f"[{label}] Unexpected error: {e} — retrying in 5s")
            time.sleep(5)


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT — both cities run simultaneously in threads
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════╗")
    print("║  Hood Brief — Pipeline Starting      ║")
    print("╚══════════════════════════════════════╝")

    # Validate config before starting
    errors = []
    if "YOUR_OPENAI" in OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY not set")
    if "YOUR_SUPABASE_URL" in SUPABASE_URL:
        errors.append("SUPABASE_URL not set")
    if "YOUR_SUPABASE" in SUPABASE_KEY:
        errors.append("SUPABASE_KEY not set")
    for city, info in CITIES.items():
        if "YOUR_" in info["stream_url"]:
            errors.append(f"{city.upper()}_STREAM_URL not set")

    if errors:
        print("\n⚠  Missing configuration:")
        for e in errors:
            print(f"   • {e}")
        print("\nSet these as Railway environment variables or edit the CONFIG block.\n")
        exit(1)

    # Start one thread per city
    threads = []
    for city in CITIES:
        t = threading.Thread(target=run_city, args=(city,), daemon=True, name=city)
        t.start()
        threads.append(t)
        print(f"  Started thread: {CITIES[city]['label']}")

    print("\nBoth city pipelines running. Press Ctrl+C to stop.\n")

    # Keep main thread alive — if a city thread crashes Railway
    # will restart the whole process cleanly
    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            print(f"[Heartbeat] Active threads: {', '.join(alive)}")
    except KeyboardInterrupt:
        print("\nShutting down Hood Brief pipeline.")
