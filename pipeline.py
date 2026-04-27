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
  • Heatmap: static P1 points loaded into Supabase on startup
  • Fugitive scraper: weekly pull from memphismostwanted.org
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
from bs4 import BeautifulSoup
from faster_whisper import WhisperModel

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

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
FUGITIVE_REFRESH_SECONDS = 604800  # 7 days

# ── Faster-Whisper local model (runs on Railway CPU, zero API cost) ──
# Model downloads on first startup (~150MB), cached for subsequent runs
_whisper_model = None

def get_whisper_model():
    global _whisper_model
    if _whisper_model is None:
        print("[Whisper] Loading faster-whisper base model on CPU...")
        _whisper_model = WhisperModel(
            "base",
            device="cpu",
            compute_type="int8",  # Fastest on CPU, lowest memory
        )
        print("[Whisper] Model ready")
    return _whisper_model


# ══════════════════════════════════════════════════════════════════
#  SUPABASE HELPERS
# ══════════════════════════════════════════════════════════════════

def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def sb_get(path, params=None):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        params=params,
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

def sb_post(path, data):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{path}",
        json=data,
        headers=sb_headers(),
        timeout=30,
    )
    r.raise_for_status()
    return r

def sb_delete(path):
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=sb_headers(),
        timeout=15,
    )
    r.raise_for_status()
    return r


# ══════════════════════════════════════════════════════════════════
#  HEATMAP — STATIC P1 POINTS
#  Source: MPD Public Safety Incidents GeoJSON (April 2026)
#  Loaded once at startup into Supabase heatmap_points table
#  (Live API at data.memphistn.gov is not accessible from Railway)
# ══════════════════════════════════════════════════════════════════

HEATMAP_STATIC_POINTS = [
    {"lat":35.11,"lng":-90.065,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.134,"lng":-90.035,"category":"ROBBERY"},
    {"lat":35.043,"lng":-89.861,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.245,"lng":-89.983,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.161,"lng":-89.954,"category":"ROBBERY"},
    {"lat":35.028,"lng":-90.044,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.114,"lng":-90.028,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.094,"lng":-90.07,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.094,"lng":-90.07,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.175,"lng":-90.025,"category":"ROBBERY"},
    {"lat":35.149,"lng":-89.916,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.138,"lng":-89.964,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.055,"lng":-90.021,"category":"ROBBERY"},
    {"lat":35.106,"lng":-90.003,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.106,"lng":-90.003,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.145,"lng":-90.032,"category":"ROBBERY"},
    {"lat":35.247,"lng":-89.977,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.107,"lng":-90.001,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.175,"lng":-89.926,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.155,"lng":-89.912,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.05,"lng":-90.005,"category":"ROBBERY"},
    {"lat":35.109,"lng":-89.952,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.161,"lng":-89.796,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.134,"lng":-90.029,"category":"ROBBERY"},
    {"lat":35.06,"lng":-90.079,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.145,"lng":-89.796,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.173,"lng":-89.784,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.133,"lng":-89.977,"category":"ROBBERY"},
    {"lat":35.113,"lng":-90.026,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.06,"lng":-90.079,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.171,"lng":-89.946,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.239,"lng":-89.942,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.046,"lng":-90.081,"category":"ROBBERY"},
    {"lat":35.024,"lng":-90.01,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.006,"lng":-90.006,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.139,"lng":-90.061,"category":"ROBBERY"},
    {"lat":35.167,"lng":-90.027,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.211,"lng":-90.026,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.207,"lng":-89.995,"category":"ROBBERY"},
    {"lat":35.231,"lng":-89.896,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.221,"lng":-89.974,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.167,"lng":-89.921,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.024,"lng":-90.01,"category":"ROBBERY"},
    {"lat":35.064,"lng":-90.058,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.115,"lng":-89.97,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.234,"lng":-89.968,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.252,"lng":-89.936,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.074,"lng":-89.926,"category":"ROBBERY"},
    {"lat":35.035,"lng":-90.004,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.109,"lng":-89.982,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.134,"lng":-90.029,"category":"ROBBERY"},
    {"lat":35.22,"lng":-89.965,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.22,"lng":-89.946,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.12,"lng":-90.034,"category":"ROBBERY"},
    {"lat":35.212,"lng":-90.026,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.143,"lng":-90.014,"category":"ROBBERY"},
    {"lat":35.117,"lng":-90.05,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.096,"lng":-89.996,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.093,"lng":-90.067,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.108,"lng":-89.973,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.093,"lng":-90.038,"category":"ROBBERY"},
    {"lat":35.094,"lng":-90.03,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.14,"lng":-90.054,"category":"ROBBERY"},
    {"lat":35.059,"lng":-89.863,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.049,"lng":-89.827,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.093,"lng":-90.038,"category":"ROBBERY"},
    {"lat":35.156,"lng":-89.783,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.173,"lng":-89.961,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.108,"lng":-89.973,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.17,"lng":-89.936,"category":"ROBBERY"},
    {"lat":35.045,"lng":-89.874,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.104,"lng":-90.011,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.172,"lng":-89.792,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.093,"lng":-90.067,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.16,"lng":-90.016,"category":"ROBBERY"},
    {"lat":35.117,"lng":-90.05,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.22,"lng":-89.926,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.187,"lng":-89.877,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.05,"lng":-89.798,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.094,"lng":-90.03,"category":"ROBBERY"},
    {"lat":35.22,"lng":-89.926,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.243,"lng":-89.948,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.157,"lng":-90.033,"category":"ROBBERY"},
    {"lat":35.204,"lng":-89.978,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.177,"lng":-89.942,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.06,"lng":-89.926,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.061,"lng":-89.848,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.111,"lng":-89.967,"category":"ROBBERY"},
    {"lat":35.227,"lng":-90.003,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.06,"lng":-89.856,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.051,"lng":-90.013,"category":"ROBBERY"},
    {"lat":35.088,"lng":-90.066,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.214,"lng":-89.96,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.072,"lng":-89.868,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.082,"lng":-89.889,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.096,"lng":-89.975,"category":"ROBBERY"},
    {"lat":35.188,"lng":-89.798,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.069,"lng":-89.934,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.181,"lng":-89.934,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.115,"lng":-90.021,"category":"ROBBERY"},
    {"lat":35.072,"lng":-89.868,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.007,"lng":-90.072,"category":"ROBBERY"},
    {"lat":35.027,"lng":-89.871,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.069,"lng":-89.934,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.065,"lng":-89.905,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.101,"lng":-90.036,"category":"ROBBERY"},
    {"lat":35.213,"lng":-89.922,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.113,"lng":-89.947,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.057,"lng":-89.93,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.021,"lng":-90.043,"category":"ROBBERY"},
    {"lat":35.046,"lng":-90.082,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.069,"lng":-89.954,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.065,"lng":-89.905,"category":"ROBBERY"},
    {"lat":35.059,"lng":-89.863,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.213,"lng":-89.922,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.204,"lng":-89.978,"category":"ROBBERY"},
    {"lat":35.007,"lng":-90.072,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.069,"lng":-89.954,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.06,"lng":-89.926,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.059,"lng":-89.863,"category":"ROBBERY"},
    {"lat":35.06,"lng":-89.856,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.172,"lng":-89.792,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.046,"lng":-90.082,"category":"ROBBERY"},
    {"lat":35.102,"lng":-90.035,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.044,"lng":-89.885,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.243,"lng":-89.948,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.082,"lng":-89.889,"category":"ROBBERY"},
    {"lat":35.096,"lng":-89.975,"category":"AGGRAVATED ASSAULT"},
    {"lat":35.113,"lng":-89.947,"category":"WEAPON LAW VIOLATION"},
    {"lat":35.035,"lng":-90.004,"category":"HOMICIDE"},
    {"lat":35.157,"lng":-90.033,"category":"HOMICIDE"},
    {"lat":35.187,"lng":-89.877,"category":"KIDNAPPING/ABDUCTION"},
    {"lat":35.082,"lng":-89.889,"category":"KIDNAPPING/ABDUCTION"},
]

def load_heatmap():
    """Load static P1 heatmap points into Supabase on startup — only if empty."""
    print("[Heatmap] Checking heatmap_points table...")
    try:
        existing = sb_get("heatmap_points", params={"select": "id", "limit": 1})
        if existing and len(existing) > 0:
            print(f"[Heatmap] Table already populated — skipping reload")
            return
        # Insert all points
        inserted = 0
        for i in range(0, len(HEATMAP_STATIC_POINTS), 500):
            batch = HEATMAP_STATIC_POINTS[i:i+500]
            sb_post("heatmap_points", batch)
            inserted += len(batch)
        print(f"[Heatmap] ✅ Loaded {inserted} P1 points into Supabase")
    except Exception as e:
        print(f"[Heatmap] Error: {e}")


# ══════════════════════════════════════════════════════════════════
#  FUGITIVE SCRAPER
#  Scrapes memphismostwanted.org weekly
#  Geocodes last known addresses and saves to Supabase fugitives table
# ══════════════════════════════════════════════════════════════════

CRIMESTOPPERS_URL = "https://www.memphismostwanted.org/"

def geocode_fugitive_address(address_text):
    """
    Geocode a fugitive's last known address.
    Tries 911 DB first, then Google, then Nominatim.
    Returns (lat, lng) or None if not found or outside region.
    """
    if not address_text or "unknown" in address_text.lower() or "at large" in address_text.lower():
        return None

    # Normalize — extract just the street address part before city/state
    # e.g. "1234 Main St in Memphis, TN 38103" -> "1234 Main St"
    clean = re.sub(r'\s+in\s+.+$', '', address_text, flags=re.IGNORECASE).strip()
    clean_upper = clean.upper()

    def in_region(lat, lng):
        # Accept Memphis and surrounding Shelby County area
        return 34.9 <= lat <= 35.4 and -90.3 <= lng <= -89.6

    # Step 1: 911 database
    try:
        rows = sb_get(
            "memphis_addresses",
            params={"address": f"eq.{clean_upper}", "select": "lat,lng", "limit": 1}
        )
        if rows:
            lat, lng = float(rows[0]['lat']), float(rows[0]['lng'])
            if in_region(lat, lng):
                print(f"  [Fugitive] 911 DB: {clean_upper} -> {lat}, {lng}")
                return lat, lng
    except Exception as e:
        print(f"  [Fugitive] 911 DB error: {e}")

    # Step 2: Google Geocoding
    google_key = os.environ.get("GOOGLE_MAPS_KEY", "")
    if google_key:
        try:
            r = requests.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params={"address": f"{clean}, Memphis, TN", "key": google_key},
                timeout=10,
            )
            data = r.json()
            if data.get("status") == "OK":
                loc = data["results"][0]["geometry"]["location"]
                lat, lng = float(loc["lat"]), float(loc["lng"])
                if in_region(lat, lng):
                    print(f"  [Fugitive] Google: {clean} -> {lat}, {lng}")
                    return lat, lng
        except Exception as e:
            print(f"  [Fugitive] Google error: {e}")

    # Step 3: Nominatim
    try:
        time.sleep(1)
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{clean}, Memphis, TN", "format": "json", "limit": 1},
            headers={"User-Agent": "HoodBrief/1.0 (hoodbrief@proton.me)"},
            timeout=10,
        )
        results = r.json()
        if results:
            lat, lng = float(results[0]["lat"]), float(results[0]["lon"])
            if in_region(lat, lng):
                print(f"  [Fugitive] Nominatim: {clean} -> {lat}, {lng}")
                return lat, lng
    except Exception as e:
        print(f"  [Fugitive] Nominatim error: {e}")

    print(f"  [Fugitive] Could not geocode: {address_text}")
    return None


def parse_fugitive_post(post_html):
    """
    Parse a single CrimeStoppers weekly post and extract fugitive records.
    Returns list of dicts with name, dob, charges, address, photo_url, warrant_num.
    """
    fugitives = []
    soup = BeautifulSoup(post_html, "html.parser")

    # Each fugitive is separated by <hr> tags
    # Structure: img -> bold name, DOB line, Wanted for line, Last Known Address line, Warrant line
    content = soup.find("div", class_="entry-content") or soup

    # Split on <hr> tags to get individual fugitive blocks
    blocks = []
    current = []
    for elem in content.children:
        tag = getattr(elem, 'name', None)
        if tag == 'hr':
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(elem)
    if current:
        blocks.append(current)

    for block in blocks:
        try:
            block_soup = BeautifulSoup(
                "".join(str(e) for e in block), "html.parser"
            )
            text = block_soup.get_text(" ", strip=True)

            # Extract photo URL
            img = block_soup.find("img")
            photo_url = img.get("src", "") if img else ""

            # Extract name — first bold text
            bold = block_soup.find("strong")
            name = bold.get_text(strip=True) if bold else ""
            if not name:
                continue

            # Extract DOB
            dob_match = re.search(r'DOB[:\s]+(\d{2}/\d{2}/\d{4})', text)
            dob = dob_match.group(1) if dob_match else ""

            # Extract charges — "Wanted for X" section
            charges_match = re.search(
                r'Wanted for\s+(.+?)(?:Last Known Address|Warrant)', text,
                re.IGNORECASE | re.DOTALL
            )
            charges = charges_match.group(1).strip().rstrip(',') if charges_match else ""

            # Extract last known address
            addr_match = re.search(
                r'Last Known Address[:\s]+(.+?)(?:Warrant|$)', text,
                re.IGNORECASE | re.DOTALL
            )
            address = addr_match.group(1).strip() if addr_match else ""

            # Extract warrant number
            warrant_match = re.search(r'Warrant\s*#?\s*(\d+)', text, re.IGNORECASE)
            warrant_num = warrant_match.group(1) if warrant_match else ""

            if name and (charges or warrant_num):
                fugitives.append({
                    "name":        name,
                    "dob":         dob,
                    "charges":     charges,
                    "address":     address,
                    "photo_url":   photo_url,
                    "warrant_num": warrant_num,
                })
        except Exception as e:
            print(f"  [Fugitive] Parse error on block: {e}")
            continue

    return fugitives


def scrape_fugitives():
    """
    Scrape CrimeStoppers Most Wanted, geocode addresses,
    and update the Supabase fugitives table.
    """
    print("[Fugitives] Starting weekly scrape from memphismostwanted.org...")
    try:
        r = requests.get(
            CRIMESTOPPERS_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HoodBrief/1.0)"},
            timeout=20,
        )
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Find all weekly posts
        posts = soup.find_all("article") or soup.find_all("div", class_="post")
        if not posts:
            # Fallback: parse whole page
            posts = [soup]

        all_fugitives = []
        for post in posts[:4]:  # Only last 4 weeks
            post_html = str(post)
            found = parse_fugitive_post(post_html)
            all_fugitives.extend(found)
            print(f"  [Fugitive] Found {len(found)} fugitives in post")

        print(f"[Fugitives] Total parsed: {len(all_fugitives)}")

        if not all_fugitives:
            print("[Fugitives] No fugitives parsed — skipping update")
            return

        # Geocode each fugitive address
        records = []
        for f in all_fugitives:
            coords = geocode_fugitive_address(f["address"])
            lat = coords[0] if coords else None
            lng = coords[1] if coords else None
            records.append({
                "name":        f["name"],
                "dob":         f["dob"],
                "charges":     f["charges"],
                "address":     f["address"],
                "lat":         lat,
                "lng":         lng,
                "photo_url":   f["photo_url"],
                "warrant_num": f["warrant_num"],
                "scraped_at":  datetime.now(timezone.utc).isoformat(),
            })

        geocoded = sum(1 for r in records if r["lat"] is not None)
        print(f"[Fugitives] Geocoded {geocoded}/{len(records)} addresses")

        # Clear old records and insert fresh batch
        sb_delete(
            "fugitives?id=neq.00000000-0000-0000-0000-000000000000"
        )

        # Insert in batches
        for i in range(0, len(records), 100):
            batch = records[i:i+100]
            sb_post("fugitives", batch)

        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print(f"[Fugitives] ✅ Saved {len(records)} fugitives — updated {ts}")

    except requests.exceptions.ConnectionError as e:
        print(f"[Fugitives] Connection error: {e}")
    except Exception as e:
        print(f"[Fugitives] Error: {e}")


def fugitive_scrape_loop():
    """Scrape fugitives at startup then every 7 days."""
    while True:
        scrape_fugitives()
        time.sleep(FUGITIVE_REFRESH_SECONDS)


# ══════════════════════════════════════════════════════════════════
#  CAD STREET NAME CORRECTIONS
#  Source: Shelby County 911 NG911 GeoDatabase — ROAD layer
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
    # Common Whisper mishearings of Memphis streets
    "shadywell":       "Shadywell Lane",
    "shady well":      "Shadywell Lane",
    "kimberville":     "Kimberley Street",
    "covington pikes": "Covington Pike",
    "coveington":      "Covington Pike",
    "brownville":      "Brownsville Road",
    "delsten":         "Delsan Road",
    "gracewood":       "Gracewood Street",
    "denver":          "Denver Street",
}

def apply_cad_corrections(location_text):
    if not location_text:
        return location_text
    result = location_text
    for cad, official in CAD_CORRECTIONS.items():
        pattern = rf'\b{re.escape(cad)}\b'
        result = re.sub(pattern, official, result, flags=re.IGNORECASE)
    return result
# ══════════════════════════════════════════════════════════════════
#  INCIDENT KEYWORD PRE-FILTER
#  Screens transcripts before sending to GPT to reduce API costs
#  Only transcripts containing at least one keyword reach GPT
# ══════════════════════════════════════════════════════════════════

INCIDENT_KEYWORDS = [
    # Violent crimes
    "shooting", "shot", "shots fired", "gun", "armed", "weapon",
    "robbery", "robber", "rob", "assault", "assaulting", "fight",
    "stabbing", "stab", "knife", "homicide", "murder", "body",
    "kidnap", "hostage", "rape", "sexual",
    # Officer safety
    "officer needs help", "need assistance", "need backup", "backup",
    "pursuit", "chase", "fleeing", "foot chase", "vehicle pursuit",
    "person with a gun", "person with a weapon", "suspect",
    # Dispatch language
    "respond", "responding", "en route", "on scene", "units",
    "dispatch", "dispatching", "priority", "code",
    # Medical
    "medical", "ambulance", "ems", "unconscious", "unresponsive",
    "overdose", "cardiac", "breathing", "not breathing", "injured",
    # Property crimes worth logging
    "burglary", "breaking", "domestic", "disturbance",
    "accident", "collision", "crash", "vehicle",
    # Translated 10-codes that indicate incidents
    "use caution", "crime in progress", "person with a gun",
    "shooting", "stabbing", "pursuit in progress",
    "officer needs help", "need assistance", "ambulance request",
    "bomb threat", "shots", "alarm",
]

def has_incident_keywords(text):
    """
    Returns True if the transcript contains at least one incident keyword.
    Case-insensitive. Prevents sending routine radio chatter to GPT.
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in INCIDENT_KEYWORDS)




# ══════════════════════════════════════════════════════════════════
#  MPD STATION DETECTION
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
#  GANG HOTSPOT ZONES
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
#  GEOCODING CHAIN
# ══════════════════════════════════════════════════════════════════

def geocode_location(location_text, city):
    if not location_text:
        return CITIES[city]["center"]

    location_text = apply_cad_corrections(location_text)

    landmark = check_landmark(location_text)
    if landmark:
        return landmark

    city_info    = CITIES[city]
    city_label   = city_info["label"]
    google_key   = os.environ.get("GOOGLE_MAPS_KEY", "")
    geocodio_key = os.environ.get("GEOCODIO_KEY", "")

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
    # Try exact match first, then prefix match to handle missing street type suffix
    try:
        normalized = location_text.strip().upper()
        # Remove common trailing words that may not be in DB
        # e.g. "3749 Denver Street Memphis TN" -> "3749 DENVER"
        clean = re.sub(r'\s+(MEMPHIS|TN|TENNESSEE).*$', '', normalized).strip()

        # Try exact match first
        rows = sb_get(
            "memphis_addresses",
            params={"address": f"eq.{clean}", "select": "lat,lng", "limit": 1}
        )
        # If no exact match, try prefix match (e.g. "3749 DENVER" matches "3749 DENVER ST")
        if not rows:
            rows = sb_get(
                "memphis_addresses",
                params={"address": f"ilike.{clean}*", "select": "lat,lng", "limit": 1}
            )
        if rows:
            lat, lng = float(rows[0]['lat']), float(rows[0]['lng'])
            if in_city(lat, lng):
                print(f"  Geocoded (911 DB): {clean} -> {lat}, {lng}")
                return lat, lng
    except Exception as e:
        print(f"  911 DB error: {e}")

    # Step 2: Google Places API
    # Skip single words — prevents false geocodes like "Claim" or "Show Down"
    if google_key and len(location_text.strip().split()) > 1:
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
            except Exception as e:
                print(f"  Google geocoding error: {e}")

    # Step 4: Geocodio
    if geocodio_key:
        for query in location_queries:
            try:
                r = requests.get(
                    "https://api.geocod.io/v1.7/geocode",
                    params={"q": f"{query}, {city_label}", "api_key": geocodio_key, "limit": 1},
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
            except Exception as e:
                print(f"  Geocodio error: {e}")

    # Step 5: Nominatim
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
        except Exception as e:
            print(f"  Nominatim error: {e}")

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
#  TRANSCRIPTION — faster-whisper (local CPU, zero API cost)
#  Replaces OpenAI Whisper API to eliminate per-call charges
#  Model: base (74M params) — better accuracy, still fast on CPU
# ══════════════════════════════════════════════════════════════════

def transcribe(audio_bytes):
    tmp_path = None
    for attempt in range(MAX_RETRIES):
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                f.write(audio_bytes)
                tmp_path = f.name

            model = get_whisper_model()
            segments, info = model.transcribe(
                tmp_path,
                language="en",
                beam_size=1,           # Fastest decode
                best_of=1,             # No sampling
                temperature=0.0,       # Greedy decode
                vad_filter=True,       # Skip silent sections automatically
                vad_parameters={
                    "min_silence_duration_ms": 300,
                    "threshold": 0.65,      # Higher = more aggressive noise rejection
                    "min_speech_duration_ms": 250,  # Ignore very short speech bursts
                },
                initial_prompt=(
                    "Police scanner radio dispatch Memphis Tennessee MPD. "
                    "Ten codes, unit numbers, street addresses."
                ),
            )
            text = " ".join(s.text for s in segments).strip()

            # Reject hallucinations — repeating phrases are a telltale sign
            if text:
                words = text.lower().split()
                if len(words) > 6:
                    # Check for excessive repetition (e.g. "26. 26. 26. 26...")
                    unique_words = set(words)
                    if len(unique_words) / len(words) < 0.25:
                        print(f"  [Whisper] Repetition detected — rejecting transcript")
                        return ""
                # Reject known hallucination phrases
                hallucination_markers = [
                    # Broadcastify audio ads
                    "buzzcutting his way to a small fortune",
                    "every time he cuts his own hair",
                    "sound of jack", "sound of claire",
                    "cooking dinner at home",
                    "fraud alert from wells fargo",
                    "flagging a charge",
                    # Whisper hallucinations
                    "15-year-old harper", "vintage rock t-shirt",
                    "french signing verse",
                    "2.5 million", "police scanner radio dispatch",
                    "all feels right in the world",
                ]
                tl = text.lower()
                if any(marker in tl for marker in hallucination_markers):
                    print(f"  [Whisper] Known hallucination — rejecting transcript")
                    return ""

            return text

        except Exception as e:
            print(f"  Transcribe attempt {attempt+1} failed: {e}")
            time.sleep(2)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
    return ""


# ══════════════════════════════════════════════════════════════════
#  RULE-BASED INCIDENT PARSER
#  Zero API cost — regex + keyword matching
#  Works on translated transcripts (10-codes already converted)
#  Handles real scanner patterns: priority, location, unit extraction
# ══════════════════════════════════════════════════════════════════

# Priority 1 — violent, weapons, pursuit, officer needs help
P1_PATTERNS = [
    r'\bpriority\s*one\b', r'\bpriority\s*1\b', r'\bp[\-\s]?1\b',
    r'\bshooting\b', r'\bshots?\s+fired\b', r'\bperson\s+with\s+a\s+gun\b',
    r'\bperson\s+with\s+a\s+weapon\b', r'\barmed\b', r'\bgun\b',
    r'\brobbery\b', r'\bpursuit\b', r'\bchase\b', r'\bfleeing\b',
    r'\bfoot\s+chase\b', r'\bvehicle\s+pursuit\b',
    r'\bofficer\s+needs\s+help\b', r'\bneed\s+assistance\b',
    r'\bneed\s+backup\b', r'\brequesting\s+backup\b',
    r'\baggravated\s+assault\b', r'\bhomicide\b', r'\bmurder\b',
    r'\bkidnap\b', r'\bhostage\b', r'\bweapon\b', r'\bknife\b',
    r'\bcrime\s+in\s+progress\b', r'\bin\s+progress\b',
    r'\bstabbing\b', r'\bstab\b', r'\bbomb\s+threat\b',
    r'\brape\b', r'\bsexual\s+assault\b',
]

# Priority 2 — urgent but not immediate
P2_PATTERNS = [
    r'\bpriority\s*two\b', r'\bpriority\s*2\b', r'\bp[\-\s]?2\b',
    r'\bdomestic\b', r'\bburglary\b', r'\bbreak[\-\s]?in\b',
    r'\baccident\b', r'\bcollision\b', r'\bcrash\b',
    r'\bassault\b', r'\bsuspicious\b', r'\bthreat\b',
    r'\bhit\s+and\s+run\b', r'\bdrug\b', r'\bnarcotic\b',
    r'\bvandalism\b', r'\btrespassing\b', r'\bstalking\b',
    r'\bbreaking\s+and\s+entering\b',
]

# Medical / EMS
MEDICAL_PATTERNS = [
    r'\bmedical\b', r'\bambulance\b', r'\bems\b',
    r'\bunconsci\w+\b', r'\bunresponsive\b', r'\boverdos\w+\b',
    r'\bnot\s+breathing\b', r'\bcardiac\b', r'\bseizure\b',
    r'\binjur\w+\b', r'\bdown\b',
]

# Noise / hallucination detection — reject these
NOISE_PHRASES = [
    "police scanner radio dispatch", "ten codes", "radio dispatch",
    "scanner radio", "t-shirt", "vintage rock", "robot", "investment",
    "sister", "harper", "real file", "thank you for", "your check",
    "great investment", "15-year-old",
    "hours on yesterday", "two front fingers",
    "sigma 5", "all feels right",
    "small fortune", "cuts his own hair",
]

# Location extraction — ordered from most to least specific
LOCATION_PATTERNS = [
    # Numbered address + street with suffix
    r'(?:at|on|to|near)\s+(\d+\s+[\w\s]{2,35}?\s+(?:ave(?:nue)?|st(?:reet)?|rd|road|blvd|boulevard|dr(?:ive)?|ln|lane|way|cir(?:cle)?|ct|court|pl(?:ace)?|pkwy|parkway|hwy|highway))',
    # Pure intersection with suffix
    r'((?:[NSEW]\s+)?[\w]+\s+(?:ave(?:nue)?|st(?:reet)?|rd|road|blvd|dr(?:ive)?|ln|way)\s+and\s+[\w\s]{3,25})',
    # Numbered address no suffix (e.g. 5137 Finchwood)
    r'(?:at|on|to|near|of)\s+(\d+\s+[A-Z][\w\s]{2,25})',
    # Any numbered address
    r'(\d{3,5}\s+[A-Z][\w]{3,20})',
    # Interstate / highway
    r'\b(interstate\s+\d+|i-\d+|highway\s+\d+|hwy\s+\d+|state\s+route\s+\d+)\b',
    # Bare street name preceded by "on", "at" (e.g. "domestic on Gracewood")
    r'(?:on|at|to|near)\s+([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]{2,})?)',
    # Known Memphis landmarks
    r'\b(beale\s+street|elvis\s+presley|graceland|overton\s+park|shelby\s+farms|mud\s+island|fedex\s+forum|autozone\s+park|the\s+med|lebonheur|st\s+jude|union\s+avenue|poplar\s+avenue|summer\s+avenue|highland\s+avenue|airways\s+boulevard|lamar\s+avenue|winchester\s+road|covington\s+pike|stage\s+road|raleigh\s+lagrange|germantown\s+road|mendenhall\s+road|hickory\s+hill|american\s+way|brooks\s+road|horn\s+lake\s+road|elvis\s+presley\s+boulevard)\b',
]

# Unit extraction
UNIT_PATTERNS = [
    r'\bunits?\s+([\d\w\-]+(?:\s*(?:and|,)\s*[\d\w\-]+)*)',
    r'\b(wp[\-\s]?\d+)\b',
    r'\b(\d{2,3})\s+(?:en\s+route|responding|on\s+scene|copy)',
]

# Title mapping — first keyword match wins
TITLE_MAP = [
    ('shooting',            'Shooting'),
    ('shots fired',         'Shots Fired'),
    ('shots',               'Shots Fired'),
    ('homicide',            'Homicide'),
    ('murder',              'Homicide Call'),
    ('person with a gun',   'Armed Person'),
    ('armed',               'Armed Subject'),
    ('robbery',             'Robbery in Progress'),
    ('kidnap',              'Kidnapping'),
    ('hostage',             'Hostage Situation'),
    ('pursuit',             'Vehicle Pursuit'),
    ('chase',               'Pursuit in Progress'),
    ('foot chase',          'Foot Pursuit'),
    ('stabbing',            'Stabbing'),
    ('aggravated assault',  'Aggravated Assault'),
    ('assault',             'Assault'),
    ('domestic',            'Domestic Disturbance'),
    ('burglary',            'Burglary'),
    ('break-in',            'Breaking and Entering'),
    ('drug',                'Drug Activity'),
    ('need assistance',     'Officer Needs Assistance'),
    ('backup',              'Officer Needs Backup'),
    ('bomb threat',         'Bomb Threat'),
    ('suspicious',          'Suspicious Person'),
    ('threat',              'Threat Report'),
    ('accident',            'Traffic Accident'),
    ('collision',           'Traffic Collision'),
    ('crash',               'Vehicle Crash'),
    ('hit and run',         'Hit and Run'),
    ('medical',             'Medical Emergency'),
    ('ambulance',           'Medical Emergency'),
    ('overdose',            'Overdose'),
    ('unconscious',         'Unconscious Person'),
    ('weapon',              'Weapons Call'),
    ('vandalism',           'Vandalism'),
    ('trespass',            'Trespassing'),
    ('hold up',             'Hold-Up Alarm'),
    ('holdup',              'Hold-Up Alarm'),
    ('commercial alarm',    'Commercial Alarm'),
    ('residential alarm',   'Residential Alarm'),
    ('alarm',               'Alarm Response'),
    ('cit',                 'Crisis Intervention Call'),
    ('welfare check',       'Welfare Check'),
    ('welfare',             'Welfare Check'),
]

def parse_incident(transcript_translated, city):
    """
    Rule-based incident parser — zero API cost.
    Extracts priority, title, location, and unit from translated transcripts.
    """
    text = transcript_translated.strip()
    tl   = text.lower()

    # Reject if too short
    if len(tl) < 15:
        return {"incident": False}

    # Reject hallucinations — noise phrases with no incident content
    noise_hits = sum(1 for p in NOISE_PHRASES if p in tl)
    has_incident_signal = (
        any(re.search(p, tl, re.I) for p in P1_PATTERNS) or
        any(re.search(p, tl, re.I) for p in P2_PATTERNS) or
        any(re.search(p, tl, re.I) for p in MEDICAL_PATTERNS)
    )
    if noise_hits >= 1 and not has_incident_signal:
        return {"incident": False}

    # Determine priority
    if any(re.search(p, tl, re.I) for p in P1_PATTERNS):
        priority = "p1"
    elif any(re.search(p, tl, re.I) for p in MEDICAL_PATTERNS):
        priority = "medical"
    elif any(re.search(p, tl, re.I) for p in P2_PATTERNS):
        priority = "p2"
    else:
        return {"incident": False}

    # Extract title — first keyword match
    title = "Incident Response"
    for keyword, label in TITLE_MAP:
        if keyword in tl:
            title = label
            break

    # Extract location — try each pattern
    location = None
    for pattern in LOCATION_PATTERNS:
        m = re.search(pattern, text, re.I)
        if m:
            raw = m.group(1).strip()
            # Clean up and title-case
            location = re.sub(r'\s+', ' ', raw).strip().title()
            # Filter out false positives that are too short
            if len(location) > 4:
                break
            location = None

    # Extract unit
    unit = None
    for pattern in UNIT_PATTERNS:
        m = re.search(pattern, tl, re.I)
        if m:
            unit = m.group(1).strip().upper()
            break

    # Don't save if we couldn't find a location — incomplete data
    if not location:
        return {"incident": False}

    return {
        "incident": True,
        "title":    title,
        "location": location,
        "priority": priority,
        "unit":     unit or "",
    }


# ══════════════════════════════════════════════════════════════════
#  SUPABASE INCIDENT WRITER
# ══════════════════════════════════════════════════════════════════

def save_incident(incident, city, transcript_original, transcript_translated,
                  gang_hotspot, gang_zone, station, is_dispatch):
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
    sb_post("incidents", payload)


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
            audio = capture_chunk(stream_url, CHUNK_SECONDS)
            if len(audio) < 1000:
                print(f"[{label}] Audio chunk too small — skipping")
                time.sleep(5)
                continue

            transcript_raw = transcribe(audio)
            if not transcript_raw or len(transcript_raw.strip()) < 8:
                print(f"[{label}] No speech detected — skipping")
                continue

            print(f"[{label}] Raw: {transcript_raw[:120]}...")

            transcript_translated = translate_ten_codes(transcript_raw, city)
            if transcript_raw != transcript_translated:
                print(f"[{label}] Translated: {transcript_translated[:120]}...")

            parsed = parse_incident(transcript_translated, city)
            if not parsed.get("incident"):
                print(f"[{label}] No incident detected — skipping")
                continue

            priority = parsed.get("priority", "")
            if priority not in ("p1", "p2", "medical"):
                print(f"[{label}] Skipping {priority.upper()} — below threshold")
                continue

            location = parsed.get("location")
            lat, lng = geocode_location(location, city)
            parsed["lat"] = lat
            parsed["lng"] = lng

            station     = detect_station(lat, lng)
            unit        = parsed.get("unit", "")
            is_dispatch = is_dispatcher_call(unit, transcript_translated)
            gang_hotspot, gang_zone = check_gang_hotspot(
                location, parsed.get("title"), city
            )

            if station:      print(f"  Station: {station}")
            if is_dispatch:  print(f"  📡 Dispatcher call: {unit}")
            if gang_hotspot: print(f"  ⚠ Gang hotspot: {gang_zone}")

            save_incident(
                parsed, city,
                transcript_raw, transcript_translated,
                gang_hotspot, gang_zone, station, is_dispatch,
            )

            tags = []
            if is_dispatch:  tags.append("📡 DISPATCH")
            if station:      tags.append(station)
            if gang_hotspot: tags.append(f"⚠ {gang_zone}")
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
    if not SUPABASE_URL:   errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY:   errors.append("SUPABASE_KEY not set")
    for city, info in CITIES.items():
        if not info["stream_url"]:
            errors.append(f"{city.upper()}_STREAM_URL not set")

    if errors:
        print("\nMissing configuration:")
        for e in errors: print(f"  ✗ {e}")
        exit(1)

    # Install beautifulsoup4 if not present
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Installing beautifulsoup4...")
        os.system("pip install beautifulsoup4 --break-system-packages -q")
        from bs4 import BeautifulSoup

    threads = []

    # Heatmap — load static points once at startup
    hm = threading.Thread(target=load_heatmap, daemon=True, name="heatmap")
    hm.start()
    threads.append(hm)
    print("  ✓ Started: Heatmap loader")

    # Fugitive scraper — runs at startup then weekly
    fg = threading.Thread(target=fugitive_scrape_loop, daemon=True, name="fugitives")
    fg.start()
    threads.append(fg)
    print("  ✓ Started: Fugitive scraper (weekly)")

    # Memphis scanner
    for city in CITIES:
        t = threading.Thread(target=run_city, args=(city,), daemon=True, name=city)
        t.start()
        threads.append(t)
        print(f"  ✓ Started: {CITIES[city]['label']}")

    print("\nAll threads running.\n")

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
