#!/usr/bin/env python3
"""
Debug script for voice generator — tests ElevenLabs API with maximum logging.
Run this directly on the server to diagnose audio generation failures.

Usage: python3 debug_voice.py
"""

import os
import sys
import json
import glob
import time
import requests
from dotenv import load_dotenv

# Load .env
load_dotenv()

print("=" * 60)
print("🔍 VOICE GENERATOR DEBUG")
print("=" * 60)

# 1. Check environment variables
api_key = os.environ.get("ELEVENLABS_API_KEY", "")
voice_id = os.environ.get("ELEVENLABS_VOICE_ID", "")

print(f"\n=== Environment ===")
print(f"  ELEVENLABS_API_KEY: {'SET (' + api_key[:8] + '...' + api_key[-4:] + ')' if api_key else '❌ NOT SET'}")
print(f"  ELEVENLABS_VOICE_ID: {voice_id if voice_id else '❌ NOT SET'}")

if not api_key:
    print("\n🚨 FATAL: ELEVENLABS_API_KEY is not set in .env")
    sys.exit(1)
if not voice_id:
    print("\n🚨 FATAL: ELEVENLABS_VOICE_ID is not set in .env")
    sys.exit(1)

# 2. Check scene plan files
base_dir = os.path.dirname(os.path.abspath(__file__))
scene_plan_dir = os.path.join(base_dir, "data", "scene_plans")

print(f"\n=== Scene Plans ===")
print(f"  Directory: {scene_plan_dir}")
print(f"  Exists: {os.path.exists(scene_plan_dir)}")

# Find all scene files
flat_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*.json")))
sub_files = sorted(glob.glob(os.path.join(scene_plan_dir, "*", "*.json")))
all_files = flat_files + sub_files

print(f"  Flat JSON files: {len(flat_files)}")
print(f"  Subdirectory JSON files: {len(sub_files)}")
for f in all_files:
    print(f"    {f}")

if not all_files:
    print("\n🚨 FATAL: No scene plan files found!")
    sys.exit(1)

# 3. Load first scene plan and extract one narration
print(f"\n=== Loading First Scene Plan ===")
first_file = all_files[0]
print(f"  File: {first_file}")

with open(first_file) as f:
    data = json.load(f)

if isinstance(data, list):
    plan = data[0]
    print(f"  Format: list of {len(data)} plans")
elif isinstance(data, dict):
    plan = data
    print(f"  Format: single dict")
else:
    print(f"  🚨 Unexpected format: {type(data)}")
    sys.exit(1)

print(f"  Title: {plan.get('title', 'N/A')}")
scenes = plan.get("scenes", [])
print(f"  Scenes: {len(scenes)}")

if not scenes:
    print("  🚨 No scenes in plan!")
    sys.exit(1)

# Get narration from first scene
first_scene = scenes[0]
narration = first_scene.get("narration") or first_scene.get("text") or ""
print(f"\n=== First Scene ===")
print(f"  Keys: {list(first_scene.keys())}")
print(f"  Narration key used: {'narration' if first_scene.get('narration') else 'text' if first_scene.get('text') else 'NONE'}")
print(f"  Narration length: {len(narration)} chars")
print(f"  Narration preview: {narration[:150]}...")

if not narration.strip():
    print("  🚨 Empty narration! Voice generator can't synthesize empty text.")
    sys.exit(1)

# 4. Test ElevenLabs API directly
print(f"\n=== Testing ElevenLabs API ===")
api_url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
print(f"  URL: {api_url}")

headers = {
    "xi-api-key": api_key,
    "Content-Type": "application/json",
    "Accept": "audio/mpeg",
}

# Use a short test text first
test_text = "This is a test of the ElevenLabs text to speech API."
payload = {
    "text": test_text,
    "model_id": "eleven_multilingual_v2",
    "voice_settings": {
        "stability": 0.5,
        "similarity_boost": 0.75,
    },
}

print(f"  Test text: '{test_text}'")
print(f"  Model: eleven_multilingual_v2")
print(f"  Sending request...")

start = time.time()
try:
    response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=60,
    )
    elapsed = round(time.time() - start, 2)

    print(f"\n=== API Response ===")
    print(f"  Status code: {response.status_code}")
    print(f"  Response time: {elapsed}s")
    print(f"  Content-Type: {response.headers.get('Content-Type', 'N/A')}")
    print(f"  Content-Length: {len(response.content)} bytes")
    print(f"  Rate limit remaining: {response.headers.get('x-ratelimit-remaining', 'N/A')}")
    print(f"  Character count: {response.headers.get('character-count', 'N/A')}")
    print(f"  Character limit: {response.headers.get('character-limit', 'N/A')}")

    if response.status_code == 200:
        if response.content and len(response.content) > 100:
            # Save test audio
            test_path = os.path.join(base_dir, "data", "audio", "debug_test.mp3")
            os.makedirs(os.path.dirname(test_path), exist_ok=True)
            with open(test_path, "wb") as f:
                f.write(response.content)
            print(f"\n  ✅ SUCCESS! Audio saved to: {test_path}")
            print(f"  File size: {os.path.getsize(test_path)} bytes")
        else:
            print(f"\n  ⚠️  Got 200 but content is too small ({len(response.content)} bytes)")
            print(f"  Response body: {response.content[:500]}")
    else:
        print(f"\n  ❌ FAILED!")
        print(f"  Response body: {response.text[:1000]}")

        if response.status_code == 401:
            print("\n  🔑 401 = Invalid API key. Check ELEVENLABS_API_KEY in .env")
        elif response.status_code == 403:
            print("\n  🚫 403 = Forbidden. API key may lack TTS permissions.")
        elif response.status_code == 404:
            print("\n  🔍 404 = Voice ID not found. Check ELEVENLABS_VOICE_ID in .env")
        elif response.status_code == 422:
            print("\n  📝 422 = Invalid request. Check payload format.")
        elif response.status_code == 429:
            print("\n  ⏳ 429 = Rate limited / quota exceeded.")

except requests.exceptions.ConnectionError as e:
    print(f"\n  ❌ CONNECTION ERROR: {e}")
    print("  Check if the server can reach api.elevenlabs.io")
except requests.exceptions.Timeout:
    print(f"\n  ❌ TIMEOUT after 60s")
except Exception as e:
    print(f"\n  ❌ UNEXPECTED ERROR: {type(e).__name__}: {e}")

# 5. Now test with actual narration text
if response.status_code == 200:
    print(f"\n=== Testing with actual narration ===")
    payload["text"] = narration
    try:
        r2 = requests.post(api_url, headers=headers, json=payload, timeout=60)
        print(f"  Status: {r2.status_code}")
        print(f"  Content size: {len(r2.content)} bytes")
        if r2.status_code == 200 and len(r2.content) > 100:
            real_path = os.path.join(base_dir, "data", "audio", "debug_real_scene.mp3")
            with open(real_path, "wb") as f:
                f.write(r2.content)
            print(f"  ✅ Real scene audio saved to: {real_path}")
        else:
            print(f"  ❌ Failed: {r2.text[:500]}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

print(f"\n{'=' * 60}")
print("Debug complete.")
