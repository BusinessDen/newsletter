import json, os, sys, time, requests

TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
if not TOKEN:
    print("No HUBSPOT_TOKEN"); sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
BASE = "https://api.hubapi.com"

def get(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    time.sleep(0.3)
    return r

print("=" * 60)
print("CTA API EXPLORATION")
print("=" * 60)

# Test various CTA endpoints
endpoints = [
    ("/marketing/v3/ctas", {"limit": 5}),
    ("/ctas/v3/ctas", {"limit": 5}),
    ("/ctas/v3/placements", {"limit": 5}),
    ("/marketing/v3/cta", {"limit": 5}),
    ("/marketing/v2/ctas", {"limit": 5}),
]

for ep, params in endpoints:
    r = get(f"{BASE}{ep}", params)
    print(f"\n{ep} -> {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  Keys: {list(data.keys())}")
        if 'results' in data:
            print(f"  Results: {len(data['results'])}")
            if data['results']:
                print(f"  First result keys: {list(data['results'][0].keys())}")
                print(f"  First result: {json.dumps(data['results'][0], indent=2)[:500]}")
        elif 'objects' in data:
            print(f"  Objects: {len(data['objects'])}")
            if data['objects']:
                print(f"  First object keys: {list(data['objects'][0].keys())}")
                print(f"  First object: {json.dumps(data['objects'][0], indent=2)[:500]}")
        else:
            print(f"  Data: {json.dumps(data, indent=2)[:800]}")
    else:
        print(f"  Response: {r.text[:300]}")

# Try legacy CTA endpoints
print("\n\n--- Legacy CTA endpoints ---")
legacy_endpoints = [
    "/ctas/v2/ctas",
    "/ctas/v2/ctas?limit=5",
    "/analytics/v2/reports/ctas/total",
]

for ep in legacy_endpoints:
    r = get(f"{BASE}{ep}")
    print(f"\n{ep} -> {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  Data: {json.dumps(data, indent=2)[:600]}")
    else:
        print(f"  Response: {r.text[:200]}")
