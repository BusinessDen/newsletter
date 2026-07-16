#!/usr/bin/env python3
"""Diagnostic: list recent HubSpot marketing emails to find campaign names."""
import os, json, requests

TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not TOKEN:
    print("ERROR: HUBSPOT_TOKEN not set")
    exit(1)

headers = {"Authorization": f"Bearer {TOKEN}"}
BASE = "https://api.hubapi.com"

# Get most recent 20 emails
r = requests.get(f"{BASE}/marketing/v3/emails", params={"limit": 20, "sort": "-publishDate"}, headers=headers)
data = r.json()

print(f"Total emails returned: {len(data.get('results', []))}\n")
print(f"{'Campaign Name':<30} {'State':<12} {'Subject':<50} {'Published':<25}")
print("-" * 120)

for email in data.get("results", []):
    campaign = email.get("campaignName", "(none)")
    state = email.get("state", "?")
    subject = email.get("subject", "(no subject)")[:48]
    pub = email.get("publishDate", "")[:24]
    print(f"{campaign:<30} {state:<12} {subject:<50} {pub:<25}")

# Also list all unique campaign names
print("\n\nAll unique campaign names in this batch:")
names = set(email.get("campaignName", "(none)") for email in data.get("results", []))
for n in sorted(names):
    count = sum(1 for e in data["results"] if e.get("campaignName") == n)
    print(f"  {n} ({count} emails)")
