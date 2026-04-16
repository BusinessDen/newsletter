#!/usr/bin/env python3
"""
BD Newsletter Analytics — Exploration Scraper

Probes HubSpot APIs and dumps raw responses to JSON files.
Designed to run via GitHub Actions — output is uploaded as an artifact.

Recipient emails are redacted (hashed) so output is safe to download and share.

Required env var: HUBSPOT_TOKEN
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

TOKEN = os.environ.get("HUBSPOT_TOKEN")

if not TOKEN:
    print("ERROR: HUBSPOT_TOKEN not set.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}
BASE = "https://api.hubapi.com"

OUT_DIR = Path("raw_output")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def redact_email(email):
    """Hash an email so structure is preserved but PII is gone."""
    if not email or "@" not in str(email):
        return email
    email = str(email)
    h = hashlib.sha256(email.lower().encode()).hexdigest()[:12]
    domain = email.split("@", 1)[1]
    return f"hash_{h}@{domain}"


def scrub(obj):
    """Recursively redact email-looking values."""
    if isinstance(obj, dict):
        return {
            k: (
                redact_email(v)
                if isinstance(v, str) and "@" in v and "." in v.split("@")[-1]
                else scrub(v)
            )
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [scrub(x) for x in obj]
    return obj


def save(filename, data):
    path = OUT_DIR / filename
    with open(path, "w") as f:
        json.dump(scrub(data), f, indent=2, default=str)
    size_kb = path.stat().st_size / 1024
    print(f"    saved {path.name} ({size_kb:.1f} KB)")


def get(url, params=None, label=""):
    """GET with error handling and gentle rate limiting."""
    print(f"  GET {url}")
    if params:
        shown = {k: v for k, v in params.items() if k != "limit"}
        if shown:
            print(f"    params: {shown}")

    r = requests.get(url, headers=HEADERS, params=params)
    time.sleep(0.2)

    if r.status_code == 200:
        return r.json()

    err = {
        "status": r.status_code,
        "url": url,
        "params": params,
        "response": r.text[:2000],
    }
    save(f"ERROR_{label or 'unknown'}.json", err)

    if r.status_code == 401:
        print(f"    x 401 unauthorized — token invalid")
    elif r.status_code == 403:
        print(f"    x 403 forbidden — missing scope for this endpoint")
    elif r.status_code == 404:
        print(f"    x 404 not found")
    else:
        print(f"    x {r.status_code}: {r.text[:200]}")
    return None


# ------------------------------------------------------------------
# Explorations
# ------------------------------------------------------------------
def explore_marketing_emails():
    """List recent marketing emails via v3 API."""
    print("\n[1] Marketing Emails (v3) — list recent")
    data = get(
        f"{BASE}/marketing/v3/emails",
        params={"limit": 10, "sort": "-publishDate"},
        label="marketing_emails_list",
    )
    if not data:
        return []
    save("01_marketing_emails_list.json", data)
    emails = data.get("results", [])
    print(f"    returned {len(emails)} emails")
    for e in emails[:5]:
        print(
            f"      - {e.get('id'):>12} | {e.get('state'):12} | {e.get('name', '')[:60]}"
        )
    return emails


def explore_email_detail(email_id):
    """Get full detail for one email, with stats."""
    print(f"\n[2] Email Detail (v3) — id={email_id}")
    data = get(
        f"{BASE}/marketing/v3/emails/{email_id}",
        params={"includeStats": "true"},
        label=f"email_detail_{email_id}",
    )
    if data:
        save(f"02_email_detail_{email_id}.json", data)
        print(f"    top-level keys: {sorted(data.keys())}")
        if "stats" in data:
            stats_keys = (
                sorted(data["stats"].keys())
                if isinstance(data["stats"], dict)
                else "(not dict)"
            )
            print(f"    stats keys: {stats_keys}")
        for field in [
            "campaignId",
            "campaignName",
            "allEmailCampaignIds",
            "emailCampaignId",
        ]:
            if field in data:
                print(f"    {field}: {data[field]}")
    return data


def explore_campaigns():
    """List marketing campaigns via v3."""
    print("\n[3] Marketing Campaigns (v3)")
    data = get(
        f"{BASE}/marketing/v3/campaigns",
        params={"limit": 10},
        label="campaigns_list",
    )
    if data:
        save("03_campaigns_list.json", data)
        results = data.get("results", [])
        print(f"    returned {len(results)} campaigns")
        for c in results[:5]:
            print(
                f"      - {c.get('id'):>12} | {c.get('properties', {}).get('hs_name', c.get('name', ''))[:60]}"
            )
    return data


def explore_email_events_v1(campaign_id=None):
    """Hit the legacy v1 email events endpoint — CLICK events."""
    print(f"\n[4] Email Events (v1 legacy) — CLICK events")
    params = {"eventType": "CLICK", "limit": 100}
    if campaign_id:
        params["campaignId"] = campaign_id
        print(f"    filtering on campaignId={campaign_id}")

    data = get(
        f"{BASE}/email/public/v1/events",
        params=params,
        label="email_events_clicks",
    )
    if data:
        save("04_email_events_clicks.json", data)
        events = data.get("events", [])
        print(f"    returned {len(events)} CLICK events")
        if events:
            print(f"    first event keys: {sorted(events[0].keys())}")
            urls = {}
            for ev in events:
                u = ev.get("url", "")
                if u:
                    parsed = urlparse(u)
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    urls[clean] = urls.get(clean, 0) + 1
            top = sorted(urls.items(), key=lambda x: -x[1])[:10]
            print(f"    top URLs clicked (this page of results):")
            for url, count in top:
                print(f"      {count:>4} x {url[:80]}")
    return data


def explore_email_events_v1_all_types(campaign_id=None):
    """Pull ALL event types (not just clicks) so we can see what's available."""
    print(f"\n[5] Email Events (v1 legacy) — ALL event types")
    params = {"limit": 200}
    if campaign_id:
        params["campaignId"] = campaign_id

    data = get(
        f"{BASE}/email/public/v1/events",
        params=params,
        label="email_events_all",
    )
    if data:
        save("05_email_events_all_types.json", data)
        events = data.get("events", [])
        print(f"    returned {len(events)} events")
        if events:
            type_counts = {}
            for ev in events:
                t = ev.get("type", "unknown")
                type_counts[t] = type_counts.get(t, 0) + 1
            print(f"    event type breakdown:")
            for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
                print(f"      {c:>6} x {t}")
    return data


def explore_email_statistics():
    """Try the newer statistics endpoints if they exist."""
    print(f"\n[6] Email statistics endpoints (exploration)")
    data = get(
        f"{BASE}/marketing/v3/emails/statistics/list",
        params={"limit": 10},
        label="email_statistics_list",
    )
    if data:
        save("06_email_statistics_list.json", data)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    print(f"BD Newsletter Analytics — exploration run")
    print(f"Output: {OUT_DIR}")
    print("-" * 60)

    # 1. List emails
    emails = explore_marketing_emails()
    if not emails:
        print("\nNo emails returned — trying events and campaigns anyway.")
        explore_campaigns()
        explore_email_events_v1()
        explore_email_events_v1_all_types()
        explore_email_statistics()
        print("-" * 60)
        print(f"Done. Output in: {OUT_DIR}")
        return

    # 2. Detail on most recent published email
    published = [e for e in emails if e.get("state") == "PUBLISHED"]
    target = published[0] if published else emails[0]
    detail = explore_email_detail(target["id"])

    # Find a campaign_id for the events query
    campaign_id = None
    if detail:
        for field in ["allEmailCampaignIds", "campaignId", "emailCampaignId"]:
            val = detail.get(field)
            if val:
                campaign_id = val[0] if isinstance(val, list) else val
                break

    # 3. Campaigns
    explore_campaigns()

    # 4. Click events for that campaign
    explore_email_events_v1(campaign_id=campaign_id)

    # 5. All event types for that campaign
    explore_email_events_v1_all_types(campaign_id=campaign_id)

    # 6. Stats endpoints probe
    explore_email_statistics()

    print("-" * 60)
    print(f"Done. Output in: {OUT_DIR}")

    # Summary of files generated
    files = sorted(OUT_DIR.glob("*.json"))
    print(f"\nFiles generated ({len(files)}):")
    for f in files:
        print(f"  {f.name} ({f.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
