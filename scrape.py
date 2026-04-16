#!/usr/bin/env python3
"""
BD Newsletter Analytics — Production Scraper

Pulls BD Newsfeed newsletter data from HubSpot, aggregates per-article
click counts, and merges with historical data. Designed for GitHub Actions
with deploy-first → scrape → deploy-after pattern.

Required env vars:
    HUBSPOT_TOKEN — HubSpot Private App access token or Personal Access Key

Optional env vars:
    BACKFILL_DAYS — How many days of history to fetch (default: 7, use 9999 for full history)

Output: newsletter-data.json
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not TOKEN:
    print("ERROR: HUBSPOT_TOKEN not set.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}
BASE = "https://api.hubapi.com"
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "7"))
DATA_FILE = Path("newsletter-data.json")
BD_CAMPAIGN_NAME = "BD Newsfeed"
BD_DOMAIN = "businessden.com"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get(url, params=None, retries=2):
    """GET with retry and rate limiting."""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            time.sleep(0.25)

            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10))
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"  ERROR {r.status_code}: {r.text[:200]}")
                if attempt < retries:
                    time.sleep(2)
                    continue
                return None
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            if attempt < retries:
                time.sleep(2)
                continue
            return None
    return None


def strip_utm(url):
    """Remove UTM and HubSpot tracking parameters from a URL."""
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    cleaned = {
        k: v for k, v in params.items()
        if not k.startswith("utm_") and k not in ("_hsmi", "_hsenc", "hsCtaTracking")
    }
    if cleaned:
        new_query = urlencode(cleaned, doseq=True)
        return parsed._replace(query=new_query).geturl()
    return parsed._replace(query="").geturl().rstrip("?")


def is_bd_article_url(url):
    """Check if a URL is a BusinessDen article (not homepage, login, etc)."""
    parsed = urlparse(url)
    if BD_DOMAIN not in parsed.netloc:
        return False
    path = parsed.path.rstrip("/")
    # Articles follow /YYYY/MM/DD/slug/ pattern
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 4:
        try:
            int(parts[0])  # year
            int(parts[1])  # month
            int(parts[2])  # day
            return True
        except ValueError:
            pass
    return False


def url_to_title(url):
    """Extract a human-readable title from a BD article URL slug."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 4:
        slug = parts[3]
        return slug.replace("-", " ").title()
    return path


def load_existing():
    """Load existing newsletter-data.json if present."""
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {"metadata": {}, "sends": []}


def save_data(data):
    """Write newsletter-data.json."""
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    size_kb = DATA_FILE.stat().st_size / 1024
    print(f"  Saved {DATA_FILE} ({size_kb:.1f} KB)")


# ---------------------------------------------------------------------------
# HubSpot API calls
# ---------------------------------------------------------------------------
def fetch_bd_newsletters(since_date):
    """Fetch all BD Newsfeed emails published after since_date."""
    print(f"\n  Fetching BD Newsfeed emails since {since_date.date()}...")
    all_emails = []
    after = None

    while True:
        params = {
            "limit": 50,
            "sort": "-publishDate",
        }
        if after:
            params["after"] = after

        data = get(f"{BASE}/marketing/v3/emails", params=params)
        if not data:
            break

        results = data.get("results", [])
        if not results:
            break

        for email in results:
            # Filter: BD Newsfeed only, published only
            if email.get("campaignName") != BD_CAMPAIGN_NAME:
                continue
            if email.get("state") != "PUBLISHED":
                continue

            pub_date = email.get("publishDate", "")
            if pub_date:
                pub_dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                if pub_dt < since_date:
                    # We've gone past our date range — stop
                    print(f"    Reached {pub_dt.date()}, stopping")
                    return all_emails

            all_emails.append(email)

        # Check pagination
        paging = data.get("paging", {})
        next_page = paging.get("next", {})
        after = next_page.get("after")
        if not after:
            break

    return all_emails


def fetch_email_stats(email_id):
    """Fetch full email detail with stats."""
    data = get(
        f"{BASE}/marketing/v3/emails/{email_id}",
        params={"includeStats": "true"},
    )
    if not data:
        return None
    return data.get("stats", {})


def fetch_click_events(email_campaign_id):
    """Fetch all CLICK events for an email campaign, paginating through all."""
    all_clicks = []
    offset = None
    page = 0

    while True:
        params = {
            "eventType": "CLICK",
            "campaignId": str(email_campaign_id),
            "limit": 1000,
        }
        if offset:
            params["offset"] = offset

        data = get(f"{BASE}/email/public/v1/events", params=params)
        if not data:
            break

        events = data.get("events", [])
        all_clicks.extend(events)
        page += 1

        if not data.get("hasMore", False):
            break
        offset = data.get("offset")
        if not offset:
            break

    return all_clicks


def aggregate_article_clicks(click_events):
    """Aggregate click events by article URL, filtering to BD articles only."""
    url_counts = {}

    for ev in click_events:
        raw_url = ev.get("url", "")
        if not raw_url:
            continue
        clean_url = strip_utm(raw_url)
        if not is_bd_article_url(clean_url):
            continue

        # Normalize: strip trailing slash for consistent matching
        clean_url = clean_url.rstrip("/")

        if clean_url not in url_counts:
            url_counts[clean_url] = 0
        url_counts[clean_url] += 1

    # Convert to sorted list
    articles = []
    for url, clicks in sorted(url_counts.items(), key=lambda x: -x[1]):
        articles.append({
            "url": url,
            "title": url_to_title(url),
            "clicks": clicks,
        })

    return articles


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def process_email(email):
    """Process a single newsletter email into a send record."""
    email_id = email["id"]
    name = email.get("name", "")
    subject = email.get("subject", "")
    pub_date = email.get("publishDate", "")
    campaign_id = email.get("primaryEmailCampaignId")

    print(f"\n  Processing: {name}")
    print(f"    email_id={email_id}, campaignId={campaign_id}")

    # Get stats
    stats = fetch_email_stats(email_id)
    if not stats:
        print(f"    WARNING: No stats returned for {email_id}")
        stats = {}

    counters = stats.get("counters", {})
    ratios = stats.get("ratios", {})
    device = stats.get("deviceBreakdown", {})

    # Get click events and aggregate
    articles = []
    total_article_clicks = 0
    if campaign_id:
        click_events = fetch_click_events(campaign_id)
        print(f"    {len(click_events)} total click events")
        articles = aggregate_article_clicks(click_events)
        total_article_clicks = sum(a["clicks"] for a in articles)
        print(f"    {len(articles)} BD articles, {total_article_clicks} article clicks")

        # Calculate click_pct for each article (clicks / opens)
        opens = counters.get("open", 0)
        if opens > 0:
            for a in articles:
                a["click_pct"] = round(a["clicks"] / opens * 100, 2)
        else:
            for a in articles:
                a["click_pct"] = 0
    else:
        print(f"    WARNING: No campaignId found, skipping click events")

    # Parse date to just YYYY-MM-DD
    send_date = ""
    if pub_date:
        try:
            send_date = datetime.fromisoformat(
                pub_date.replace("Z", "+00:00")
            ).strftime("%Y-%m-%d")
        except ValueError:
            send_date = pub_date[:10]

    return {
        "date": send_date,
        "email_id": str(email_id),
        "name": name,
        "recipients": counters.get("selected", 0),
        "sent": counters.get("sent", 0),
        "delivered": counters.get("delivered", 0),
        "opens": counters.get("open", 0),
        "clicks": counters.get("click", 0),
        "bounces": counters.get("bounce", 0),
        "hard_bounces": counters.get("hardbounced", 0),
        "soft_bounces": counters.get("softbounced", 0),
        "unsubscribes": counters.get("unsubscribed", 0),
        "spam_reports": counters.get("spamreport", 0),
        "contacts_lost": counters.get("contactslost", 0),
        "dropped": counters.get("dropped", 0),
        "suppressed": counters.get("suppressed", 0),
        "pending": counters.get("pending", 0),
        "open_rate": round(ratios.get("openratio", 0), 2),
        "click_rate": round(ratios.get("clickratio", 0), 2),
        "click_through_rate": round(ratios.get("clickthroughratio", 0), 2),
        "bounce_rate": round(ratios.get("bounceratio", 0), 2),
        "unsubscribe_rate": round(ratios.get("unsubscribedratio", 0), 2),
        "device_opens": device.get("open_device_type", {}),
        "device_clicks": device.get("click_device_type", {}),
        "article_clicks": total_article_clicks,
        "articles": articles,
    }


def main():
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=BACKFILL_DAYS)

    print(f"BD Newsletter Analytics — scraper run")
    print(f"  Time: {now.isoformat()}")
    print(f"  Backfill: {BACKFILL_DAYS} days (since {since.date()})")

    # Load existing data
    existing = load_existing()
    existing_dates = {s["date"] for s in existing.get("sends", [])}
    print(f"  Existing data: {len(existing_dates)} sends on file")

    # Fetch recent BD newsletters
    emails = fetch_bd_newsletters(since)
    print(f"\n  Found {len(emails)} BD Newsfeed emails in range")

    if not emails:
        print("  No new emails to process.")
        return

    # Process each email
    new_sends = []
    for email in emails:
        pub_date = email.get("publishDate", "")
        if pub_date:
            try:
                send_date = datetime.fromisoformat(
                    pub_date.replace("Z", "+00:00")
                ).strftime("%Y-%m-%d")
            except ValueError:
                send_date = pub_date[:10]
        else:
            send_date = ""

        # Always re-fetch recent sends (stats update over time)
        # Only skip if older than 3 days and already in data
        days_old = (now.date() - datetime.strptime(send_date, "%Y-%m-%d").date()).days if send_date else 999
        if send_date in existing_dates and days_old > 3:
            print(f"\n  Skipping {email.get('name', '')} (already have data, {days_old}d old)")
            continue

        send = process_email(email)
        new_sends.append(send)

    # Merge: new sends replace existing for same date
    sends_by_date = {s["date"]: s for s in existing.get("sends", [])}
    for s in new_sends:
        sends_by_date[s["date"]] = s

    # Sort by date descending
    all_sends = sorted(sends_by_date.values(), key=lambda x: x["date"], reverse=True)

    # Build output
    output = {
        "metadata": {
            "generated": now.isoformat(),
            "total_sends": len(all_sends),
            "date_range": {
                "start": all_sends[-1]["date"] if all_sends else "",
                "end": all_sends[0]["date"] if all_sends else "",
            },
        },
        "sends": all_sends,
    }

    save_data(output)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  Total sends in file: {len(all_sends)}")
    print(f"  New/updated: {len(new_sends)}")
    if all_sends:
        latest = all_sends[0]
        print(f"  Latest: {latest['name']} ({latest['date']})")
        print(f"    Recipients: {latest['recipients']:,}")
        print(f"    Open rate: {latest['open_rate']}%")
        print(f"    Click rate: {latest['click_rate']}%")
        print(f"    Articles clicked: {len(latest['articles'])}")
        print(f"    Unsubscribes: {latest['unsubscribes']}")


if __name__ == "__main__":
    main()
