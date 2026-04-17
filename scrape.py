#!/usr/bin/env python3
"""
BD Newsletter Analytics — Production Scraper v4

Collects from HubSpot:
- Per-article clicks + unique clickers + sponsored content flag
- Editorial clicks (news publications) separated from ad clicks (advertisers)
- Geographic distribution of clickers
- Click shelf life by day (clicks_by_age) and by hour (clicks_by_hour)
- Send-level stats (opens, bounces, unsubscribes, device breakdown)

Collects from GA4 (optional, if GA4_SERVICE_ACCOUNT set):
- Sponsored content traffic by source (Newsletter, Homepage, Other)

Env vars:
    HUBSPOT_TOKEN (required)
    GA4_SERVICE_ACCOUNT (optional) — service account JSON string
    GA4_PROPERTY_ID (optional, default 363209481)
    BACKFILL_DAYS (optional, default 7, use 9999 for full history)
"""

import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode
from collections import defaultdict

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not TOKEN:
    print("ERROR: HUBSPOT_TOKEN not set.")
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
BASE = "https://api.hubapi.com"
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "7"))
DATA_FILE = Path("newsletter-data.json")
BD_CAMPAIGN_NAME = "BD Newsfeed"
BD_DOMAIN = "businessden.com"
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "363209481")

NEWS_DOMAINS = {
    "cbsnews.com","cnbc.com","coloradosun.com","canyoncourier.com",
    "summitdaily.com","vaildaily.com","westword.com","fastcompany.com",
    "telegraph.co.uk","inc.com","denver7.com","nypost.com","seattletimes.com",
    "apnews.com","reuters.com","wsj.com","nytimes.com","washingtonpost.com",
    "denverpost.com","cpr.org","9news.com","kdvr.com","fox31.com",
    "bloomberg.com","bbc.com","bbc.co.uk","cnn.com","npr.org",
    "coloradopolitics.com","coloradoindependent.com","thedenverchannel.com",
    "bizjournals.com","axios.com","politico.com","theguardian.com",
    "usatoday.com","fortune.com","forbes.com","businessinsider.com",
    "ft.com","economist.com","theatlantic.com","newyorker.com",
    "chicagotribune.com","latimes.com","sfchronicle.com",
    "dailycamera.com","reporterherald.com","coloradoan.com","gazette.com",
    "denvergazette.com","steamboatpilot.com","craigdailypress.com",
    "gjsentinel.com","durangoherald.com","aspentimes.com","aspendailynews.com",
    "postindependent.com","greeleytribune.com","timescall.com",
    "longmontleader.com","complete-colorado.com","sentinelcolorado.com",
    "5280.com","sfstandard.com","sfgate.com","wired.com",
    "therealdeal.com","ktnv.com","finance.yahoo.com","yahoo.com",
    "powder.com","restaurantbusinessonline.com","si.com","abcnews.com",
    "unofficialnetworks.com",
}

SPONSORED_SLUG_EXCEPTIONS = [
    "/2023/04/19/solving-5-equipment-maintenance-challenges-with-technology",
    "/2022/07/25/first-interstate-bank-arrives-in-colorado-after-successful-merger-with-great-western-bank",
]

def is_news_domain(domain):
    d = domain.lower().replace("www.", "")
    if d in NEWS_DOMAINS:
        return True
    for nd in NEWS_DOMAINS:
        if d.endswith("." + nd):
            return True
    return False

def get(url, params=None, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            time.sleep(0.25)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 10)))
                continue
            print(f"  ERROR {r.status_code}: {r.text[:200]}")
            if attempt < retries:
                time.sleep(2)
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            if attempt < retries:
                time.sleep(2)
    return None

def hash_email(email):
    if not email or "@" not in str(email):
        return None
    return hashlib.sha256(str(email).lower().strip().encode()).hexdigest()[:16]

def strip_utm(url):
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = {k: v for k, v in parse_qs(parsed.query, keep_blank_values=True).items()
              if not k.startswith("utm_") and k not in ("_hsmi","_hsenc","hsCtaTracking","hsCtaAttrib")}
    if params:
        return parsed._replace(query=urlencode(params, doseq=True)).geturl()
    return parsed._replace(query="").geturl().rstrip("?")

def is_bd_article_url(url):
    parsed = urlparse(url)
    if BD_DOMAIN not in parsed.netloc:
        return False
    parts = [p for p in parsed.path.rstrip("/").split("/") if p]
    if len(parts) >= 4:
        try:
            int(parts[0]); int(parts[1]); int(parts[2])
            return True
        except ValueError:
            pass
    return False

def is_bd_internal_url(url):
    return BD_DOMAIN in urlparse(url).netloc

def is_sponsored_content(url):
    parsed = urlparse(url)
    if "sponsored-content" in parsed.path.lower():
        return True
    path = parsed.path.rstrip("/")
    return any(path.endswith(exc.rstrip("/")) for exc in SPONSORED_SLUG_EXCEPTIONS)

def url_to_title(url):
    parts = [p for p in urlparse(url).path.rstrip("/").split("/") if p]
    return parts[3].replace("-", " ").title() if len(parts) >= 4 else urlparse(url).path

def load_existing():
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {"metadata": {}, "sends": []}

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved {DATA_FILE} ({DATA_FILE.stat().st_size / 1024:.1f} KB)")

def needs_upgrade(send):
    return ("unique_clickers" not in send or "ad_clicks" not in send
            or "geo" not in send or "clicks_by_age" not in send
            or "editorial_clicks" not in send or "clicks_by_hour" not in send)

def fetch_bd_newsletters(since_date):
    print(f"\n  Fetching BD Newsfeed emails since {since_date.date()}...")
    all_emails, after = [], None
    while True:
        params = {"limit": 50, "sort": "-publishDate"}
        if after:
            params["after"] = after
        data = get(f"{BASE}/marketing/v3/emails", params=params)
        if not data:
            break
        for email in data.get("results", []):
            if email.get("campaignName") != BD_CAMPAIGN_NAME or email.get("state") != "PUBLISHED":
                continue
            pub = email.get("publishDate", "")
            if pub:
                if datetime.fromisoformat(pub.replace("Z", "+00:00")) < since_date:
                    return all_emails
            all_emails.append(email)
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return all_emails

def fetch_email_stats(email_id):
    data = get(f"{BASE}/marketing/v3/emails/{email_id}", params={"includeStats": "true"})
    return data.get("stats", {}) if data else {}

def fetch_click_events(campaign_id):
    all_clicks, offset = [], None
    while True:
        params = {"eventType": "CLICK", "campaignId": str(campaign_id), "limit": 1000}
        if offset:
            params["offset"] = offset
        data = get(f"{BASE}/email/public/v1/events", params=params)
        if not data:
            break
        all_clicks.extend(data.get("events", []))
        if not data.get("hasMore", False):
            break
        offset = data.get("offset")
        if not offset:
            break
    return all_clicks

def process_clicks(click_events, send_date_str="", send_timestamp=""):
    article_data = defaultdict(lambda: {"clicks": 0, "recipients": set()})
    ad_data = defaultdict(lambda: {"clicks": 0, "recipients": set(), "domain": ""})
    editorial_data = defaultdict(lambda: {"clicks": 0, "recipients": set(), "domain": ""})
    geo_data = defaultdict(lambda: {"clicks": 0, "recipients": set(), "lat": 0, "lng": 0})
    all_recipients = set()
    age_counts = defaultdict(int)
    hour_counts = defaultdict(int)

    send_dt = None
    send_dt_precise = None
    if send_date_str:
        try:
            send_dt = datetime.strptime(send_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if send_timestamp:
        try:
            send_dt_precise = datetime.fromisoformat(send_timestamp.replace("Z", "+00:00"))
        except ValueError:
            pass

    for ev in click_events:
        raw_url = ev.get("url", "")
        if not raw_url:
            continue
        clean_url = strip_utm(raw_url).rstrip("/")
        rh = hash_email(ev.get("recipient", ""))
        loc = ev.get("location", {})
        if rh:
            all_recipients.add(rh)

        if (send_dt or send_dt_precise) and ev.get("created"):
            try:
                ts = ev["created"]
                click_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if isinstance(ts, (int, float)) else datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if send_dt:
                    age = (click_dt.date() - send_dt.date()).days
                    if 0 <= age <= 14:
                        age_counts[age] += 1
                if send_dt_precise:
                    hours_after = (click_dt - send_dt_precise).total_seconds() / 3600
                    if 0 <= hours_after < 24:
                        hour_counts[int(hours_after)] += 1
            except (ValueError, TypeError, OSError):
                pass

        if is_bd_article_url(clean_url):
            article_data[clean_url]["clicks"] += 1
            if rh:
                article_data[clean_url]["recipients"].add(rh)
        elif not is_bd_internal_url(clean_url):
            domain = urlparse(clean_url).netloc.replace("www.", "")
            if any(skip in domain for skip in ("hubspot","hsforms","hs-sites")):
                continue
            if is_news_domain(domain):
                editorial_data[clean_url]["clicks"] += 1
                editorial_data[clean_url]["domain"] = domain
                if rh:
                    editorial_data[clean_url]["recipients"].add(rh)
            else:
                ad_data[clean_url]["clicks"] += 1
                ad_data[clean_url]["domain"] = domain
                if rh:
                    ad_data[clean_url]["recipients"].add(rh)

        if loc:
            city, state = loc.get("city", ""), loc.get("state", "")
            if city and state:
                gk = f"{city}|{state}"
                geo_data[gk]["clicks"] += 1
                if rh:
                    geo_data[gk]["recipients"].add(rh)
                if not geo_data[gk]["lat"]:
                    geo_data[gk]["lat"] = loc.get("latitude", 0)
                    geo_data[gk]["lng"] = loc.get("longitude", 0)

    articles = [{"url": u, "title": url_to_title(u), "clicks": d["clicks"],
                 "unique_clickers": len(d["recipients"]), "is_sponsored": is_sponsored_content(u)}
                for u, d in sorted(article_data.items(), key=lambda x: -x[1]["clicks"])]
    ad_clicks = [{"url": u, "domain": d["domain"], "clicks": d["clicks"],
                  "unique_clickers": len(d["recipients"])}
                 for u, d in sorted(ad_data.items(), key=lambda x: -x[1]["clicks"])]
    editorial_clicks = [{"url": u, "domain": d["domain"], "clicks": d["clicks"],
                         "unique_clickers": len(d["recipients"])}
                        for u, d in sorted(editorial_data.items(), key=lambda x: -x[1]["clicks"])]
    geo = [{"city": k.split("|")[0], "state": k.split("|")[1], "lat": d["lat"], "lng": d["lng"],
            "clicks": d["clicks"], "unique_clickers": len(d["recipients"])}
           for k, d in sorted(geo_data.items(), key=lambda x: -x[1]["clicks"])]
    clicks_by_age = {str(k): v for k, v in sorted(age_counts.items()) if k <= 14}
    clicks_by_hour = {str(k): v for k, v in sorted(hour_counts.items())}
    return articles, ad_clicks, editorial_clicks, geo, len(all_recipients), clicks_by_age, clicks_by_hour

def process_email(email):
    email_id = email["id"]
    name = email.get("name", "")
    pub_date = email.get("publishDate", "")
    campaign_id = email.get("primaryEmailCampaignId")
    print(f"\n  Processing: {name}")
    send_date = ""
    if pub_date:
        try:
            send_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except ValueError:
            send_date = pub_date[:10]

    stats = fetch_email_stats(email_id)
    counters = stats.get("counters", {})
    ratios = stats.get("ratios", {})
    device = stats.get("deviceBreakdown", {})
    articles, ad_clicks, editorial_clicks, geo, unique_clickers, clicks_by_age, clicks_by_hour = [], [], [], [], 0, {}, {}

    if campaign_id:
        evts = fetch_click_events(campaign_id)
        print(f"    {len(evts)} click events")
        articles, ad_clicks, editorial_clicks, geo, unique_clickers, clicks_by_age, clicks_by_hour = process_clicks(evts, send_date, pub_date)
        ta = sum(a["clicks"] for a in articles)
        tad = sum(a["clicks"] for a in ad_clicks)
        ted = sum(a["clicks"] for a in editorial_clicks)
        print(f"    {len(articles)} articles ({ta}), {len(ad_clicks)} ads ({tad}), {len(editorial_clicks)} editorial ({ted}), {unique_clickers} unique, {len(geo)} geo")
        opens = counters.get("open", 0)
        for a in articles:
            a["click_pct"] = round(a["clicks"] / opens * 100, 2) if opens > 0 else 0

    ct = counters.get("click", 0)
    cpc = round(ct / unique_clickers, 2) if unique_clickers > 0 else 0
    return {
        "date": send_date, "email_id": str(email_id), "name": name,
        "recipients": counters.get("selected", 0), "sent": counters.get("sent", 0),
        "delivered": counters.get("delivered", 0), "opens": counters.get("open", 0),
        "clicks": ct, "unique_clickers": unique_clickers, "clicks_per_clicker": cpc,
        "bounces": counters.get("bounce", 0), "hard_bounces": counters.get("hardbounced", 0),
        "soft_bounces": counters.get("softbounced", 0),
        "unsubscribes": counters.get("unsubscribed", 0),
        "spam_reports": counters.get("spamreport", 0),
        "contacts_lost": counters.get("contactslost", 0),
        "dropped": counters.get("dropped", 0), "suppressed": counters.get("suppressed", 0),
        "pending": counters.get("pending", 0),
        "open_rate": round(ratios.get("openratio", 0), 2),
        "click_rate": round(ratios.get("clickratio", 0), 2),
        "click_through_rate": round(ratios.get("clickthroughratio", 0), 2),
        "bounce_rate": round(ratios.get("bounceratio", 0), 2),
        "unsubscribe_rate": round(ratios.get("unsubscribedratio", 0), 2),
        "device_opens": device.get("open_device_type", {}),
        "device_clicks": device.get("click_device_type", {}),
        "article_clicks": sum(a["clicks"] for a in articles),
        "articles": articles, "ad_clicks": ad_clicks, "editorial_clicks": editorial_clicks,
        "geo": geo, "clicks_by_age": clicks_by_age, "clicks_by_hour": clicks_by_hour,
    }

def fetch_ga4_sponsored(sponsored_paths):
    sa_json = os.environ.get("GA4_SERVICE_ACCOUNT")
    if not sa_json:
        print("\n  GA4_SERVICE_ACCOUNT not set, skipping sponsored content source breakdown")
        return {}
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest, Dimension, Metric, DateRange,
            FilterExpression, FilterExpressionList, Filter, StringFilter,
        )
    except ImportError as e:
        print(f"\n  google-analytics-data import failed: {e}")
        return {}

    print(f"\n  Querying GA4 for {len(sponsored_paths)} sponsored content URLs...")
    try:
        sa_data = json.loads(sa_json)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sa_data, f)
            sa_path = f.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    except (json.JSONDecodeError, IOError) as e:
        print(f"  GA4 service account error: {e}")
        return {}
    try:
        client = BetaAnalyticsDataClient()
        # Try both with and without trailing slash for each path
        path_variants = set()
        for p in sponsored_paths:
            path_variants.add(p.rstrip("/") + "/")
            path_variants.add(p.rstrip("/"))

        # Build OR group of exact-match string filters (InListFilter not available in v1beta)
        path_filters = [
            FilterExpression(filter=Filter(
                field_name="pagePath",
                string_filter=StringFilter(
                    match_type=StringFilter.MatchType.EXACT,
                    value=path,
                ),
            ))
            for path in path_variants
        ]

        dimension_filter = FilterExpression(
            or_group=FilterExpressionList(expressions=path_filters)
        )

        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="pagePath"),Dimension(name="sessionDefaultChannelGrouping"),Dimension(name="sessionSource")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
            dimension_filter=dimension_filter,
            limit=10000,
        )
        response = client.run_report(request)
        results = {}
        for row in response.rows:
            page_path = row.dimension_values[0].value.rstrip("/")
            channel = row.dimension_values[1].value.lower()
            source = row.dimension_values[2].value.lower()
            views = int(row.metric_values[0].value)
            if page_path not in results:
                results[page_path] = {"newsletter": 0, "homepage": 0, "other": 0, "total": 0}
            results[page_path]["total"] += views
            if channel == "email":
                results[page_path]["newsletter"] += views
            elif channel == "referral" and "businessden" in source:
                results[page_path]["homepage"] += views
            else:
                results[page_path]["other"] += views
        print(f"  GA4 returned data for {len(results)} sponsored URLs")
        return results
    except Exception as e:
        print(f"  GA4 query error: {e}")
        return {}
    finally:
        try:
            os.unlink(sa_path)
        except OSError:
            pass

def main():
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=BACKFILL_DAYS)
    print(f"BD Newsletter Analytics — scraper v4")
    print(f"  Time: {now.isoformat()}")
    print(f"  Backfill: {BACKFILL_DAYS} days")

    existing = load_existing()
    existing_by_date = {s["date"]: s for s in existing.get("sends", [])}
    print(f"  Existing: {len(existing_by_date)} sends")
    needs_v4 = sum(1 for s in existing_by_date.values() if needs_upgrade(s))
    if needs_v4:
        print(f"  {needs_v4} sends need upgrade")

    emails = fetch_bd_newsletters(since)
    print(f"\n  Found {len(emails)} BD Newsfeed emails in range")
    if not emails and not needs_v4:
        print("  Nothing to process.")
        return

    new_sends = []
    for email in emails:
        pub = email.get("publishDate", "")
        sd = ""
        if pub:
            try:
                sd = datetime.fromisoformat(pub.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except ValueError:
                sd = pub[:10]
        days_old = (now.date() - datetime.strptime(sd, "%Y-%m-%d").date()).days if sd else 999
        ex = existing_by_date.get(sd)
        if ex and days_old > 3 and not needs_upgrade(ex):
            continue
        new_sends.append(process_email(email))

    sends_by_date = dict(existing_by_date)
    for s in new_sends:
        sends_by_date[s["date"]] = s
    all_sends = sorted(sends_by_date.values(), key=lambda x: x["date"], reverse=True)

    sponsored_paths = set()
    for s in all_sends:
        for a in s.get("articles", []):
            if a.get("is_sponsored"):
                sponsored_paths.add(urlparse(a["url"]).path)

    sponsored_ga4 = {}
    if sponsored_paths:
        sponsored_ga4 = fetch_ga4_sponsored(sponsored_paths)

    output = {
        "metadata": {"generated": now.isoformat(), "version": 4,
            "total_sends": len(all_sends),
            "date_range": {"start": all_sends[-1]["date"] if all_sends else "", "end": all_sends[0]["date"] if all_sends else ""}},
        "sends": all_sends,
        "sponsored_ga4": sponsored_ga4,
    }
    save_data(output)
    print(f"\n{'='*60}")
    print(f"  Total: {len(all_sends)} sends")
    print(f"  New/updated: {len(new_sends)}")
    v4 = sum(1 for s in all_sends if not needs_upgrade(s))
    print(f"  v4 complete: {v4}")
    print(f"  Sponsored content URLs: {len(sponsored_paths)}")
    print(f"  Sponsored GA4 data: {len(sponsored_ga4)} URLs")

if __name__ == "__main__":
    main()
