import json, os, sys, time, requests
from urllib.parse import urlparse

TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
if not TOKEN:
    print("No HUBSPOT_TOKEN"); sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
BASE = "https://api.hubapi.com"

def get(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    time.sleep(0.3)
    if r.status_code == 200:
        return r.json()
    return {"_error": r.status_code, "_body": r.text[:300]}

print("=" * 60)
print("DEEP CTA / AD CLICK INVESTIGATION")
print("=" * 60)

# Step 1: Get a recent BD Newsfeed email
data = get(f"{BASE}/marketing/v3/emails", {"limit": 10, "sort": "-publishDate"})
email = None
for e in data.get("results", []):
    if e.get("campaignName") == "BD Newsfeed" and e.get("state") == "PUBLISHED":
        email = e
        break

if not email:
    print("No email found"); sys.exit(1)

eid = email["id"]
cid = email.get("primaryEmailCampaignId")
print(f"\n1. Email: {email.get('name')} (ID: {eid}, Campaign: {cid})")

# Step 2: Get the RAW HTML of the email
print(f"\n2. Fetching email HTML content...")
email_detail = get(f"{BASE}/marketing/v3/emails/{eid}")
if "_error" not in email_detail:
    # Look for CTA-related HTML patterns
    for key in ["primaryRichTextModuleHtml", "emailBody", "content"]:
        if key in email_detail:
            html = str(email_detail[key])
            print(f"  Found '{key}': {len(html)} chars")
            # Search for CTA patterns
            import re
            cta_patterns = [
                r'hs-cta', r'hsCtaAttrib', r'cta-redirect',
                r'hscta', r'data-cta', r'hs_cos_wrapper_type.*cta'
            ]
            for pat in cta_patterns:
                matches = re.findall(pat, html, re.IGNORECASE)
                if matches:
                    print(f"    Pattern '{pat}': {len(matches)} matches")

    # Print all keys available
    print(f"\n  Email detail keys: {sorted(email_detail.keys())}")

    # Look for any field containing HTML with links
    for key, val in email_detail.items():
        if isinstance(val, str) and '<a ' in val.lower():
            # Extract all href values
            hrefs = re.findall(r'href=["\']([^"\']+)["\']', val)
            ad_hrefs = [h for h in hrefs if 'irelandstapleton' in h or 'northpeak' in h
                       or 'timberline' in h or 'inbank' in h or 'alpinebank' in h
                       or 'joshsteck' in h or 'hsCtaAttrib' in h or 'cta-redirect' in h]
            if ad_hrefs:
                print(f"\n  Found ad/CTA links in '{key}':")
                for h in ad_hrefs[:10]:
                    print(f"    {h[:120]}")
else:
    print(f"  Error: {email_detail}")

# Step 3: Get ALL click events and look at every unique URL
print(f"\n3. Fetching ALL click events for campaign {cid}...")
all_clicks = []
offset = None
while True:
    params = {"eventType": "CLICK", "campaignId": str(cid), "limit": 1000}
    if offset:
        params["offset"] = offset
    d = get(f"{BASE}/email/public/v1/events", params)
    if "_error" in d:
        print(f"  Error: {d}"); break
    all_clicks.extend(d.get("events", []))
    if not d.get("hasMore"):
        break
    offset = d.get("offset")

print(f"  Total click events: {len(all_clicks)}")

# Group by EXACT URL (not cleaned)
raw_urls = {}
for c in all_clicks:
    url = c.get("url", "")
    if not url:
        continue
    if url not in raw_urls:
        raw_urls[url] = 0
    raw_urls[url] += 1

# Look for any URL that could be an ad
print(f"\n  Unique raw URLs: {len(raw_urls)}")
print(f"\n  URLs containing hubspot/hs/cta:")
for url, count in sorted(raw_urls.items(), key=lambda x: -x[1]):
    lower = url.lower()
    if any(x in lower for x in ['hubspot', 'hsforms', 'hs-sites', 'hscta', 'cta', 'hsctaattrib']):
        print(f"    {count:>4}x  {url[:120]}")

print(f"\n  URLs NOT matching businessden.com or known news domains:")
news = {'cbsnews.com','cnbc.com','coloradosun.com','summitdaily.com','vaildaily.com',
        'westword.com','fastcompany.com','telegraph.co.uk','inc.com','denver7.com',
        'nypost.com','seattletimes.com','apnews.com','reuters.com','cnn.com','npr.org',
        '9news.com','kdvr.com','forbes.com','denvergazette.com'}
for url, count in sorted(raw_urls.items(), key=lambda x: -x[1]):
    domain = urlparse(url).netloc.replace("www.", "")
    if 'businessden.com' in domain:
        continue
    if domain in news:
        continue
    print(f"    {count:>4}x  {domain:30s}  {url[:80]}")

# Step 4: Try to get the email HTML via the content API
print(f"\n4. Attempting to fetch email HTML via content endpoint...")
for endpoint in [
    f"/marketing/v3/emails/{eid}?properties=emailBody",
    f"/content/api/v2/email-templates/{eid}",
]:
    d = get(f"{BASE}{endpoint}")
    if "_error" not in d:
        print(f"  {endpoint}: SUCCESS ({len(str(d))} chars)")
        break
    else:
        print(f"  {endpoint}: {d.get('_error')}")

# Step 5: Check if there's a custom events API with CTA data
print(f"\n5. Checking custom events / behavioral events...")
endpoints = [
    "/events/v3/events?objectType=contact&limit=5",
    "/events/v3/event-definitions?limit=10",
]
for ep in endpoints:
    d = get(f"{BASE}{ep}")
    if "_error" not in d:
        print(f"  {ep}: SUCCESS")
        if isinstance(d, dict):
            print(f"    Keys: {list(d.keys())}")
            if 'results' in d:
                print(f"    Results: {len(d['results'])}")
                for r in d['results'][:3]:
                    print(f"      {json.dumps(r, indent=2)[:200]}")
    else:
        print(f"  {ep}: {d.get('_error')} {d.get('_body','')[:100]}")

# Step 6: Check the marketing events API
print(f"\n6. Marketing events...")
for ep in [
    "/marketing/v3/marketing-events/events?limit=5",
    "/marketing/v3/marketing-events/attendance?limit=5",
]:
    d = get(f"{BASE}{ep}")
    if "_error" not in d:
        print(f"  {ep}: SUCCESS - {json.dumps(d)[:300]}")
    else:
        print(f"  {ep}: {d.get('_error')}")

# Step 7: Try HubSpot's web analytics API
print(f"\n7. Web analytics / CTA reports...")
for ep in [
    "/analytics/v2/reports/ctas/total?start=20260101&end=20260417",
    "/analytics/v2/reports/ctas",
]:
    d = get(f"{BASE}{ep}")
    if "_error" not in d:
        print(f"  {ep}: SUCCESS")
        print(f"    {json.dumps(d)[:500]}")
    else:
        print(f"  {ep}: {d.get('_error')} - {d.get('_body','')[:150]}")

print(f"\n{'='*60}")
print("INVESTIGATION COMPLETE")
print("="*60)
