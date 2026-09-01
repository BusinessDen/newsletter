#!/usr/bin/env python3
"""
Newsletter list size — BD Newsfeed and RBS Newsfeed.

Tracks how each publication's morning newsletter list grows and shrinks,
send by send. Separate from scrape.py, which handles click and article
analytics. This tool touches nothing that scrape.py owns.

Two modes:
    --backfill        pull everything HubSpot will give us (run once)
    --daily           refresh the last N days (default 7, run on a schedule)

The token comes from the HUBSPOT_TOKEN environment variable. Nothing else
in this file knows or cares where that value came from, which is what makes
it portable: a GitHub Actions secret today, a keychain entry on the Mac mini
later. fetch_sends() and update() are the whole public surface — a desktop
wrapper imports them and supplies its own token and output path.

Standard library only.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = "https://api.hubapi.com"
SCHEMA_VERSION = 1

# The only two campaigns this tool follows. Anything else in the portal —
# marketing sends, promos, unnamed campaigns — is deliberately ignored.
CAMPAIGNS = {
    "BD Newsfeed": "BD",
    "RBS Newsfeed": "RBS",
}

# HubSpot allows roughly 190 requests per 10 seconds for a private app.
# A backfill is thousands of calls, so we stay well under it.
THROTTLE_SECONDS = 0.12
MAX_RETRIES = 4


class HubSpotError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def _request(token, path, params=None, timeout=45):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Content-Type", "application/json")

    delay = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                time.sleep(THROTTLE_SECONDS)
                return json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                time.sleep(delay)
                delay *= 2
                continue
            body = ""
            try:
                body = e.read().decode()[:300]
            except Exception:
                pass
            if e.code == 401:
                raise HubSpotError("Token rejected (401). It may have been rotated.")
            if e.code == 403:
                raise HubSpotError("Token lacks the marketing-email read scope (403).")
            raise HubSpotError("HTTP %d on %s. %s" % (e.code, path, body))
        except urllib.error.URLError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
                delay *= 2
                continue
            raise HubSpotError("Could not reach HubSpot: %s" % e)
    raise HubSpotError("Gave up on %s" % path)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def list_emails(token, since=None, log=print):
    """Every published send for the tracked campaigns, newest first.

    `since` is a timezone-aware datetime, or None for the full history.
    """
    found, after, pages = [], None, 0
    while True:
        params = {"limit": 50, "sort": "-publishDate"}
        if after:
            params["after"] = after
        data = _request(token, "/marketing/v3/emails", params)
        results = data.get("results", [])
        if not results:
            break

        stop = False
        for email in results:
            if email.get("state") != "PUBLISHED":
                continue
            when = _parse_dt(email.get("publishDate"))
            if since and when and when < since:
                stop = True
                break
            campaign = email.get("campaignName")
            if campaign not in CAMPAIGNS:
                continue
            found.append({
                "id": str(email.get("id")),
                "campaign": campaign,
                "publication": CAMPAIGNS[campaign],
                "name": email.get("name", ""),
                "published": email.get("publishDate", ""),
            })

        pages += 1
        if pages % 10 == 0:
            log("  page %d — %d tracked sends so far" % (pages, len(found)))
        if stop:
            break
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
    return found


def fetch_send(token, email):
    """One send's counters, normalized. Returns a flat record."""
    data = _request(token, "/marketing/v3/emails/%s" % email["id"],
                    {"includeStats": "true"})
    stats = data.get("stats") or {}
    c = stats.get("counters", {})
    r = stats.get("ratios", {})
    when = _parse_dt(email["published"])

    selected = c.get("selected", 0)
    sent = c.get("sent", 0)
    delivered = c.get("delivered", 0)

    return {
        "email_id": email["id"],
        "publication": email["publication"],
        "campaign": email["campaign"],
        "name": email["name"],
        "date": when.strftime("%Y-%m-%d") if when else "",
        "sent_at": email["published"],
        # list size and reach
        "list_size": selected,
        "sent": sent,
        "delivered": delivered,
        # why the list and the reach differ
        "suppressed": c.get("suppressed", 0),
        "dropped": c.get("dropped", 0),
        # attrition events
        "unsubscribes": c.get("unsubscribed", 0),
        "hard_bounces": c.get("hardbounced", 0),
        "soft_bounces": c.get("softbounced", 0),
        "bounces": c.get("bounce", 0),
        "spam_reports": c.get("spamreport", 0),
        "contacts_lost": c.get("contactslost", 0),
        # engagement, for context only
        "opens": c.get("open", 0),
        "clicks": c.get("click", 0),
        "open_rate": round(r.get("openratio", 0), 2),
        "click_rate": round(r.get("clickratio", 0), 2),
        "unsubscribe_rate": round(r.get("unsubscribedratio", 0), 4),
        "_schema": SCHEMA_VERSION,
    }


def fetch_sends(token, since=None, log=print):
    """List the tracked sends, then pull counters for each one."""
    emails = list_emails(token, since=since, log=log)
    by_pub = defaultdict(int)
    for e in emails:
        by_pub[e["publication"]] += 1
    log("  found %d tracked sends (%s)"
        % (len(emails), ", ".join("%s %d" % (k, v) for k, v in sorted(by_pub.items()))
           or "none"))

    records, failures = [], []
    for i, email in enumerate(emails, 1):
        try:
            records.append(fetch_send(token, email))
        except HubSpotError as e:
            failures.append({"email_id": email["id"], "error": str(e)})
        if i % 50 == 0:
            log("  counters: %d of %d" % (i, len(emails)))
    if failures:
        log("  %d send(s) could not be read" % len(failures))
    return records, failures


# ---------------------------------------------------------------------------
# Derived series
# ---------------------------------------------------------------------------

def build_daily(records):
    """Roll sends up to one row per publication per day.

    List size is taken from the last send of the day rather than summed —
    two sends on one day go to substantially the same people, so adding
    them would double count. Attrition events are summed, because those
    are distinct events.
    """
    buckets = defaultdict(list)
    for r in records:
        if r["date"]:
            buckets[(r["publication"], r["date"])].append(r)

    rows = []
    for (pub, date), sends in buckets.items():
        sends.sort(key=lambda x: x["sent_at"])
        last = sends[-1]
        rows.append({
            "publication": pub,
            "date": date,
            "sends": len(sends),
            "list_size": last["list_size"],
            "delivered": last["delivered"],
            "suppressed": last["suppressed"],
            "dropped": last["dropped"],
            "reachable": last["sent"],
            "unsubscribes": sum(s["unsubscribes"] for s in sends),
            "hard_bounces": sum(s["hard_bounces"] for s in sends),
            "contacts_lost": sum(s["contacts_lost"] for s in sends),
            "spam_reports": sum(s["spam_reports"] for s in sends),
        })

    rows.sort(key=lambda x: (x["publication"], x["date"]))

    # Net change and the implied-additions residual, per publication.
    prev = {}
    for row in rows:
        p = row["publication"]
        if p in prev:
            row["net_change"] = row["list_size"] - prev[p]
            losses = (row["unsubscribes"] + row["hard_bounces"]
                      + row["contacts_lost"])
            row["known_losses"] = losses
            row["implied_additions"] = row["net_change"] + losses
        else:
            row["net_change"] = None
            row["known_losses"] = None
            row["implied_additions"] = None
        prev[p] = row["list_size"]

    rows.sort(key=lambda x: (x["date"], x["publication"]), reverse=True)
    return rows


def summarize(records, daily):
    """Current state per publication, for a dashboard tile."""
    out = {}
    for pub in sorted({r["publication"] for r in records}):
        series = [d for d in daily if d["publication"] == pub]
        if not series:
            continue
        series.sort(key=lambda x: x["date"])
        latest = series[-1]

        def lookback(days):
            cutoff = (datetime.strptime(latest["date"], "%Y-%m-%d")
                      - timedelta(days=days)).strftime("%Y-%m-%d")
            earlier = [d for d in series if d["date"] <= cutoff]
            if not earlier:
                return None
            return latest["list_size"] - earlier[-1]["list_size"]

        reachable = latest["reachable"] or 0
        out[pub] = {
            "as_of": latest["date"],
            "list_size": latest["list_size"],
            "delivered": latest["delivered"],
            "reachable": reachable,
            "suppressed": latest["suppressed"],
            "dropped": latest["dropped"],
            "unreachable_pct": (round((latest["list_size"] - reachable)
                                      / latest["list_size"] * 100, 2)
                                if latest["list_size"] else None),
            "change_7d": lookback(7),
            "change_30d": lookback(30),
            "change_365d": lookback(365),
            "first_date": series[0]["date"],
            "days_tracked": len(series),
        }
    return out


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

def load(path):
    p = Path(path)
    if not p.exists():
        return {"metadata": {}, "sends": [], "daily": [], "summary": {}}
    with open(p) as f:
        return json.load(f)


def save(path, payload):
    p = Path(path)
    if p.parent and str(p.parent) not in ("", "."):
        p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(p)


def merge(existing, fresh):
    """Fresh records win. Keyed on email_id so two sends on one date both
    survive — RBS sends more often than once a day and date keys would
    silently drop one."""
    by_id = {r["email_id"]: r for r in existing}
    added = updated = 0
    for r in fresh:
        if r["email_id"] in by_id:
            updated += 1
        else:
            added += 1
        by_id[r["email_id"]] = r
    records = sorted(by_id.values(), key=lambda x: (x["sent_at"] or "", x["email_id"]),
                     reverse=True)
    return records, added, updated


def update(token, path, since=None, log=print):
    """Fetch, merge, rebuild derived series, write. Returns a report."""
    existing = load(path)
    before = len(existing.get("sends", []))

    fresh, failures = fetch_sends(token, since=since, log=log)
    if not fresh and not before:
        raise HubSpotError("No sends returned and no existing data. Refusing to "
                           "write an empty file.")

    records, added, updated_count = merge(existing.get("sends", []), fresh)

    # Guard against a silent wipe: the record count must never fall.
    if len(records) < before:
        raise HubSpotError("Merge would reduce the dataset from %d to %d records. "
                           "Refusing to write." % (before, len(records)))

    daily = build_daily(records)
    payload = {
        "metadata": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "schema": SCHEMA_VERSION,
            "campaigns": CAMPAIGNS,
            "total_sends": len(records),
            "publications": sorted({r["publication"] for r in records}),
            "date_range": {
                "start": min((r["date"] for r in records if r["date"]), default=""),
                "end": max((r["date"] for r in records if r["date"]), default=""),
            },
            "failures": failures,
        },
        "summary": summarize(records, daily),
        "daily": daily,
        "sends": records,
    }
    save(path, payload)

    return {"before": before, "after": len(records), "added": added,
            "updated": updated_count, "failures": len(failures),
            "summary": payload["summary"]}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description="Track BD and RBS newsletter list size.")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--backfill", action="store_true",
                      help="pull the full available history")
    mode.add_argument("--daily", action="store_true",
                      help="refresh recent sends only (default)")
    ap.add_argument("--days", type=int, default=7,
                    help="how many days back --daily refreshes (default 7)")
    ap.add_argument("--out", default=os.environ.get("LIST_SIZE_FILE",
                                                    "list-size-data.json"))
    args = ap.parse_args(argv)

    token = os.environ.get("HUBSPOT_TOKEN", "").strip()
    if not token:
        print("HUBSPOT_TOKEN is not set.")
        return 2

    since = None
    if not args.backfill:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        print("Refreshing the last %d days." % args.days)
    else:
        print("Backfilling the full history. This will take a few minutes.")

    try:
        report = update(token, args.out, since=since)
    except HubSpotError as e:
        print("Failed: %s" % e)
        return 1

    print("\nRecords: %d before, %d after (%d new, %d refreshed)"
          % (report["before"], report["after"], report["added"], report["updated"]))
    if report["failures"]:
        print("%d send(s) could not be read — see metadata.failures"
              % report["failures"])
    for pub, s in sorted(report["summary"].items()):
        print("\n%s as of %s" % (pub, s["as_of"]))
        print("  list %s   delivered %s   unreachable %s%%"
              % (f"{s['list_size']:,}", f"{s['delivered']:,}", s["unreachable_pct"]))
        for label, key in (("7 days", "change_7d"), ("30 days", "change_30d"),
                           ("12 months", "change_365d")):
            v = s.get(key)
            if v is not None:
                print("  %-10s %+,d" % (label, v))
    print("\nWrote %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
