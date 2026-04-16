#!/usr/bin/env python3
"""
BD Newsletter — Author Backfill

One-time script that resolves article authors from the WordPress REST API.
Reads newsletter-data.json, extracts all unique article URLs, looks up
each one via the BD REST API, and writes author-lookup.json.

Run this ONCE while the REST API is still live. The resulting lookup file
gets committed to the repo and used by the dashboard going forward.

Usage (GitHub Actions — see workflow):
    python backfill_authors.py

Or locally:
    python backfill_authors.py

Requires: requests, newsletter-data.json in working directory
"""

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

WP_API = "https://businessden.com/wp-json/wp/v2"
DATA_FILE = Path("newsletter-data.json")
OUTPUT_FILE = Path("author-lookup.json")
EXISTING_FILE = Path("author-lookup.json")

# Rate limiting — BD REST API is aggressive
DELAY_BETWEEN_POSTS = 1.0    # seconds between post lookups
DELAY_BETWEEN_AUTHORS = 0.5  # seconds between author lookups


def load_newsletter_data():
    if not DATA_FILE.exists():
        print(f"ERROR: {DATA_FILE} not found.")
        print("Run from a directory containing newsletter-data.json")
        sys.exit(1)
    with open(DATA_FILE) as f:
        return json.load(f)


def load_existing_lookup():
    if EXISTING_FILE.exists():
        with open(EXISTING_FILE) as f:
            return json.load(f)
    return {}


def extract_slug(url):
    """Extract the post slug from a BD article URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    parts = [p for p in path.split("/") if p]
    # BD URLs: /YYYY/MM/DD/slug/
    if len(parts) >= 4:
        return parts[3]
    return None


def lookup_post_by_slug(slug):
    """Look up a post by slug via WP REST API. Returns (author_id, title) or None."""
    try:
        r = requests.get(
            f"{WP_API}/posts",
            params={"slug": slug, "per_page": 1, "_fields": "id,author,title"},
            timeout=15,
        )
        time.sleep(DELAY_BETWEEN_POSTS)

        if r.status_code == 200:
            posts = r.json()
            if posts:
                post = posts[0]
                title = post.get("title", {}).get("rendered", "")
                return post.get("author"), title
        elif r.status_code == 429:
            print(f"    Rate limited, waiting 10s...")
            time.sleep(10)
            return lookup_post_by_slug(slug)  # retry once
        else:
            print(f"    POST lookup failed: {r.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"    Request error: {e}")

    return None, None


def lookup_author_name(author_id):
    """Look up an author's name by ID."""
    try:
        r = requests.get(
            f"{WP_API}/users/{author_id}",
            params={"_fields": "id,name"},
            timeout=15,
        )
        time.sleep(DELAY_BETWEEN_AUTHORS)

        if r.status_code == 200:
            return r.json().get("name", "")
        elif r.status_code == 429:
            print(f"    Rate limited on author lookup, waiting 10s...")
            time.sleep(10)
            return lookup_author_name(author_id)
    except requests.exceptions.RequestException as e:
        print(f"    Author lookup error: {e}")

    return None


def main():
    print("BD Newsletter — Author Backfill")
    print("=" * 60)

    # Load newsletter data
    data = load_newsletter_data()
    sends = data.get("sends", [])
    print(f"  Newsletter data: {len(sends)} sends")

    # Extract all unique article URLs
    all_urls = set()
    for s in sends:
        for a in s.get("articles", []):
            url = a.get("url", "").rstrip("/")
            if url:
                all_urls.add(url)

    print(f"  Unique article URLs: {len(all_urls)}")

    # Load existing lookup (don't re-fetch what we already have)
    lookup = load_existing_lookup()
    already_resolved = sum(1 for u in all_urls if u in lookup)
    to_resolve = [u for u in all_urls if u not in lookup]
    print(f"  Already resolved: {already_resolved}")
    print(f"  To resolve: {len(to_resolve)}")

    if not to_resolve:
        print("\n  All articles already resolved. Nothing to do.")
        return

    # Resolve authors
    author_cache = {}  # author_id -> name
    resolved = 0
    failed = 0

    for i, url in enumerate(sorted(to_resolve)):
        slug = extract_slug(url)
        if not slug:
            print(f"  [{i+1}/{len(to_resolve)}] Skipping (no slug): {url}")
            failed += 1
            continue

        print(f"  [{i+1}/{len(to_resolve)}] {slug}")
        author_id, title = lookup_post_by_slug(slug)

        if author_id is None:
            print(f"    Not found in WP")
            lookup[url] = {"author": "Unknown", "title": title or ""}
            failed += 1
            continue

        # Resolve author name (cached)
        if author_id in author_cache:
            author_name = author_cache[author_id]
        else:
            author_name = lookup_author_name(author_id)
            if author_name:
                author_cache[author_id] = author_name
                print(f"    Author #{author_id}: {author_name}")
            else:
                author_name = f"Author #{author_id}"
                print(f"    Could not resolve author #{author_id}")

        lookup[url] = {"author": author_name, "title": title or ""}
        resolved += 1

        # Checkpoint every 25 articles
        if (i + 1) % 25 == 0:
            with open(OUTPUT_FILE, "w") as f:
                json.dump(lookup, f, indent=2)
            print(f"    Checkpoint saved ({len(lookup)} entries)")

    # Final save
    with open(OUTPUT_FILE, "w") as f:
        json.dump(lookup, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  Resolved: {resolved}")
    print(f"  Failed: {failed}")
    print(f"  Total in lookup: {len(lookup)}")
    print(f"  Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
