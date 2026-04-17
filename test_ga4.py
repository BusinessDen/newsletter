#!/usr/bin/env python3
"""
GA4 Diagnostic — trace every step of the sponsored content query
"""
import json, os, sys, tempfile

GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "363209481")
sa_json = os.environ.get("GA4_SERVICE_ACCOUNT", "")

print("=" * 60)
print("GA4 DIAGNOSTIC")
print("=" * 60)

# Step 1: Check secret exists
print(f"\n1. GA4_SERVICE_ACCOUNT set: {bool(sa_json)}")
if not sa_json:
    print("   FATAL: Secret not set. Exiting.")
    sys.exit(1)
print(f"   Length: {len(sa_json)} chars")
print(f"   Starts with: {sa_json[:20]}...")

# Step 2: Parse JSON
print(f"\n2. Parsing service account JSON...")
try:
    sa_data = json.loads(sa_json)
    print(f"   project_id: {sa_data.get('project_id', 'MISSING')}")
    print(f"   client_email: {sa_data.get('client_email', 'MISSING')}")
    print(f"   type: {sa_data.get('type', 'MISSING')}")
except json.JSONDecodeError as e:
    print(f"   FATAL: JSON parse error: {e}")
    sys.exit(1)

# Step 3: Write temp file
print(f"\n3. Writing temp credentials file...")
try:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sa_data, f)
        sa_path = f.name
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    print(f"   Written to: {sa_path}")
except Exception as e:
    print(f"   FATAL: {e}")
    sys.exit(1)

# Step 4: Import library
print(f"\n4. Importing google-analytics-data...")
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, DateRange,
        FilterExpression, Filter, InListFilter, StringFilter,
    )
    print(f"   OK")
except ImportError as e:
    print(f"   FATAL: Import failed: {e}")
    print(f"   Installed packages:")
    import subprocess
    subprocess.run(["pip", "list"], capture_output=False)
    sys.exit(1)

# Step 5: Create client
print(f"\n5. Creating BetaAnalyticsDataClient...")
try:
    client = BetaAnalyticsDataClient()
    print(f"   OK")
except Exception as e:
    print(f"   FATAL: Client creation failed: {e}")
    sys.exit(1)

# Step 6: Simple test query — total pageviews for the whole property
print(f"\n6. Test query: total pageviews (no filters)...")
try:
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2025-01-01", end_date="today")],
        limit=1,
    )
    response = client.run_report(request)
    total = response.rows[0].metric_values[0].value if response.rows else "NO ROWS"
    print(f"   Total pageviews since 2025-01-01: {total}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback
    traceback.print_exc()

# Step 7: Query a known BD page path to verify pagePath format
print(f"\n7. Query: top 10 BD page paths by views...")
try:
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2025-01-01", end_date="today")],
        limit=10,
    )
    response = client.run_report(request)
    print(f"   Rows returned: {len(response.rows)}")
    for row in response.rows:
        path = row.dimension_values[0].value
        views = row.metric_values[0].value
        print(f"   {views:>8} views  {path[:80]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback
    traceback.print_exc()

# Step 8: Query specifically for sponsored content paths
sponsored_paths = [
    "/2025/12/02/sponsored-content-meet-a-trusted-local-partner-for-plumbing-solutions-midwest-plumbing-co/",
    "/2025/07/14/sponsored-content-gator-home-remodel-is-elevating-homes/",
    "/2024/05/02/sponsored-content-financial-feed/",
    "/2024/04/05/sponsored-content-energize-denver-new-energy-performance-regulations-for-building-owners-take-effect/",
    "/2023/10/02/sponsored-content-building-smart-hospitality-construction-mythbusters/",
]

print(f"\n8. Query: sponsored content paths with InListFilter...")
print(f"   Paths being queried:")
for p in sponsored_paths:
    print(f"     {p}")

try:
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="sessionDefaultChannelGrouping"),
            Dimension(name="sessionSource"),
        ],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                in_list_filter=InListFilter(values=sponsored_paths),
            )
        ),
        limit=10000,
    )
    response = client.run_report(request)
    print(f"   Rows returned: {len(response.rows)}")
    for row in response.rows[:20]:
        path = row.dimension_values[0].value
        channel = row.dimension_values[1].value
        source = row.dimension_values[2].value
        views = row.metric_values[0].value
        print(f"   {views:>6} views  {channel:20s}  {source:20s}  {path[:60]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback
    traceback.print_exc()

# Step 9: Try without trailing slash
print(f"\n9. Query: same paths WITHOUT trailing slash...")
no_slash = [p.rstrip("/") for p in sponsored_paths]
print(f"   Paths being queried:")
for p in no_slash:
    print(f"     {p}")

try:
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                in_list_filter=InListFilter(values=no_slash),
            )
        ),
        limit=100,
    )
    response = client.run_report(request)
    print(f"   Rows returned: {len(response.rows)}")
    for row in response.rows[:10]:
        path = row.dimension_values[0].value
        views = row.metric_values[0].value
        print(f"   {views:>8} views  {path[:80]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback
    traceback.print_exc()

# Step 10: Try a contains filter for "sponsored-content"
print(f"\n10. Query: pagePath contains 'sponsored-content'...")
try:
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=StringFilter(
                    match_type=StringFilter.MatchType.CONTAINS,
                    value="sponsored-content",
                ),
            )
        ),
        limit=20,
    )
    response = client.run_report(request)
    print(f"   Rows returned: {len(response.rows)}")
    for row in response.rows:
        path = row.dimension_values[0].value
        views = row.metric_values[0].value
        print(f"   {views:>8} views  {path[:80]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
try:
    os.unlink(sa_path)
except:
    pass

print(f"\n{'=' * 60}")
print("DIAGNOSTIC COMPLETE")
print("=" * 60)
