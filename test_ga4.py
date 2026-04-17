#!/usr/bin/env python3
"""GA4 Diagnostic — trace every step of the sponsored content query"""
import json, os, sys, tempfile

GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "363209481")
sa_json = os.environ.get("GA4_SERVICE_ACCOUNT", "")

print("=" * 60)
print("GA4 DIAGNOSTIC")
print("=" * 60)

print(f"\n1. GA4_SERVICE_ACCOUNT set: {bool(sa_json)}")
if not sa_json:
    print("   FATAL: Secret not set."); sys.exit(1)
print(f"   Length: {len(sa_json)} chars")

print(f"\n2. Parsing service account JSON...")
try:
    sa_data = json.loads(sa_json)
    print(f"   project_id: {sa_data.get('project_id')}")
    print(f"   client_email: {sa_data.get('client_email')}")
except json.JSONDecodeError as e:
    print(f"   FATAL: {e}"); sys.exit(1)

print(f"\n3. Writing temp credentials file...")
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    json.dump(sa_data, f); sa_path = f.name
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path

print(f"\n4. Importing google-analytics-data...")
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, DateRange,
        FilterExpression, Filter,
    )
    print(f"   OK — Filter has InListFilter: {hasattr(Filter, 'InListFilter')}")
    print(f"   Filter has StringFilter: {hasattr(Filter, 'StringFilter')}")
except ImportError as e:
    print(f"   FATAL: {e}"); sys.exit(1)

print(f"\n5. Creating client...")
try:
    client = BetaAnalyticsDataClient()
    print("   OK")
except Exception as e:
    print(f"   FATAL: {e}"); sys.exit(1)

print(f"\n6. Test: total pageviews (no filter)...")
try:
    resp = client.run_report(RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2025-01-01", end_date="today")],
        limit=1,
    ))
    print(f"   Total views: {resp.rows[0].metric_values[0].value if resp.rows else 'NO ROWS'}")
except Exception as e:
    print(f"   FAILED: {e}")

print(f"\n7. Test: top 10 page paths...")
try:
    resp = client.run_report(RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2025-01-01", end_date="today")],
        limit=10,
    ))
    for row in resp.rows:
        print(f"   {row.metric_values[0].value:>8} views  {row.dimension_values[0].value[:80]}")
except Exception as e:
    print(f"   FAILED: {e}")

print(f"\n8. Test: Filter.InListFilter with sponsored content paths...")
sponsored_paths = [
    "/2025/12/02/sponsored-content-meet-a-trusted-local-partner-for-plumbing-solutions-midwest-plumbing-co/",
    "/2025/12/02/sponsored-content-meet-a-trusted-local-partner-for-plumbing-solutions-midwest-plumbing-co",
    "/2025/07/14/sponsored-content-gator-home-remodel-is-elevating-homes/",
    "/2025/07/14/sponsored-content-gator-home-remodel-is-elevating-homes",
    "/2024/05/02/sponsored-content-financial-feed/",
    "/2024/05/02/sponsored-content-financial-feed",
]
try:
    resp = client.run_report(RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath"), Dimension(name="sessionDefaultChannelGrouping")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                in_list_filter=Filter.InListFilter(values=sponsored_paths),
            )
        ),
        limit=100,
    ))
    print(f"   Rows returned: {len(resp.rows)}")
    for row in resp.rows:
        print(f"   {row.metric_values[0].value:>6} views  {row.dimension_values[1].value:20s}  {row.dimension_values[0].value[:60]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback; traceback.print_exc()

print(f"\n9. Test: StringFilter CONTAINS 'sponsored-content'...")
try:
    resp = client.run_report(RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="2021-01-01", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value="sponsored-content",
                ),
            )
        ),
        limit=20,
    ))
    print(f"   Rows returned: {len(resp.rows)}")
    for row in resp.rows:
        print(f"   {row.metric_values[0].value:>8} views  {row.dimension_values[0].value[:80]}")
except Exception as e:
    print(f"   FAILED: {e}")
    import traceback; traceback.print_exc()

try: os.unlink(sa_path)
except: pass

print(f"\n{'='*60}")
print("DIAGNOSTIC COMPLETE")
print("="*60)
