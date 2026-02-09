#!/usr/bin/env python3
"""
Relix Shop Daily Revenue Report
Pulls yesterday's sales data from Shopify and generates a formatted report.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode, urlparse, parse_qs

try:
    import requests
except ImportError:
    print("Error: 'requests' package is required. Install with: pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "relix-store.myshopify.com")
SHOPIFY_API_TOKEN = os.environ.get("SHOPIFY_API_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
API_VERSION = "2024-01"
BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

CATEGORIES = ["Vinyl", "Books", "Posters", "Tees"]
CATEGORY_KEYWORDS = {
    "vinyl": "Vinyl",
    "record": "Vinyl",
    "lp": "Vinyl",
    "book": "Books",
    "poster": "Posters",
    "print": "Posters",
    "tee": "Tees",
    "t-shirt": "Tees",
    "shirt": "Tees",
}

RATE_LIMIT_DELAY = 0.5  # seconds between API calls
MAX_RETRIES = 5

# ---------------------------------------------------------------------------
# Timezone helper — pure stdlib, no pytz needed
# ---------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python < 3.9 fallback
    from datetime import timezone as _tz

    class _Eastern(_tz):
        """Minimal Eastern timezone (handles EST only, good enough for date boundaries)."""
        _offset = timedelta(hours=-5)
        _name = "EST"

        def __init__(self):
            super().__init__(self._offset, self._name)

        def utcoffset(self, dt):
            return self._offset

        def tzname(self, dt):
            return self._name

        def dst(self, dt):
            return timedelta(0)

    class ZoneInfo:
        _zones = {"America/New_York": _Eastern()}

        def __new__(cls, key):
            return cls._zones[key]


EASTERN = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def get_date_ranges(reference_date=None):
    """Return all date ranges needed for the report, in Eastern time."""
    now_et = datetime.now(EASTERN)
    today_et = (reference_date or now_et).replace(hour=0, minute=0, second=0, microsecond=0)
    if reference_date is None:
        today_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)

    yesterday_start = today_et - timedelta(days=1)
    yesterday_end = today_et - timedelta(seconds=1)

    # Quarter start
    month = today_et.month
    if month <= 3:
        q_start_month = 1
    elif month <= 6:
        q_start_month = 4
    elif month <= 9:
        q_start_month = 7
    else:
        q_start_month = 10

    qtd_start = today_et.replace(month=q_start_month, day=1)
    qtd_end = yesterday_end  # through end of yesterday

    # Prior year same QTD window
    prior_qtd_start = qtd_start.replace(year=qtd_start.year - 1)
    prior_qtd_end = yesterday_end.replace(year=yesterday_end.year - 1)

    return {
        "yesterday_start": yesterday_start,
        "yesterday_end": yesterday_end,
        "qtd_start": qtd_start,
        "qtd_end": qtd_end,
        "prior_qtd_start": prior_qtd_start,
        "prior_qtd_end": prior_qtd_end,
        "yesterday_label": yesterday_start.strftime("%A, %B %-d"),
    }


def fmt_iso(dt):
    """Format a datetime as ISO 8601 with timezone offset for Shopify."""
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Shopify API helpers
# ---------------------------------------------------------------------------
def shopify_headers():
    return {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json",
    }


def shopify_get(endpoint, params=None):
    """Make a GET request to the Shopify Admin API with retry + rate limiting."""
    url = f"{BASE_URL}/{endpoint}.json"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=shopify_headers(), params=params, timeout=30)

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"  Rate limited, retrying in {retry_after}s …")
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                print(f"  Request error: {e}. Retrying in {wait}s …")
                time.sleep(wait)
            else:
                print(f"  ERROR: {e}")
                return None
    return None


def parse_link_header(resp):
    """Extract the next page URL from Shopify's Link header (cursor pagination)."""
    link = resp.headers.get("Link", "")
    for part in link.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip().strip("<>")
            return url
    return None


def shopify_get_url(url):
    """GET an absolute URL (used for cursor-based pagination)."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=shopify_headers(), timeout=30)

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"  Rate limited, retrying in {retry_after}s …")
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                print(f"  Request error: {e}. Retrying in {wait}s …")
                time.sleep(wait)
            else:
                print(f"  ERROR: {e}")
                return None
    return None


def fetch_all_orders(start, end, fields=None):
    """Fetch all orders in a date range, handling cursor-based pagination."""
    params = {
        "created_at_min": fmt_iso(start),
        "created_at_max": fmt_iso(end),
        "status": "any",
        "financial_status": "paid,partially_paid",
        "limit": 250,
    }
    if fields:
        params["fields"] = fields

    all_orders = []
    page = 1
    print(f"  Fetching orders {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')} …")

    resp = shopify_get("orders", params)
    if resp is None:
        return all_orders

    data = resp.json()
    orders = data.get("orders", [])
    all_orders.extend(orders)
    print(f"    Page {page}: {len(orders)} orders")

    # Cursor-based pagination
    next_url = parse_link_header(resp)
    while next_url:
        page += 1
        resp = shopify_get_url(next_url)
        if resp is None:
            break
        data = resp.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)
        print(f"    Page {page}: {len(orders)} orders")
        next_url = parse_link_header(resp)

    print(f"    Total: {len(all_orders)} orders")
    return all_orders


# ---------------------------------------------------------------------------
# Revenue calculation
# ---------------------------------------------------------------------------
def sum_revenue(orders):
    """Sum total_price across orders (gross revenue)."""
    total = Decimal("0.00")
    for order in orders:
        try:
            total += Decimal(str(order.get("total_price", "0")))
        except Exception:
            pass
    return total


# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------
def classify_line_item(item):
    """Classify a line item into a category. Returns category name or None."""
    product_type = (item.get("product_type") or "").strip()

    # Direct match on product_type
    for cat in CATEGORIES:
        if product_type.lower() == cat.lower():
            return cat

    # Keyword match on product_type
    pt_lower = product_type.lower()
    for keyword, cat in CATEGORY_KEYWORDS.items():
        if keyword in pt_lower:
            return cat

    # Fallback: keyword match on product title
    title = (item.get("title") or "").lower()
    for keyword, cat in CATEGORY_KEYWORDS.items():
        if keyword in title:
            return cat

    return None


def count_units_by_category(orders):
    """Count units sold per category from order line items."""
    counts = {cat: 0 for cat in CATEGORIES}
    for order in orders:
        for item in order.get("line_items", []):
            cat = classify_line_item(item)
            if cat:
                counts[cat] += item.get("quantity", 0)
    return counts


def get_sample_line_items(orders, n=5):
    """Return a sample of line items for debugging category matching."""
    samples = []
    for order in orders:
        for item in order.get("line_items", []):
            samples.append({
                "title": item.get("title"),
                "product_type": item.get("product_type"),
                "quantity": item.get("quantity"),
                "classified_as": classify_line_item(item),
            })
            if len(samples) >= n:
                return samples
    return samples


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------
def format_currency(amount):
    """Format a Decimal as $X,XXX.XX"""
    rounded = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"${rounded:,.2f}"


def format_yoy(current, prior):
    """Format year-over-year change as +X.X% or -X.X%."""
    if prior == 0:
        return "N/A (no prior year data)"
    change = ((current - prior) / prior) * 100
    change = change.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    sign = "+" if change >= 0 else ""
    return f"{sign}{change}%"


def build_report(yesterday_revenue, qtd_revenue, prior_qtd_revenue,
                 category_counts, date_label):
    """Build the formatted report string."""
    yoy = format_yoy(qtd_revenue, prior_qtd_revenue)
    vinyl_units = category_counts.get("Vinyl", 0)

    lines = [
        f"Yesterday: {format_currency(yesterday_revenue)}",
        f"QTD: {format_currency(qtd_revenue)} (vs {format_currency(prior_qtd_revenue)} last year → {yoy})",
        "",
        f"🎵 Vinyl units yesterday: {vinyl_units}",
    ]

    # Categories with 10+ units
    cats_over_10 = [(cat, count) for cat, count in category_counts.items() if count >= 10]
    if cats_over_10:
        lines.append("")
        lines.append("Categories with 10+ units sold:")
        for cat, count in cats_over_10:
            lines.append(f"• {cat}: {count} units")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Clipboard
# ---------------------------------------------------------------------------
def copy_to_clipboard(text):
    """Copy text to macOS clipboard via pbcopy."""
    try:
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.communicate(text.encode("utf-8"))
        return True
    except Exception as e:
        print(f"Warning: Could not copy to clipboard: {e}")
        return False


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
def send_to_slack(text):
    """Post the report to Slack via Incoming Webhook."""
    if not SLACK_WEBHOOK_URL:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is not set.")
        return False
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        if resp.status_code == 200:
            return True
        print(f"Slack error: {resp.status_code} {resp.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Slack error: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Relix Shop Daily Revenue Report")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show date ranges without making API calls")
    parser.add_argument("--slack", action="store_true",
                        help="Send the report to Slack via webhook")
    args = parser.parse_args()

    # Validate token
    if not args.dry_run and not SHOPIFY_API_TOKEN:
        print("ERROR: SHOPIFY_API_TOKEN environment variable is not set.")
        print("Set it with: export SHOPIFY_API_TOKEN='your-token-here'")
        sys.exit(1)

    ranges = get_date_ranges()

    if args.dry_run:
        print("=== DRY RUN — Date ranges that would be queried ===\n")
        print(f"Report label:       {ranges['yesterday_label']}")
        print(f"Yesterday:          {fmt_iso(ranges['yesterday_start'])}  →  {fmt_iso(ranges['yesterday_end'])}")
        print(f"QTD (current):      {fmt_iso(ranges['qtd_start'])}  →  {fmt_iso(ranges['qtd_end'])}")
        print(f"QTD (prior year):   {fmt_iso(ranges['prior_qtd_start'])}  →  {fmt_iso(ranges['prior_qtd_end'])}")
        print(f"\nStore:              {SHOPIFY_STORE}")
        print(f"API version:        {API_VERSION}")
        print(f"Token set:          {'Yes' if SHOPIFY_API_TOKEN else 'No'}")
        return

    print("Generating Relix Shop Daily Report …\n")

    # 1. Yesterday's orders (full data for line items)
    print("[1/3] Fetching yesterday's orders …")
    yesterday_orders = fetch_all_orders(ranges["yesterday_start"], ranges["yesterday_end"])
    yesterday_revenue = sum_revenue(yesterday_orders)
    category_counts = count_units_by_category(yesterday_orders)

    # 2. QTD revenue
    print("\n[2/3] Fetching QTD orders …")
    qtd_orders = fetch_all_orders(
        ranges["qtd_start"], ranges["qtd_end"],
        fields="id,total_price"
    )
    qtd_revenue = sum_revenue(qtd_orders)

    # 3. Prior year QTD revenue
    print("\n[3/3] Fetching prior year QTD orders …")
    prior_orders = fetch_all_orders(
        ranges["prior_qtd_start"], ranges["prior_qtd_end"],
        fields="id,total_price"
    )
    prior_qtd_revenue = sum_revenue(prior_orders)

    # Build report
    report = build_report(
        yesterday_revenue, qtd_revenue, prior_qtd_revenue,
        category_counts, ranges["yesterday_label"]
    )

    print("\n" + "=" * 60)
    print(report)
    print("=" * 60)

    # Send to Slack
    if args.slack:
        if send_to_slack(report):
            print("\n✅ Report sent to Slack.")
        else:
            sys.exit(1)
    else:
        # Copy to clipboard when running manually
        if copy_to_clipboard(report):
            print("\n✅ Report copied to clipboard.")

        # Show sample line items for debugging
        samples = get_sample_line_items(yesterday_orders, n=5)
        if samples:
            print("\n--- Sample line items (for category verification) ---")
            for i, s in enumerate(samples, 1):
                print(f"  {i}. \"{s['title']}\"")
                print(f"     product_type: \"{s['product_type']}\"  →  classified as: {s['classified_as']}")
                print(f"     quantity: {s['quantity']}")


if __name__ == "__main__":
    main()
