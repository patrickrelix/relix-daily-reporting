#!/usr/bin/env python3
"""
Relix Shop Daily Revenue Report
Pulls yesterday's sales data from Shopify and generates a formatted report.
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

try:
    import requests
except ImportError:
    print("Error: 'requests' package is required. Install with: pip install requests")
    sys.exit(1)

SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "relix-store.myshopify.com")
SHOPIFY_API_TOKEN = os.environ.get("SHOPIFY_API_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
API_VERSION = "2024-01"
BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

EXCLUDED_PRODUCT_TYPES = {"Stickers", "stickers"}

QTD_OFFSET = Decimal("17140.00")

RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 5

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from datetime import timezone as _tz

    class _Eastern(_tz):
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


def get_date_ranges(reference_date=None):
    now_et = datetime.now(EASTERN)
    today_et = (reference_date or now_et).replace(hour=0, minute=0, second=0, microsecond=0)
    if reference_date is None:
        today_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)

    yesterday_start = today_et - timedelta(days=1)
    yesterday_end = today_et - timedelta(seconds=1)

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
    qtd_end = yesterday_end

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
    return dt.isoformat()


def shopify_headers():
    return {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json",
    }


def shopify_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}.json"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=shopify_headers(), params=params, timeout=30)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"ERROR: {e}")
                return None
    return None


def parse_link_header(resp):
    link = resp.headers.get("Link", "")
    for part in link.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip().strip("<>")
            return url
    return None


def shopify_get_url(url):
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=shopify_headers(), timeout=30)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"ERROR: {e}")
                return None
    return None


def fetch_all_orders(start, end, fields=None):
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
    print(f"  Fetching orders {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")

    resp = shopify_get("orders", params)
    if resp is None:
        return all_orders

    data = resp.json()
    orders = data.get("orders", [])
    all_orders.extend(orders)
    print(f"    Page {page}: {len(orders)} orders")

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


def sum_revenue(orders):
    total = Decimal("0.00")
    for order in orders:
        try:
            total += Decimal(str(order.get("total_price", "0")))
        except Exception:
            pass
    return total


def classify_line_item(item):
    product_type = (item.get("product_type") or "").strip()
    if not product_type:
        return None
    if product_type in EXCLUDED_PRODUCT_TYPES:
        return None
    return product_type


def count_units_by_category(orders):
    counts = {}
    for order in orders:
        for item in order.get("line_items", []):
            cat = classify_line_item(item)
            if cat:
                counts[cat] = counts.get(cat, 0) + item.get("quantity", 0)
    return counts


def get_sample_line_items(orders, n=10):
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


def top_products_by_revenue(orders, n=3):
    products = {}
    for order in orders:
        for item in order.get("line_items", []):
            title = item.get("title", "Unknown")
            price = Decimal(str(item.get("price", "0")))
            qty = item.get("quantity", 0)
            products[title] = products.get(title, Decimal("0")) + price * qty
    return sorted(products.items(), key=lambda x: x[1], reverse=True)[:n]


def top_products_by_units(orders, n=3):
    products = {}
    for order in orders:
        for item in order.get("line_items", []):
            title = item.get("title", "Unknown")
            qty = item.get("quantity", 0)
            products[title] = products.get(title, 0) + qty
    return sorted(products.items(), key=lambda x: x[1], reverse=True)[:n]


def format_currency(amount):
    rounded = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"${rounded:,.2f}"


def format_currency_nodecimal(amount):
    rounded = amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"${rounded:,}"


def format_yoy(current, prior):
    if prior == 0:
        return "N/A"
    change = ((current - prior) / prior) * 100
    change = change.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    sign = "+" if change >= 0 else ""
    return f"{sign}{change}%"


def build_report(yesterday_revenue, qtd_revenue, prior_qtd_revenue,
                 category_counts, date_label, top_by_revenue, top_by_units):
    adjusted_qtd = qtd_revenue + QTD_OFFSET
    yoy = format_yoy(adjusted_qtd, prior_qtd_revenue)

    lines = [
        f"Yesterday: {format_currency(yesterday_revenue)}",
        f"QTD: {format_currency_nodecimal(adjusted_qtd)} (vs {format_currency(prior_qtd_revenue)} last year -> {yoy})",
        "",
    ]

    cats_over_10 = sorted(
        [(cat, count) for cat, count in category_counts.items() if count >= 10],
        key=lambda x: x[1], reverse=True
    )
    if cats_over_10:
        lines.append("Categories with 10+ units sold:")
        for cat, count in cats_over_10:
            lines.append(f"* {cat}: {count} units")

    if top_by_revenue:
        lines.append("")
        lines.append("Top 3 products by gross sales:")
        for i, (title, revenue) in enumerate(top_by_revenue, 1):
            lines.append(f"{i}. {title} - {format_currency(revenue)}")

    if top_by_units:
        lines.append("")
        lines.append("Top 3 products by units:")
        for i, (title, units) in enumerate(top_by_units, 1):
            lines.append(f"{i}. {title} - {units} units")

    return "\n".join(lines)


def copy_to_clipboard(text):
    try:
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.communicate(text.encode("utf-8"))
        return True
    except Exception as e:
        print(f"Warning: Could not copy to clipboard: {e}")
        return False


def send_to_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("ERROR: SLACK_WEBHOOK_URL not set.")
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


def main():
    parser = argparse.ArgumentParser(description="Relix Shop Daily Revenue Report")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--slack", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not SHOPIFY_API_TOKEN:
        print("ERROR: SHOPIFY_API_TOKEN not set.")
        sys.exit(1)

    ranges = get_date_ranges()

    if args.dry_run:
        print("=== DRY RUN ===")
        print(f"Yesterday: {fmt_iso(ranges['yesterday_start'])} to {fmt_iso(ranges['yesterday_end'])}")
        print(f"QTD: {fmt_iso(ranges['qtd_start'])} to {fmt_iso(ranges['qtd_end'])}")
        print(f"Prior year QTD: {fmt_iso(ranges['prior_qtd_start'])} to {fmt_iso(ranges['prior_qtd_end'])}")
        print(f"Token set: {'Yes' if SHOPIFY_API_TOKEN else 'No'}")
        return

    print("Generating Relix Shop Daily Report\n")

    print("[1/3] Fetching yesterday's orders")
    yesterday_orders = fetch_all_orders(ranges["yesterday_start"], ranges["yesterday_end"])
    yesterday_revenue = sum_revenue(yesterday_orders)
    category_counts = count_units_by_category(yesterday_orders)

    print("\n[2/3] Fetching QTD orders")
    qtd_orders = fetch_all_orders(ranges["qtd_start"], ranges["qtd_end"], fields="id,total_price")
    qtd_revenue = sum_revenue(qtd_orders)

    print("\n[3/3] Fetching prior year QTD orders")
    prior_orders = fetch_all_orders(ranges["prior_qtd_start"], ranges["prior_qtd_end"], fields="id,total_price")
    prior_qtd_revenue = sum_revenue(prior_orders)

    top_by_revenue = top_products_by_revenue(yesterday_orders)
    top_by_units = top_products_by_units(yesterday_orders)

    report = build_report(
        yesterday_revenue, qtd_revenue, prior_qtd_revenue,
        category_counts, ranges["yesterday_label"],
        top_by_revenue, top_by_units
    )

    print("\n" + "=" * 60)
    print(report)
    print("=" * 60)

    if args.slack:
        if send_to_slack(report):
            print("\nReport sent to Slack.")
        else:
            sys.exit(1)
    else:
        if copy_to_clipboard(report):
            print("\nReport copied to clipboard.")

        samples = get_sample_line_items(yesterday_orders, n=10)
        if samples:
            print("\n--- Sample line items ---")
            for i, s in enumerate(samples, 1):
                print(f"  {i}. {s['title']}")
                print(f"     product_type: {s['product_type']}  classified as: {s['classified_as']}")
                print(f"     quantity: {s['quantity']}")


if __name__ == "__main__":
    main()