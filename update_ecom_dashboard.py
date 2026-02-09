#!/usr/bin/env python3
"""
Relix Ecom Dashboard Updater
Pulls Shopify sales data and updates the Google Sheet dashboard.

Tabs updated:
  - Daily Revenue Tracker   (append yesterday's row)
  - Top Products (7 Days)   (overwrite with rolling window)
  - Top Products (30 Days)  (overwrite with rolling window)
  - Q1 2026 Goals            (update Actual Ecom Revenue only)
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

try:
    import requests
except ImportError:
    print("Error: 'requests' package is required. Install with: pip install requests")
    sys.exit(1)

try:
    import gspread
except ImportError:
    print("Error: 'gspread' package is required. Install with: pip install gspread")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "relix-store.myshopify.com")
SHOPIFY_API_TOKEN = os.environ.get("SHOPIFY_API_TOKEN", "")
API_VERSION = "2024-01"
BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

SPREADSHEET_ID = "1zd6yW2lxiohX29TMSfAVA-z0Z3UGGiR2oysokOrilcY"

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

LOG_DIR = os.path.expanduser("~/relix-tools/logs")
LOG_FILE = os.path.join(LOG_DIR, "dashboard_update.log")

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("dashboard_update")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console_handler)


# ---------------------------------------------------------------------------
# Timezone helper — matches daily_revenue_report.py
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def get_date_ranges():
    """Return all date ranges needed for the dashboard update, in Eastern time."""
    now_et = datetime.now(EASTERN)
    today = now_et.replace(hour=0, minute=0, second=0, microsecond=0)

    yesterday_start = today - timedelta(days=1)
    yesterday_end = today - timedelta(seconds=1)

    # Rolling windows
    seven_days_start = today - timedelta(days=7)
    thirty_days_start = today - timedelta(days=30)

    # Quarter start (Q1 = Jan 1)
    month = today.month
    if month <= 3:
        q_start_month = 1
    elif month <= 6:
        q_start_month = 4
    elif month <= 9:
        q_start_month = 7
    else:
        q_start_month = 10

    qtd_start = today.replace(month=q_start_month, day=1)
    qtd_end = yesterday_end

    # Prior year same window
    prior_yesterday_start = yesterday_start.replace(year=yesterday_start.year - 1)
    prior_yesterday_end = yesterday_end.replace(year=yesterday_end.year - 1)

    return {
        "yesterday_start": yesterday_start,
        "yesterday_end": yesterday_end,
        "yesterday_date": yesterday_start.strftime("%Y-%m-%d"),
        "seven_days_start": seven_days_start,
        "thirty_days_start": thirty_days_start,
        "window_end": yesterday_end,
        "qtd_start": qtd_start,
        "qtd_end": qtd_end,
        "prior_yesterday_start": prior_yesterday_start,
        "prior_yesterday_end": prior_yesterday_end,
    }


def fmt_iso(dt):
    """Format a datetime as ISO 8601 with timezone offset for Shopify."""
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Shopify API helpers — matches daily_revenue_report.py
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
                logger.warning("Rate limited, retrying in %ss …", retry_after)
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                logger.warning("Request error: %s. Retrying in %ss …", e, wait)
                time.sleep(wait)
            else:
                logger.error("Request failed after %d retries: %s", MAX_RETRIES, e)
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
                logger.warning("Rate limited, retrying in %ss …", retry_after)
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                logger.warning("Request error: %s. Retrying in %ss …", e, wait)
                time.sleep(wait)
            else:
                logger.error("Request failed after %d retries: %s", MAX_RETRIES, e)
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
    logger.info("  Fetching orders %s → %s …",
                start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    resp = shopify_get("orders", params)
    if resp is None:
        return all_orders

    data = resp.json()
    orders = data.get("orders", [])
    all_orders.extend(orders)
    logger.info("    Page %d: %d orders", page, len(orders))

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
        logger.info("    Page %d: %d orders", page, len(orders))
        next_url = parse_link_header(resp)

    logger.info("    Total: %d orders", len(all_orders))
    return all_orders


# ---------------------------------------------------------------------------
# Revenue / product helpers
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


def top_products(orders, n=20):
    """Return top N products sorted by units sold descending.

    Returns list of dicts: {title, units, revenue, category}
    """
    products = {}
    for order in orders:
        for item in order.get("line_items", []):
            title = item.get("title", "Unknown")
            price = Decimal(str(item.get("price", "0")))
            qty = item.get("quantity", 0)
            cat = classify_line_item(item) or "Other"

            if title not in products:
                products[title] = {
                    "title": title,
                    "units": 0,
                    "revenue": Decimal("0.00"),
                    "category": cat,
                }
            products[title]["units"] += qty
            products[title]["revenue"] += price * qty

    sorted_products = sorted(products.values(), key=lambda p: p["units"], reverse=True)
    return sorted_products[:n]


def monthly_revenue(orders):
    """Break down order revenue by month. Returns {1: Decimal, 2: Decimal, ...}."""
    by_month = defaultdict(lambda: Decimal("0.00"))
    for order in orders:
        try:
            created = order.get("created_at", "")
            month = int(created[5:7])
            by_month[month] += Decimal(str(order.get("total_price", "0")))
        except Exception:
            pass
    return by_month


# ---------------------------------------------------------------------------
# Google Sheets update helpers
# ---------------------------------------------------------------------------
def open_spreadsheet():
    """Authenticate and open the dashboard spreadsheet.

    In CI (GitHub Actions), reads credentials from GOOGLE_SERVICE_ACCOUNT_JSON
    env var. Locally, falls back to ~/.config/gspread/service_account.json.
    """
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        # Write the JSON to a temp file for gspread
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(sa_json)
        tmp.close()
        gc = gspread.service_account(filename=tmp.name)
        os.unlink(tmp.name)
    else:
        gc = gspread.service_account()
    return gc.open_by_key(SPREADSHEET_ID)


def update_daily_revenue(sh, ranges, yesterday_orders, qtd_orders, prior_yesterday_orders):
    """Append a row to Daily Revenue Tracker for yesterday."""
    ws = sh.worksheet("Daily Revenue Tracker")

    # Check if yesterday's date already exists (prevent duplicate runs)
    date_col = ws.col_values(1)
    yesterday_str = ranges["yesterday_date"]
    if yesterday_str in date_col:
        logger.info("  Daily Revenue: %s already exists, skipping.", yesterday_str)
        return

    yesterday_revenue = sum_revenue(yesterday_orders)
    qtd_revenue = sum_revenue(qtd_orders)
    prior_yesterday_revenue = sum_revenue(prior_yesterday_orders)

    # YoY change as a decimal for percentage formatting (0.15 = 15%)
    if prior_yesterday_revenue > 0:
        yoy = float((yesterday_revenue - prior_yesterday_revenue) / prior_yesterday_revenue)
    else:
        yoy = ""

    row = [
        yesterday_str,
        float(yesterday_revenue),
        float(qtd_revenue),
        float(prior_yesterday_revenue),
        yoy,
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info("  Daily Revenue: appended %s — $%s", yesterday_str, yesterday_revenue)


def update_top_products_tab(sh, tab_name, orders):
    """Overwrite a Top Products tab with fresh data."""
    ws = sh.worksheet(tab_name)

    products = top_products(orders, n=20)

    # Clear data rows (keep header)
    if ws.row_count > 1:
        ws.batch_clear(["A2:E22"])

    if not products:
        logger.info("  %s: no products to write.", tab_name)
        return

    rows = []
    for rank, p in enumerate(products, 1):
        rows.append([
            rank,
            p["title"],
            p["units"],
            float(p["revenue"]),
            p["category"],
        ])

    ws.update(f"A2:E{1 + len(rows)}", rows, value_input_option="USER_ENTERED")
    logger.info("  %s: wrote %d products.", tab_name, len(rows))


def update_goals_tab(sh, qtd_orders):
    """Update Actual Ecom Revenue (column C) in Q1 2026 Goals.

    Does NOT touch Ad Revenue columns (D, E) — those are manual entry.
    """
    ws = sh.worksheet("Q1 2026 Goals")

    by_month = monthly_revenue(qtd_orders)

    # Rows: 2=January, 3=February, 4=March
    # Column C = Actual Ecom Revenue
    updates = []
    for month_num, row_num in [(1, 2), (2, 3), (3, 4)]:
        revenue = by_month.get(month_num, Decimal("0.00"))
        updates.append({
            "range": f"C{row_num}",
            "values": [[float(revenue)]],
        })

    ws.batch_update(updates, value_input_option="USER_ENTERED")
    logger.info("  Q1 Goals: updated Actual Ecom Revenue — Jan=$%s, Feb=$%s, Mar=$%s",
                by_month.get(1, 0), by_month.get(2, 0), by_month.get(3, 0))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Relix Ecom Dashboard Updater")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show date ranges and exit without API calls")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Dashboard update started")

    ranges = get_date_ranges()

    if args.dry_run:
        logger.info("DRY RUN — date ranges that would be queried:")
        logger.info("  Yesterday:       %s → %s",
                     fmt_iso(ranges["yesterday_start"]), fmt_iso(ranges["yesterday_end"]))
        logger.info("  7-day window:    %s → %s",
                     fmt_iso(ranges["seven_days_start"]), fmt_iso(ranges["window_end"]))
        logger.info("  30-day window:   %s → %s",
                     fmt_iso(ranges["thirty_days_start"]), fmt_iso(ranges["window_end"]))
        logger.info("  QTD:             %s → %s",
                     fmt_iso(ranges["qtd_start"]), fmt_iso(ranges["qtd_end"]))
        logger.info("  Prior year day:  %s → %s",
                     fmt_iso(ranges["prior_yesterday_start"]),
                     fmt_iso(ranges["prior_yesterday_end"]))
        logger.info("  Store:           %s", SHOPIFY_STORE)
        logger.info("  Token set:       %s", "Yes" if SHOPIFY_API_TOKEN else "No")
        logger.info("  Spreadsheet:     %s", SPREADSHEET_ID)
        return

    # Validate
    if not SHOPIFY_API_TOKEN:
        logger.error("SHOPIFY_API_TOKEN environment variable is not set.")
        sys.exit(1)

    # ----- Fetch data from Shopify -----

    logger.info("[1/5] Fetching yesterday's orders …")
    yesterday_orders = fetch_all_orders(
        ranges["yesterday_start"], ranges["yesterday_end"])

    logger.info("[2/5] Fetching prior year same-day orders …")
    prior_yesterday_orders = fetch_all_orders(
        ranges["prior_yesterday_start"], ranges["prior_yesterday_end"],
        fields="id,total_price")

    logger.info("[3/5] Fetching last 7 days orders …")
    seven_day_orders = fetch_all_orders(
        ranges["seven_days_start"], ranges["window_end"])

    logger.info("[4/5] Fetching last 30 days orders …")
    thirty_day_orders = fetch_all_orders(
        ranges["thirty_days_start"], ranges["window_end"])

    logger.info("[5/5] Fetching QTD orders …")
    qtd_orders = fetch_all_orders(
        ranges["qtd_start"], ranges["qtd_end"],
        fields="id,total_price,created_at")

    # ----- Update Google Sheet -----

    logger.info("Opening spreadsheet …")
    sh = open_spreadsheet()

    logger.info("Updating Daily Revenue Tracker …")
    update_daily_revenue(sh, ranges, yesterday_orders, qtd_orders, prior_yesterday_orders)

    logger.info("Updating Top Products (7 Days) …")
    update_top_products_tab(sh, "Top Products (7 Days)", seven_day_orders)

    logger.info("Updating Top Products (30 Days) …")
    update_top_products_tab(sh, "Top Products (30 Days)", thirty_day_orders)

    logger.info("Updating Q1 2026 Goals …")
    update_goals_tab(sh, qtd_orders)

    logger.info("Dashboard update complete.")
    logger.info("View at: https://docs.google.com/spreadsheets/d/%s/edit", SPREADSHEET_ID)


if __name__ == "__main__":
    main()
