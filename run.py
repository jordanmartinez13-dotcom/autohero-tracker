import requests
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import date
from pathlib import Path
from time import sleep

# --- Config ---
SHEET_ID = "1YAZWwCZ5Vf-GhMkRJaBoMlysJtkxyQothxnte6xU_cg"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
URL = "https://api-customer.prod.retail.auto1.cloud/v1/retail-customer-gateway/graphql/searchAdV9AdsV2"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://www.autohero.com",
    "Referer": "https://www.autohero.com/",
}
MARKETS = ["DE", "FR", "ES", "IT", "NL", "BE", "PL", "AT"]
PAGE_SIZE = 100
YESTERDAY_IDS_PATH = "yesterday_ids.json"

# --- Collection ---
def search(country, offset=0, limit=PAGE_SIZE):
    payload = {
        "operationName": "searchAdV9AdsV2",
        "variables": {
            "search": {
                "offset": offset,
                "limit": limit,
                "sort": "most_popular",
                "filter": {
                    "field": None,
                    "op": "and",
                    "value": [
                        {"field": "countryCode", "op": "eq", "value": country}
                    ]
                }
            }
        },
        "query": "query searchAdV9AdsV2($search: EsSearchRequestProjectionInput!, $tradeInId: UUID) { searchAdV9AdsV2(search: $search, tradeInId: $tradeInId) }"
    }
    r = requests.post(URL, headers=HEADERS, json=payload, timeout=20)
    return r.json()["data"]["searchAdV9AdsV2"]

def get_all_listings(country):
    total = search(country, offset=0, limit=1)["total"]
    print(f"  {country}: {total} total listings, paginating...")
    all_listings = []
    offset = 0
    while offset < total:
        batch = search(country, offset=offset, limit=PAGE_SIZE)["data"]
        if not batch:
            break
        all_listings.extend(batch)
        offset += len(batch)
        print(f"    fetched {offset}/{total}")
        sleep(0.5)
    return all_listings

def collect():
    results = []
    for country in MARKETS:
        print(f"\nCollecting {country}...")
        listings = get_all_listings(country)
        prices_by_make = {}
        for l in listings:
            make = l.get("manufacturer", "Unknown")
            price = l.get("offerPrice", {}).get("amountMinorUnits", 0) / 100
            prices_by_make.setdefault(make, []).append(price)
        results.append({
            "country": country,
            "total": len(listings),
            "ids": [l["id"] for l in listings],
            "avg_prices_by_make": {
                make: round(sum(p) / len(p), 2)
                for make, p in prices_by_make.items()
            }
        })
        print(f"  Done. {len(listings)} listings collected.")
    return results

# --- Writing ---
def get_sheet(tab_name):
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet(tab_name)

def write_all(data):
    today = str(date.today())

    # Load yesterday's IDs
    if Path(YESTERDAY_IDS_PATH).exists():
        yesterday = json.loads(Path(YESTERDAY_IDS_PATH).read_text())
    else:
        yesterday = {}

    today_ids = {}
    snapshot_rows = []
    breakdown_rows = []
    price_rows = []

    for market in data:
        country = market["country"]
        total = market["total"]
        current_ids = set(market["ids"])
        today_ids[country] = list(current_ids)

        prev_ids = set(yesterday.get(country, []))
        new = len(current_ids - prev_ids) if prev_ids else 0
        removed = len(prev_ids - current_ids) if prev_ids else 0

        snapshot_rows.append([today, country, total, new, removed])
        breakdown_rows.append([today, country, total])

        for make, avg_price in market["avg_prices_by_make"].items():
            price_rows.append([today, country, make, avg_price])

    print("\nWriting to Google Sheets...")
    get_sheet("daily_snapshots").append_rows(snapshot_rows, value_input_option="USER_ENTERED")
    print(f"  daily_snapshots: {len(snapshot_rows)} rows")

    get_sheet("market_breakdown").append_rows(breakdown_rows, value_input_option="USER_ENTERED")
    print(f"  market_breakdown: {len(breakdown_rows)} rows")

    get_sheet("prices_by_make").append_rows(price_rows, value_input_option="USER_ENTERED")
    print(f"  prices_by_make: {len(price_rows)} rows")

    Path(YESTERDAY_IDS_PATH).write_text(json.dumps(today_ids))
    print("  yesterday_ids.json updated")

# --- Main ---
def run():
    print("=== AutoHero daily collection ===")
    print(f"Date: {date.today()}")
    data = collect()
    write_all(data)

    print("\n=== Summary ===")
    print(f"{'Country':<10} {'Total':>8} {'New':>8} {'Removed':>8}")
    print("-" * 36)

    if Path(YESTERDAY_IDS_PATH).exists():
        yesterday = json.loads(Path(YESTERDAY_IDS_PATH).read_text())
    else:
        yesterday = {}

    for m in data:
        c = m["country"]
        curr = set(m["ids"])
        prev = set(yesterday.get(c, []))
        new = len(curr - prev) if prev else 0
        removed = len(prev - curr) if prev else 0
        print(f"{c:<10} {m['total']:>8,} {new:>8,} {removed:>8,}")

    print("\nDone!")

try:
    run()
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()