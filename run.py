import requests
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import date, datetime, timezone, timedelta
from time import sleep

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

def get_client():
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def get_sheet(client, tab_name):
    return client.worksheet(tab_name)

def load_id_cache(client):
    ws = get_sheet(client, "id_cache")
    rows = ws.get_all_values()
    cache = {}
    for row in rows:
        if len(row) >= 3:
            country, listing_id, published_at = row[0], row[1], row[2]
            if country not in cache:
                cache[country] = {}
            cache[country][listing_id] = published_at
        elif len(row) >= 2:
            country, listing_id = row[0], row[1]
            if country not in cache:
                cache[country] = {}
            cache[country][listing_id] = ""
    print(f"Loaded ID cache for {list(cache.keys())}")
    return cache

def save_id_cache(client, today_ids):
    ws = get_sheet(client, "id_cache")
    ws.clear()
    rows = []
    for country, id_map in today_ids.items():
        for listing_id, published_at in id_map.items():
            rows.append([country, listing_id, published_at])
    ws.update(rows, "A1")
    print(f"Saved {len(rows)} IDs to cache")

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

def parse_published_at(ts):
    if not ts:
        return None
    try:
        clean = ts.replace("Z", "+00:00").replace(".000+00:00", "+00:00")
        # Format: 20260324T064814.000Z
        dt = datetime.strptime(ts[:15], "%Y%m%dT%H%M%S")
        return dt.replace(tzinfo=timezone.utc)
    except:
        return None

def collect():
    results = []
    for country in MARKETS:
        print(f"\nCollecting {country}...")
        listings = get_all_listings(country)
        prices_by_make = {}
        id_map = {}
        for l in listings:
            lid = l["id"]
            published_at = l.get("firstPublishedAt", "")
            id_map[lid] = published_at

            make = l.get("manufacturer", "Unknown")
            price = l.get("offerPrice", {}).get("amountMinorUnits", 0) / 100
            prices_by_make.setdefault(make, []).append(price)

        results.append({
            "country": country,
            "total": len(listings),
            "id_map": id_map,
            "prices_by_make": {
                make: {
                    "avg": round(sum(p) / len(p), 2),
                    "count": len(p)
                }
                for make, p in prices_by_make.items()
            }
        })
        print(f"  Done. {len(listings)} listings collected.")
    return results

def compute_velocity(country, today_id_map, yesterday_cache):
    today_ids = set(today_id_map.keys())
    yesterday_ids = set(yesterday_cache.get(country, {}).keys())

    if not yesterday_ids:
        return 0, 0

    # Removed = in yesterday but not today (genuine delistings/sales)
    removed = len(yesterday_ids - today_ids)

    # New = in today but not yesterday AND firstPublishedAt within last 48hrs
    # This filters out sort-shuffle artefacts
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    genuinely_new = 0
    for lid in (today_ids - yesterday_ids):
        published_at = today_id_map.get(lid, "")
        dt = parse_published_at(published_at)
        if dt and dt >= cutoff:
            genuinely_new += 1
        elif not dt:
            genuinely_new += 1

    return genuinely_new, removed

def write_all(client, data, yesterday_cache):
    today = str(date.today())
    today_ids = {}
    snapshot_rows = []
    breakdown_rows = []
    price_rows = []

    for market in data:
        country = market["country"]
        total = market["total"]
        today_ids[country] = market["id_map"]

        new, removed = compute_velocity(country, market["id_map"], yesterday_cache)

        snapshot_rows.append([today, country, total, new, removed])
        breakdown_rows.append([today, country, total])

        for make, stats in market["prices_by_make"].items():
            price_rows.append([today, country, make, stats["avg"], stats["count"]])

    print("\nWriting to Google Sheets...")
    get_sheet(client, "daily_snapshots").append_rows(snapshot_rows, value_input_option="USER_ENTERED")
    print(f"  daily_snapshots: {len(snapshot_rows)} rows")
    get_sheet(client, "market_breakdown").append_rows(breakdown_rows, value_input_option="USER_ENTERED")
    print(f"  market_breakdown: {len(breakdown_rows)} rows")
    get_sheet(client, "prices_by_make").append_rows(price_rows, value_input_option="USER_ENTERED")
    print(f"  prices_by_make: {len(price_rows)} rows")

    return today_ids

def run():
    print("=== AutoHero daily collection ===")
    print(f"Date: {date.today()}")

    client = get_client()
    yesterday_cache = load_id_cache(client)
    data = collect()
    today_ids = write_all(client, data, yesterday_cache)
    save_id_cache(client, today_ids)

    print("\n=== Summary ===")
    print(f"{'Country':<10} {'Total':>8} {'New':>8} {'Removed':>8}")
    print("-" * 36)
    for m in data:
        c = m["country"]
        new, removed = compute_velocity(c, m["id_map"], yesterday_cache)
        print(f"{c:<10} {m['total']:>8,} {new:>8,} {removed:>8,}")

    print("\nDone!")

try:
    run()
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()