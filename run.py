def load_id_cache(client):
    ws = get_sheet(client, "id_cache")
    rows = ws.get_all_values()
    cache = {}
    for row in rows:
        if len(row) >= 2:
            country, listing_id = row[0], row[1]
            if country not in cache:
                cache[country] = set()
            cache[country].add(listing_id)
    print(f"Loaded ID cache for {list(cache.keys())}")
    return cache

def save_id_cache(client, today_ids):
    ws = get_sheet(client, "id_cache")
    ws.clear()
    rows = []
    for country, ids in today_ids.items():
        for listing_id in ids:
            rows.append([country, listing_id])
    ws.update(rows, "A1")
    print(f"Saved {len(rows)} IDs to cache")
```

This stores one row per listing ID like:
```
DE    e1261a76-f83f-4c2b-a7d3-0af5cff1d25d
DE    7910c553-033f-4106-8ba8-7731e7ed0db7
FR    ...