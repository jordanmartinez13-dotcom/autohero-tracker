import requests
import json

url = "https://api-customer.prod.retail.auto1.cloud/v1/retail-customer-gateway/graphql/searchAdV9AdsV2"

headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://www.autohero.com",
    "Referer": "https://www.autohero.com/",
}

payload = {
    "operationName": "searchAdV9AdsV2",
    "variables": {
        "search": {
            "offset": 0,
            "limit": 10,
            "sort": "most_popular",
            "filter": {
                "field": None,
                "op": "and",
                "value": [
                    {
                        "field": "countryCode",
                        "op": "eq",
                        "value": "DE"
                    }
                ]
            }
        }
    },
    "query": "query searchAdV9AdsV2($search: EsSearchRequestProjectionInput!, $tradeInId: UUID) { searchAdV9AdsV2(search: $search, tradeInId: $tradeInId) }"
}

response = requests.post(url, headers=headers, json=payload, timeout=15)

print(f"Status code: {response.status_code}")
print("\n--- Response ---")

data = response.json()
json.dump(data, open("test_response.json", "w"), indent=2)
print("Full response saved to test_response.json")
print("\nTop-level keys:", list(data.keys()) if isinstance(data, dict) else "not a dict")