from playwright.sync_api import sync_playwright
import json, time

def discover_api():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        captured = []

        def on_request(request):
            url = request.url
            # Skip obvious noise
            skip = ["google", "facebook", "tiktok", "bing", "doubleclick",
                    "analytics", "tvsquared", "translate", "appleid", "collect"]
            if any(s in url for s in skip):
                return
            try:
                post_data = request.post_data
                captured.append({
                    "url": url,
                    "method": request.method,
                    "resource_type": request.resource_type,
                    "body": json.loads(post_data) if post_data else None,
                })
            except:
                captured.append({"url": url, "method": request.method})

        page.on("request", on_request)
        page.goto("https://www.autohero.com/de/search/", wait_until="domcontentloaded", timeout=60000)
        print("Page loaded. Waiting 10 seconds...")
        time.sleep(10)

        input("Done. Press Enter to save and close...")
        browser.close()

    json.dump(captured, open("api_discovery.json", "w"), indent=2)
    print(f"\nCaptured {len(captured)} calls.")
    print("\n--- All URLs ---")
    for r in captured:
        print(f"[{r.get('resource_type','?').upper()}] {r['url'][:120]}")

discover_api()