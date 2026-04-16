import httpx, json

slug = "btc-updown-5m-1776294000"
r = httpx.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
events = r.json()
if events:
    e = events[0]
    m = e.get("markets", [{}])[0]
    # Print all keys to find price_to_beat related fields
    for k in sorted(m.keys()):
        v = m[k]
        if isinstance(v, (int, float)):
            print(f"  {k}: {v}")
        elif isinstance(v, str) and len(v) < 100:
            print(f"  {k}: {v}")
        else:
            print(f"  {k}: ({type(v).__name__}, len={len(str(v))})")

    # Also try different price-to-beat URL patterns
    condition_id = m.get("conditionId", "")
    print(f"\nCondition ID: {condition_id}")
    
    # Try with condition_id
    for url_pattern in [
        f"https://polymarket.com/api/equity/price-to-beat/{slug}",
        f"https://gamma-api.polymarket.com/markets/{condition_id}",
    ]:
        try:
            r2 = httpx.get(url_pattern, timeout=5)
            print(f"\n{url_pattern}\n  Status: {r2.status_code}")
            if r2.status_code == 200:
                data = r2.json()
                if isinstance(data, dict):
                    for k2 in sorted(data.keys()):
                        v2 = data[k2]
                        if isinstance(v2, (int, float)):
                            print(f"  {k2}: {v2}")
                        elif isinstance(v2, str) and (k2.lower().find('price') >= 0 or len(v2) < 60):
                            print(f"  {k2}: {v2}")
        except Exception as ex:
            print(f"  Error: {ex}")
else:
    print("No events found")
