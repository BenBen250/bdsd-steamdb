import requests
import json
import random
from pathlib import Path
import time

# Step 1: Get all Steam apps
def fetch_app_list():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    data = response.json()
    return data["applist"]["apps"]

# Step 2: Pick 1000 random apps
def pick_1000_apps(apps, n=1000):
    return random.sample(apps, n)

# Step 3: Fetch details for each app
def fetch_app_details(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if str(appid) in data and data[str(appid)]["success"]:
            return data[str(appid)]["data"]
    except Exception as e:
        print(f"Error fetching {appid}: {e}")
    return None

# Step 4: Collect dataset
def collect_games():
    raw_dir = Path("raw")
    raw_dir.mkdir(exist_ok=True)

    apps = fetch_app_list()
    print(f"Total apps on Steam: {len(apps)}")

    sample_apps = pick_1000_apps(apps)
    print(f"Picked {len(sample_apps)} random apps.")

    games_data = []
    for i, app in enumerate(sample_apps, 1):
        details = fetch_app_details(app["appid"])
        if details:
            games_data.append({
                "appid": app["appid"],
                "name": details.get("name"),
                "release_date": details.get("release_date", {}).get("date"),
                "price": details.get("price_overview", {}).get("final") if details.get("price_overview") else None,
                "genres": [g["description"] for g in details.get("genres", [])],
                "type": details.get("type")
            })
        if i % 50 == 0:
            print(f"Fetched {i} apps so far...")
        time.sleep(0.2)  # avoid hitting API too fast

    output_file = raw_dir / "games_1000.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(games_data, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(games_data)} games to {output_file}")

if __name__ == "__main__":
    collect_games()
