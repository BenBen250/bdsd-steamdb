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