import requests
import json
import random
from pathlib import Path
import time

def fetch_app_list():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    data = response.json()
    return data["applist"]["apps"]


def pick_1000_apps(apps, n=1000):
    return random.sample(apps, n)


def fetch_app_details(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url, timeout=5)