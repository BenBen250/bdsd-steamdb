import json
from decimal import Decimal

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

input_file = "../data/games.json"
output_file = "../data/games_clean.jsonl"

with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)  # parse whole JSON file at once

with open(output_file, "w", encoding="utf-8") as out:
    for key, value in data.items():
        out.write(json.dumps(value, ensure_ascii=False, default=convert_decimal) + "\n")

