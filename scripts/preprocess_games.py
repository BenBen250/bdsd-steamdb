import json
import pandas as pd
from pathlib import Path

def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def preprocess(file_list, output_file):
    all_records = []
    for file_path in file_list:
        data = load_json(file_path)
        for r in data:
            all_records.append({
                "appid": r.get("appid"),
                "name": r.get("name"),
                "release_date": r.get("release_date"),
                "price": r.get("price"),
                "genres": ", ".join(r.get("genres", [])),
                "type": r.get("type"),
                "timestamp": Path(file_path).stem
            })

    df = pd.DataFrame(all_records)
    df.to_csv(output_file, index=False)
    print(f"Saved cleaned dataset with {len(df)} rows to {output_file}")

if __name__ == "__main__":
    files_to_process = [
        "../raw/games_1000.json",
        "../raw/top100.json"  # replace with your actual top100 file
    ]
    output_csv = "../data/games_clean.csv"
    Path("../data").mkdir(exist_ok=True)
    preprocess(files_to_process, output_csv)
