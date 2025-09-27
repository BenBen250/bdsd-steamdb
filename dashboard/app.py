from flask import Flask, jsonify, render_template_string
import pandas as pd
import os
import json

app = Flask(__name__)

# ---------- Load & Clean Data ----------
data_path = os.path.join(os.path.dirname(__file__), "..", "data", "steam_games_processed")

df = pd.read_parquet(data_path)

# Convert columns that must be numeric
for col in ["price", "recommendations", "user_score"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Drop rows where these critical fields are missing
df = df.dropna(subset=["price"])
df["recommendations"] = df["recommendations"].fillna(0)
df["user_score"] = df["user_score"].fillna(0)

# ---------- Routes ----------
@app.route("/")
def index():
    total_games = len(df)
    avg_price = df["price"].mean()

    # Top genres (top 100 for text viz)
    top_genres = (
        df["genres"].dropna()
        .str.split(",")
        .explode()
        .str.strip()
        .value_counts()
        .head(100)
        .to_dict()
    )

    # Price ranges
    bins = [0, 5, 10, 15, 20, 25, 30, 100]
    labels = ["0-5", "5-10", "10-15", "15-20", "20-25", "25-30", "30+"]
    price_ranges = pd.cut(df["price"], bins=bins, labels=labels).value_counts().to_dict()

    # Top downloaded / highest rated
    top_downloaded = (
        df.sort_values("recommendations", ascending=False)
          .head(15)[["name", "recommendations"]]
          .to_dict("records")
    )
    highest_rated = (
        df.sort_values("user_score", ascending=False)
          .head(5)[["name", "user_score"]]
          .to_dict("records")
    )

    # Prepare data for charts
    genres_labels = list(top_genres.keys())
    genres_data = list(top_genres.values())
    genres_parents = [''] * len(top_genres)
    price_labels = list(price_ranges.keys())
    price_data = list(price_ranges.values())
    downloaded_labels = [r['name'] for r in top_downloaded]
    downloaded_data = [int(r['recommendations']) for r in top_downloaded]
    rated_labels = [r['name'] for r in highest_rated]
    rated_data = [r['user_score'] for r in highest_rated]

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Steam Games Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <h1>Steam Games Dashboard</h1>
        <h2>Stats</h2>
        <p>Total Games: {total_games}</p>
        <p>Average Price: ${avg_price:.2f}</p>
        
        <h2>Top 100 Genres Treemap</h2>
        <div id="genresTreemap" style="width:800px;height:600px;"></div>
        
        <h2>Price Ranges</h2>
        <canvas id="priceChart" width="400" height="200"></canvas>
        
        <h2>Top 15 Downloaded Games</h2>
        <canvas id="downloadedChart" width="400" height="200"></canvas>
        
        <h2>Highest Rated</h2>
        <canvas id="ratedChart" width="400" height="200"></canvas>
        
        <script>
            // Genres Treemap
            Plotly.newPlot('genresTreemap', [{{
                type: 'treemap',
                labels: {genres_labels},
                parents: {genres_parents},
                values: {genres_data},
                pathbar: {{visible: false}},
                textinfo: 'label'
            }}]);

            // Price Ranges Chart
            new Chart(document.getElementById('priceChart'), {{
                type: 'bar',
                data: {{
                    labels: {price_labels},
                    datasets: [{{
                        label: 'Count',
                        data: {price_data},
                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                        borderColor: 'rgba(255, 99, 132, 1)',
                        borderWidth: 1
                    }}]
                }}
            }});
            
            // Top Downloaded Pie Chart
            new Chart(document.getElementById('downloadedChart'), {{
                type: 'pie',
                data: {{
                    labels: {downloaded_labels},
                    datasets: [{{
                        label: 'Recommendations',
                        data: {downloaded_data},
                        backgroundColor: [
                            'rgba(255, 99, 132, 0.2)',
                            'rgba(54, 162, 235, 0.2)',
                            'rgba(255, 206, 86, 0.2)',
                            'rgba(75, 192, 192, 0.2)',
                            'rgba(153, 102, 255, 0.2)',
                            'rgba(255, 159, 64, 0.2)',
                            'rgba(199, 199, 199, 0.2)',
                            'rgba(83, 102, 255, 0.2)',
                            'rgba(255, 99, 255, 0.2)',
                            'rgba(99, 255, 132, 0.2)',
                            'rgba(255, 132, 99, 0.2)',
                            'rgba(132, 255, 99, 0.2)',
                            'rgba(99, 132, 255, 0.2)',
                            'rgba(255, 255, 99, 0.2)',
                            'rgba(99, 255, 255, 0.2)'
                        ],
                        borderColor: [
                            'rgba(255, 99, 132, 1)',
                            'rgba(54, 162, 235, 1)',
                            'rgba(255, 206, 86, 1)',
                            'rgba(75, 192, 192, 1)',
                            'rgba(153, 102, 255, 1)',
                            'rgba(255, 159, 64, 1)',
                            'rgba(199, 199, 199, 1)',
                            'rgba(83, 102, 255, 1)',
                            'rgba(255, 99, 255, 1)',
                            'rgba(99, 255, 132, 1)',
                            'rgba(255, 132, 99, 1)',
                            'rgba(132, 255, 99, 1)',
                            'rgba(99, 132, 255, 1)',
                            'rgba(255, 255, 99, 1)',
                            'rgba(99, 255, 255, 1)'
                        ],
                        borderWidth: 1
                    }}]
                }}
            }});
            
            // Highest Rated Chart
            new Chart(document.getElementById('ratedChart'), {{
                type: 'bar',
                data: {{
                    labels: {rated_labels},
                    datasets: [{{
                        label: 'User Score',
                        data: {rated_data},
                        backgroundColor: 'rgba(255, 206, 86, 0.2)',
                        borderColor: 'rgba(255, 206, 86, 1)',
                        borderWidth: 1
                    }}]
                }}
            }});
        </script>
    </body>
    </html>
    """
    return html


@app.route("/genre")
def genre_data():
    counts = (
        df["genres"].dropna()
        .str.split(",")
        .explode()
        .str.strip()
        .value_counts()
        .to_dict()
    )
    return jsonify(counts)


@app.route("/price_ranges")
def price_ranges_api():
    bins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 100]
    labels = ["0-5", "5-10", "10-15", "15-20", "20-25", "25-30",
              "30-35", "35-40", "40-45", "45-50", "50+"]
    df_tmp = df.copy()
    df_tmp["price_range"] = pd.cut(df_tmp["price"], bins=bins, labels=labels, right=False)
    return jsonify(df_tmp["price_range"].value_counts().sort_index().to_dict())


@app.route("/top_downloaded")
def top_downloaded_api():
    top = df.sort_values("recommendations", ascending=False).head(10)[
        ["name", "recommendations"]
    ].to_dict("records")
    return jsonify(top)


@app.route("/highest_rated")
def highest_rated_api():
    top = df.sort_values("user_score", ascending=False).head(10)[
        ["name", "user_score"]
    ].to_dict("records")
    return jsonify(top)


if __name__ == "__main__":
    app.run(debug=True)
