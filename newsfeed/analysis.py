import json
import re
from collections import Counter, defaultdict, deque
from html import escape

import numpy as np
import pandas as pd


def _date_series(df, date_col):
    values = df[date_col]
    if pd.api.types.is_numeric_dtype(values):
        values = values.astype(str)
    return pd.to_datetime(values, errors="coerce")


def _split_tokens(value):
    if pd.isna(value):
        return []
    parts = re.split(r"[;,|]", str(value))
    return [part.split(",")[0].strip() for part in parts if part.split(",")[0].strip()]


def _write_html(html, output=None):
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(html)
    return html


def event_frequency_trends(df, date_col="SQLDATE", freq="D"):
    dates = _date_series(df, date_col)
    result = (
        pd.DataFrame({"date": dates})
        .dropna()
        .set_index("date")
        .resample(freq)
        .size()
        .reset_index(name="event_count")
    )
    return result


def sentiment_trends(df, date_col="SQLDATE", tone_col="AvgTone", freq="D"):
    dates = _date_series(df, date_col)
    values = pd.to_numeric(df[tone_col], errors="coerce")
    result = (
        pd.DataFrame({"date": dates, "sentiment": values})
        .dropna()
        .set_index("date")
        .resample(freq)["sentiment"]
        .agg(["mean", "min", "max", "std", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_sentiment"})
    )
    return result


def goldstein_trends(df, date_col="SQLDATE", goldstein_col="GoldsteinScale", freq="D"):
    dates = _date_series(df, date_col)
    values = pd.to_numeric(df[goldstein_col], errors="coerce")
    return (
        pd.DataFrame({"date": dates, "goldstein": values})
        .dropna()
        .set_index("date")
        .resample(freq)["goldstein"]
        .agg(["mean", "min", "max", "std", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_goldstein"})
    )


def actor_activity_trends(df, actor_cols=("Actor1Name", "Actor2Name"), date_col="SQLDATE", freq="D", top_n=20):
    dates = _date_series(df, date_col)
    rows = []
    for col in actor_cols:
        if col not in df:
            continue
        tmp = pd.DataFrame({"date": dates, "actor": df[col]}).dropna()
        rows.append(tmp[tmp["actor"].astype(str).str.len() > 0])
    if not rows:
        return pd.DataFrame(columns=["date", "actor", "activity_count"])
    actors = pd.concat(rows, ignore_index=True)
    top = actors["actor"].value_counts().head(top_n).index
    return (
        actors[actors["actor"].isin(top)]
        .groupby(["actor", pd.Grouper(key="date", freq=freq)])
        .size()
        .reset_index(name="activity_count")
    )


def trend_visualization_html(df, x="date", y="event_count", title="Trend", output=None):
    payload = {
        "x": pd.Series(df[x]).astype(str).tolist(),
        "y": pd.Series(df[y]).fillna(0).tolist(),
        "title": title,
    }
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script></head>
<body><div id="chart" style="width:100%;height:640px"></div>
<script>
const data = {json.dumps(payload)};
Plotly.newPlot("chart", [{{x: data.x, y: data.y, type: "scatter", mode: "lines+markers"}}], {{title: data.title}});
</script></body></html>"""
    return _write_html(html, output)


def event_heatmap_data(df, lat_col="ActionGeo_Lat", lon_col="ActionGeo_Long", weight_col=None):
    data = df[[lat_col, lon_col] + ([weight_col] if weight_col else [])].copy()
    data[lat_col] = pd.to_numeric(data[lat_col], errors="coerce")
    data[lon_col] = pd.to_numeric(data[lon_col], errors="coerce")
    data = data.dropna(subset=[lat_col, lon_col])
    data["weight"] = pd.to_numeric(data[weight_col], errors="coerce").fillna(1) if weight_col else 1
    return data.rename(columns={lat_col: "lat", lon_col: "lon"})[["lat", "lon", "weight"]]


def actor_location_markers(df, actor_col="Actor1Name", lat_col="Actor1Geo_Lat", lon_col="Actor1Geo_Long"):
    markers = df[[actor_col, lat_col, lon_col]].copy()
    markers[lat_col] = pd.to_numeric(markers[lat_col], errors="coerce")
    markers[lon_col] = pd.to_numeric(markers[lon_col], errors="coerce")
    return markers.dropna().rename(
        columns={actor_col: "actor", lat_col: "lat", lon_col: "lon"}
    )


def event_spread_animation_data(df, date_col="SQLDATE", lat_col="ActionGeo_Lat", lon_col="ActionGeo_Long", freq="D"):
    heatmap = event_heatmap_data(df, lat_col=lat_col, lon_col=lon_col)
    heatmap["date"] = _date_series(df.loc[heatmap.index], date_col).dt.to_period(freq).astype(str)
    return heatmap[["date", "lat", "lon", "weight"]]


def multi_layer_map_data(df):
    return {
        "heatmap": event_heatmap_data(df).to_dict("records"),
        "actor_markers": actor_location_markers(df).to_dict("records"),
        "animation": event_spread_animation_data(df).to_dict("records"),
    }


def map_visualization_html(df, title="Event Map", output=None):
    points = event_heatmap_data(df).to_dict("records")
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script></head>
<body><div id="map" style="width:100%;height:720px"></div>
<script>
const points = {json.dumps(points)};
Plotly.newPlot("map", [{{type: "scattergeo", mode: "markers",
lat: points.map(p => p.lat), lon: points.map(p => p.lon),
marker: {{size: points.map(p => Math.max(4, p.weight)), color: points.map(p => p.weight), colorscale: "Viridis"}}}}],
{{title: {json.dumps(title)}, geo: {{projection: {{type: "natural earth"}}}}}});
</script></body></html>"""
    return _write_html(html, output)


def extract_top_themes(df, theme_col="THEMES", top_n=20):
    counts = Counter()
    for value in df.get(theme_col, []):
        counts.update(_split_tokens(value))
    return pd.DataFrame(counts.most_common(top_n), columns=["theme", "count"])


def theme_evolution_over_time(df, theme_col="THEMES", date_col="DATE", freq="D", top_n=20):
    dates = _date_series(df, date_col)
    top = set(extract_top_themes(df, theme_col, top_n)["theme"])
    rows = []
    for idx, value in df[theme_col].items():
        for theme in _split_tokens(value):
            if theme in top:
                rows.append({"date": dates.loc[idx], "theme": theme})
    if not rows:
        return pd.DataFrame(columns=["date", "theme", "count"])
    return (
        pd.DataFrame(rows).dropna()
        .groupby(["theme", pd.Grouper(key="date", freq=freq)])
        .size()
        .reset_index(name="count")
    )


def theme_similarity_matrix(df, theme_col="THEMES", top_n=20):
    theme_docs = defaultdict(set)
    top = set(extract_top_themes(df, theme_col, top_n)["theme"])
    for idx, value in df[theme_col].items():
        for theme in _split_tokens(value):
            if theme in top:
                theme_docs[theme].add(idx)
    themes = sorted(theme_docs)
    matrix = pd.DataFrame(index=themes, columns=themes, dtype=float)
    for a in themes:
        for b in themes:
            union = theme_docs[a] | theme_docs[b]
            matrix.loc[a, b] = len(theme_docs[a] & theme_docs[b]) / len(union) if union else 0
    return matrix


def topic_clustering(df, theme_col="THEMES", min_similarity=0.2, top_n=50):
    matrix = theme_similarity_matrix(df, theme_col, top_n)
    visited = set()
    rows = []
    cluster_id = 0
    for theme in matrix.index:
        if theme in visited:
            continue
        cluster_id += 1
        queue = deque([theme])
        visited.add(theme)
        while queue:
            current = queue.popleft()
            rows.append({"theme": current, "cluster": cluster_id})
            neighbors = matrix.columns[matrix.loc[current] >= min_similarity]
            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
    return pd.DataFrame(rows)


def theme_word_cloud_data(df, theme_col="THEMES", top_n=100):
    return extract_top_themes(df, theme_col, top_n).rename(columns={"theme": "word"})


def theme_word_cloud_html(df, theme_col="THEMES", top_n=100, title="Theme Word Cloud", output=None):
    words = theme_word_cloud_data(df, theme_col, top_n)
    if words.empty:
        spans = ""
    else:
        max_count = max(words["count"].max(), 1)
        spans = " ".join(
            "<span style='font-size:{}px'>{}</span>".format(
                12 + int(48 * row["count"] / max_count),
                escape(str(row["word"]))
            )
            for _, row in words.iterrows()
        )
    html = f"<!doctype html><html><head><meta charset='utf-8'><title>{escape(title)}</title></head><body><h1>{escape(title)}</h1><div style='line-height:1.4'>{spans}</div></body></html>"
    return _write_html(html, output)


def actor_relationship_network(df, actor1_col="Actor1Name", actor2_col="Actor2Name"):
    edges = df[[actor1_col, actor2_col]].dropna()
    edges = edges[(edges[actor1_col] != "") & (edges[actor2_col] != "")]
    return (
        edges.groupby([actor1_col, actor2_col])
        .size()
        .reset_index(name="weight")
        .rename(columns={actor1_col: "source", actor2_col: "target"})
    )


def centrality_metrics(edges):
    degree = Counter()
    weighted = Counter()
    for _, row in edges.iterrows():
        degree[row["source"]] += 1
        degree[row["target"]] += 1
        weighted[row["source"]] += row.get("weight", 1)
        weighted[row["target"]] += row.get("weight", 1)
    return pd.DataFrame([
        {"node": node, "degree": degree[node], "weighted_degree": weighted[node]}
        for node in sorted(degree)
    ])


def community_detection(edges):
    graph = defaultdict(set)
    for _, row in edges.iterrows():
        graph[row["source"]].add(row["target"])
        graph[row["target"]].add(row["source"])
    rows = []
    seen = set()
    community = 0
    for node in graph:
        if node in seen:
            continue
        community += 1
        queue = deque([node])
        seen.add(node)
        while queue:
            current = queue.popleft()
            rows.append({"node": current, "community": community})
            for neighbor in graph[current]:
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
    return pd.DataFrame(rows)


def event_cascade_analysis(df, date_col="SQLDATE", event_col="EventCode", freq="D"):
    dates = _date_series(df, date_col)
    return (
        pd.DataFrame({"date": dates, "event": df[event_col]})
        .dropna()
        .groupby(["event", pd.Grouper(key="date", freq=freq)])
        .size()
        .reset_index(name="count")
    )


def network_visualization_html(edges, title="Actor Network", output=None):
    payload = edges.to_dict("records")
    nodes = sorted(set(edges["source"]).union(set(edges["target"]))) if not edges.empty else []
    node_payload = [{"id": node, "label": str(node)} for node in nodes]
    edge_payload = [
        {
            "from": row["source"],
            "to": row["target"],
            "value": row.get("weight", 1),
            "title": str(row.get("weight", 1)),
        }
        for _, row in edges.iterrows()
    ]
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{escape(title)}</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script></head>
<body><div id="network" style="width:100%;height:720px;border:1px solid #ddd"></div>
<script type="application/json" id="edges">{json.dumps(payload)}</script>
<script>
const nodes = new vis.DataSet({json.dumps(node_payload)});
const edges = new vis.DataSet({json.dumps(edge_payload)});
new vis.Network(document.getElementById("network"), {{nodes, edges}}, {{
  interaction: {{hover: true}},
  physics: {{stabilization: true}},
  edges: {{arrows: "to", scaling: {{min: 1, max: 8}}}}
}});
</script></body></html>"""
    return _write_html(html, output)


def multidimensional_sentiment(df, tone_col="V1TONE"):
    values = df[tone_col].apply(lambda value: [pd.to_numeric(part, errors="coerce") for part in str(value).split(",")])
    names = ["tone", "positive", "negative", "polarity", "activity_reference_density", "self_group_reference_density"]
    rows = []
    for parts in values:
        row = {name: (parts[i] if i < len(parts) else np.nan) for i, name in enumerate(names)}
        rows.append(row)
    return pd.DataFrame(rows)


def sentiment_polarity_classification(df, tone_col="AvgTone", positive_threshold=1.0, negative_threshold=-1.0):
    values = pd.to_numeric(df[tone_col], errors="coerce")
    labels = np.where(values >= positive_threshold, "positive",
                      np.where(values <= negative_threshold, "negative", "neutral"))
    return pd.DataFrame({"sentiment": values, "polarity": labels})


def sentiment_anomaly_detection(df, tone_col="AvgTone", z_threshold=2.0):
    values = pd.to_numeric(df[tone_col], errors="coerce")
    z = (values - values.mean()) / values.std(ddof=0)
    return pd.DataFrame({"sentiment": values, "z_score": z, "is_anomaly": z.abs() >= z_threshold})


def sentiment_fluctuation_analysis(df, date_col="SQLDATE", tone_col="AvgTone", freq="D", window=3):
    trend = sentiment_trends(df, date_col, tone_col, freq)
    trend["rolling_std"] = trend["avg_sentiment"].rolling(window=window, min_periods=1).std().fillna(0)
    trend["change"] = trend["avg_sentiment"].diff().fillna(0)
    return trend


def sentiment_heatmap_data(df, region_col="ActionGeo_CountryCode", tone_col="AvgTone"):
    values = pd.to_numeric(df[tone_col], errors="coerce")
    return (
        pd.DataFrame({"region": df[region_col], "sentiment": values})
        .dropna()
        .groupby("region")["sentiment"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_sentiment"})
    )
