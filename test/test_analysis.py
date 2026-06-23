import pandas as pd

from newsfeed import analysis


def sample_events():
    return pd.DataFrame([
        {
            "SQLDATE": "20210101",
            "AvgTone": 2.0,
            "GoldsteinScale": 1.5,
            "Actor1Name": "A",
            "Actor2Name": "B",
            "Actor1Geo_Lat": 10.0,
            "Actor1Geo_Long": 20.0,
            "ActionGeo_Lat": 11.0,
            "ActionGeo_Long": 21.0,
            "ActionGeo_CountryCode": "US",
            "EventCode": "010",
        },
        {
            "SQLDATE": "20210101",
            "AvgTone": -3.0,
            "GoldsteinScale": -1.0,
            "Actor1Name": "A",
            "Actor2Name": "C",
            "Actor1Geo_Lat": 12.0,
            "Actor1Geo_Long": 22.0,
            "ActionGeo_Lat": 13.0,
            "ActionGeo_Long": 23.0,
            "ActionGeo_CountryCode": "GB",
            "EventCode": "020",
        },
        {
            "SQLDATE": "20210102",
            "AvgTone": 4.0,
            "GoldsteinScale": 3.0,
            "Actor1Name": "B",
            "Actor2Name": "C",
            "Actor1Geo_Lat": 14.0,
            "Actor1Geo_Long": 24.0,
            "ActionGeo_Lat": 15.0,
            "ActionGeo_Long": 25.0,
            "ActionGeo_CountryCode": "US",
            "EventCode": "010",
        },
    ])


def sample_gkg():
    return pd.DataFrame([
        {"DATE": "20210101", "THEMES": "TAX_FNCACT;ECON_STOCKMARKET;"},
        {"DATE": "20210102", "THEMES": "TAX_FNCACT;HEALTH_PANDEMIC;"},
        {"DATE": "20210102", "THEMES": "ECON_STOCKMARKET;HEALTH_PANDEMIC;"},
    ])


def test_trend_analysis_helpers():
    df = sample_events()

    frequency = analysis.event_frequency_trends(df)
    sentiment = analysis.sentiment_trends(df)
    goldstein = analysis.goldstein_trends(df)
    actor_activity = analysis.actor_activity_trends(df)
    html = analysis.trend_visualization_html(frequency)

    assert frequency["event_count"].tolist() == [2, 1]
    assert round(sentiment.loc[0, "avg_sentiment"], 2) == -0.5
    assert "avg_goldstein" in goldstein.columns
    assert {"date", "actor", "activity_count"}.issubset(actor_activity.columns)
    assert "Plotly.newPlot" in html


def test_geographical_visualization_helpers():
    df = sample_events()

    heatmap = analysis.event_heatmap_data(df)
    markers = analysis.actor_location_markers(df)
    animation = analysis.event_spread_animation_data(df)
    layers = analysis.multi_layer_map_data(df)
    html = analysis.map_visualization_html(df)

    assert heatmap[["lat", "lon", "weight"]].shape == (3, 3)
    assert markers["actor"].tolist() == ["A", "A", "B"]
    assert {"date", "lat", "lon", "weight"}.issubset(animation.columns)
    assert {"heatmap", "actor_markers", "animation"} == set(layers)
    assert "scattergeo" in html


def test_text_analysis_helpers():
    df = sample_gkg()

    top = analysis.extract_top_themes(df)
    evolution = analysis.theme_evolution_over_time(df)
    similarity = analysis.theme_similarity_matrix(df)
    clusters = analysis.topic_clustering(df)
    word_cloud = analysis.theme_word_cloud_data(df)
    word_cloud_html = analysis.theme_word_cloud_html(df)

    assert top.iloc[0]["count"] == 2
    assert {"date", "theme", "count"}.issubset(evolution.columns)
    assert similarity.loc["ECON_STOCKMARKET", "ECON_STOCKMARKET"] == 1
    assert {"theme", "cluster"}.issubset(clusters.columns)
    assert {"word", "count"}.issubset(word_cloud.columns)
    assert "Theme Word Cloud" in word_cloud_html


def test_network_analysis_helpers():
    df = sample_events()

    edges = analysis.actor_relationship_network(df)
    centrality = analysis.centrality_metrics(edges)
    communities = analysis.community_detection(edges)
    cascades = analysis.event_cascade_analysis(df)
    html = analysis.network_visualization_html(edges)

    assert {"source", "target", "weight"}.issubset(edges.columns)
    assert {"node", "degree", "weighted_degree"}.issubset(centrality.columns)
    assert {"node", "community"}.issubset(communities.columns)
    assert {"event", "date", "count"}.issubset(cascades.columns)
    assert "vis.Network" in html


def test_sentiment_analysis_helpers():
    df = sample_events()
    gkg = pd.DataFrame({"V1TONE": ["1.0,2.0,3.0,4.0,5.0,6.0"]})

    multi = analysis.multidimensional_sentiment(gkg)
    polarity = analysis.sentiment_polarity_classification(df)
    anomalies = analysis.sentiment_anomaly_detection(df, z_threshold=1.0)
    fluctuations = analysis.sentiment_fluctuation_analysis(df)
    heatmap = analysis.sentiment_heatmap_data(df)

    assert multi.loc[0, "positive"] == 2.0
    assert set(polarity["polarity"]) == {"positive", "negative"}
    assert anomalies["is_anomaly"].any()
    assert {"rolling_std", "change"}.issubset(fluctuations.columns)
    assert {"region", "avg_sentiment", "count"}.issubset(heatmap.columns)
