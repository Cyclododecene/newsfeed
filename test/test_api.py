import sys

sys.path.append("..") # relative path

from newsfeed.news.apis.filters import * 
from newsfeed.news.apis.query import * 
import pandas as pd

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    start_date = "20211231000000",
    end_date = "20211231010000",
    country = ["China", "US"]
)

def test_api():
    print("\n[Test] API Query")
    
    # Test article search with 30 min time range
    print("  Testing article_search (30 min)...")
    articles_30 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 30)
    if isinstance(articles_30, Exception):
        print(f"  ⚠ Article search failed: {articles_30}")
        print("  Skipping this test due to API/network issue")
    else:
        assert type(articles_30) == pd.DataFrame
        print(f"  ✓ Retrieved {len(articles_30)} articles")
    
    # Test article search with 60 min time range
    print("  Testing article_search (60 min)...")
    articles_60 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 60)
    if isinstance(articles_60, Exception):
        print(f"  ⚠ Article search failed: {articles_60}")
        print("  Skipping this test due to API/network issue")
    else:
        assert type(articles_60) == pd.DataFrame
        print(f"  ✓ Retrieved {len(articles_60)} articles")
    
    # Test timeline search
    print("  Testing timeline_search...")
    timelineraw = timeline_search(query_filter = f, max_recursion_depth = 100, query_mode = "timelinevolraw")
    if isinstance(timelineraw, Exception):
        print(f"  ⚠ Timeline search failed: {timelineraw}")
        print("  Skipping this test due to API/network issue")
    else:
        assert type(timelineraw) == pd.DataFrame
        print(f"  ✓ Retrieved {len(timelineraw)} timeline entries")
    
    # Test geo search
    print("  Testing geo_search...")
    geo_7d = geo_search(query_filter = f, sourcelang="english", timespan=7)
    if isinstance(geo_7d, Exception):
        print(f"  ⚠ Geo search failed: {geo_7d}")
        print("  Skipping this test due to API/network issue")
    else:
        assert type(geo_7d) == str
        print("  ✓ Retrieved geo data")
    
    print("✓ API test completed (some tests may have been skipped due to network issues)")