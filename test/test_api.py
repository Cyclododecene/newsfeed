import sys

sys.path.append("..") # relative path

from newsfeed.news.apis.filters import * 
from newsfeed.news.apis.query import * 

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    start_date = "20211231000000",
    end_date = "20211231010000",
    country = ["China", "US"]
)

def test_api():
    articles_30 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 30)
    articles_60 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 60)
    timelineraw = timeline_search(query_filter = f, max_recursion_depth = 100, query_mode = "timelinevolraw")
    geo_7d = geo_search(query_filter = f, sourcelang="english", timespan=7)
    assert type(articles_30) == pd.DataFrame
    assert type(articles_60) == pd.DataFrame
    assert type(timelineraw) == pd.DataFrame
    assert type(geo_7d) == str