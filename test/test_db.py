import sys

sys.path.append("..") # relative path

from newsfeed.news.db.events import *
from newsfeed.news.db.gkg import *
from newsfeed.news.db.others import *

def test_db__v1():
    gdelt_events_v1_events = EventV1(start_date = "2021-01-01", end_date = "2021-01-02")
    results_v1_events_nowtime = gdelt_events_v1_events.query_nowtime(date="2021-01-01")
    assert type(results_v1_events_nowtime) == pd.DataFrame
    
    gdelt_events_v1_gkg = GKGV1(start_date = "2021-01-01", end_date = "2021-01-02")
    results_v1_gkg_nowtime = gdelt_events_v1_gkg.query_nowtime()
    assert type(results_v1_gkg_nowtime) == pd.DataFrame

def test_db__v2():
    gdelt_events_v2_events = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
    results_v2_events_nowtime = gdelt_events_v2_events.query_nowtime()
    assert type(results_v2_events_nowtime) == pd.DataFrame

    gdelt_events_v2_mentions = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00", table = "mentions")
    results_v2_mentions_nowtime = gdelt_events_v2_mentions.query_nowtime()
    assert type(results_v2_mentions_nowtime) == pd.DataFrame

    gdelt_events_v2_gkg = GKGV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
    results_v2_gkg_nowtime = gdelt_events_v2_gkg.query_nowtime()
    assert type(results_v2_gkg_nowtime) == pd.DataFrame
