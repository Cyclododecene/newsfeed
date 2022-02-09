import sys

sys.path.append("..") # relative path

from newsfeed.news.db.events import *
from newsfeed.news.db.gkg import *
from newsfeed.news.db.others import *

def test_db__v1():
    gdelt_events_v1_events = EventV1(start_date = "2021-01-01", end_date = "2021-01-02")
    results_v1_events = gdelt_events_v1_events.query()
    results_v1_events_nowtime = gdelt_events_v1_events.query_nowtime(date="2021-01-01")
    assert type(results_v1_events) == pd.DataFrame
    assert type(results_v1_events_nowtime) == pd.DataFrame
    
    gdelt_events_v1_gkg = GKGV1(start_date = "2021-01-01", end_date = "2021-01-02")
    results_v1_gkg = gdelt_events_v1_gkg.query()
    results_v1_gkg_nowtime = gdelt_events_v1_gkg.query_nowtime()
    assert type(results_v1_gkg) == pd.DataFrame
    assert type(results_v1_gkg_nowtime) == pd.DataFrame

def test_db__v2():
    gdelt_events_v2_events = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
    results_v2_events = gdelt_events_v2_events.query()
    results_v2_events_nowtime = gdelt_events_v2_events.query_nowtime()
    assert type(results_v2_events) == pd.DataFrame
    assert type(results_v2_events_nowtime) == pd.DataFrame

    gdelt_events_v2_mentions = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00", table = "mentions")
    results_v2_mentions = gdelt_events_v2_mentions.query()
    results_v2_mentions_nowtime = gdelt_events_v2_mentions.query_nowtime()
    assert type(results_v2_mentions) == pd.DataFrame
    assert type(results_v2_mentions_nowtime) == pd.DataFrame

    gdelt_events_v2_gkg = GKGV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
    results_v2_gkg = gdelt_events_v2_gkg.query()
    results_v2_gkg_nowtime = gdelt_events_v2_gkg.query_nowtime()
    assert type(results_v2_gkg) == pd.DataFrame
    assert type(results_v2_gkg_nowtime) == pd.DataFrame

def test_db_v3():
    gdelt_v3_geg = GEG(start_date = "2020-01-01", end_date = "2020-01-02")
    gdelt_v3_geg_result = gdelt_v3_geg.query()
    assert type(gdelt_v3_geg_result) == pd.DataFrame

    # GDELT Visual Global Entity Graph
    gdelt_v3_vgeg = VGEG(query_date = "2020-01-01", domain = "CNN")
    gdelt_v3_vgeg_result = gdelt_v3_vgeg.query() 
    assert type(gdelt_v3_vgeg_result) == pd.DataFrame

    # GDELT Global Difference Graph
    gdelt_v3_gdg = GDG(query_date="2018-08-27-14-00-00")
    gdelt_v3_gdg_result = gdelt_v3_gdg.query()
    assert type(gdelt_v3_gdg_result) == pd.DataFrame

    # GDELT Global Frontpage Graph
    gdelt_v3_gfg = GFG(query_date="2018-03-02-02-00-00")
    gdelt_v3_gfg_result = gdelt_v3_gfg.query()
    assert type(gdelt_v3_gfg_result) == pd.DataFrame