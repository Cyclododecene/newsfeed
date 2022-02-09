# NewsFeed
[![py_version](https://img.shields.io/badge/python-3.7+-brightgreen)](https://www.python.org/)
[![GDLET_version](https://img.shields.io/badge/GDELT-V1&V2-orange)](https://gdeltproject.org)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

## Introduction

Newsfeed based on GDELT Project

### Installation

```shell
conda create -n newsfeed python=3.7
python setup install
```

## GDELT API

Based on the [gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api/), we consider a continuous querying mechanism by spliting the time range into multiple sub range (default setting is every 60 minutes).

* FIPS 2 letter Contries list: please check: [LOOK-UP COUNTRIES](http://data.gdeltproject.org/api/v2/guides/LOOKUP-COUNTRIES.TXT)
* GKG Themes list: please check: [LOOK-UP THEMES](http://data.gdeltproject.org/documentation/GKG-MASTER-THEMELIST.TXT)

The URL encoding reference: [url encode](https://www.eso.org/~ndelmott/url_encode.html)


 - [x] [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
 - [x] [GDELT GEO 2.0 API](https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts) # BETA VERSION
 - [ ] [GDELT TV 2.0 API](https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/) # NOT YET


## GDELT Database Query

### GDELT 1.0

 - [x] [GDELT Events Database 1.0](http://data.gdeltproject.org/events/index.html)
 - [x] [GDELT Global Knowledge Graph 1.0](http://data.gdeltproject.org/gkg/index.html)

### GDELT 2.0

 - [x] [GDELT Events Database 2.0](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
 - [x] [GDELT Mentions Database (new)](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
 - [x] [GDELT Global Knowledge Graph 2.0](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)

### GDELT Others
- [x] [GDELT Global Entity Graph](https://blog.gdeltproject.org/announcing-the-global-entity-graph-geg-and-a-new-11-billion-entity-dataset/)
- [x] [GDELT Visual Global Entity Graph](https://blog.gdeltproject.org/what-googles-cloud-video-ai-sees-watching-decade-of-television-news-the-visual-global-entity-graph-2-0/)
- [x] [GDELT Different Graph](https://blog.gdeltproject.org/announcing-the-gdelt-global-difference-graph-gdg-planetary-scale-change-detection-for-the-global-news-media/)  
- [x] [GDELT Global Frontpage Graph](https://blog.gdeltproject.org/announcing-gdelt-global-frontpage-graph-gfg/)

## HOWTO

### APIs

#### For Article query:

```python
from newsfeed.news.apis.filters import * 
from newsfeed.news.apis.query import * 

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    start_date = "20211231000000",
    end_date = "20211231010000",
    country = ["China", "US"]
)

articles_30 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 30)
articles_60 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 60)
```

#### For Timeline query:

```python
from newsfeed.news.apis.filters import * 
from newsfeed.news.apis.query import * 

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    start_date = "2021-12-31-00-00-00",
    end_date = "2021-12-31-01-00-00",
    country = ["China", "US"]
)
timelineraw = timeline_search(query_filter = f, max_recursion_depth = 100, query_mode = "timelinevolraw")
```

#### For GEO query:

```python
from newsfeed.news.apis.filters import * 
from newsfeed.news.apis.query import * 

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    country = ["China", "US"]
)
geo_7d = geo_search(query_filter = f, sourcelang="english", timespan=7)
```

query_mode:
* artlist: `article_search`
* timeline: `timelinevol`, `timelinevolraw`, `timelinetone`, `timelinelang`, `timelinesourcecountry`

most of the parameters are the same with [gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api/), however, to specify the precise date range, we remove the `timespan` and use `start_date` and `time_range` for iteratively collecting articles.


### Database Query

For event database (both V1 and V2):

```python
from newsfeed.news.db.events import *
# GDELT Event Database Version 1.0
gdelt_events_v1_events = EventV1(start_date = "2021-01-01", end_date = "2021-01-02")
results_v1_events = gdelt_events_v1_events.query()
results_v1_events_nowtime = gdelt_events_v1_events.query_nowtime()

# GDELT Event Database Version 2.0 - Event
gdelt_events_v2_events = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
results_v2_events = gdelt_events_v2_events.query()
results_v2_events_nowtime = gdelt_events_v2_events.query_nowtime()

# GDELT Event Database Version 2.0 - Mentions
gdelt_events_v2_mentions = EventV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00", table = "mentions")
results_v2_mentions = gdelt_events_v2_mentions.query()
results_v2_mentions_nowtime = gdelt_events_v2_mentions.query_nowtime()

```

For GKG databse (both V1 and V2):

```python
from newsfeed.news.db.gkg import *
# GDELT GKG Database Version 1.0
gdelt_events_v1_gkg = GKGV1(start_date = "2021-01-01", end_date = "2021-01-02")
results_v1_gkg = gdelt_events_v1_gkg.query()
results_v1_gkg_nowtime = gdelt_events_v1_gkg.query_nowtime()

from newsfeed.news.db.gkg import *
# GDELT GKG Database Version 2.0
gdelt_events_v2_gkg = GKGV2(start_date = "2021-01-01-00-00-00", end_date = "2021-01-02-00-00-00")
results_v2_gkg = gdelt_events_v2_gkg.query()
results_v2_gkg_nowtime = gdelt_events_v2_gkg.query_nowtime()
```

For GEG, VGEG and GDG:

```python
from newsfeed.news.db.others import *
# GDELT Global Entity Graph
gdelt_v3_geg = GEG(start_date = "2020-01-01", end_date = "2020-01-02")
gdelt_v3_geg_result = gdelt_v3_geg.query()

# GDELT Visual Global Entity Graph
gdelt_v3_vgeg = VGEG(query_date = "2020-01-01", domain = "CNN")
gdelt_v3_vgeg_result = gdelt_v3_vgeg.query() 

# GDELT Global Difference Graph
gdelt_v3_gdg = GDG(query_date="2018-08-27-14-00-00")
gdelt_v3_gdg_result = gdelt_v3_gdg.query()

# GDELT Global Frontpage Graph
gdelt_v3_gfg = GFG(query_date="2018-03-02-02-00-00")
gdelt_v3_gfg_result = gdelt_v3_gfg.query()
```

### Utilities

Full-text downloader (based on [`newspaper3k`](https://github.com/codelucas/newspaper) and [Wayback Machine](https://archive.org/help/wayback_api.php))

```python
from newsfeed.utils import fulltext as ft
art = ft.download(url="https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html")
print("full text: \n {}".format(art.text))
```
