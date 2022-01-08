# About

## API

Based on the [gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api/), we consider a continuous querying mechanism by spliting the time range into multiple sub range (default setting is every 60 minutes).

* FIPS 2 letter Contries list: please check: [LOOK-UP COUNTRIES](http://data.gdeltproject.org/api/v2/guides/LOOKUP-COUNTRIES.TXT)
* GKG Themes list: please check: [LOOK-UP THEMES](http://data.gdeltproject.org/api/v2/guides/LOOKUP-GKGTHEMES.TXT)

The URL encoding reference: [url encode](https://www.eso.org/~ndelmott/url_encode.html)


 - [x] [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
 - [ ] [GDELT Article List](https://blog.gdeltproject.org/announcing-the-gdelt-article-list-rss-feed/)
 - [ ] [GDELT GEO 2.0 API](https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/)
 - [ ] [GDELT TV API](https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/)

## Database Query

### GDELT 1.0

 - [ ] [GDELT Events Database 1.0](http://data.gdeltproject.org/events/index.html)
 - [ ] [GDELT Global Knowledge Graph 1.0](http://data.gdeltproject.org/gkg/index.html)

### GDELT 2.0

 - [ ] [GDELT Events Database 2.0](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
 - [ ] [GDELT Global Knowledge Graph 2.0](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)

### GDELT Others
- [ ] [GDELT Different Graph](https://blog.gdeltproject.org/announcing-the-gdelt-global-difference-graph-gdg-planetary-scale-change-detection-for-the-global-news-media/)  
- [ ] [GDELT Global Frontpage Graph](https://blog.gdeltproject.org/announcing-gdelt-global-frontpage-graph-gfg/)
- [ ] [GDELT Global Entity Graph](https://blog.gdeltproject.org/announcing-the-global-entity-graph-geg-and-a-new-11-billion-entity-dataset/)

## API 

### Doc API

#### For Article query:

```python
from apis.filters import * 
from apis.query import * 

f = Art_Filter(
    keyword = ["Exchange Rate", "World"],
    start_date = "2017-01-01-00-00-00",
    end_date = "2021-12-31-00-00-00",
    country = ["China", "US"]
)

articles_30 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 30)
articles_60 = article_search(query_filter = f, max_recursion_depth = 100, time_range = 60)
```
#### For Timeline query:

```python
from apis.filters import * 
from apis.query import * 

f = Art_Filter(
    keyword = ["China", "United State"],
    start_date = "2017-01-01-00-00-00",
    end_date = "2021-12-31-00-00-00",
    country = ["China", "US"]
)

timelineraw = timeline_search(query_filter = f, max_recursion_depth = 100, query_mode = "timelinevolraw")
```

query_mode:
* artlist: `article_search`
* timeline: `timelinevol`, `timelinevolraw`, `timelinetone`, `timelinelang`, `timelinesourcecountry`

most of the parameters are the same with [gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api/), however, to specify the precise date range, we remove the `timespan` and use `start_date` and `time_range` for iteratively collecting articles.