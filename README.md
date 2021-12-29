# GNAF

GNAF: GDELT News Aggregation and Feed

## API

Based on the [gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api/), we consider a continuous querying mechanism by spliting the time range into multiple sub range (default setting is every 12 hours).

```python
from code.api.filters import * 
from code.api.api import * 

f = Filter(
    keyword = "climate change",
    start_date = "2021-05-09-00-00-00",
    end_date = "2021-05-12-00-00-00"
)
articles = article_search(query_filter = f, max_recursion_depth = 100, time_range = 6)
```