# Proxy Pool

## Why

> A proxy pool allows us to make a higher volume of requests to a target website without being banned.

Multiple open-source proxy pool solutions can be found in [GitHub](https://github.com/topics/proxypool), we *reivent the wheel* because most of the free proxy sources (or providers) are invalid, thus, we need some stable proxies (both in Mainland China and the other region/countries).

## Some Free Proxy Sources

```python
proxy_source = {
    "proxydb":"http://proxydb.net/?&availability=90",
    "geonode":"https://proxylist.geonode.com/api/proxy-list?",
    "ip3366":"http://www.ip3366.net/free/?",
    "jiangxianli":"https://ip.jiangxianli.com/api/proxy_ips?",
    "nimadaili":"http://nimadaili.com/",
    "daili66":"http://www.66ip.cn/"
}
```

## Components

### proxy crawler

```python
from getproxy import * 
proxies_list = proxies(source_name = "proxydb", country="US", proxy_type="http", anonymity="elite").collect()
```

### proxy validation

```python
for i in range(0 len(proxies_list)):
    test = validater(proxies = proxies_list[i], proxy_type="https")
    test.validate()
```

### storage

Store proxy and its status into a database, for now, we are using `sqlite`

```python
import sqlite3

con = sqlite3.connect("data/proxy.db")
cur = con.cursor()
create_db(cursor = cur)
pass_data(cursor = cur, data = proxies)
```

## HOWTO

### Local Deployment

First, clone the code and install required packages:

```shell
git clone https://github.com/Cyclododecene/GNAF.git
cd code/proxypool

python -m pip install requests fake-useragent pathos flask Flask-Limiter
```

then, run both `main.py` and `api.py`

```shell
python main.py # in ttf1
python api.py  # in ttf2
```


### Use Our Demo

We also provide a public demo API for you, you can check [data.ckalu.ac.cn](https://data.cklau.ac.cn/proxy/api/v1.0/info):

```python
import requests
response = requests.get("http://data.cklau.ac.cn/proxy/api/v1.0/getdata?ProxyType=socks5&Num=5")
proxies_list = response.json()
```

## TODO

- [ ] use `selenium` for automation
- [ ] use `redis` or other NoSQL database to replace `sqlite`
