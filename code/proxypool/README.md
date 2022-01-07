# Proxy Pool

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

```shell
git clone https://github.com/Cyclododecene/GNAF.git
cd code/proxypool

python -m pip install requests fake-useragent pathos flask Flask-Limiter
python main.py && python api.py
```


### Use Our Demo

```python
import requests
response = requests.get("http://data.cklau.ac.cn/proxy/api/v1.0/getdata?ProxyType=socks5&Num=5")
proxies_list = response.json()
```

## TODO

- [ ] use `selenium` for automation
- [ ] use `redis` or other NoSQL database to replace `sqlite`
