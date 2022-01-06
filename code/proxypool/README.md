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


## TODO

- [ ] use `selenium` for automation
- [ ] use `redis` or other NoSQL database to replace `sqlite`
