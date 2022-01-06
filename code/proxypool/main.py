from concurrent.futures import process
from getproxy import * 
from validate import * 
from db import * 
from tqdm import tqdm

from random import randint
from time import sleep

from pathos.multiprocessing import ProcessPool as Pool

#sleep(randint(10,100))

proxies_list = []
# proxydb - US
countries_list, proxy_type_list, anonymity = ["US", "CN", "HK"], ["http", "https", "socks5"], ["anonymous", "elite", "transparent"]
for i in tqdm(range(0, len(countries_list))):
    sleep(randint(1,10))
    for j in tqdm(range(0, len(proxy_type_list)), leave = False):
        for k in tqdm(range(0, len(anonymity)), leave = False):
            proxies_list.append(proxies(source_name = "proxydb", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())

# geonode
for i in tqdm(range(0, len(countries_list))):
    sleep(randint(1,10))
    for j in tqdm(range(0, len(proxy_type_list)), leave = False):
        for k in range(0, len(anonymity)):
            proxies_list.append(proxies(source_name = "geonode", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())

proxies_list.append(proxies(source_name = "ip3366").collect())
proxies_list.append(proxies(source_name = "daili66", country = "HK").collect())

combined_list = [item for sublist in proxies_list for item in sublist]

## multiprocessing for validation
"""
for i in tqdm(range(0, len(combined_list))):
    tmp_proxy = combined_list[i]
    test = validater()
    test.validate(proxy = tmp_proxy, proxy_type="https")
    pass_data(data = tmp_result)

"""
for i in tqdm(range(0, len(combined_list), 5)):
    tmp_list = combined_list
    count = len(tmp_list)
    p = Pool(count)
    t = validater()
    start = time.time()
    result = p.amap(validater.validate, [t] * count, tmp_list)
    result.get()
    round(time.time() - start,3)
    sleep(randint(1,10))
