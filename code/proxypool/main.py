from concurrent.futures import process
from getproxy import * 
from validate import * 
from db import * 
import multiprocessing as mp
from tqdm import tqdm

from random import randint
from time import sleep

#sleep(randint(10,100))

proxies_list = []
# proxydb - US
countries_list, proxy_type_list, anonymity = ["US", "CN", "HK"], ["http", "https", "socks5"], ["anonymous", "elite", "transparent"]
for i in tqdm(range(0, len(countries_list))):
    for j in tqdm(range(0, len(proxy_type_list)), leave = False):
        for k in tqdm(range(0, len(anonymity)), leave = False):
            proxies_list.append(proxies(source_name = "proxydb", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())
            sleep(randint(1,10))

# ip3366
proxies_list.append(proxies(source_name = "ip3366").collect())

# geonode
for i in tqdm(range(0, len(countries_list))):
    for j in tqdm(range(0, len(proxy_type_list)), leave = False):
        for k in range(0, len(anonymity)):
            proxies_list.append(proxies(source_name = "geonode", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())
            sleep(randint(1,10))
# daili66
proxies_list.append(proxies(source_name = "daili66", country = "HK").collect())

combined_list = [item for sublist in proxies_list for item in sublist]

## multiprocessing for validation
"""
for i in tqdm(range(0, len(combined_list))):
    tmp_proxy = combined_list[i]
    test = validater(proxies = tmp_proxy, proxy_type="https")
    tmp_result = test.validate()
    pass_data(data = tmp_result)

q = mp.Manager().Queue()
cpus = mp.cpu_count()
pool = mp.Pool(processes = cpus - 2)

for proxy in combined_list:
    pool.apply_async(validater, args=(proxy, "https"))

pool.close()
pool.join()
information = []
while not q.empty():
    information.append(q.get())
"""
