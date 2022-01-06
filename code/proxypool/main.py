from getproxy import * 
from validate import * 
from db import * 
from multiprocessing import Pool
from tqdm import tqdm

from random import randint
from time import sleep

#sleep(randint(10,100))

proxies_list = []
# proxydb - US
countries_list, proxy_type_list, anonymity = ["US", "CN", "HK"], ["http", "https", "socks5"], ["anonymous", "elite", "transparent"]
for i in range(0, len(countries_list)):
    for j in range(0, len(proxy_type_list)):
        for k in range(0, len(anonymity)):
            proxies_list.append(proxies(source_name = "proxydb", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())
            sleep(randint(1,20))

# ip3366
proxies_list.append(proxies(source_name = "ip3366").collect())

# geonode
for i in range(0, len(countries_list)):
    for j in range(0, len(proxy_type_list)):
        for k in range(0, len(anonymity)):
            proxies_list.append(proxies(source_name = "geonode", country=countries_list[i], proxy_type=proxy_type_list[j], anonymity=anonymity[k]).collect())
            sleep(randint(1,20))
# daili66
proxies_list.append(proxies(source_name = "daili66", country = "HK").collect())

combined_list = [item for sublist in proxies_list for item in sublist]

for i in tqdm(range(0, len(combined_list))):
    tmp_proxy = combined_list[i]
    test = validater(proxies = tmp_proxy, proxy_type="https")
    tmp_result = test.validate()
    pass_data(data = tmp_result)