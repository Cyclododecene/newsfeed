"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
"""

from concurrent.futures import process
from getproxy import *
from validate import *
from db import *
from tqdm import tqdm

from random import randint
from time import sleep

from pathos.multiprocessing import ProcessPool as Pool
from pathos.threading import ThreadPool as TPool

con = sqlite3.connect("data/proxy.db")
cur = con.cursor()


def proxydb():
    proxies_list = []
    # proxydb
    try:
        print("[+] Getting proxies from proxydb.com...")
        countries_list, proxy_type_list, anonymity = ["US", "CN", "HK"], [
            "http", "https", "socks5"
        ], ["anonymous", "elite", "transparent"]
        for i in tqdm(range(0, len(countries_list))):
            sleep(randint(1, 10))
            for j in tqdm(range(0, len(proxy_type_list)), leave=False):
                for k in tqdm(range(0, len(anonymity)), leave=False):
                    proxies_list.append(
                        proxies(source_name="proxydb",
                                country=countries_list[i],
                                proxy_type=proxy_type_list[j],
                                anonymity=anonymity[k]).collect())

        combined_list = [item for sublist in proxies_list for item in sublist]
        return combined_list
    except Exception as e:
        print("Error:", e)
        pass


    # geonode
def geonode():
    proxies_list = []
    try:
        print("[+] Getting proxies from geonode.org...")
        countries_list, proxy_type_list, anonymity = ["US", "CN", "HK"], [
            "http", "https", "socks5"
        ], ["anonymous", "elite", "transparent"]
        for i in tqdm(range(0, len(countries_list))):
            sleep(randint(1, 10))
            for j in tqdm(range(0, len(proxy_type_list)), leave=False):
                for k in range(0, len(anonymity)):
                    proxies_list.append(
                        proxies(source_name="geonode",
                                country=countries_list[i],
                                proxy_type=proxy_type_list[j],
                                anonymity=anonymity[k]).collect())

        combined_list = [item for sublist in proxies_list for item in sublist]
        return combined_list
    except Exception as e:
        pass


def other_proxy():
    proxies_list = []
    try:
        print("[+] Getting proxies from ip.jiangxianli.com...")
        proxies_list.append(proxies(source_name="jiangxianli").collect())
    except Exception as e:
        pass

    try:
        print("[+] Getting proxies from nimadaili.com...")
        proxies_list.append(
            proxies(source_name="nimadaili", proxy_type="https").collect())
        proxies_list.append(
            proxies(source_name="nimadaili", proxy_type="http").collect())
    except Exception as e:
        pass

    try:
        print("[+] Getting proxies from ip3366.com...")
        proxies_list.append(proxies(source_name="ip3366").collect())
    except Exception as e:
        pass

    try:
        print("[+] Getting proxies from 66ip.com...")
        proxies_list.append(proxies(source_name="daili66").collect())
        proxies_list.append(
            proxies(source_name="daili66", country="HK").collect())
        proxies_list.append(
            proxies(source_name="daili66", country="TW").collect())
    except Exception as e:
        pass

    try:
        print("[+] Getting proxies from www.proxy-list.download...")
        proxies_list.append(proxies(source_name="proxy-list").collect())
    except Exception as e:
        pass

    try:
        print("[+] Getting proxies from https://spys.one...")
        proxies_list.append(
            proxies(source_name="spys", proxy_type="https").collect())
        proxies_list.append(
            proxies(source_name="spys", proxy_type="http").collect())
        proxies_list.append(
            proxies(source_name="spys", proxy_type="socks5").collect())
    except Exception as e:
        pass

    combined_list = [item for sublist in proxies_list for item in sublist]
    return combined_list


def multiprocess_validate(cursor=None,
                          connect=None,
                          proxy_list: list = None,
                          num_processes: int = 4):
    combined_list = proxy_list
    for i in tqdm(range(0, len(combined_list), num_processes)):
        tmp_list = combined_list[i:i + num_processes]
        p = Pool(num_processes)
        t = validater()
        result = p.amap(validater.validate, [t] * num_processes, tmp_list)
        tmp_results = result.get()
        for i in range(0, len(tmp_results)):
            pass_data(cursor=cursor, connect=connect, data=tmp_results[i])


def validate_status():
    data = get_db_data(condition=False)


if __name__ == "__main__":
    con = sqlite3.connect("data/proxy.db")
    cur = con.cursor()
    create_db(db_name="Proxy")
    while True:
        try:
            combined_list_proxydb = proxydb()
            multiprocess_validate(cursor=cur,
                                  connect=con,
                                  proxy_list=combined_list_proxydb,
                                  num_processes=100)
        except Exception as e:
            print(e)
            pass

        try:
            combined_list_geonode = geonode()
            multiprocess_validate(cursor=cur,
                                  connect=con,
                                  proxy_list=combined_list_geonode,
                                  num_processes=100)
        except Exception as e:
            print(e)
            pass
        try:
            combined_list_other = other_proxy()
            multiprocess_validate(cursor=cur,
                                  connect=con,
                                  proxy_list=combined_list_other,
                                  num_processes=100)
        except Exception as e:
            print(e)
            pass

        update_adapted_db(cursor=cur, connect=con)
        time.sleep(600)