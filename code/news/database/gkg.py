import io
from multiprocessing.sharedctypes import Value
import os
import re
import time
import tqdm
import datetime
import requests
import pandas as pd
from lxml import html
import multiprocessing
from fake_useragent import UserAgent

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class Gkg_V1(object):
    base_url = "http://data.gdeltproject.org/gkg/"
    cpu_num = multiprocessing.cpu_count()

    header = ['DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS',
                'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS']


    def __init__(self, start_date:str = "2020-01-01", end_date:str = "2021-12-31", proxy:dict = None):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:
        print("[+] Getting data from GDELT Project...")
        page = requests.get("http://data.gdeltproject.org/gkg/index.html", headers = self._generate_header(), proxies = self.proxy)
        webpage = html.fromstring(page.content)
        url_list = webpage.xpath("//a/@href")
        #url_list = [item for item in url_list if len(item) == 23] report other url
        download_url_list = list(filter(lambda x: x[0:8] >= self.start_date and x[0:8] < self.end_date, url_list))
        download_url_list = list(filter(lambda x: len(x) == 20, download_url_list))
        return download_url_list

    def _download_file(self, url:str="20200101.gkg.csv.zip"):
        download_url = self.base_url + url
        time.sleep(0.0005)
        try:
            response = requests.get(download_url, headers = self._generate_header(), proxies =self.proxy, timeout = 10)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_csv(response_text, compression="zip", sep = "\t", header = None, 
                                    warn_bad_lines = False, low_memory=False)
                response_text.flush()
                response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        pool = multiprocessing.Pool(self.cpu_num)
        try:
            print("[+] Downloading...")
            downloaded_dfs = list(tqdm.tqdm(pool.imap_unordered(self._download_file, download_url_list), total = len(download_url_list)))
            pool.close()
            pool.terminate()
            pool.join()
            results = pd.concat(downloaded_dfs)
            del downloaded_dfs
            results.reset_index(drop = True, inplace = True)
            results.columns = self.header
            return results
        except Exception as e:
            return e
	
if __name__ == "__main__":
    
    # GDELT GKG Database Version 1.0
    gdelt_events_v1_gkg = Gkg_V1(start_date = "2021-01-01", end_date = "2021-01-02")
    results_v1_gkg = gdelt_events_v1_gkg.query()
