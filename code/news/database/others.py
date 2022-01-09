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

class GEG(object):
    cpu_num = multiprocessing.cpu_count()
    columns_name = ['date', 'url', 'lang', 'polarity', 'magnitude', 'score', 'entities']


    def __init__(self, start_date:str = "2021-01-01", end_date:str = "2021-12-31", proxy:dict = None):
        self.start_date = "".join(start_date.split("-")) + "000000"
        self.end_date = "".join(end_date.split("-")) + "000000"
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:

        print("[+] Scraping data from GDELT Project...")
        page = pd.read_csv("http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT", sep = " ", 
                            engine = "c", na_filter = False, low_memory = False, names = ["url"])
        url_list = page['url']
        del page
        download_url_list = list(filter(lambda x: x[49:61] >= self.start_date and x[49:61] < self.end_date, url_list))
        return download_url_list

    def _download_file(self, url:str="http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/20160717144500.geg-gcnlapi.json.gz"):
        download_url = url
        time.sleep(0.25)
        try:
            response = requests.get(download_url, headers = self._generate_header(), proxies =self.proxy, 
                                    timeout = 10, verify = False, stream=True)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_json(response_text, compression="gzip", lines = True)
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
            results.columns = self.columns_name
            return results
        except Exception as e:
            return e

if __name__ == "__main__":
    gdelt_v3_geg = GEG(start_date = "2020-01-01", end_date = "2020-01-02")
    gdelt_v3_geg_result = gdelt_v3_geg.query()
