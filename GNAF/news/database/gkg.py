"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
"""

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


class GKG_V1(object):
    base_url = "http://data.gdeltproject.org/gkg/"
    cpu_num = multiprocessing.cpu_count() * 10

    columns_name = [
        'DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS',
        'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS'
    ]

    def __init__(self,
                 start_date: str = "2020-01-01",
                 end_date: str = "2021-12-31",
                 proxy: dict = None):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:
        print("[+] Scraping data from GDELT Project...")
        page = requests.get("http://data.gdeltproject.org/gkg/index.html",
                            headers=self._generate_header(),
                            proxies=self.proxy)
        webpage = html.fromstring(page.content)
        url_list = webpage.xpath("//a/@href")
        #url_list = [item for item in url_list if len(item) == 23] report other url
        download_url_list = list(
            filter(
                lambda x: x[0:8] >= self.start_date and x[0:8] < self.end_date,
                url_list))
        download_url_list = list(
            filter(lambda x: len(x) == 20, download_url_list))
        return download_url_list

    def _download_file(self, url: str = "20200101.gkg.csv.zip"):
        download_url = self.base_url + url
        time.sleep(0.0005)
        try:
            response = requests.get(download_url,
                                    headers=self._generate_header(),
                                    proxies=self.proxy,
                                    timeout=10)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_csv(response_text,
                                          compression="zip",
                                          sep="\t",
                                          header=None,
                                          warn_bad_lines=False,
                                          low_memory=False)
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
            downloaded_dfs = list(
                tqdm.tqdm(pool.imap_unordered(self._download_file,
                                              download_url_list),
                          total=len(download_url_list)))
            pool.close()
            pool.terminate()
            pool.join()
            results = pd.concat(downloaded_dfs)
            del downloaded_dfs
            results.reset_index(drop=True, inplace=True)
            results.columns = self.columns_name
            return results
        except Exception as e:
            return e


class GKG_V2(object):
    cpu_num = multiprocessing.cpu_count() * 10

    columns_name = [
        'GKGRECORDID', 'V2.1DATE', ' V2SOURCECOLLECTIONIDENTIFIER',
        ' V2SOURCECOMMONNAME', 'V2DOCUMENTIDENTIFIER', ' V1COUNTS',
        ' V2COUNTS', ' V1THEMES', 'V2ENHANCEDTHEMES', ' V1LOCATIONS',
        ' V2ENHANCEDLOCATIONS', ' V1PERSONS', ' V2ENHANCEDPERSONS',
        ' V1ORGANIZATIONS', ' V2ENHANCEDORGANIZATIONS', 'V1TONE',
        'V2ENHANCEDDATES', 'V2GCAM', ' V2SHARINGIMAGE', ' V2RELATEDIMAGES',
        ' V2SOCIALIMAGEEMBEDS', ' V2SOCIALVIDEOEMBEDS', ' V2QUOTATIONS',
        ' V2ALLNAMES', ' V2AMOUNTS', ' V2TRANSLATIONINFO', ' V2EXTRASXML'
    ]

    def __init__(self,
                 start_date: str = "2020-01-01",
                 end_date: str = "2021-12-31",
                 translation: bool = False,
                 proxy: dict = None):
        self.start_date = "".join(start_date.split("-")) + "000000"
        self.end_date = "".join(end_date.split("-")) + "000000"
        self.translation = translation
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:

        if self.translation == True:
            print("[+] Scraping data from GDELT Project...")
            page = pd.read_csv(
                "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
                sep=" ",
                engine="c",
                na_filter=False,
                low_memory=False,
                names=["a", "b", "url"])
            url_list = list(page[page["url"].str.contains("gkg")]["url"])
            del page
            download_url_list = list(
                filter(
                    lambda x: x[37:51] >= self.start_date and x[37:51] < self.
                    end_date, url_list))
            return download_url_list

        else:
            print("[+] Scraping data from GDELT Project...")
            page = pd.read_csv(
                "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
                sep=" ",
                engine="c",
                na_filter=False,
                low_memory=False,
                names=["a", "b", "url"])
            url_list = list(page[page["url"].str.contains("gkg")]["url"])
            del page
            download_url_list = list(
                filter(
                    lambda x: x[37:51] >= self.start_date and x[37:51] < self.
                    end_date, url_list))
            return download_url_list

    def _download_file(
        self,
        url:
        str = "http://data.gdeltproject.org/gdeltv2/20220108000000.gkg.csv.zip"
    ):
        download_url = url
        time.sleep(0.001)
        try:
            response = requests.get(download_url,
                                    headers=self._generate_header(),
                                    proxies=self.proxy,
                                    timeout=15,
                                    verify=False,
                                    stream=True)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                try:
                    response_df = pd.read_csv(response_text,
                                              compression="zip",
                                              sep="\t",
                                              header=None,
                                              low_memory=False,
                                              encoding="utf-8")
                except UnicodeDecodeError:
                    response_df = pd.read_csv(response_text,
                                              compression="zip",
                                              sep="\t",
                                              header=None,
                                              low_memory=False,
                                              encoding="latin-1")
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
            downloaded_dfs = list(
                tqdm.tqdm(pool.imap_unordered(self._download_file,
                                              download_url_list),
                          total=len(download_url_list)))
            pool.close()
            pool.terminate()
            pool.join()
            results = pd.concat(downloaded_dfs)
            del downloaded_dfs
            results.reset_index(drop=True, inplace=True)
            results.columns = self.columns_name
            return results
        except Exception as e:
            return e


"""
### test ###
page = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", sep = " ", engine = "c", na_filter = False, 
                                low_memory = False, names = ["a", "b", "url"])
url_list = list(page[page["url"].str.contains("gkg")]["url"])
download_url_list = list(filter(lambda x: x[37:51] >= "20210101000000" and x[37:51] < "20210102000000", url_list))
result = []
for i in tqdm.tqdm(range(0, len(download_url_list))):
    response = requests.get(download_url_list[i], timeout = 10, stream = True)
    response_text = io.BytesIO(response.content)
    try:
        response_df = pd.read_csv(response_text, compression="zip", sep = "\t", header = None, low_memory=False, encoding = "utf-8")
    except UnicodeDecodeError:
        response_df = pd.read_csv(response_text, compression="zip", sep = "\t", header = None, low_memory=False, encoding = "latin-1")
    result.append(response_df)
"""

if __name__ == "__main__":

    # GDELT GKG Database Version 1.0
    gdelt_events_v1_gkg = GKG_V1(start_date="2021-01-01",
                                 end_date="2021-01-02")
    results_v1_gkg = gdelt_events_v1_gkg.query()

    # GDELT GKG Database Version 2.0
    gdelt_events_v2_gkg = GKG_V2(start_date="2021-01-01",
                                 end_date="2021-01-02",
                                 translation=False)
    results_v2_gkg = gdelt_events_v2_gkg.query()
