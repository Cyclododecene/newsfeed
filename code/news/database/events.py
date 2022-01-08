import io
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

class Event_V1(object):
    base_url = "http://data.gdeltproject.org/events/"
    cpu_num = multiprocessing.cpu_count()

    header = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 
            'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 
            'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 
            'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 
            'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 
            'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName', 
            'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', 
            'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 
            'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat', 'ActionGeo_Long', 
            'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']


    def __init__(self, start_date:str = "2020-01-01", end_date:str = "2021-12-31", proxy:dict = None):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:
        page = requests.get("http://data.gdeltproject.org/events/index.html", headers = self._generate_header(), proxies = self.proxy)
        webpage = html.fromstring(page.content)
        url_list = webpage.xpath("//a/@href")
        #url_list = [item for item in url_list if len(item) == 23] report other url
        download_url_list = list(filter(lambda x: x[0:8] >= self.start_date and x[0:8] <= self.end_date, url_list))

        return download_url_list

    def _download_file(self, url:str="20200101.export.CSV.zip"):
        download_url = self.base_url + url
        time.sleep(0.002)
        try:
            response = requests.get(download_url, headers = self._generate_header(), proxies =self.proxy, timeout = 10)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_csv(response_text, compression="zip", sep = "\t", header = None, 
                                            warn_bad_lines = False, low_memory=False, dtype ={26: 'str', 27: 'str', 28: 'str'})
                response_text.flush()
                response_text.close()
                return response_df

        except Exception as e:
            return e


        

    def query(self):
        download_url_list = self._query_list()
        pool = multiprocessing.Pool(self.cpu_num)
        try:
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
    gdelt_events_v1 = Event_V1(start_date = "2021-01-01", end_date = "2021-01-02")
    results = gdelt_events_v1.query()