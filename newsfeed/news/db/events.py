"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
modified: Sat 05 Jan, 2022
"""
import time
import tqdm
import requests
import pandas as pd
from lxml import html
import multiprocessing
from datetime import datetime, timedelta, timezone
from fake_useragent import UserAgent

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class EventV1(object):
    base_url = "http://data.gdeltproject.org/events/"
    cpu_num = multiprocessing.cpu_count() * 2

    columns_name = [
        'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
        'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
        'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code',
        'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code',
        'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode',
        'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code',
        'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code',
        'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode',
        'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions',
        'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type',
        'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
        'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID',
        'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode',
        'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
        'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName',
        'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat',
        'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL'
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
        download_url_list = [
            datetime.strftime(i, "%Y%m%d") + ".export.CSV.zip"
            for i in pd.date_range(self.start_date, self.end_date, freq="D")
        ]
        return download_url_list

    # tp = pd.read_csv('Check1_900.csv', sep='\t', iterator=True, chunksize=1000)
    def _download_file(self, url: str = "20200101.export.CSV.zip"):
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
                #response_text = io.BytesIO(response.content)
                response_df = pd.read_csv(download_url,
                                          compression="zip",
                                          sep="\t",
                                          header=None,
                                          warn_bad_lines=False,
                                          low_memory=False)
                #response_text.flush()
                #response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        pool = multiprocessing.Pool(self.cpu_num)
        try:
            print("[+] Downloading... [startdate={} & enddate={}]".format(
                self.start_date, self.end_date))
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

    def query_nowtime(self, date: str = None):
        # by default the self.start_date variable is None, then the func will query for the nearest files
        # if self.start_date is valued, then the func will query the given datetime
        if date == None:
            dt = datetime.now(timezone.utc) - timedelta(days=1)
        else:
            dt = datetime.strptime(date, "%Y-%m-%d")

        url = datetime.strftime(dt, "%Y%m%d") + ".export.CSV.zip"
        print("[+] Downloading... date:{}".format(
            datetime.strftime(dt, "%Y-%m-%d")))
        results = self._download_file(url=url)
        if type(results) != pd.DataFrame:
            print(results)
            results = self.query_nowtime(date=datetime.strftime(dt - timedelta(days=1), "%Y-%m-%d"))
            return results
        else:
            results.reset_index(drop=True, inplace=True)
            results.columns = self.columns_name
            return results


class EventV2(object):
    cpu_num = multiprocessing.cpu_count() * 2
    base_url = "http://data.gdeltproject.org/gdeltv2/"

    columns_name_events = [
        'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
        'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
        'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code',
        'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code',
        'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode',
        'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code',
        'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code',
        'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode',
        'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions',
        'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type',
        'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
        'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
        'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName',
        'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code',
        'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID',
        'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
        'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 'ActionGeo_Lat',
        'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL'
    ]

    columns_name_mentions = [
        'GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate', 'MentionType',
        'MentionSourceName', 'Mentionidentifier', 'SentenceID',
        'Actor1CharOffset', 'Actor2CharOffset', 'ActionCharOffset',
        'InRawText', 'Confidence', 'MentionDocLen', 'MentionDocTone',
        'MentionDocTranslationInfo', "Extras"
    ]

    def __init__(self,
                 start_date: str = "2021-12-30-00-00-00",
                 end_date: str = "2021-12-31-00-00-00",
                 table: str = "events",
                 translation: bool = False,
                 proxy: dict = None):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.table = table
        self.translation = translation
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:

        if self.table != "events" and self.table != "mentions":
            return ValueError(
                "Wrong table name, EventV2 is only used for querying Event and Mentions"
            )

        else:
            if self.translation == True:
                print("[+] Scraping data from GDELT Project...")
                if self.table == "events":
                    download_url_list = [
                        datetime.strftime(i, "%Y%m%d%H%M%S") +
                        ".translation.export.CSV.zip" for i in pd.date_range(
                            self.start_date, self.end_date, freq="15min")
                    ]
                    return download_url_list
                elif self.table == "mentions":
                    download_url_list = [
                        datetime.strftime(i, "%Y%m%d%H%M%S") +
                        ".translation.mentions.CSV.zip" for i in pd.date_range(
                            self.start_date, self.end_date, freq="15min")
                    ]
                    return download_url_list

            else:
                print("[+] Scraping data from GDELT Project...")
                if self.table == "events":
                    download_url_list = [
                        datetime.strftime(i, "%Y%m%d%H%M%S") +
                        ".export.CSV.zip" for i in pd.date_range(
                            self.start_date, self.end_date, freq="15min")
                    ]
                    return download_url_list
                elif self.table == "mentions":
                    download_url_list = [
                        datetime.strftime(i, "%Y%m%d%H%M%S") +
                        ".mentions.CSV.zip" for i in pd.date_range(
                            self.start_date, self.end_date, freq="15min")
                    ]
                    return download_url_list

    def _download_file(self, url: str = "20220108160000.export.CSV.zip"):
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
                #response_text = io.BytesIO(response.content)
                response_df = pd.read_csv(download_url,
                                          compression="zip",
                                          sep="\t",
                                          header=None,
                                          warn_bad_lines=False,
                                          low_memory=False)
                #response_text.flush()
                #response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        pool = multiprocessing.Pool(self.cpu_num)
        try:
            print("[+] Downloading... [startdate={} & enddate={}]".format(
                self.start_date, self.end_date))
            downloaded_dfs = list(
                tqdm.tqdm(pool.imap_unordered(self._download_file,
                                              download_url_list),
                          total=len(download_url_list)))
            pool.close()
            pool.terminate()
            pool.join()
            results = [
                data for data in downloaded_dfs if type(data) == pd.DataFrame
            ]  # remove non DataFrame (e.g. Error)
            results = pd.concat(results)
            del downloaded_dfs
            results.reset_index(drop=True, inplace=True)
            if self.table == "events":
                results.columns = self.columns_name_events
                return results
            else:
                results.columns = self.columns_name_mentions
                return results
        except Exception as e:
            return e

    def query_nowtime(self, date: str = None):
        # by default the self.start_date variable is None, then the func will query for the nearest files
        # if self.start_date is valued, then the func will query the given datetime
        if date == None:
            dt = datetime.now(timezone.utc)
        else:
            dt = datetime.strptime(date, "%Y-%m-%d-%H-%M-%S")

        if self.translation:
            if self.table != "mentions":
                url = datetime.strftime(
                    datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                             (dt.minute // 15)),
                    "%Y%m%d%H%M%S") + ".translation.export.CSV.zip"
            else:
                url = datetime.strftime(
                    datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                             (dt.minute // 15)),
                    "%Y%m%d%H%M%S") + ".translation.mentions.CSV.zip"
        else:
            if self.table != "mentions":
                url = datetime.strftime(
                    datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                             (dt.minute // 15)),
                    "%Y%m%d%H%M%S") + ".export.CSV.zip"
            else:
                url = datetime.strftime(
                    datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                             (dt.minute // 15)),
                    "%Y%m%d%H%M%S") + ".mentions.CSV.zip"

        print("[+] Downloading... date:{}".format(
            datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")))
        results = self._download_file(url=url)
        results.reset_index(drop=True, inplace=True)
        if self.table == "events":
            results.columns = self.columns_name_events
            return results
        else:
            results.columns = self.columns_name_mentions
            return results


if __name__ == "__main__":

    # GDELT Event Database Version 1.0
    gdelt_events_v1_events = EventV1(start_date="2021-01-01",
                                      end_date="2021-01-02")
    results_v1_events = gdelt_events_v1_events.query()

    # GDELT Event Database Version 2.0 - Event
    gdelt_events_v2_events = EventV2(start_date="2021-01-01",
                                      end_date="2021-01-02")
    results_v2_events = gdelt_events_v2_events.query()

    # GDELT Event Database Version 2.0 - Mentions
    gdelt_events_v2_mentions = EventV2(start_date="2021-01-01",
                                        end_date="2021-01-02",
                                        table="mentions")
    results_v2_mentions = gdelt_events_v2_mentions.query()