"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
modified: Sat 05 Jan, 2022
updated: 2026 - Performance optimizations
"""
import time
import tqdm
import requests
import pandas as pd
from lxml import html
import multiprocessing
from datetime import datetime, timedelta, timezone
from fake_useragent import UserAgent
from typing import Optional
from tenacity import retry, stop_after_attempt

from newsfeed.utils.cache import get_cache_manager
from newsfeed.utils.incremental import get_incremental_manager
from newsfeed.utils.async_downloader import run_async_download

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
                 proxy: dict = None,
                 use_cache: bool = False,
                 use_incremental: bool = False,
                 force_redownload: bool = False,
                 use_async: bool = False,
                 output_format: str = "csv"):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.proxy = proxy
        self.use_cache = use_cache
        self.use_incremental = use_incremental
        self.force_redownload = force_redownload
        self.use_async = use_async
        self.output_format = output_format
        self.cache_manager = get_cache_manager() if use_cache else None
        self.incremental_manager = get_incremental_manager() if use_incremental else None

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    @retry(stop=stop_after_attempt(3))
    def _query_list(self) -> list:
        download_url_list = [
            datetime.strftime(i, "%Y%m%d") + ".export.CSV.zip"
            for i in pd.date_range(self.start_date, self.end_date, freq="D")
        ]
        return download_url_list

    @retry(stop=stop_after_attempt(3))
    def _download_file(self, url: str = "20200101.export.CSV.zip"):
        """
        fixed: add retry mechanism
        fixed: replace write_bad_lines with on_bad_lines
        """
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
                response_df = pd.read_csv(download_url,
                                          compression="zip",
                                          sep="\t",
                                          header=None,
                                          on_bad_lines='skip',
                                          low_memory=False)
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        
        # Check cache first
        if self.use_cache and not self.force_redownload:
            cached_data = self.cache_manager.get(
                db_type="EVENT",
                version="V1",
                start_date=self.start_date,
                end_date=self.end_date
            )
            if cached_data is not None:
                print("[+] Loading from cache...")
                return cached_data
        
        # Apply incremental query
        if self.use_incremental and not self.force_redownload:
            all_files = download_url_list
            new_files = self.incremental_manager.get_new_files(
                all_files,
                db_type="EVENT",
                version="V1",
                start_date=self.start_date,
                end_date=self.end_date
            )
            if len(new_files) == 0:
                print("[+] No new files to download (incremental mode)")
                return pd.DataFrame(columns=self.columns_name)
            download_url_list = new_files
        
        # Use async download if enabled
        if self.use_async:
            print("[+] Using async download...")
            downloaded_dfs, errors = run_async_download(
                self.base_url,
                download_url_list,
                max_concurrent=20,
                proxy=self.proxy
            )
            if errors:
                print(f"[+] {len(errors)} files failed to download")
        else:
            # Original synchronous download
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
            except Exception as e:
                return e
        
        # Process downloaded data
        downloaded_dfs = [df for df in downloaded_dfs if isinstance(df, pd.DataFrame)]
        if not downloaded_dfs:
            print("[+] No valid data downloaded")
            return pd.DataFrame(columns=self.columns_name)
        
        results = pd.concat(downloaded_dfs)
        del downloaded_dfs
        results.reset_index(drop=True, inplace=True)
        results.columns = self.columns_name
        
        # Save to cache if enabled
        if self.use_cache:
            self.cache_manager.set(results,
                              db_type="EVENT",
                              version="V1",
                              start_date=self.start_date,
                              end_date=self.end_date)
        
        # Save incremental history if enabled
        if self.use_incremental:
            self.incremental_manager.save_query_history(
                download_url_list,
                db_type="EVENT",
                version="V1",
                start_date=self.start_date,
                end_date=self.end_date
            )
        
        return results

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
                 proxy: dict = None,
                 use_cache: bool = False,
                 use_incremental: bool = False,
                 force_redownload: bool = False,
                 use_async: bool = False,
                 output_format: str = "csv"):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
        self.table = table
        self.translation = translation
        self.proxy = proxy
        self.use_cache = use_cache
        self.use_incremental = use_incremental
        self.force_redownload = force_redownload
        self.use_async = use_async
        self.output_format = output_format
        self.cache_manager = get_cache_manager() if use_cache else None
        self.incremental_manager = get_incremental_manager() if use_incremental else None

    def _generate_header(self):
        ua = UserAgent()
        header = {"User-Agent": str(ua.random)}
        return header

    @retry(stop=stop_after_attempt(3))
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

    @retry(stop=stop_after_attempt(3))
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
                response_df = pd.read_csv(download_url,
                                          compression="zip",
                                          sep="\t",
                                          header=None,
                                          on_bad_lines='skip',
                                          low_memory=False)
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        
        # Check cache first
        if self.use_cache and not self.force_redownload:
            cached_data = self.cache_manager.get(
                db_type="EVENT",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
                table_type=self.table,
                translation=self.translation
            )
            if cached_data is not None:
                print("[+] Loading from cache...")
                return cached_data
        
        # Apply incremental query
        if self.use_incremental and not self.force_redownload:
            all_files = download_url_list
            new_files = self.incremental_manager.get_new_files(
                all_files,
                db_type="EVENT",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
                table_type=self.table,
                translation=self.translation
            )
            if len(new_files) == 0:
                print("[+] No new files to download (incremental mode)")
                if self.table == "events":
                    return pd.DataFrame(columns=self.columns_name_events)
                else:
                    return pd.DataFrame(columns=self.columns_name_mentions)
            download_url_list = new_files
        
        # Use async download if enabled
        if self.use_async:
            print("[+] Using async download...")
            downloaded_dfs, errors = run_async_download(
                self.base_url,
                download_url_list,
                max_concurrent=20,
                proxy=self.proxy
            )
            if errors:
                print(f"[+] {len(errors)} files failed to download")
        else:
            # Original synchronous download
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
            except Exception as e:
                return e
        
        # Process downloaded data
        downloaded_dfs = [df for df in downloaded_dfs if isinstance(df, pd.DataFrame)]
        if not downloaded_dfs:
            print("[+] No valid data downloaded")
            if self.table == "events":
                return pd.DataFrame(columns=self.columns_name_events)
            else:
                return pd.DataFrame(columns=self.columns_name_mentions)
        
        results = pd.concat(downloaded_dfs)
        del downloaded_dfs
        results.reset_index(drop=True, inplace=True)
        
        # Set columns based on table type
        if self.table == "events":
            results.columns = self.columns_name_events
        else:
            results.columns = self.columns_name_mentions
        
        # Save to cache if enabled
        if self.use_cache:
            self.cache_manager.set(results,
                              db_type="EVENT",
                              version="V2",
                              start_date=self.start_date,
                              end_date=self.end_date,
                              table_type=self.table,
                              translation=self.translation)
        
        # Save incremental history if enabled
        if self.use_incremental:
            self.incremental_manager.save_query_history(
                download_url_list,
                db_type="EVENT",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
                table_type=self.table,
                translation=self.translation
            )
        
        return results

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