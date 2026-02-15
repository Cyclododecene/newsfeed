"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
updated: 2026 - Performance optimizations
"""

import io
import time
import tqdm
import requests
import pandas as pd
from lxml import html
import multiprocessing
from datetime import datetime, timezone, timedelta
from fake_useragent import UserAgent
from typing import Optional

from tenacity import retry, stop_after_attempt

from newsfeed.utils.cache import get_cache_manager
from newsfeed.utils.incremental import get_incremental_manager
from newsfeed.utils.async_downloader import run_async_download

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class GKGV1(object):
    base_url = "http://data.gdeltproject.org/gkg/"
    cpu_num = multiprocessing.cpu_count() * 2

    columns_name = [
        'DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS',
        'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS'
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
        print("[+] Scraping data from GDELT Project...")
        page = requests.get("http://data.gdeltproject.org/gkg/index.html",
                            headers=self._generate_header(),
                            proxies=self.proxy)
        webpage = html.fromstring(page.content)
        url_list = webpage.xpath("//a/@href")
        download_url_list = [
            datetime.strftime(i, "%Y%m%d") + ".gkg.csv.zip"
            for i in pd.date_range(self.start_date, self.end_date, freq="D")
        ]
        return download_url_list

    @retry(stop=stop_after_attempt(3))
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
                                          on_bad_lines='skip',
                                          low_memory=False)
                response_text.flush()
                response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        
        # Check cache first
        if self.use_cache and not self.force_redownload:
            cached_data = self.cache_manager.get(
                db_type="GKG",
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
                db_type="GKG",
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
                              db_type="GKG",
                              version="V1",
                              start_date=self.start_date,
                              end_date=self.end_date)
        
        # Save incremental history if enabled
        if self.use_incremental:
            self.incremental_manager.save_query_history(
                download_url_list,
                db_type="GKG",
                version="V1",
                start_date=self.start_date,
                end_date=self.end_date
            )
        
        return results

    def query_nowtime(self, date: str = None):
        if date == None:
            dt = datetime.now(timezone.utc) - timedelta(days=1)
        else:
            dt = datetime.strptime(date, "%Y-%m-%d")

        url = datetime.strftime(dt, "%Y%m%d") + ".gkg.csv.zip"
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


class GKGV2(object):
    cpu_num = multiprocessing.cpu_count() * 2
    base_url = "http://data.gdeltproject.org/gdeltv2/"
    
    columns_name = [
        'GKGRECORDID', 'V2.1DATE', 'V2SOURCECOLLECTIONIDENTIFIER',
        'V2SOURCECOMMONNAME', 'V2DOCUMENTIDENTIFIER', 'V1COUNTS', 'V2COUNTS',
        'V1THEMES', 'V2ENHANCEDTHEMES', 'V1LOCATIONS', 'V2ENHANCEDLOCATIONS',
        'V1PERSONS', 'V2ENHANCEDPERSONS', 'V1ORGANIZATIONS',
        'V2ENHANCEDORGANIZATIONS', 'V1TONE', 'V2ENHANCEDDATES', 'V2GCAM',
        'V2SHARINGIMAGE', 'V2RELATEDIMAGES', 'V2SOCIALIMAGEEMBEDS',
        'V2SOCIALVIDEOEMBEDS', 'V2QUOTATIONS', 'V2ALLNAMES', 'V2AMOUNTS',
        'V2TRANSLATIONINFO', 'V2EXTRASXML'
    ]

    def __init__(self,
                 start_date: str = "2020-01-01-00-00-00",
                 end_date: str = "2021-12-31-00-00-00",
                 translation: bool = False,
                 proxy: dict = None,
                 use_cache: bool = False,
                 use_incremental: bool = False,
                 force_redownload: bool = False,
                 use_async: bool = False,
                 output_format: str = "csv"):
        self.start_date = "".join(start_date.split("-"))
        self.end_date = "".join(end_date.split("-"))
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
        if self.translation == True:
            print("[+] Scraping data from GDELT Project...")
            download_url_list = [
                datetime.strftime(i, "%Y%m%d%H%M%S") +
                ".translation.gkg.csv.zip" for i in pd.date_range(
                    self.start_date, self.end_date, freq="15min")
            ]
            return download_url_list
        else:
            print("[+] Scraping data from GDELT Project...")
            download_url_list = [
                datetime.strftime(i, "%Y%m%d%H%M%S") + ".gkg.csv.zip" for i in
                pd.date_range(self.start_date, self.end_date, freq="15min")
            ]
            return download_url_list

    @retry(stop=stop_after_attempt(3))
    def _download_file(self, url: str = None):
        download_url = self.base_url + url
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
                                              on_bad_lines='skip',
                                              low_memory=False,
                                              encoding="utf-8")
                except UnicodeDecodeError:
                    response_df = pd.read_csv(response_text,
                                              compression="zip",
                                              sep="\t",
                                              header=None,
                                              on_bad_lines='skip',
                                              low_memory=False,
                                              encoding="latin-1")
                response_text.flush()
                response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        
        # Check cache first
        if self.use_cache and not self.force_redownload:
            cached_data = self.cache_manager.get(
                db_type="GKG",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
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
                db_type="GKG",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
                translation=self.translation
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
                              db_type="GKG",
                              version="V2",
                              start_date=self.start_date,
                              end_date=self.end_date,
                              translation=self.translation)
        
        # Save incremental history if enabled
        if self.use_incremental:
            self.incremental_manager.save_query_history(
                download_url_list,
                db_type="GKG",
                version="V2",
                start_date=self.start_date,
                end_date=self.end_date,
                translation=self.translation
            )
        
        return results

    def query_nowtime(self, date: str = None):
        if date == None:
            dt = datetime.now(timezone.utc)
        else:
            dt = datetime.strptime(date, "%Y-%m-%d-%H-%M-%S")

        if self.translation:
            url = datetime.strftime(
                datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                         (dt.minute // 15)) - timedelta(minutes=15),
                "%Y%m%d%H%M%S") + ".translation.gkg.csv.zip"
        else:
            url = datetime.strftime(
                datetime(dt.year, dt.month, dt.day, dt.hour, 15 *
                         (dt.minute // 15)), "%Y%m%d%H%M%S") + ".gkg.csv.zip"

        print("[+] Downloading... date:{}".format(
            datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")))
        results = self._download_file(url=url)
        results.reset_index(drop=True, inplace=True)
        results.columns = self.columns_name
        return results


if __name__ == "__main__":

    # GDELT GKG Database Version 1.0
    gdelt_events_v1_gkg = GKGV1(start_date="2021-01-01", end_date="2021-01-02")
    results_v1_gkg = gdelt_events_v1_gkg.query()

    # GDELT GKG Database Version 2.0
    gdelt_events_v2_gkg = GKGV2(start_date="2021-01-01",
                                end_date="2021-01-02",
                                translation=False)
    results_v2_gkg = gdelt_events_v2_gkg.query()