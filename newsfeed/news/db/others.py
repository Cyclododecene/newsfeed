"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
"""
import re
import io
from multiprocessing.sharedctypes import Value
import time
import tqdm
import requests
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta
from fake_useragent import UserAgent

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class GEG(object):
    cpu_num = multiprocessing.cpu_count() * 2
    columns_name = [
        'date', 'url', 'lang', 'polarity', 'magnitude', 'score', 'entities'
    ]

    def __init__(self,
                 start_date: str = "2021-01-01",
                 end_date: str = "2021-12-31",
                 proxy: dict = None):
        self.start_date = "".join(start_date.split("-")) + "000000"
        self.end_date = "".join(end_date.split("-")) + "000000"
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:

        print("[+] Scraping data from GDELT Project...")
        page = pd.read_csv(
            "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT",
            sep=" ",
            engine="c",
            na_filter=False,
            low_memory=False,
            names=["url"])
        url_list = page['url']
        del page
        download_url_list = list(
            filter(
                lambda x: x[49:61] >= self.start_date and x[49:61] < self.
                end_date, url_list))
        return download_url_list

    def _download_file(
        self,
        url:
        str = "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/20160717144500.geg-gcnlapi.json.gz"
    ):
        download_url = url
        time.sleep(0.25)
        try:
            response = requests.get(download_url,
                                    headers=self._generate_header(),
                                    proxies=self.proxy,
                                    timeout=10,
                                    verify=False,
                                    stream=True)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_json(response_text,
                                           compression="gzip",
                                           lines=True)
                response_text.flush()
                response_text.close()
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


class VGEG(object):

    base_url = "http://data.gdeltproject.org/gdeltv3/iatv/vgegv2/"
    columns_name_vgeg = [
        'date', 'showOffset', 'iaShowId', 'station', 'showName', 'iaClipUrl',
        'iaThumbnailUrl', 'processedDate', 'numOCRChars', 'OCRText',
        'numShotChanges', 'shotID', 'numSpeakerChanges', 'numSpokenWords',
        'numDistinctEntities', 'entities', 'numDistinctPresenceEntities',
        'presenceEntities'
    ]
    columns_names_raw = ['annotation_results']

    def __init__(self,
                 query_date: str = "2021-01-01",
                 domain: str = None,
                 raw: bool = False,
                 proxy: dict = None):
        self.query_date = "".join(query_date.split("-"))
        self.domain = domain
        self.raw = raw
        self.proxy = proxy
        self.cpu_num = multiprocessing.cpu_count() * 2

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def _query_list(self) -> list:
        url = self.base_url + self.query_date + ".txt"
        print("[+] Scraping data from GDELT Project...")
        page = pd.read_csv(url,
                           sep=" ",
                           engine="c",
                           na_filter=False,
                           low_memory=False,
                           names=["url"])
        self.domain = self.domain.upper()
        if self.domain == None:
            download_url_list = list(page["url"])
            return download_url_list
        else:
            url_list = page[page['url'].str.contains(self.domain)]
            if self.raw == True:
                download_url_list = list(
                    url_list[url_list["url"].str.contains("raw")]["url"])
                if download_url_list == []:
                    return ValueError(
                        "There is no data of {} in {}, please check your input again"
                        .format(self.domain, url))
                else:
                    return download_url_list
            elif self.raw == False:
                download_url_list = list(
                    url_list[url_list["url"].str.contains("vgeg.v2")]["url"])
                if download_url_list == []:
                    return ValueError(
                        "There is no data of {} in {}, please check your input again"
                        .format(self.domain, url))
                else:
                    return download_url_list

    def _download_file(
        self,
        url:
        str = "http://data.gdeltproject.org/gdeltv3/iatv/vgegv2/BBCNEWS_20200601_000000_BBC_World_News.vgeg.v2.json.gz"
    ):
        download_url = url
        time.sleep(0.25)
        try:
            response = requests.get(download_url,
                                    headers=self._generate_header(),
                                    proxies=self.proxy,
                                    timeout=10,
                                    verify=False,
                                    stream=True)
            if response.status_code == 404:
                return "GDELT does not contains this url: {}".format(url)

            else:
                response_text = io.BytesIO(response.content)
                response_df = pd.read_json(response_text,
                                           compression="gzip",
                                           lines=True)
                response_text.flush()
                response_text.close()
                return response_df

        except Exception as e:
            return e

    def query(self):
        download_url_list = self._query_list()
        pool = multiprocessing.Pool(self.cpu_num)
        try:
            print("[+] Downloading... [startdate={}]".format(
                self.query_date))
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
            if self.raw == True:
                results.columns = self.columns_names_raw
            else:
                results.columns = self.columns_name_vgeg
            return results
        except Exception as e:
            return e


class GDG(object):

    base_url = "http://data.gdeltproject.org/gdeltv3/gdg/"

    def __init__(self,
                 query_date: str = "2018-07-27-14-00-00",
                 proxy: dict = None):
        self.query_date = "".join(query_date.split("-"))
        self.proxy = proxy

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def query(self):
        url = self.base_url + datetime.strftime(
            datetime.strptime(self.query_date, "%Y%m%d%H%M%S") +
            timedelta(minutes=1), "%Y%m%d%H%M%S") + ".gdg.v3.json.gz"
        response = requests.get(url,
                                headers=self._generate_header(),
                                proxies=self.proxy)
        if response.ok:
            response = io.BytesIO(response.content)
            result = pd.read_json(response, compression="gzip", lines=True)
            return result
        else:
            return ValueError(
                "GDELT does not contains GDG data of date: {}".format(
                    self.query_date))


class GFG(object):

    base_url = "http://data.gdeltproject.org/gdeltv3/gfg/alpha/"
    columns_name = ["DATE", "FromFrontPageURL", "LinkID", "LinkPercentMaxID", "ToLinkURL", "LinkText"]
    def __init__(self,
                 query_date: str = "2018-07-27-14-00-00",
                 proxy: dict = None):
        self.query_date = "".join(query_date.split("-"))
        self.proxy = proxy
        self.latest_date()

    def _generate_header(self):
        ua = UserAgent(verify_ssl=False)
        header = {"User-Agent": str(ua.random)}
        return header

    def latest_date(self):
        url = self.base_url + "lastupdate.txt"
        latest_url = list(
            pd.read_csv(url, sep=" ", names=["a", "b", "url"])["url"])[0]
        reg = re.compile("\\d{14}")
        date = reg.findall(latest_url)[0]
        print("The latest file is: {}".format(
            datetime.strftime(datetime.strptime(date, "%Y%m%d%H%M%S"),
                              "%Y-%m-%d-%H-%M-%S")))

    def query(self):
        url = self.base_url + self.query_date + ".LINKS.TXT.gz"
        response = requests.get(url,
                                headers=self._generate_header(),
                                proxies=self.proxy)
        if response.ok:
            print("[+] Loading...")
            result = pd.read_csv(url,
                                 compression="gzip",
                                 sep="\t",
                                 on_bad_lines="skip")
            result.columns = self.columns_name
            return result
        else:
            return ValueError(
                "GDELT does not contains GFG data of date: {}".format(
                    self.query_date))


if __name__ == "__main__":
    # GDELT Global Entity Graph
    gdelt_v3_geg = GEG(start_date="2020-01-01", end_date="2020-01-02")
    gdelt_v3_geg_result = gdelt_v3_geg.query()

    # GDELT Visual Global Entity Graph
    gdelt_v3_vgeg = VGEG(query_date="2020-01-01", domain="CNN")
    gdelt_v3_vgeg_result = gdelt_v3_vgeg.query()

    # GDELT Glocal Difference Graph
    gdelt_v3_gdg = GDG(query_date="2018-08-27-14-00-00")
    gdelt_v3_gdg_result = gdelt_v3_gdg.query()
    gdelt_v3_gfg = GFG(query_date="2018-03-02-02-00-00")
    gdelt_v3_gfg_result = gdelt_v3_gfg.query()
