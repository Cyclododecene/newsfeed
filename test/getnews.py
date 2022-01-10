##################################################
###                                            ###
### Example of getting news from GDELT Doc API ###
### __AUTHOR__ = "Terence Liu"                 ###
### __DATE__ = "2022-01-11"                    ###
###                                            ###
##################################################

import re
import tqdm
import random
import requests
import pandas as pd
from random import randint
from newspaper import Config
from newspaper import Source
from newspaper import Article
from GNAF.news.apis.query import *
from GNAF.news.apis.filters import *
from fake_useragent import UserAgent


def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header


f = Art_Filter(keyword=["Exchange Rate", "World"],
               start_date="2017-12-31-00-00-00",
               end_date="2017-12-31-01-00-00",
               country=["China", "US"])

articles_30 = article_search(query_filter=f,
                             max_recursion_depth=100,
                             time_range=30)

articles_e = articles_30[articles_30['language'] == "English"]
articles_e.reset_index(drop=True, inplace=True)


def get_news(url):
    config = Config()
    config.browser_user_agent = generate_header()["User-Agent"]
    config.request_timeout = 50
    config.number_threads = 60
    config.thread_timeout_seconds = 10
    article = Article(url, config=config)
    article.download()
    article.parse()
    article_info = {
        "title": article.title,
        "authors": article.authors,
        "publish_date": article.publish_date,
        "text": article.text,
        "url": url
    }
    return article_info


## directly get news from url
def download_direct(url):
    try:
        return get_news(url)
    except Exception as e:
        print("Error in downloading {} \n Error: \n{}".format(url, e))
        pass  # return None


## get news from url from internet archive
def download_arxiv(url):
    url = url.split("//")[1]
    response = requests.get(
        "https://archive.org/wayback/available?url={}".format(url),
        timeout=20,
        headers=generate_header(),
        stream=True)
    response_json = response.json()
    if response_json["archived_snapshots"] == {}:
        print("No archive found")
        pass  # return None
    else:
        archive_url = response_json["archived_snapshots"]["closest"]["url"]
        archive_url = archive_url.replace("http://", "https://")
        try:
            return get_news(archive_url)
        except Exception as e:
            print("Error in downloading {} \n Error: \n{}".format(
                archive_url, e))
            pass  # return None


def check_url(url):
    print("\n[+]Checking if page exists...")
    try:
        # Requesting for only the HTTP header without downloading the page
        # If the page doesn't exist (404), it's a waste of resources to try scraping.
        response = requests.head(url, timeout=10, headers=generate_header())
        if "not found" in response.text:
            return 404
        else:
            return int(response.status_code)
    except:
        print("Connection related Error/Timeout, skipping...")
        return int(404)


def download_news(url):
    try:
        if check_url(url=url) != 404:
            return download_direct(url)
        else:
            return download_arxiv(url)
    except Exception as e:
        print(e)
        pass  # return None


result = []
for i in tqdm.tqdm(range(0, 20)):
    result.append(download_news(articles_e.loc[i, 'url']))