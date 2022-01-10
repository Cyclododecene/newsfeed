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
    config.request_timeout = 10
    config.number_threads = 20
    config.thread_timeout_seconds = 2
    article = Article(url, config=config)
    article.download()
    article.parse()
    article_info = {
        "title": article.title,
        "authors": article.authors,
        "publish_date": article.publish_date,
        "text": article.text,
    }
    return article_info


## directly get news from url
result_direct = []
for i in tqdm.tqdm(range(0, 10)):
    url = articles_e.loc[i, "url"]
    try:
        result_direct.append(get_news(url))
    except:
        continue

result_direct = pd.DataFrame(result_direct)

## get news from url from internet archive
result_archive = []
for i in tqdm.tqdm(range(0, 10)):
    url = articles_e.loc[i, "url"]
    url = url.split("//")[1]
    response = requests.get(
        "https://archive.org/wayback/available?url={}".format(url),
        timeout=10,
        headers=generate_header(),
        stream=True)
    response_json = response.json()
    if response_json["archived_snapshots"] == {}:
        continue
    else:
        archive_url = response_json["archived_snapshots"]["closest"]["url"]
        archive_url = archive_url.replace("http://", "https://")
        try:
            result.append(get_news(archive_url))
        except:
            continue

result_archive = pd.DataFrame(result_archive)
