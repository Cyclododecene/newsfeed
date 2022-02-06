"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021

The original code is from gdelt-doc-api (with MIT License):
"https://github.com/alex9smith/gdelt-doc-api/blob/2a545fb1e113dbb7fd4de8fd4eab8f1f62817543/gdeltdoc/api_client.py"

by default, the system(GDELT) only provide at max 250 results
thus, we are trying to remove the boundary by spliting
the date range into multiple chunks:

with start_date and end_date in filters, we split one day into two 12 hours:

      --------------    ------------
      | start date |    | end date |
      --------------    ------------
                              |
------------------    ------------------
| tmp start date |    | tmp end date 1 | (tmp end date = start date + 12 hours)
------------------    ------------------                
                              | + 12 hours
--------------------  ------------------
| tmp start date   |  | tmp end date 2 |
| = tmp end date 1 |  |                |
--------------------  ------------------           
...
"""
import re
import json
import tqdm
import requests
import pandas as pd
import multiprocessing
from functools import partial
from datetime import datetime

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def text_regex(str_1, str_2, newstring, text):
    reg = "%s(.*?)%s" % (str_1, str_2)
    r = re.compile(reg, re.DOTALL)
    return (r.sub(newstring, text))


def load_json(json_message,
              max_recursion_depth: int = 100,
              recursion_depth: int = 0):
    try:
        result = json.loads(json_message)
    except Exception as e:

        if recursion_depth >= max_recursion_depth:
            raise ValueError(
                "Max Recursion depth is reached. JSON can´t be parsed!")
        # Find the offending character index:
        idx_to_replace = int(e.pos)

        # Remove the offending character:
        if isinstance(json_message, bytes):
            json_message.decode("utf-8")
        json_message = list(json_message)
        json_message[idx_to_replace] = ' '
        new_message = ''.join(str(m) for m in json_message)
        return load_json(json_message=new_message,
                         max_recursion_depth=max_recursion_depth,
                         recursion_depth=recursion_depth + 1)
    return result


def doc_query_search(query_string=None,
                     max_recursion_depth: int = 100,
                     mode=None,
                     proxy: dict = None):

    if query_string == None:
        return ValueError("Query string must be provided")
    elif mode == None:
        return ValueError("Query mode must be provided")
    else:
        query_string = query_string
        response = requests.get(
            f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode={mode}&format=json",
            proxies=proxy)

        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")

        else:
            if mode == "artlist":
                pattern = re.compile('\d{14}')
                output = pd.DataFrame(
                    load_json(
                        response.text,
                        max_recursion_depth=max_recursion_depth)["articles"])
                output["timeadded"] = [pattern.findall(query_string)[1]
                                       ] * len(output)
                return output
            elif mode == "timelinevol" or "timelinevolraw" or "timelinetone" or "timelinetone" or "timelinelang" or "timelinesourcecountry":
                return pd.DataFrame(
                    load_json(response.text)["timeline"][0]["data"])


def geo_query_search(query_string:str = None,
                     format: str = "GeoJSON",
                     timespan: int = 1,
                     proxy: dict = None):

    if query_string == None:
        return ValueError("Query string must be provided")
    else:
        query_string = query_string
        response = requests.get(
            f"https://api.gdeltproject.org/api/v2/geo/geo?query={query_string}&format={format}&timespan={timespan}d",
            proxies=proxy)

        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")

        return response.text


def article_search(query_filter=None,
                   max_recursion_depth: int = 100,
                   time_range: int = 60,
                   proxy: dict = None):

    cpu_num = multiprocessing.cpu_count() * 2

    if query_filter == None:
        return ValueError("Filter must be provided")

    if time_range < 30:
        return ValueError("time range has to larger than 30 mins")

    try:
        date_range = [
            datetime.strftime(date, "%Y%m%d%H%M%S")
            for date in pd.date_range(query_filter.start_date,
                                      query_filter.end_date,
                                      freq="{}min".format(time_range))
        ]
        tmp_query_string = query_filter.query_string
        query_string = []
        for i in range(0, len(date_range) - 1):
            tmp_start_date, tmp_end_date = date_range[i], date_range[i + 1]
            tmp_date_string = "&startdatetime=" + tmp_start_date + "&enddatetime=" + tmp_end_date + "&maxrecords"
            tmp_query_string = text_regex(str_1="&startdatetime",
                                          str_2="&maxrecords",
                                          newstring=tmp_date_string,
                                          text=tmp_query_string)
            query_string.append(tmp_query_string)

        pool = multiprocessing.Pool(cpu_num)
        worker = partial(doc_query_search,
                         max_recursion_depth=max_recursion_depth,
                         mode="artlist",
                         proxy=proxy)
        print("[+] Downloading...")
        articles_list = list(
            tqdm.tqdm(pool.imap_unordered(worker, query_string),
                      total=len(query_string)))

        return pd.concat(articles_list).drop_duplicates().reset_index(
            drop=True)
    except Exception as e:
        return (e)


def timeline_search(query_filter=None,
                    max_recursion_depth: int = 100,
                    query_mode: str = "timelinevol"):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = query_filter.query_string
    timeline = doc_query_search(query_string=tmp_query_string,
                                max_recursion_depth=max_recursion_depth,
                                mode=query_mode)
    timeline["date"] = pd.to_datetime(timeline["date"],
                                      format="%Y%m%dT%H%M%SZ")
    return timeline


def geo_search(query_filter=None,
               sourcelang: str = None,
               format: str = "GeoJSON",
               timespan: int = 1, proxy:dict=None):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = " ".join(query_filter.query_string.split(" ")[:-1])
    if sourcelang != None:
        tmp_query_string = tmp_query_string + " sourcelang:{}".format(
            sourcelang)

    timeline = geo_query_search(query_string=tmp_query_string,
                                format=format,
                                timespan=timespan, proxy=proxy)

    return timeline