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
from io import StringIO
from functools import partial
from datetime import datetime

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


TV_NO_QUERY_MODES = {"stationdetails", "trendingtopics"}
DOC_TIMELINE_MODES = {
    "timelinevol",
    "timelinevolraw",
    "timelinetone",
    "timelinelang",
    "timelinesourcecountry",
}


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


def _compact_datetime(date_str: str) -> str:
    if date_str is None:
        return None
    compact = date_str.replace("-", "")
    if len(compact) != 14 or not compact.isdigit():
        raise ValueError(
            "Date must use YYYY-MM-DD-HH-MM-SS or YYYYMMDDHHMMSS format")
    return compact


def _strip_doc_api_params(query_string: str) -> str:
    query_string = re.sub(r"&startdatetime=\d+", "", query_string)
    query_string = re.sub(r"&enddatetime=\d+", "", query_string)
    query_string = re.sub(r"&maxrecords=\d+", "", query_string)
    return query_string.strip()


def doc_query_search(query_string=None,
                     max_recursion_depth: int = 100,
                     mode=None,
                     proxy: dict = None,
                     timeout: int = 30):

    if query_string == None:
        return ValueError("Query string must be provided")
    elif mode == None:
        return ValueError("Query mode must be provided")
    else:
        try:
            response = requests.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={
                    "query": query_string,
                    "mode": mode,
                    "format": "json",
                },
                proxies=proxy,
                timeout=timeout)
        except Exception as e:
            return e

        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")
        if not response.ok:
            return ValueError("GDELT DOC API request failed with status {}".format(
                response.status_code))

        try:
            payload = load_json(response.text,
                                max_recursion_depth=max_recursion_depth)
        except Exception as e:
            return e
        if not isinstance(payload, dict):
            return ValueError("GDELT DOC API response was not a JSON object")

        if mode == "artlist":
            pattern = re.compile(r'\d{14}')
            articles = payload.get("articles")
            if not isinstance(articles, list):
                return ValueError("GDELT DOC API artlist response did not include an articles list")
            output = pd.DataFrame(articles)
            # Safely extract timestamp from query_string
            timestamps = pattern.findall(query_string)
            if len(timestamps) >= 2:
                timeadded = timestamps[1]
            elif len(timestamps) == 1:
                timeadded = timestamps[0]
            else:
                # Fallback: use current time or empty string
                timeadded = ""
            output["timeadded"] = [timeadded] * len(output)
            return output
        elif mode in DOC_TIMELINE_MODES:
            timeline = payload.get("timeline")
            if not isinstance(timeline, list) or not timeline:
                return ValueError("GDELT DOC API timeline response did not include timeline data")
            data = timeline[0].get("data") if isinstance(timeline[0], dict) else None
            if not isinstance(data, list):
                return ValueError("GDELT DOC API timeline response did not include a data list")
            return pd.DataFrame(data)

        return ValueError("Unsupported query mode: {}".format(mode))


def geo_query_search(query_string:str = None,
                     format: str = "GeoJSON",
                     timespan: int = 1,
                     proxy: dict = None,
                     timeout: int = 30,
                     parse_json: bool = False):

    if query_string == None:
        return ValueError("Query string must be provided")
    if timespan <= 0:
        return ValueError("Timespan must be a positive integer")
    else:
        try:
            response = requests.get(
                "https://api.gdeltproject.org/api/v2/geo/geo",
                params={
                    "query": query_string,
                    "format": format,
                    "timespan": "{}d".format(timespan)
                },
                proxies=proxy,
                timeout=timeout)
        except Exception as e:
            return e

        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")
        if not response.ok:
            return ValueError("GDELT GEO API request failed with status {}".format(
                response.status_code))

        if parse_json:
            try:
                return response.json()
            except ValueError as e:
                return ValueError("GDELT GEO API response is not valid JSON: {}".format(e))

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

        errors = [item for item in articles_list if isinstance(item, Exception)]
        articles_list = [item for item in articles_list if isinstance(item, pd.DataFrame)]
        if not articles_list:
            if errors:
                return errors[0]
            return ValueError("No article data returned from GDELT DOC API")

        return pd.concat(articles_list).drop_duplicates().reset_index(
            drop=True)
    except Exception as e:
        return (e)


def timeline_search(query_filter=None,
                    max_recursion_depth: int = 100,
                    query_mode: str = "timelinevol",
                    proxy: dict = None):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = query_filter.query_string
    timeline = doc_query_search(query_string=tmp_query_string,
                                max_recursion_depth=max_recursion_depth,
                                mode=query_mode,
                                proxy=proxy)
    if isinstance(timeline, Exception):
        return timeline
    if "date" not in timeline.columns:
        return ValueError("GDELT DOC API timeline response did not include a date column")
    timeline["date"] = pd.to_datetime(timeline["date"],
                                      format="%Y%m%dT%H%M%SZ")
    return timeline


def geo_search(query_filter=None,
               sourcelang: str = None,
               format: str = "GeoJSON",
               timespan: int = 1,
               proxy:dict=None,
               timeout: int = 30,
               parse_json: bool = False):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = _strip_doc_api_params(query_filter.query_string)
    if sourcelang != None:
        tmp_query_string = tmp_query_string + " sourcelang:{}".format(
            sourcelang)

    timeline = geo_query_search(query_string=tmp_query_string,
                                format=format,
                                timespan=timespan,
                                proxy=proxy,
                                timeout=timeout,
                                parse_json=parse_json)

    return timeline


def tv_query_search(query_string: str = None,
                    mode: str = "timelinevol",
                    format: str = "json",
                    start_date: str = None,
                    end_date: str = None,
                    timespan: str = None,
                    datanorm: str = "perc",
                    timelinesmooth: int = 0,
                    datacomb: str = "sep",
                    last24: bool = None,
                    timezoom: bool = None,
                    maxrecords: int = None,
                    sort: str = None,
                    dateres: str = None,
                    timezoneadj: str = None,
                    proxy: dict = None,
                    timeout: int = 30,
                    parse_json: bool = True):
    """
    Query the GDELT TV 2.0 API.

    JSON responses are returned as dictionaries by default, CSV responses as
    DataFrames, and display-oriented formats such as HTML/RSS as raw text.
    """
    mode = mode.lower()
    output_format = format.lower()
    if query_string is None and mode not in TV_NO_QUERY_MODES:
        return ValueError("Query string must be provided")
    if timespan is not None and (start_date is not None or end_date is not None):
        return ValueError("Use either timespan or start/end dates, not both")
    if maxrecords is not None and maxrecords <= 0:
        return ValueError("maxrecords must be a positive integer")

    params = {
        "mode": mode,
        "format": output_format,
        "datanorm": datanorm,
        "timelinesmooth": timelinesmooth,
        "datacomb": datacomb,
    }
    if query_string is not None:
        params["query"] = query_string
    if start_date is not None:
        params["STARTDATETIME"] = _compact_datetime(start_date)
    if end_date is not None:
        params["ENDDATETIME"] = _compact_datetime(end_date)
    if timespan is not None:
        params["TIMESPAN"] = timespan
    if last24 is not None:
        params["last24"] = "yes" if last24 else "no"
    if timezoom is not None:
        params["timezoom"] = "yes" if timezoom else "no"
    if maxrecords is not None:
        params["maxrecords"] = maxrecords
    if sort is not None:
        params["sort"] = sort
    if dateres is not None:
        params["dateres"] = dateres
    if timezoneadj is not None:
        params["timezoneadj"] = timezoneadj

    response = requests.get(
        "https://api.gdeltproject.org/api/v2/tv/tv",
        params=params,
        proxies=proxy,
        timeout=timeout)

    if not response.ok:
        return ValueError("GDELT TV API request failed with status {}".format(
            response.status_code))
    if response.text == "Timespan is too short.\n":
        return ValueError("Timespan is too short.")

    if output_format == "json":
        if parse_json:
            try:
                return response.json()
            except ValueError as e:
                return ValueError("GDELT TV API response is not valid JSON: {}".format(e))
        return response.text
    if output_format == "csv":
        return pd.read_csv(StringIO(response.text))

    return response.text


def tv_search(query_filter=None,
              query_string: str = None,
              station: str = None,
              network: str = None,
              market: str = None,
              show: str = None,
              context: str = None,
              mode: str = "timelinevol",
              format: str = "json",
              start_date: str = None,
              end_date: str = None,
              timespan: str = None,
              proxy: dict = None,
              timeout: int = 30,
              **kwargs):
    """Build a TV query from a raw query string or an Art_Filter."""
    if query_filter is None and query_string is None:
        return ValueError("Filter or query string must be provided")

    if query_filter is not None:
        tmp_query_string = _strip_doc_api_params(query_filter.query_string)
        if start_date is None:
            start_date = query_filter.start_date
        if end_date is None:
            end_date = query_filter.end_date
    else:
        tmp_query_string = query_string

    operators = []
    if station:
        operators.append("station:{}".format(station))
    if network:
        operators.append("network:{}".format(network))
    if market:
        operators.append('market:"{}"'.format(market))
    if show:
        operators.append('show:"{}"'.format(show))
    if context:
        operators.append('context:"{}"'.format(context))

    if operators:
        tmp_query_string = "{} {}".format(tmp_query_string, " ".join(operators)).strip()

    return tv_query_search(query_string=tmp_query_string,
                           mode=mode,
                           format=format,
                           start_date=start_date,
                           end_date=end_date,
                           timespan=timespan,
                           proxy=proxy,
                           timeout=timeout,
                           **kwargs)
