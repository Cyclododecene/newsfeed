"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021

The original code is from gdelt-doc-api:
"https://github.com/alex9smith/gdelt-doc-api/blob/main/gdeltdoc/api_client.py"
by default, the system only provide at max 250 results
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

from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import gzip
import re
import io

def text_regex(str_1, str_2, newstring, text):
    reg = "%s(.*?)%s" % (str_1,str_2)
    r = re.compile(reg,re.DOTALL)
    return (r.sub(newstring, text))

def load_json(json_message, max_recursion_depth: int = 100, recursion_depth: int = 0):
    try:
        result = json.loads(json_message)
    except Exception as e:
        
        if recursion_depth >= max_recursion_depth:
            raise ValueError("Max Recursion depth is reached. JSON can´t be parsed!")
        # Find the offending character index:
        idx_to_replace = int(e.pos)
        
        # Remove the offending character:
        if isinstance(json_message, bytes):
            json_message.decode("utf-8")
        json_message = list(json_message)
        json_message[idx_to_replace] = ' '
        new_message = ''.join(str(m) for m in json_message)
        return load_json(json_message=new_message, max_recursion_depth=max_recursion_depth, recursion_depth=recursion_depth + 1)
    return result

def doc_query_search(query_string = None, max_recursion_depth: int = 100, mode = None, proxy:dict=None):

    if query_string == None:
        return ValueError("Query string must be provided")
    elif mode == None:
        return ValueError("Query mode must be provided")
    else:
        query_string = query_string
        response = requests.get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode={mode}&format=json", proxies=proxy)

        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")

        else:
            if mode == "artlist":
                return pd.DataFrame(load_json(response.text, max_recursion_depth = max_recursion_depth)["articles"])
            elif mode == "timelinevol" or "timelinevolraw" or "timelinetone" or "timelinetone" or "timelinelang" or "timelinesourcecountry":
                return pd.DataFrame(load_json(response.text)["timeline"][0]["data"])

def article_search(query_filter = None, max_recursion_depth:int = 100, time_range:int = 60, proxy:dict=None):
    articles_list = []

    if query_filter == None:
        return ValueError("Filter must be provided")
    
    if time_range < 30:
        return ValueError("time range has to larger than 30")

    else:
        new_end_date = datetime.strptime(query_filter.start_date, "%Y-%m-%d-%H-%M-%S") + timedelta(minutes = time_range) # tmp_end_date
        tmp_query_string = query_filter.query_string
        try:
        # tmp_end_date <= real end date
            while new_end_date <= datetime.strptime(query_filter.end_date, "%Y-%m-%d-%H-%M-%S"): 
                # subsitute the query parameters (enddatetime)
                tmp_end_date_string = "&enddatetime=" + datetime.strftime(new_end_date, "%Y-%m-%d-%H-%M-%S").replace("-", "") + "&maxrecords"
                tmp_query_string = text_regex(str_1="&enddatetime=", str_2 = "&maxrecords", newstring = tmp_end_date_string, text = tmp_query_string)
                print(tmp_query_string)
                tmp_articles = doc_query_search(query_string = tmp_query_string, max_recursion_depth = max_recursion_depth, mode = "artlist", proxy=proxy)
                articles_list.append(tmp_articles)
                # subsitute the query parameters (startdatetime)
                tmp_start_date_string = "&startdatetime=" + datetime.strftime(new_end_date, "%Y-%m-%d-%H-%M-%S").replace("-", "") + "&enddatetime="
                tmp_query_string = text_regex(str_1="&startdatetime", str_2 = "&enddatetime", newstring = tmp_start_date_string, text = tmp_query_string)
                new_end_date = new_end_date + timedelta(minutes = time_range)
                
            return pd.concat(articles_list).drop_duplicates().reset_index(drop = True)
        except Exception as e:
            return (e)

def timeline_search(query_filter = None, max_recursion_depth:int = 100, query_mode: str = "timelinevol"):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = query_filter.query_string
    timeline = doc_query_search(query_string = tmp_query_string, max_recursion_depth = max_recursion_depth, mode = query_mode)
    timeline["date"] = pd.to_datetime(timeline["date"], format="%Y%m%dT%H%M%SZ")
    return timeline
