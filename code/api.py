from datetime import datetime, timedelta
from dateutil import rrule
import pandas as pd
import requests
import json

def get_delta(date_1, date_2):
    delta = date_2 - date_1
    return delta

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
        return load_json(json_message=new_message, max_recursion_depth=max_recursion_depth,
                         recursion_depth=recursion_depth+1)
    return result

def article_search(filter = None, max_recursion_depth: int = 100):
    if filter == None:
        return ValueError("Filter must be provided")
    else:
        query_string = filter.query_string
        response = requests.get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode=artlist&format=json")
        if response.text == "Timespan is too short.\n":
            return ValueError("Timespan is too short.")
        else:
            return pd.DataFrame(load_json(response.text, max_recursion_depth = max_recursion_depth)["articles"])


def article_search_large_time_range(filter = None, max_recursion_depth: int = 100):
    articles_list = []
    if filter == None:
        return ValueError("Filter must be provided")
    else:
        new_end_date = datetime.strptime(filter.start_date, "%Y-%m-%d-%H-%M-%S") + timedelta(hours=12)
        tmp_f = filter
        while new_end_date <= datetime.strptime(filter.end_date, "%Y-%m-%d-%H-%M-%S"):
            tmp_f.query_string = tmp_f.query_string.replace(tmp_f.end_date.replace("-", ""), datetime.strftime(new_end_date, "%Y-%m-%d-%H-%M-%S").replace("-", "")) #TODO fix this
            tmp_articles = article_search(tmp_f, max_recursion_depth=100)
            articles_list.append(tmp_articles)
            tmp_f.query_string = tmp_f.query_string.replace(tmp_f.start_date.replace("-", ""), datetime.strftime(new_end_date, "%Y-%m-%d-%H-%M-%S").replace("-", ""))
            new_end_date = new_end_date + timedelta(hours=12)
                return pd.concat(articles_list)

f = Filter(
    keyword = "climate change",
    start_date = "2021-05-10-12-01-00",
    end_date = "2021-05-12-12-02-00"
)