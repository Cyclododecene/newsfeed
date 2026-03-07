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
    result = r.sub(newstring, text)
    
    # Check if substitution actually happened
    if result == text:
        raise ValueError(f"text_regex failed to find pattern '{str_1}' and '{str_2}' in query string. Text: {text[:200]}...")
    
    return result


def replace_datetime_params(text, start_datetime, end_datetime):
    """
    Replace startdatetime and enddatetime parameters in query string.
    
    Args:
        text: Original query string
        start_datetime: New start datetime (e.g., "20211231000000")
        end_datetime: New end datetime (e.g., "20211231010000")
    
    Returns:
        Updated query string
    """
    # Replace startdatetime
    text = re.sub(
        r'&startdatetime=\d{14}',
        f'&startdatetime={start_datetime}',
        text
    )
    
    # Replace enddatetime
    text = re.sub(
        r'&enddatetime=\d{14}',
        f'&enddatetime={end_datetime}',
        text
    )
    
    return text


def load_json(json_message,
              max_recursion_depth: int = 100,
              recursion_depth: int = 0):
    """
    Parse JSON with error handling and recovery.
    
    This function attempts to parse JSON and can handle some malformed JSON
    by cleaning common issues (control characters, etc.).
    """
    try:
        # Ensure we're working with a string
        if isinstance(json_message, bytes):
            json_message = json_message.decode("utf-8")

        # Empty response — nothing to parse
        if not json_message or not json_message.strip():
            raise ValueError("Empty response from API")

        # Try to parse JSON directly first
        result = json.loads(json_message)
        return result
        
    except json.JSONDecodeError as e:
        if recursion_depth >= max_recursion_depth:
            raise ValueError(
                f"Failed to parse JSON after {max_recursion_depth} attempts. "
                f"Last error: {e.msg} at line {e.lineno} column {e.colno}")
        
        # Get position information
        error_pos = getattr(e, 'pos', None)
        error_msg = str(e.msg)
        
        # Check if position is valid
        if error_pos is None or error_pos < 0 or error_pos >= len(json_message):
            # Position is invalid, raise error
            raise ValueError(
                f"JSON parsing error at line {e.lineno}, column {e.colno}: {error_msg}\n"
                f"Position {error_pos} is invalid for string of length {len(json_message)}"
            )
        
        # Try to fix common JSON issues
        
        # Strategy 1: Try replacing the problematic character with a space
        if recursion_depth % 3 == 0:
            json_list = list(json_message)
            # Only replace if position is valid
            if error_pos < len(json_list):
                problematic_char = json_list[error_pos]
                # Replace with space for common problematic characters
                if ord(problematic_char) < 32:  # Control character
                    json_list[error_pos] = ' '
                elif problematic_char in ['\n', '\r', '\t']:
                    json_list[error_pos] = ' '
                new_message = ''.join(json_list)
                return load_json(new_message, max_recursion_depth, recursion_depth + 1)
        
        # Strategy 2: Try removing the problematic character
        if recursion_depth % 3 == 1:
            new_message = json_message[:error_pos] + json_message[error_pos+1:]
            return load_json(new_message, max_recursion_depth, recursion_depth + 1)
        
        # Strategy 3: Try truncating at error position
        if recursion_depth % 3 == 2:
            # Try to find the last valid JSON structure before error
            # Look for closing braces/brackets
            truncated = json_message[:error_pos].rstrip()
            return load_json(truncated, max_recursion_depth, recursion_depth + 1)
        
    except Exception as e:
        raise ValueError(f"Unexpected error while parsing JSON: {type(e).__name__}: {e}")


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
                pattern = re.compile(r'\d{14}')
                
                # Parse JSON response
                try:
                    json_data = load_json(
                        response.text,
                        max_recursion_depth=max_recursion_depth)
                except Exception as e:
                    return ValueError(f"Failed to parse API response: {e}")
                
                # Check if 'articles' key exists
                if "articles" not in json_data:
                    return ValueError(f"API response missing 'articles' key. Response: {response.text[:200]}")
                
                # Create DataFrame from articles
                try:
                    output = pd.DataFrame(json_data["articles"])
                except Exception as e:
                    return ValueError(f"Failed to create DataFrame: {e}")
                
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
            elif mode in ["timelinevol", "timelinevolraw", "timelinetone", "timelinelang", "timelinesourcecountry"]:
                try:
                    json_data = load_json(response.text, max_recursion_depth=max_recursion_depth)
                    return pd.DataFrame(json_data["timeline"][0]["data"])
                except Exception as e:
                    return ValueError(f"Failed to parse timeline response: {e}")


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
        
        # Validate date_range
        if len(date_range) < 2:
            return ValueError(f"Date range too short. Only {len(date_range)} time point(s) generated with time_range={time_range}min. Try increasing time_range parameter or extending start/end dates.")
        
        tmp_query_string = query_filter.query_string
        query_string = []
        for i in range(0, len(date_range) - 1):
            tmp_start_date, tmp_end_date = date_range[i], date_range[i + 1]
            
            # Use the new replace_datetime_params function for cleaner replacement
            try:
                tmp_query_string = replace_datetime_params(
                    text=tmp_query_string,
                    start_datetime=tmp_start_date,
                    end_datetime=tmp_end_date
                )
            except Exception as e:
                return ValueError(f"Failed to build query string: {e}")
            
            query_string.append(tmp_query_string)

        pool = multiprocessing.Pool(cpu_num)
        worker = partial(doc_query_search,
                         max_recursion_depth=max_recursion_depth,
                         mode="artlist",
                         proxy=proxy)
        print("[+] Downloading...")
        try:
            articles_list = list(
                tqdm.tqdm(pool.imap_unordered(worker, query_string),
                          total=len(query_string)))
        finally:
            pool.close()
            pool.terminate()
            pool.join()

        # Debug: Check what we got
        errors = [item for item in articles_list if isinstance(item, Exception)]
        if errors:
            first_error = errors[0]
            error_detail = str(first_error) if first_error else "Unknown error"
            print(f"[!] Warning: {len(errors)} queries failed. First error type: {type(first_error).__name__}, Error: {error_detail}")

        # Filter out errors and keep only DataFrames
        valid_articles = [df for df in articles_list if isinstance(df, pd.DataFrame) and len(df) > 0]
        
        if not valid_articles:
            error_msg = "No valid articles found. "
            if errors:
                first_error = errors[0]
                error_detail = str(first_error) if first_error else "Unknown error"
                error_msg += f"First error ({type(first_error).__name__}): {error_detail}"
            else:
                error_msg += "All queries returned empty results."
            return ValueError(error_msg)
        
        return pd.concat(valid_articles).drop_duplicates().reset_index(drop=True)
    except Exception as e:
        # Return exception directly, not wrapped in a tuple
        return e


def timeline_search(query_filter=None,
                    max_recursion_depth: int = 100,
                    query_mode: str = "timelinevol"):

    if query_filter == None:
        return ValueError("Filter must be provided")

    tmp_query_string = query_filter.query_string
    timeline = doc_query_search(query_string=tmp_query_string,
                                max_recursion_depth=max_recursion_depth,
                                mode=query_mode)
    if isinstance(timeline, Exception):
        return timeline
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