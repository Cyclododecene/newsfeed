"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021

The original code is from gdelt-doc-api (with MIT License):
"https://github.com/alex9smith/gdelt-doc-api/blob/2a545fb1e113dbb7fd4de8fd4eab8f1f62817543/gdeltdoc/filters.py"

difference between this and the original:
1. remove the parameter: timespan, use start-date and end-date only
2. start-date and end-date, we also consider the HH:MM:SS for more precise querying
3. 
"""

from typing import Optional, List, Union, Tuple

Filter = Union[List[str], str]


def near(n: int, *args) -> str:
    if len(args) < 2:
        raise ValueError("At least two words must be provided")

    return f"near{str(n)}:" + '"' + " ".join([a for a in args]) + '" '


def repeat(n: int, keyword: str) -> str:
    if " " in keyword:
        raise ValueError("Only single words can be repeated")

    return f'repeat{str(n)}:"{keyword}" '


def multi_repeat(repeats: List[Tuple[int, str]], method: str) -> str:
    if method not in ["AND", "OR"]:
        raise ValueError(f"method must be one of AND or OR, not {method}")

    to_repeat = [repeat(n, keyword) for (n, keyword) in repeats]
    return method.join(to_repeat)


class Art_Filter:

    def __init__(
        self,
        timespan: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        num_records: int = 250,
        keyword: Optional[Filter] = None,
        domain: Optional[Filter] = None,
        domain_exact: Optional[Filter] = None,
        near: Optional[str] = None,
        repeat: Optional[str] = None,
        country: Optional[Filter] = None,
        theme: Optional[Filter] = None,
    ) -> None:

        self.query_params: List[str] = []
        self._valid_countries: List[str] = []
        self._valid_themes: List[str] = []
        self.start_date = None
        self.end_date = None

        # check date
        if not start_date and not end_date:
            raise ValueError("Must provide either start_date and end_date")
        if len(start_date) < 10 and len(end_date) < 10:
            raise ValueError(
                "Format of time: 'YYYY-MM-DD' or 'YYYY-MM-DD-HH-MM-SS'")

        if keyword:
            self.query_params.append(self._keyword_to_string(keyword))

        if domain:
            self.query_params.append(self._filter_to_string("domain", domain))

        if domain_exact:
            self.query_params.append(
                self._filter_to_string("domainis", domain_exact))

        if country:
            self.query_params.append(
                self._filter_to_string("sourcecountry", country))

        if theme:
            self.query_params.append(self._filter_to_string("theme", theme))

        if near:
            self.query_params.append(near)

        if repeat:
            self.query_params.append(repeat)

        if start_date:
            if len(start_date) > 10 and len(end_date) > 10:
                self.query_params.append(
                    f"&startdatetime={start_date.replace('-', '')}")
                self.query_params.append(
                    f"&enddatetime={end_date.replace('-', '')}")
            else:
                self.query_params.append(
                    f'&startdatetime={start_date.replace("-", "")}000000')
                self.query_params.append(
                    f'&enddatetime={end_date.replace("-", "")}000000')

            self.start_date = start_date
            self.end_date = end_date

        if num_records > 250:
            raise ValueError(
                f"num_records must 250 or less, not {num_records}")

        self.query_params.append(f"&maxrecords={str(num_records)}")

    @property
    def query_string(self) -> str:
        return "".join(self.query_params)

    @staticmethod
    def _filter_to_string(name: str, f: Filter) -> str:
        if type(f) == str:
            return f"{name}:{f} "

        else:
            # Build an OR statement
            return "(" + " OR ".join([f"{name}:{clause}"
                                      for clause in f]) + ") "

    @staticmethod
    def _keyword_to_string(keywords: Filter) -> str:
        if type(keywords) == str:
            return f'"{keywords}" '

        else:
            return ("(" + " OR ".join(
                [f'"{word}"' if " " in word else word
                 for word in keywords]) + ") ")
