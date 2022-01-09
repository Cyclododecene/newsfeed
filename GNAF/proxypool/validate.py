"""
author: Terence Junjie LIU
start_date: Mon 27 Dec, 2021
"""

import re
import json
import time
import requests
from fake_useragent import UserAgent
'''
1. check proxy type's accuracy
2. 
'''


def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.chrome)}
    return header


class validater(object):

    http_validate_url = "http://httpbin.org/get"
    https_validate_url = "https://httpbin.org/ip"
    geoinfo_url = "http://ip-api.com/json/"

    def __init__(self):
        """
        params: proxies:dict, proxy_type:str
        """
        self.status = None
        self.response_time = None
        self.anonymity = None

    def _validate_proxy_status(self, proxies: dict, proxy_type: str):
        """
        output: status, response_time, speed, anonymity
        """
        status = self.status
        response_time = self.response_time
        anonymity = self.anonymity

        if proxy_type == "http":
            try:
                start = time.time()
                response = requests.get(self.http_validate_url,
                                        headers=generate_header(),
                                        proxies=proxies,
                                        timeout=5)
                if response.ok:
                    status = 1
                    response_time = round(time.time() - start, 3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content[
                        "origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"

                    return status, response_time, anonymity
                else:
                    status = 0
                    return status, response_time, anonymity
            except Exception as e:
                status = 0
                return status, response_time, anonymity

        elif proxy_type == "https":
            try:
                start = time.time()
                response = requests.get(self.https_validate_url,
                                        headers=generate_header(),
                                        proxies=proxies,
                                        timeout=5)
                if response.ok:
                    status = 1
                    response_time = round(time.time() - start, 3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content[
                        "origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"

                    return status, response_time, anonymity
                else:
                    status = 0
                    return status, response_time, anonymity
            except Exception as e:
                status = 0
                return status, response_time, anonymity

        elif proxy_type == "socks5":
            try:
                start = time.time()
                response = requests.get(self.https_validate_url,
                                        headers=generate_header(),
                                        timeout=5)
                if response.ok:
                    status = 1
                    response_time = round(time.time() - start, 3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content[
                        "origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"

                    return status, response_time, anonymity
                else:
                    status = 0
                    return status, response_time, anonymity
            except Exception as e:
                status = 0
                return status, response_time, anonymity

    #TODO: validate ip location
    def _validate_ip_location(self, proxies: dict):
        try:
            proxy_pattern = re.compile("\d+\.\d+\.\d+\.\d+")
            proxy_ip = proxy_pattern.findall(proxies["https"])[0]
            url = self.geoinfo_url + "{}?fields=status,message,country,countryCode,city,isp".format(
                proxy_ip)
            urlcontent = requests.get(url,
                                      headers=generate_header(),
                                      timeout=10).json()
            country = urlcontent["country"]
            short_code = urlcontent["countryCode"]
            isp = urlcontent["isp"]
        except Exception as e:
            country, short_code, isp = "Unknown", "Unknown", "Unknown"
        return country, short_code, isp

    def _validate_google(self, proxies: dict):
        google_passed = 0
        try:
            urlcontent = requests.get("https://google.com",
                                      proxies=proxies,
                                      timeout=5)
            if urlcontent.ok:
                google_passed = 1
                return google_passed
            else:
                google_passed = 0
                return google_passed

        except Exception as e:
            return google_passed

    def validate(self, proxy):

        proxies = proxy
        FLAGS = True
        while FLAGS:

            # test proxy type: http, https, socks5

            proxy_type = "https"
            proxies_dict = {
                "http": "http://{}".format(proxies),
                "https": "https://{}".format(proxies)
            }

            http_status, http_response_time, http_anonymity = self._validate_proxy_status(
                proxies=proxies_dict, proxy_type="http")
            https_status, https_response_time, https_anonymity = self._validate_proxy_status(
                proxies=proxies_dict, proxy_type="https")

            if http_status and http_status:
                proxy_type = "http-https"
                status = http_status
                anonymity = http_anonymity
                response_time = http_response_time
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy

            elif http_status:
                proxy_type = "http"
                status = http_status
                anonymity = http_anonymity
                response_time = http_response_time
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy

            elif https_status:
                proxy_type = "https"
                status = https_status
                anonymity = https_anonymity
                response_time = https_response_time
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy

            proxy_type = "http"
            proxies_dict = {
                "http": "http://{}".format(proxies),
                "https": "https://{}".format(proxies)
            }

            http_status, http_response_time, http_anonymity = self._validate_proxy_status(
                proxies=proxies_dict, proxy_type="http")

            if http_status:
                proxy_type = "http"
                status = http_status
                anonymity = http_anonymity
                response_time = http_response_time
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy

            proxies_dict = {
                "http": "socks5://{}".format(proxies),
                "https": "socks5://{}".format(proxies)
            }
            http_status, http_response_time, http_anonymity = self._validate_proxy_status(
                proxies=proxies_dict, proxy_type="http")
            https_status, https_response_time, https_anonymity = self._validate_proxy_status(
                proxies=proxies_dict, proxy_type="https")

            if http_status and http_status:
                proxy_type = "socks5"
                status = http_status
                anonymity = https_anonymity
                response_time = http_response_time
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy
            else:
                proxy_type = "none"
                status = https_status
                response_time = -1
                anonymity = -1
                country, short_code, isp = self._validate_ip_location(
                    proxies=proxies_dict)
                google_passed = self._validate_google(proxies=proxies_dict)

                tested_proxy = {
                    "origin": proxies,
                    "type": proxy_type,
                    "status": status,
                    "response_time": response_time,
                    "anonymity": anonymity,
                    "country": country,
                    "short_code": short_code,
                    "google_passed": google_passed,
                    "isp": isp
                }
                return tested_proxy


if __name__ == "__main__":
    http_proxy = "103.149.162.194:80"  # 68.188.59.198:80
    test = validater()
    test.validate(proxy=http_proxy)
    socks5_proxy = "18.162.79.205:10001"
    test = validater()
    test.validate(proxy=socks5_proxy)
