import os 
import json
import time
from tkinter.messagebox import NO
from weakref import proxy
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

    def __init__(self, proxies:str, proxy_type:str="https"):
        """
        params: proxies:dict, proxy_type:str
        """
        self.http_validate_url = "http://httpbin.org/get"
        self.https_validate_url = "https://httpbin.org/ip"
        self.validate_url = "https://api.myip.com/"

        self.proxies = proxies
        self.type = proxy_type
        
        self.status = None
        self.response_time = None
        self.anonymity = None
        
        if proxies == None or proxy_type == None:
            return ValueError("proxies and proxy_type cannot be empty")

    def _validate_proxy_status(self, proxies:dict, proxy_type:str):
        """
        output: status, response_time, speed, anonymity
        """
        status = self.status
        response_time = self.response_time
        anonymity = self.anonymity

        if proxy_type == "http":
            try:
                start = time.time()
                response = requests.get(self.http_validate_url, headers = generate_header(), proxies = proxies, timeout = 10)
                if response.ok:
                    status = True
                    response_time = round(time.time() - start,3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content["origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"
                        
                    return status, response_time, anonymity
                else:
                    status = False
                    return status, response_time, anonymity
            except Exception as e:
                status = False
                return status, response_time, anonymity

        elif proxy_type == "https":
            try:
                start = time.time()
                response = requests.get(self.https_validate_url, headers = generate_header(), proxies = proxies, timeout = 10)
                if response.ok:
                    status = True
                    response_time = round(time.time() - start,3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content["origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"
                        
                    return status, response_time, anonymity
                else:
                    status = False
                    return status, response_time, anonymity
            except Exception as e:
                status = False
                return status, response_time, anonymity

        elif proxy_type == "socks5":
            try:
                start = time.time()
                response = requests.get(self.https_validate_url, headers = generate_header(), timeout = 10)
                if response.ok:
                    status = True
                    response_time = round(time.time() - start,3)
                    resp_content = json.loads(response.text)
                    header, ip = resp_content["headers"], resp_content["origin"]
                    proxy_connection = header.get("Proxy-Connection", None)
                    if "," in ip:
                        anonymity = "transparent"
                    elif proxy_connection:
                        anonymity = "anonymous"
                    else:
                        anonymity = "elite"
                        
                    return status, response_time, anonymity
                else:
                    status = False
                    return status, response_time, anonymity
            except Exception as e:
                status = False
                return status, response_time, anonymity
                    
    def validate(self):
        proxies = self.proxies
        proxy_type = self.type
        if proxy_type != "socks5":
            proxies_dict = {"http:":"http://{}".format(proxies), "https:":"https://{}".format(proxies)}

            http_status, http_response_time, http_anonymity = self._validate_proxy_status(proxies = proxies_dict, proxy_type = "http")
            https_status, https_response_time, https_anonymity = self._validate_proxy_status(proxies = proxies_dict, proxy_type = "https")

            if http_status and http_status:
                proxy_type = "http & https"
                status = http_status
                anonymity = https_anonymity
                response_time = https_response_time
            elif http_status:
                proxy_type = "http"
                status = http_status
                anonymity = http_anonymity
                response_time = http_response_time
            elif https_status:
                proxy_type = "https"
                status = https_status
                anonymity = https_anonymity
                response_time = https_response_time
            else:
                proxy_type = proxy_type
                status = False
                response_time = -1
                anonymity = -1
            
            tested_proxy = {"origin":self.proxies, "type":proxy_type, "status":status, 
                            "response_time":response_time, "anonymity":anonymity}
            return tested_proxy
        
        else:
            proxies_dict = {"http:":"socks5://{}".format(proxies), "https:":"socks5://{}".format(proxies)}

            http_status, http_response_time, http_anonymity = self._validate_proxy_status(proxies = proxies_dict, proxy_type = "http")
            https_status, https_response_time, https_anonymity = self._validate_proxy_status(proxies = proxies_dict, proxy_type = "https")

            if http_status and http_status:
                proxy_type = "socks5"
                status = http_status
                anonymity = https_anonymity
                response_time = https_response_time
            else:
                status = https_status
                response_time = -1
                anonymity = -1
            
            tested_proxy = {"origin":self.proxies, "type":proxy_type, "status":status, 
                            "response_time":response_time, "anonymity":anonymity}
            return tested_proxy



if __name__ == "__main__":
    http_proxy = "103.149.162.194:80"
    test = validater(proxies = http_proxy, proxy_type="https")
    test.validate()
    socks5_proxy = "18.162.79.205:10001"
    test = validater(proxies = socks5_proxy, proxy_type="socks5")
    test.validate()
