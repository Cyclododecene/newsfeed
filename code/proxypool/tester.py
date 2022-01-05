import os 
import requests
from fake_useragent import UserAgent

def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.chrome)}
    return header

class tester(object):
    
    http_test_url = 'http://httpbin.org/ip'
    https_test_url = 'https://httpbin.org/ip'
    validate_url = 'https://www.myip.com/'

    def __init__(self, proxies:dict=None, proxy_type:str="https"):
        self.proxies = proxies
        self.type = proxy_type
        self.http_test_url = ""
        if proxies == None or proxy_type == None:
            return ValueError("proxies and proxy_type cannot be empty")
    
    def validate(self):
        proxies = self.proxies
        proxy_type = self.type
        proxies_status = []
        for i in range(0,len(proxies)):
            if proxy_type == "http":
                proxy_dict = "http://" + proxies[i]
                os.environ["http_proxy"] = proxy_dict
                response = requests.get(self.http_test_url, headers = generate_header(), timeout = 2)
                proxies_status.append(response.status_code)
                return proxies_status
            else:
                proxy_dict = "https://" + proxies[i]
                os.environ["https_proxy"] = proxy_dict
                response = requests.get(self.https_test_url, headers = generate_header(), timeout = 2)
                proxies_status.append(response.status_code)
                return proxies_status
