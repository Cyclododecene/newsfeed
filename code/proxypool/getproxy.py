import re
import base64
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

proxy_source = {
    "proxydb":"http://proxydb.net/",
    "geonode":"https://geonode.com/free-proxy-list/?",
    "pubproxy":"https://pubproxy.com/api/proxy?",
    "sslproxies":"https://www.sslproxies.org/",
}

def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header

class proxies(object):
    def __init__(self, source_name:str="proxydb", country:str=None, proxy_type:str="https",
    response:int=-1, speed:int=-1):
        self.source_name = source_name
        self.proxy_type = proxy_type
        self.country = country
    
    def collect(self):
        page = 1
        proxies = []
        if self.source_name == "proxydb":
            url = proxy_source["proxydb"] + "protocol={}&country={}".format(self.proxy_type, self.country)
            urlcontent = requests.get(url, headers = generate_header())
            proxies_table = BeautifulSoup(urlcontent.text, "html.parser").find("table").get_text()
            proxy_pattern = "\d+\.\d+\.\d+\.\d+:\d+"
            proxies_list = re.findall(proxy_pattern, proxies_table)
            return proxies_list
        
        if self.source_name == "pubproxy":
            url = proxy_source["pubproxy"] + "country={}&type={}&limit=5&format=json&last_check=60&speed=1"
            urlcontent = requests.get(url, headers = generate_header())

"""
#example
proxies_CN = proxies(source_name="proxydb", country="CN", proxy_type="https")
proxies_CN_list = proxies_CN.collect()
proxies_HK = proxies(source_name="proxydb", country="HK", proxy_type="https")
proxies_HK_list = proxies_HK.collect()
proxies_US = proxies(source_name="proxydb", country="US", proxy_type="https")
proxies_US_list = proxies_US.collect()
"""