import re
import base64
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

proxy_source = {
    "proxydb":"http://proxydb.net/?&availability=90",
    "geonode":"https://proxylist.geonode.com/api/proxy-list?",
    "ip3366":"http://www.ip3366.net/free/?",
    "jiangxianli":"https://ip.jiangxianli.com/api/proxy_ips?",
    #"nimadaili":"https://"
}

def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header

anonymity = {
    4:"elite",
    2:"anonymous",
    1:"transparent"
}

class proxies(object):
    def __init__(self, source_name:str="proxydb", country:str=None, proxy_type:str=None, anonymityLevel:str=None,
    response:int=-1, speed:int=-1):

        if country == None:
            return ValueError("country need specified")
        elif proxy_type == None:
            return ValueError("proxy type need specified")
        else:
            self.source_name = source_name
            self.proxy_type = proxy_type
            self.country = country
            self.anonymity = anonymityLevel
    
    def collect(self):
        if self.source_name == "proxydb":
            if self.anonymity == "transparent":
                self.anonymity = int([i for i, j in anonymity.items() if j == "transparent"][0])
            elif self.anonymity == "anonymous":
                self.anonymity = int([i for i, j in anonymity.items() if j == "anonymous"][0])
            else:
                self.anonymity = int([i for i, j in anonymity.items() if j == "elite"][0])

            url = proxy_source["proxydb"] + "&protocol={}&country={}&anonlvl={}".format(self.proxy_type, self.country, self.anonymity)
            urlcontent = requests.get(url, headers = generate_header())
            proxy_pattern = re.complie("\d+\.\d+\.\d+\.\d+:\d+")
            proxies_list = re.findall(proxy_pattern, urlcontent.text)
            return proxies_list
        
        if self.source_name == "ip3366":
            url = proxy_source["ip3366"]
            proxy_pattern = re.compile('<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>')
            proxies_list = []
            for i in range(1, 4):
                for j in range(1, 5):
                    url=url + "stype={}&page={}".format(i, j)
                    urlcontent = requests.get(url, headers = generate_header())
                    proxy_table = proxy_pattern.findall(urlcontent.text)
                    for k in range(0, len(proxy_table)):
                        proxies_list.append(":".join(proxy_table[k]))
            
            return proxies_list

        if self.source_name == "geonode":
            # https://proxylist.geonode.com/api/proxy-list?limit=50&page=1&sort_by=lastChecked&sort_type=desc&filterLastChecked=60&filterUpTime=100&protocols=https
            # https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc
            url = proxy_source["geonode"] + "limit=200&page=1&sort_by=lastChecked&sort_type=desc&filterLastChecked=60&&country={}&protocols={}&anonymityLevel={}".format(self.country, self.proxy_type, self.anonymity)
            urlcontent = requests.get(url, headers = generate_header())
            proxies_list = urlcontent.json() #TODO: formatting
            return proxies_list
        
        if self.source_name == "jiangxianli":
            for i in range(1, 3):
                url = proxy_source["jiangxianli"] + "page=1&orderby&order_by=created_at&order_rule=DESC"
                urlcontent = requests.get(url, headers = generate_header())
                proxies_list = urlcontent.json() #TODO: formatting
        # TODO: need add  https://github.com/Lucareful/IPProxyPool
        if self.source_name == "nimadaili":
            for i in range(1, 10):
                url = proxy_source["nimadali"] + ""

        




"""
#example
from getproxy import * 

proxies_CN = proxies(source_name="proxydb", country="CN", proxy_type="socks5")
proxies_CN_list = proxies_CN.collect()
proxies_HK = proxies(source_name="proxydb", country="HK", proxy_type="socks5")
proxies_HK_list = proxies_HK.collect()
proxies_US = proxies(source_name="proxydb", country="US", proxy_type="socks5")
proxies_US_list = proxies_US.collect()
"""