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
    "nimadaili":"http://nimadaili.com/",
    "daili66":"http://www.66ip.cn/"
}

anonymity = {
    4:"elite",
    2:"anonymous",
    1:"transparent"
}

@staticmethod
def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header

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
            proxy_pattern = re.compile("\d+\.\d+\.\d+\.\d+:\d+")
            proxies_list = proxy_pattern.findall(urlcontent.text)
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
            url = proxy_source["geonode"] + "limit=200&page=1&sort_by=lastChecked&sort_type=desc&filterLastChecked=60&&country={}&protocols={}&anonymityLevel={}".format(self.country, self.proxy_type, self.anonymity)
            urlcontent = requests.get(url, headers = generate_header())
            proxies_list = urlcontent.json() #TODO: formatting
            return proxies_list
        
        if self.source_name == "jiangxianli":
            for i in range(1, 3):
                url = proxy_source["jiangxianli"] + "page=1&orderby&order_by=created_at&order_rule=DESC"
                urlcontent = requests.get(url, headers = generate_header())
                proxies_list = urlcontent.json() #TODO: formatting

        if self.source_name == "nimadaili":
            if self.proxy_type == "https":
                proxies_list = []
                for i in range(1, 20):
                    url = proxy_source["nimadali"] + "https/" + "{}/".format(i)
                    urlcontent = requests.get(url, headers = generate_header())
                    proxy_pattern = re.compile("\d+\.\d+\.\d+\.\d+:\d+")
                    proxies_list.append(proxy_pattern.findall(urlcontent.text))
                return proxies_list
            elif self.proxy_type == "http":
                proxies_list = []
                for i in range(1, 5):
                    url = proxy_source["nimadali"] + "https/" + "{}/".format(i)
                    urlcontent = requests.get(url, headers = generate_header())
                    proxy_pattern = re.compile("\d+\.\d+\.\d+\.\d+:\d+")
                    proxies_list.append(proxy_pattern.findall(urlcontent.text))
                return proxies_list
        
        if self.source_name == "daili66":
            if self.country != "HK" or self.country != "TW" or self.country != "MU":
                proxies_list = []
                for i in range(1, 5):
                    url = proxy_source["daili66"] + "{}.html".format(i)
                    urlcontent = requests.get(url, headers = generate_header())
                    proxies_table = BeautifulSoup(urlcontent.text, "lxml")
                    trs = proxies_table.find_all("tr")
                    for i in range(2, len(trs)):
                        tr = trs[i]
                        tds = tr.find_all("td")
                        ip = tds[0].text
                        port = tds[1].text
                        proxies_list.append("%s:%s" % (ip, port))

                return proxies_list

            elif self.country == "HK":
                proxies_list = []
                url = proxy_source["daili66"] + "areaindex_33/1.html"
                urlcontent = requests.get(url, headers = generate_header())
                proxies_table = BeautifulSoup(urlcontent.text, "lxml")
                trs = proxies_table.find_all("tr")
                for i in range(2, len(trs)):
                    tr = trs[i]
                    tds = tr.find_all("td")
                    ip = tds[0].text
                    port = tds[1].text
                    proxies_list.append("%s:%s" % (ip, port))

                return proxies_list    

            elif self.country == "TW":
                proxies_list = []
                url = proxy_source["daili66"] + "areaindex_27/1.html"
                urlcontent = requests.get(url, headers = generate_header())
                proxies_table = BeautifulSoup(urlcontent.text, "lxml")
                trs = proxies_table.find_all("tr")
                for i in range(2, len(trs)):
                    tr = trs[i]
                    tds = tr.find_all("td")
                    ip = tds[0].text
                    port = tds[1].text
                    proxies_list.append("%s:%s" % (ip, port))

                return proxies_list          




"""
#example
"""