import requests
from newspaper import Config
from newspaper import Article
from fake_useragent import UserAgent


def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header


# original code for newspaper3k and internet archive: https://github.com/johnbumgarner/newspaper3_usage_overview
def _get(url):
    config = Config()
    config.browser_user_agent = generate_header()["User-Agent"]
    config.request_timeout = 50
    config.number_threads = 60
    config.thread_timeout_seconds = 10
    article = Article(url, config=config)
    article.download()
    article.parse()
    article_info = article
    return article_info


## directly get news from url
def download_direct(url):
    try:
        return _get(url)
    except Exception as e:
        print(":( Error in downloading {} \n Error: \n{}".format(url, e))
        pass  # return None


## get news from url from internet archive
def download_arxiv(url):  # arxiv means internet archive
    url = url.split("//")[1]
    response = requests.get(
        "https://archive.org/wayback/available?url={}".format(url),
        timeout=20,
        headers=generate_header(),
        stream=True)
    response_json = response.json()
    if response_json["archived_snapshots"] == {}:
        print(":( No archive found")
        pass  # return None
    else:
        archive_url = response_json["archived_snapshots"]["closest"]["url"]
        archive_url = archive_url.replace("http://", "https://")
        try:
            return _get(archive_url)
        except Exception as e:
            print(":( Error in downloading {} \n Error: \n{}".format(
                archive_url, e))
            pass  # return None


def check_url(url):
    print("[+] Checking if page exists...")
    try:
        # Requesting for only the HTTP header without downloading the page
        # If the page doesn't exist (404), it's a waste of resources to try scraping.
        response = requests.head(url, timeout=10, headers=generate_header())
        if "not found" in response.text:
            return 404
        else:
            return int(response.status_code)
    except:
        print(":( Connection related Error/Timeout, skipping...")
        return int(404)


def download(url):
    try:
        if check_url(url=url) != 404:
            return download_direct(url)
        else:
            return download_arxiv(url)
    except Exception as e:
        print(e)
        pass  # return None


if __name__ == "__main__":
    art = download(
        url=
        "https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html"
    )
    print("full text: \n {}".format(art.text))
