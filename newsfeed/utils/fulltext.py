import requests
from newspaper import Config
from newspaper import Article
from fake_useragent import UserAgent
from typing import List, Tuple, Optional

from urllib.parse import urlparse

from newsfeed.utils.async_downloader import run_async_fulltext_download


def generate_header():
    ua = UserAgent()
    header = {"User-Agent": str(ua.random)}
    return header


# original code for newspaper4k and internet archive: https://newspaper4k.readthedocs.io/en/latest/
def _get(url):
    config = Config()
    config.browser_user_agent = generate_header()["User-Agent"]
    config.request_timeout = 240
    config.number_threads = 60
    config.thread_timeout_seconds = 240
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
    response = requests.get(
        "https://archive.org/wayback/available?url={}".format(url),
        timeout=120,
        headers=generate_header(),
        stream=True)
    response_json = response.json()
    if response_json["archived_snapshots"] == {}:
        print(":( No archive found")
        pass  # return None
    else:
        archive_url = response_json["archived_snapshots"]["closest"]["url"]
        try:
            return _get(archive_url)
        except Exception as e:
            print(":( Error in downloading {} \n Error: \n{}".format(
                archive_url, e))
            pass  # return None

def reconstruct_url(url):
    # check protocol
    url_root = urlparse(url).scheme + "://" + urlparse(url).netloc
    response = requests.get(url_root, headers = generate_header())
    url_root = response.url
    url = url_root + urlparse(url).path + urlparse(url).params + urlparse(url).query + urlparse(url).fragment
    return url

def check_url(url):
    print("[+] Checking if page exists...")

    try:
        # Requesting for only the HTTP header without downloading the page
        # If the page doesn't exist, it's a waste of resources to try scraping (directly).
        response = requests.get(url, timeout=240, headers=generate_header())
        if response.status_code != 200:
            return int(404)
    except:
        print(":( Connection related Error/Timeout, skipping...")
        return int(404)


def download(url):
    url = reconstruct_url(url)
    try:
        if check_url(url=url) != 404:
            return download_direct(url)
        else:
            return download_arxiv(url)
    except Exception as e:
        print(e)
        pass  # return None


def download_batch(urls: List[str], 
                  use_async: bool = False, 
                  max_concurrent: int = 20,
                  show_progress: bool = True) -> Tuple[List, List]:
    """
    Download multiple articles in batch.
    
    Args:
        urls: List of URLs to download
        use_async: Use async download (faster for large batches)
        max_concurrent: Maximum concurrent downloads (only if use_async=True)
        show_progress: Show progress bar
    
    Returns:
        Tuple of (successful_articles, errors)
    """
    if use_async:
        print("[+] Using async download for batch...")
        articles, errors = run_async_fulltext_download(
            urls=urls,
            max_concurrent=max_concurrent,
            show_progress=show_progress
        )
        return articles, errors
    else:
        # Synchronous download
        print("[+] Using synchronous download for batch...")
        articles = []
        errors = []
        
        from tqdm import tqdm
        for url in tqdm(urls, desc="Downloading articles"):
            try:
                article = download(url)
                if article is not None:
                    articles.append(article)
                else:
                    errors.append({"url": url, "error": "Download failed"})
            except Exception as e:
                errors.append({"url": url, "error": str(e)})
        
        return articles, errors


def download_from_dataframe(df, 
                           url_column: str = 'SOURCEURL',
                           use_async: bool = False,
                           max_concurrent: int = 20,
                           show_progress: bool = True) -> Tuple[List, List]:
    """
    Download articles from a DataFrame containing URLs.
    
    Args:
        df: DataFrame containing URLs
        url_column: Name of column containing URLs
        use_async: Use async download (faster for large batches)
        max_concurrent: Maximum concurrent downloads (only if use_async=True)
        show_progress: Show progress bar
    
    Returns:
        Tuple of (successful_articles, errors)
    """
    import pandas as pd
    urls = df[url_column].dropna().unique().tolist()
    print(f"[+] Found {len(urls)} unique URLs to download")
    
    return download_batch(
        urls=urls,
        use_async=use_async,
        max_concurrent=max_concurrent,
        show_progress=show_progress
    )


if __name__ == "__main__":
    # Single article download
    art = download(url="https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html")
    if art:
        print("full text: \n {}".format(art.text))
    
    # Example of batch download (commented out)
    # urls = [
    #     "https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html",
    #     "https://example.com/article2"
    # ]
    # articles, errors = download_batch(urls, use_async=True)
    # print(f"Successfully downloaded {len(articles)} articles")
    # print(f"Failed to download {len(errors)} articles")