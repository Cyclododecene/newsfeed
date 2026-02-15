"""
Async downloader for GDELT data files
author: Terence Junjie LIU
date: 2026
"""
import asyncio
import aiohttp
import aiofiles
import pandas as pd
import io
from typing import List, Optional, Tuple
from fake_useragent import UserAgent
from tqdm.asyncio import tqdm
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class AsyncDownloader:
    """Asynchronous downloader for multiple files"""
    
    def __init__(self, max_concurrent: int = 20, timeout: int = 30, 
                 proxy: Optional[dict] = None, retry_times: int = 3):
        """
        Initialize async downloader
        
        Args:
            max_concurrent: Maximum number of concurrent downloads
            timeout: Request timeout in seconds
            proxy: Proxy configuration
            retry_times: Number of retry attempts
        """
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.proxy = proxy
        self.retry_times = retry_times
        self.ua = UserAgent()
    
    def _generate_header(self) -> dict:
        """Generate random user agent header"""
        return {"User-Agent": str(self.ua.random)}
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _download_single_file(
        self, 
        session: aiohttp.ClientSession, 
        url: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
        """
        Download a single file asynchronously
        
        Args:
            session: aiohttp session
            url: File URL
            semaphore: Semaphore for limiting concurrency
            
        Returns:
            Tuple of (url, dataframe, error_message)
        """
        async with semaphore:
            try:
                async with session.get(
                    url, 
                    headers=self._generate_header(),
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    if response.status == 404:
                        return url, None, f"404 Not Found: {url}"
                    
                    if response.status != 200:
                        return url, None, f"HTTP {response.status}: {url}"
                    
                    # Read content
                    content = await response.read()
                    
                    # Parse CSV from zip file
                    df = pd.read_csv(
                        io.BytesIO(content),
                        compression="zip",
                        sep="\t",
                        header=None,
                        on_bad_lines='skip',
                        low_memory=False
                    )
                    
                    return url, df, None
                    
            except asyncio.TimeoutError:
                return url, None, f"Timeout: {url}"
            except Exception as e:
                return url, None, f"Error: {str(e)} - {url}"
    
    async def download_files(
        self, 
        base_url: str, 
        file_names: List[str],
        show_progress: bool = True
    ) -> Tuple[List[pd.DataFrame], List[Tuple[str, str]]]:
        """
        Download multiple files asynchronously
        
        Args:
            base_url: Base URL for files
            file_names: List of file names to download
            show_progress: Whether to show progress bar
            
        Returns:
            Tuple of (list of dataframes, list of (url, error) tuples)
        """
        full_urls = [base_url + fname for fname in file_names]
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for url in full_urls:
                task = self._download_single_file(session, url, semaphore)
                tasks.append(task)
            
            # Execute all tasks with progress bar
            if show_progress:
                results = await tqdm.gather(*tasks, total=len(tasks), desc="Downloading files")
            else:
                results = await asyncio.gather(*tasks)
            
            # Separate successful and failed downloads
            dataframes = []
            errors = []
            
            for url, df, error in results:
                if df is not None:
                    dataframes.append(df)
                if error is not None:
                    errors.append((url, error))
            
            return dataframes, errors
    
    async def download_fulltext_async(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        url: str
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Download full text from a single URL
        
        Args:
            session: aiohttp session
            semaphore: Semaphore for limiting concurrency
            url: Article URL
            
        Returns:
            Tuple of (url, full_text, error_message)
        """
        async with semaphore:
            try:
                # First, check if URL exists and reconstruct if needed
                from urllib.parse import urlparse
                
                # Reconstruct URL (similar to fulltext.py's reconstruct_url)
                url_root = urlparse(url).scheme + "://" + urlparse(url).netloc
                try:
                    async with session.get(
                        url_root,
                        headers=self._generate_header(),
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as root_response:
                        if root_response.status == 200:
                            final_url = root_response.url
                            url = final_url + urlparse(url).path + urlparse(url).params + urlparse(url).query + urlparse(url).fragment
                except:
                    pass  # Use original URL if reconstruction fails
                
                # Try to download the article
                async with session.get(
                    url,
                    headers=self._generate_header(),
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        return url, None, f"HTTP {response.status}"
                    
                    html_content = await response.text()
                    
                    # Extract text using newspaper3k (needs to run in thread pool)
                    from newspaper import Article
                    import concurrent.futures
                    
                    def parse_article():
                        article = Article(url)
                        article.set_html(html_content)
                        article.parse()
                        return article.text
                    
                    # Run CPU-bound parsing in thread pool
                    loop = asyncio.get_event_loop()
                    text = await loop.run_in_executor(None, parse_article)
                    
                    return url, text, None
                    
            except asyncio.TimeoutError:
                return url, None, "Timeout"
            except Exception as e:
                return url, None, f"Error: {str(e)}"
    
    async def download_fulltexts(
        self,
        urls: List[str],
        show_progress: bool = True
    ) -> Tuple[dict, List[Tuple[str, str]]]:
        """
        Download full text from multiple URLs
        
        Args:
            urls: List of article URLs
            show_progress: Whether to show progress bar
            
        Returns:
            Tuple of (dict of {url: full_text}, list of (url, error) tuples)
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for url in urls:
                task = self.download_fulltext_async(session, semaphore, url)
                tasks.append(task)
            
            # Execute all tasks
            if show_progress:
                results = await tqdm.gather(*tasks, total=len(tasks), desc="Downloading full texts")
            else:
                results = await asyncio.gather(*tasks)
            
            # Separate successful and failed downloads
            fulltexts = {}
            errors = []
            
            for url, text, error in results:
                if text is not None:
                    fulltexts[url] = text
                if error is not None:
                    errors.append((url, error))
            
            return fulltexts, errors


def run_async_download(
    base_url: str,
    file_names: List[str],
    max_concurrent: int = 20,
    timeout: int = 30,
    proxy: Optional[dict] = None,
    show_progress: bool = True
) -> Tuple[List[pd.DataFrame], List[Tuple[str, str]]]:
    """
    Synchronous wrapper for async file download
    
    Args:
        base_url: Base URL for files
        file_names: List of file names to download
        max_concurrent: Maximum concurrent downloads
        timeout: Request timeout in seconds
        proxy: Proxy configuration
        show_progress: Whether to show progress bar
        
    Returns:
        Tuple of (list of dataframes, list of (url, error) tuples)
    """
    downloader = AsyncDownloader(
        max_concurrent=max_concurrent,
        timeout=timeout,
        proxy=proxy
    )
    
    return asyncio.run(downloader.download_files(base_url, file_names, show_progress))


def run_async_fulltext_download(
    urls: List[str],
    max_concurrent: int = 20,
    show_progress: bool = True
) -> Tuple[dict, List[Tuple[str, str]]]:
    """
    Synchronous wrapper for async full text download
    
    Args:
        urls: List of article URLs
        max_concurrent: Maximum concurrent downloads
        show_progress: Whether to show progress bar
        
    Returns:
        Tuple of (dict of {url: full_text}, list of (url, error) tuples)
    """
    downloader = AsyncDownloader(max_concurrent=max_concurrent)
    return asyncio.run(downloader.download_fulltexts(urls, show_progress))