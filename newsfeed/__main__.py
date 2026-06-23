#!/usr/bin/env python
"""
newsfeed CLI - GDELT Project Database Query Tool

Usage:
    # Query databases
    python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00
    python -m newsfeed --db GKG --version V2 --start 2021-01-01 --end 2021-01-02 --format json
    
    # Query with performance optimizations
    python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --use-cache
    python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --incremental
    python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --async
    
    # Download full text from URLs
    python -m newsfeed --fulltext --url "https://example.com/article" --output article.json
    python -m newsfeed --fulltext --input urls.txt --output fulltexts.csv
    python -m newsfeed --fulltext --input results.csv --url-column SOURCEURL --output with_fulltext.csv
    
    # Query and download full text
    python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --download-fulltext
"""

import argparse
import json
import sys
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2
from newsfeed.news.db.others import GEG, VGEG, GDG, GFG, GAL, GSG
from newsfeed.utils.fulltext import download, download_batch


SUPPORTED_OUTPUT_FORMATS = ["csv", "json", "txt", "parquet"]
VERSIONED_DATABASES = ["EVENT", "GKG", "MENTIONS"]
V3_DATABASES = ["GEG", "VGEG", "GDG", "GFG", "GAL", "GSG"]
SUPPORTED_DATABASES = VERSIONED_DATABASES + V3_DATABASES


def save_results(results, output_path: str, output_format: str, allow_txt: bool = False) -> str:
    """
    Save query or full-text results to disk.

    Args:
        results: A pandas DataFrame or a list of dictionaries.
        output_path: Destination file path.
        output_format: csv, json, txt, or parquet.
        allow_txt: Whether TXT output is valid for this result type.

    Returns:
        The format that was actually written.
    """
    output_format = output_format.lower()
    if isinstance(results, pd.DataFrame):
        df = results
        records = None
    else:
        records = results
        df = pd.DataFrame(results)

    if output_format == "csv":
        df.to_csv(output_path, index=False)
        return "csv"
    if output_format == "json":
        if records is not None:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        else:
            df.to_json(output_path, orient='records', force_ascii=False)
        return "json"
    if output_format == "parquet":
        df.to_parquet(output_path, index=False)
        return "parquet"
    if output_format == "txt":
        if allow_txt and records is not None and len(records) == 1 and records[0].get('success'):
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(records[0]['text'])
            return "txt"

        print("Warning: TXT format is only supported for successful single URL full text downloads. Using CSV instead.")
        df.to_csv(output_path, index=False)
        return "csv"

    raise ValueError(f"Unsupported output format: {output_format}")


def parse_date(version: str, date_str: str) -> str:
    """
    Parse and validate date based on version.
    
    Args:
        version: V1 or V2
        date_str: Date string
        
    Returns:
        Validated date string
    """
    if version.upper() == "V1":
        # V1 uses YYYY-MM-DD format
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid date format for V1: '{date_str}'. Expected format: YYYY-MM-DD (e.g., 2021-01-01)"
            )
    else:
        # V2 uses YYYY-MM-DD-HH-MM-SS format
        try:
            datetime.strptime(date_str, "%Y-%m-%d-%H-%M-%S")
            return date_str
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid date format for V2: '{date_str}'. Expected format: YYYY-MM-DD-HH-MM-SS (e.g., 2021-01-01-00-00-00)"
            )


def parse_v3_date(db_type: str, date_str: str) -> str:
    """Parse dates for GDELT V3 graph datasets."""
    if db_type in ["GEG", "VGEG"] or (db_type == "GSG" and len(date_str) == 10):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid date format for {db_type}: '{date_str}'. Expected format: YYYY-MM-DD (e.g., 2021-01-01)"
            )

    try:
        datetime.strptime(date_str, "%Y-%m-%d-%H-%M-%S")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format for {db_type}: '{date_str}'. Expected format: YYYY-MM-DD-HH-MM-SS (e.g., 2018-07-27-14-00-00)"
        )


def download_fulltext(url: str) -> dict:
    """
    Download full text from a single URL.
    
    Args:
        url: Article URL
        
    Returns:
        Dictionary with article data or None if failed
    """
    try:
        article = download(url)
        if article and hasattr(article, 'text') and article.text:
            return {
                'url': url,
                'title': article.title if hasattr(article, 'title') else '',
                'text': article.text,
                'publish_date': article.publish_date if hasattr(article, 'publish_date') else '',
                'authors': article.authors if hasattr(article, 'authors') else [],
                'keywords': article.keywords if hasattr(article, 'keywords') else [],
                'top_image': article.top_image if hasattr(article, 'top_image') else '',
                'success': True
            }
    except Exception as e:
        pass
    return {'url': url, 'success': False}


def download_fulltext_from_urls(urls: list, use_async: bool = False, show_progress: bool = True) -> list:
    """
    Download full text from multiple URLs.
    
    Args:
        urls: List of URLs
        use_async: Use async download
        show_progress: Whether to show progress bar
        
    Returns:
        List of dictionaries with article data
    """
    if use_async:
        print("Using async download for full text...")
        articles, errors = download_batch(urls, use_async=True, show_progress=show_progress)
        
        # Convert to expected format
        results = []
        
        # articles is a dict {url: full_text} when using async
        if isinstance(articles, dict):
            for url, text in articles.items():
                if text and text.strip():
                    results.append({
                        'url': url,
                        'title': '',
                        'text': text,
                        'publish_date': '',
                        'authors': [],
                        'keywords': [],
                        'top_image': '',
                        'success': True
                    })
                else:
                    results.append({'url': url, 'success': False})
        else:
            # Handle case where articles is a list (shouldn't happen with async)
            for article in articles:
                if hasattr(article, 'text') and article.text:
                    results.append({
                        'url': article.url if hasattr(article, 'url') else '',
                        'title': article.title if hasattr(article, 'title') else '',
                        'text': article.text,
                        'publish_date': article.publish_date if hasattr(article, 'publish_date') else '',
                        'authors': article.authors if hasattr(article, 'authors') else [],
                        'keywords': article.keywords if hasattr(article, 'keywords') else [],
                        'top_image': article.top_image if hasattr(article, 'top_image') else '',
                        'success': True
                    })
                else:
                    results.append({'url': getattr(article, 'url', ''), 'success': False})
        
        # Add errors
        for error in errors:
            # Handle both tuple and dict error formats
            if isinstance(error, dict):
                url = error.get('url', '')
            elif isinstance(error, tuple) and len(error) > 0:
                url = error[0]
            else:
                url = ''
            results.append({'url': url, 'success': False})
        
        return results
    else:
        # Synchronous download
        results = []
        failed_urls = []
        
        iterator = tqdm(urls, desc="Downloading articles") if show_progress else urls
        
        for url in iterator:
            result = download_fulltext(url)
            results.append(result)
            if not result.get('success', False):
                failed_urls.append(url)
        
        # Print summary
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"\nDownload completed: {success_count}/{len(urls)} successful")
        
        if failed_urls:
            print(f"Failed to download {len(failed_urls)} URLs:")
            for url in failed_urls[:10]:  # Show first 10 failed URLs
                print(f"  - {url}")
            if len(failed_urls) > 10:
                print(f"  ... and {len(failed_urls) - 10} more")
        
        return results


def read_urls_from_file(input_file: str, url_column: str = None) -> list:
    """
    Read URLs from a file.
    
    Args:
        input_file: Path to input file
        url_column: Column name if CSV file
        
    Returns:
        List of URLs
    """
    urls = []
    
    if input_file.endswith('.csv'):
        # Read from CSV
        try:
            df = pd.read_csv(input_file)
            column = url_column if url_column in df.columns else df.columns[0]
            urls = df[column].dropna().unique().tolist()
            print(f"Found {len(urls)} unique URLs in column '{column}'")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            sys.exit(1)
    else:
        # Read from text file (one URL per line)
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
            print(f"Found {len(urls)} URLs in {input_file}")
        except Exception as e:
            print(f"Error reading file: {e}")
            sys.exit(1)
    
    return urls


def main():
    parser = argparse.ArgumentParser(
        description="Query GDELT Project databases and download full text articles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query Events V2
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00

  # Query GKG V1
  python -m newsfeed --db GKG --version V1 --start 2021-01-01 --end 2021-01-02

  # Query with cache
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --use-cache

  # Query with incremental updates
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --incremental

  # Query with async download
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --async

  # Query with all optimizations
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --use-cache --incremental --async

  # Query Mentions V2 with JSON output
  python -m newsfeed --db MENTIONS --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --format json

  # Query GDELT V3 graph datasets
  python -m newsfeed --db GEG --start 2020-01-01 --end 2020-01-02
  python -m newsfeed --db VGEG --start 2020-01-01 --domain CNN
  python -m newsfeed --db GDG --start 2018-08-27-14-00-00
  python -m newsfeed --db GFG --start 2018-03-02-02-00-00
  python -m newsfeed --db GAL --start 2020-01-01-00-01-00
  python -m newsfeed --db GAL --rss --format json
  python -m newsfeed --db GSG --gsg-dataset docembed --start 2020-01-01-00-00-00
  python -m newsfeed --db GSG --gsg-dataset iatvsentembed --start 2009-07-02 --domain CNN

  # Query and download full text
  python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00 --download-fulltext

  # Download full text from single URL
  python -m newsfeed --fulltext --url "https://example.com/article" --output article.json

  # Download full text from URL list file with async
  python -m newsfeed --fulltext --input urls.txt --output fulltexts.csv --async
        """
    )
    
    # Database query arguments
    parser.add_argument(
        "--db",
        type=str.upper,
        choices=SUPPORTED_DATABASES,
        help="Database type to query"
    )
    
    parser.add_argument(
        "--version",
        type=str.upper,
        choices=["V1", "V2", "V3"],
        help="Database version (required for EVENT/GKG/MENTIONS; V3 graph datasets infer V3)"
    )
    
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS)"
    )
    
    parser.add_argument(
        "--end",
        type=str,
        help="End date (required for EVENT/GKG/MENTIONS and GEG)"
    )

    parser.add_argument(
        "--domain",
        type=str,
        help="Station/domain filter for VGEG queries, for example CNN"
    )

    parser.add_argument(
        "--raw",
        action="store_true",
        help="Download raw VGEG records instead of normalized VGEG v2 records"
    )

    parser.add_argument(
        "--rss",
        action="store_true",
        help="Query the GDELT Article List rolling RSS feed when --db GAL is used"
    )

    parser.add_argument(
        "--gsg-dataset",
        type=str.lower,
        default="docembed",
        choices=["docembed", "iatvsentembed"],
        help="GSG dataset to query when --db GSG is used"
    )
    
    # Performance optimization arguments
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached query results (90-95%% faster for repeated queries)"
    )
    
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental query mode (only download new files, 80-90%% faster for updates)"
    )
    
    parser.add_argument(
        "--force-redownload",
        action="store_true",
        help="Force fresh download, bypass cache and incremental history"
    )
    
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",
        help="Use async concurrent downloads (3-5x faster)"
    )
    
    # Full text download arguments
    parser.add_argument(
        "--fulltext",
        action="store_true",
        help="Enable full text download mode"
    )
    
    parser.add_argument(
        "--download-fulltext",
        action="store_true",
        help="Download full text after database query"
    )
    
    parser.add_argument(
        "--url",
        type=str,
        help="Single URL to download full text from"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        help="Input file with URLs (txt or csv)"
    )
    
    parser.add_argument(
        "--url-column",
        type=str,
        default="SOURCEURL",
        help="URL column name in CSV file (default: SOURCEURL)"
    )
    
    parser.add_argument(
        "--fulltext-column",
        type=str,
        default="FULLTEXT",
        help="Column name for full text in output (default: FULLTEXT)"
    )
    
    # Output arguments
    parser.add_argument(
        "--format",
        type=str.lower,
        default="csv",
        choices=SUPPORTED_OUTPUT_FORMATS,
        help="Output format (default: csv)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Output filename (default: auto-generated)"
    )
    
    args = parser.parse_args()
    
    # Check if we're in fulltext mode
    if args.fulltext:
        # Full text download mode
        if not args.url and not args.input:
            parser.error("--fulltext requires either --url or --input")
        
        # Get URLs
        if args.url:
            urls = [args.url]
            print(f"Downloading full text from single URL: {args.url}")
        else:
            urls = read_urls_from_file(args.input, args.url_column)
        
        # Set default output filename
        if args.output is None:
            if args.url:
                args.output = f"article.{args.format}"
            else:
                args.output = f"fulltexts.{args.format}"
        
        print(f"\n{'='*60}")
        print(f"Newsfeed CLI - Full Text Download")
        print(f"{'='*60}")
        print(f"URLs to download: {len(urls)}")
        print(f"Output format: {args.format}")
        print(f"Output file: {args.output}")
        print(f"Use async: {args.use_async}")
        print(f"{'='*60}\n")
        
        # Download full text
        try:
            results = download_fulltext_from_urls(urls, use_async=args.use_async, show_progress=True)
            
            # Save results
            print(f"\nSaving results to {args.output}...")
            
            actual_format = save_results(results, args.output, args.format, allow_txt=True)
            if actual_format != args.format:
                print(f"Saved as {actual_format.upper()} because {args.format.upper()} was not valid for this result.")
            
            print(f"Results saved to: {os.path.abspath(args.output)}")
            print(f"\n{'='*60}")
            print("Done!")
            print(f"{'='*60}\n")
            
        except KeyboardInterrupt:
            print("\n\nDownload interrupted by user.")
            sys.exit(1)
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        return
    
    # Database query mode
    if not args.db:
        parser.error("--db is required for database query")

    if args.db in VERSIONED_DATABASES:
        if not args.version or not args.start or not args.end:
            parser.error("--db, --version, --start, and --end are required for EVENT/GKG/MENTIONS queries")
        if args.version == "V3":
            parser.error("EVENT, GKG, and MENTIONS only support --version V1 or V2")

        # Validate dates based on version
        try:
            start_date = parse_date(args.version, args.start)
            end_date = parse_date(args.version, args.end)
        except argparse.ArgumentTypeError as e:
            print(f"Error: {e}")
            sys.exit(1)
        display_version = args.version
    else:
        if not args.start and not (args.db == "GAL" and args.rss):
            parser.error(f"--start is required for {args.db} queries")
        if args.version and args.version != "V3":
            parser.error(f"{args.db} is a V3 dataset; omit --version or use --version V3")
        if args.db == "GEG" and not args.end:
            parser.error("--end is required for GEG queries")

        try:
            start_date = parse_v3_date(args.db, args.start) if args.start else None
            end_date = parse_v3_date(args.db, args.end) if args.end else None
        except argparse.ArgumentTypeError as e:
            print(f"Error: {e}")
            sys.exit(1)
        display_version = "V3"
    
    # Generate default output filename if not specified
    if args.output is None:
        # Remove hyphens from dates for filename
        if args.db == "GAL" and args.rss:
            args.output = f"{args.db}_RSS.{args.format}"
        else:
            start_clean = start_date.replace("-", "")
        if args.db == "GAL" and args.rss:
            pass
        elif end_date:
            end_clean = end_date.replace("-", "")
            args.output = f"{args.db}_{display_version}_{start_clean}_{end_clean}.{args.format}"
        else:
            args.output = f"{args.db}_{display_version}_{start_clean}.{args.format}"
    
    print(f"\n{'='*60}")
    print(f"Newsfeed CLI - GDELT Database Query")
    print(f"{'='*60}")
    print(f"Database:      {args.db}")
    print(f"Version:       {display_version}")
    print(f"Start Date:    {start_date or 'N/A'}")
    print(f"End Date:      {end_date or 'N/A'}")
    if args.db == "GAL":
        print(f"RSS Feed:      {args.rss}")
    if args.db == "GSG":
        print(f"GSG Dataset:   {args.gsg_dataset}")
        print(f"Station:       {args.domain or 'all'}")
    if args.db == "VGEG":
        print(f"Domain:        {args.domain or 'all'}")
        print(f"Raw VGEG:      {args.raw}")
    print(f"Output Format: {args.format}")
    print(f"Output File:   {args.output}")
    print(f"Use Cache:     {args.use_cache}")
    print(f"Incremental:   {args.incremental}")
    print(f"Force Reload:  {args.force_redownload}")
    print(f"Use Async:     {args.use_async}")
    print(f"Download Fulltext: {args.download_fulltext}")
    print(f"{'='*60}\n")
    
    try:
        # Initialize appropriate database class
        if args.db == "EVENT":
            if args.version == "V1":
                db = EventV1(start_date=start_date, end_date=end_date,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async,
                           output_format=args.format)
            else:  # V2
                db = EventV2(start_date=start_date, end_date=end_date, table="events", translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async,
                           output_format=args.format)
        elif args.db == "GKG":
            if args.version == "V1":
                db = GKGV1(start_date=start_date, end_date=end_date,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async,
                           output_format=args.format)
            else:  # V2
                db = GKGV2(start_date=start_date, end_date=end_date, translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async,
                           output_format=args.format)
        elif args.db == "MENTIONS":
            if args.version == "V1":
                print("Error: Mentions database is only available in V2")
                sys.exit(1)
            else:  # V2
                db = EventV2(start_date=start_date, end_date=end_date, table="mentions", translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async,
                           output_format=args.format)
        elif args.db == "GEG":
            db = GEG(start_date=start_date, end_date=end_date,
                     use_cache=args.use_cache, use_incremental=args.incremental,
                     force_redownload=args.force_redownload, use_async=args.use_async)
        elif args.db == "VGEG":
            db = VGEG(query_date=start_date, domain=args.domain, raw=args.raw,
                      use_cache=args.use_cache, use_incremental=args.incremental,
                      force_redownload=args.force_redownload, use_async=args.use_async)
        elif args.db == "GDG":
            if args.incremental or args.use_async:
                print("Warning: GDG does not support --incremental or --async; ignoring those options.")
            db = GDG(query_date=start_date, use_cache=args.use_cache,
                     force_redownload=args.force_redownload)
        elif args.db == "GFG":
            if args.incremental or args.use_async:
                print("Warning: GFG does not support --incremental or --async; ignoring those options.")
            db = GFG(query_date=start_date, use_cache=args.use_cache,
                     force_redownload=args.force_redownload)
        elif args.db == "GAL":
            if args.incremental or args.use_async:
                print("Warning: GAL does not support --incremental or --async; ignoring those options.")
            db = GAL(start_date=start_date or "2020-01-01-00-01-00",
                     end_date=end_date,
                     use_cache=args.use_cache,
                     force_redownload=args.force_redownload)
        elif args.db == "GSG":
            db = GSG(start_date=start_date,
                     end_date=end_date,
                     dataset=args.gsg_dataset,
                     station=args.domain,
                     use_cache=args.use_cache,
                     use_incremental=args.incremental,
                     force_redownload=args.force_redownload,
                     use_async=args.use_async)
        
        # Query the database
        print(f"Starting query...\n")
        if args.db == "GAL" and args.rss:
            results = db.query_rss_feed()
        else:
            results = db.query()
        
        # Check if query was successful
        if isinstance(results, Exception):
            print(f"Error during query: {results}")
            sys.exit(1)
        
        print(f"\nQuery completed successfully!")
        print(f"Retrieved {len(results)} records\n")
        
        # Download full text if requested
        if args.download_fulltext:
            # Find URL column
            url_column = None
            if args.url_column in results.columns:
                url_column = args.url_column
            else:
                # Try common column names
                for col in ['SOURCEURL', 'url', 'URL', 'Url']:
                    if col in results.columns:
                        url_column = col
                        break
            
            if not url_column:
                print(f"Warning: Could not find URL column. Tried: {args.url_column}, SOURCEURL, url, URL, Url")
                print("Skipping full text download.")
            else:
                print(f"Downloading full text from column '{url_column}'...")
                urls = results[url_column].dropna().unique().tolist()
                print(f"Found {len(urls)} unique URLs")
                
                # Download full text
                fulltext_results = download_fulltext_from_urls(urls, use_async=args.use_async, show_progress=True)
                
                # Debug: Show download statistics
                successful_downloads = [item for item in fulltext_results if item.get('success', False)]
                failed_downloads = [item for item in fulltext_results if not item.get('success', False)]
                print(f"\nFull text download summary:")
                print(f"  Successful: {len(successful_downloads)}")
                print(f"  Failed: {len(failed_downloads)}")
                
                if failed_downloads:
                    print(f"\n  Sample failed URLs (first 5):")
                    for item in failed_downloads[:5]:
                        print(f"    - {item.get('url', 'unknown')}")
                
                # Create mapping from URL to full text
                url_to_text = {}
                for item in fulltext_results:
                    if item.get('success') and item.get('text'):
                        url_to_text[item['url']] = item['text']
                
                print(f"\n  URLs in mapping: {len(url_to_text)}")
                print(f"  URLs in results[url_column]: {results[url_column].notna().sum()}")
                
                # Add full text column
                results[args.fulltext_column] = results[url_column].map(url_to_text)
                non_empty_count = results[args.fulltext_column].notna().sum()
                print(f"\nAdded full text to {non_empty_count} records")
                
                if non_empty_count == 0:
                    print("\n⚠️  WARNING: No full text was successfully downloaded!")
                    print("   This could be due to:")
                    print("   - Website blocking automated downloads")
                    print("   - Invalid or expired URLs")
                    print("   - Network issues")
                    print("   - Article extraction failures")
                    print("\n   You can try:")
                    print("   - Using fewer URLs (--limit parameter)")
                    print("   - Using synchronous mode (remove --async)")
                    print("   - Adding delays between requests")
        
        # Export results
        print(f"\nExporting results to {args.output}...")
        
        actual_format = save_results(results, args.output, args.format)
        if actual_format != args.format:
            print(f"Saved as {actual_format.upper()} because {args.format.upper()} was not valid for database results.")
        
        print(f"Results saved to: {os.path.abspath(args.output)}")
        print(f"\n{'='*60}")
        print("Done!")
        print(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        print("\n\nQuery interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
