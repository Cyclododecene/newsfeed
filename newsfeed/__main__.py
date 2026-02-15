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
import sys
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2
from newsfeed.utils.fulltext import download, download_batch


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
            results.append({'url': error.get('url', ''), 'success': False})
        
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
        description="Query GDELT Project databases (Events, GKG, Mentions) and download full text articles",
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
        choices=["EVENT", "GKG", "MENTIONS"],
        help="Database type to query"
    )
    
    parser.add_argument(
        "--version",
        type=str.upper,
        choices=["V1", "V2"],
        help="Database version (V1 or V2)"
    )
    
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS)"
    )
    
    parser.add_argument(
        "--end",
        type=str,
        help="End date (V1: YYYY-MM-DD, V2: YYYY-MM-DD-HH-MM-SS)"
    )
    
    # Performance optimization arguments
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached query results (90-95% faster for repeated queries)"
    )
    
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental query mode (only download new files, 80-90% faster for updates)"
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
        choices=["csv", "json", "txt"],
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
                args.output = "article.json"
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
            
            if args.format == "json":
                # For JSON, save all results
                import json
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
            elif args.format == "csv":
                # For CSV, create DataFrame
                df = pd.DataFrame(results)
                df.to_csv(args.output, index=False)
            elif args.format == "txt":
                # For TXT, save text content (only for single URL)
                if len(results) == 1 and results[0].get('success'):
                    with open(args.output, 'w', encoding='utf-8') as f:
                        f.write(results[0]['text'])
                else:
                    print("Warning: TXT format is only supported for single URL download")
                    # Fall back to CSV
                    df = pd.DataFrame(results)
                    df.to_csv(args.output, index=False)
            
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
    if not args.db or not args.version or not args.start or not args.end:
        parser.error("--db, --version, --start, and --end are required for database query")
    
    # Validate dates based on version
    try:
        start_date = parse_date(args.version, args.start)
        end_date = parse_date(args.version, args.end)
    except argparse.ArgumentTypeError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Generate default output filename if not specified
    if args.output is None:
        # Remove hyphens from dates for filename
        start_clean = start_date.replace("-", "")
        end_clean = end_date.replace("-", "")
        args.output = f"{args.db}_{args.version}_{start_clean}_{end_clean}.{args.format}"
    
    print(f"\n{'='*60}")
    print(f"Newsfeed CLI - GDELT Database Query")
    print(f"{'='*60}")
    print(f"Database:      {args.db}")
    print(f"Version:       {args.version}")
    print(f"Start Date:    {start_date}")
    print(f"End Date:      {end_date}")
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
                           force_redownload=args.force_redownload, use_async=args.use_async)
            else:  # V2
                db = EventV2(start_date=start_date, end_date=end_date, table="events", translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async)
        elif args.db == "GKG":
            if args.version == "V1":
                db = GKGV1(start_date=start_date, end_date=end_date,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async)
            else:  # V2
                db = GKGV2(start_date=start_date, end_date=end_date, translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async)
        elif args.db == "MENTIONS":
            if args.version == "V1":
                print("Error: Mentions database is only available in V2")
                sys.exit(1)
            else:  # V2
                db = EventV2(start_date=start_date, end_date=end_date, table="mentions", translation=False,
                           use_cache=args.use_cache, use_incremental=args.incremental,
                           force_redownload=args.force_redownload, use_async=args.use_async)
        
        # Query the database
        print(f"Starting query...\n")
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
                
                # Create mapping from URL to full text
                url_to_text = {}
                for item in fulltext_results:
                    if item.get('success') and item.get('text'):
                        url_to_text[item['url']] = item['text']
                
                # Add full text column
                results[args.fulltext_column] = results[url_column].map(url_to_text)
                print(f"Added full text to {len(url_to_text)} records")
        
        # Export results
        print(f"\nExporting results to {args.output}...")
        
        if args.format == "csv":
            results.to_csv(args.output, index=False)
        elif args.format == "json":
            results.to_json(args.output, orient='records', force_ascii=False)
        elif args.format == "txt":
            print("Warning: TXT format is only supported for single URL download. Using CSV instead.")
            results.to_csv(args.output, index=False)
        
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