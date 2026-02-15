#!/usr/bin/env python
"""
Newsfeed Scripts - Common Use Cases for GDELT Databases

Usage:
    # Scenario 1: Search news by keywords/themes
    python skills/script.py --scenario search-topics --days 1 --keywords "AI,GPT,LLM"
    
    # Scenario 2: View bilateral diplomatic events
    python skills/script.py --scenario bilateral-events --days 7 --country1 USA --country2 CHN
    
    # Scenario 3: View event media mentions
    python skills/script.py --scenario event-mentions --event-id 12345
"""

import argparse
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2


def search_topics(days: int = 1, keywords: list = None, output: str = None, use_cache: bool = True):
    """
    Scenario 1: Search news by keywords/themes from GKG database
    
    Args:
        days: Number of days to search back
        keywords: List of keywords/themes to search
        output: Output filename
        use_cache: Use cached results
    """
    print(f"\n{'='*60}")
    print("Scenario 1: Search News by Keywords/Themes")
    print(f"{'='*60}")
    print(f"Days to search: {days}")
    print(f"Keywords: {keywords if keywords else 'None (required)'}")
    print(f"Use cache: {use_cache}")
    print(f"{'='*60}\n")
    
    # Validate keywords
    if not keywords:
        print("Error: --keywords is required for search-topics scenario")
        print("Example: --keywords \"AI,GPT,LLM\"")
        return None
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format for GKG V2
    start_str = start_date.strftime("%Y-%m-%d-00-00-00")
    end_str = end_date.strftime("%Y-%m-%d-00-00-00")
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Query GKG database
    print("\nQuerying GKG database...")
    gkg = GKGV2(
        start_date=start_str,
        end_date=end_str,
        use_cache=use_cache,
        use_async=True
    )
    results = gkg.query()
    
    if len(results) == 0:
        print("No data found for the specified date range.")
        return None
    
    # Filter by keywords
    print(f"\nFiltering by keywords...")
    keyword_pattern = '|'.join(keywords)
    filtered_news = results[
        results['V2ENHANCEDTHEMES'].str.contains(
            keyword_pattern, case=False, na=False
        )
    ]
    
    print(f"Found {len(filtered_news)} articles matching keywords")
    
    # Show statistics
    print(f"\nStatistics:")
    print(f"  Total articles: {len(results)}")
    print(f"  Matching articles: {len(filtered_news)}")
    print(f"  Percentage: {len(filtered_news)/len(results)*100:.2f}%")
    
    # Show sample results
    print(f"\nSample results (first 5):")
    sample_cols = ['V2.1DATE', 'V2SOURCECOMMONNAME', 'V2ENHANCEDTHEMES', 'V2TONE']
    available_cols = [col for col in sample_cols if col in filtered_news.columns]
    print(filtered_news[available_cols].head(5).to_string(index=False))
    
    # Export results
    if output is None:
        keyword_str = '_'.join(keywords[:3]) if len(keywords) > 3 else '_'.join(keywords)
        output = f"search_{keyword_str}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    
    print(f"\nSaving to: {output}")
    filtered_news.to_csv(output, index=False)
    print(f"Results saved: {os.path.abspath(output)}")
    
    return filtered_news


def view_bilateral_events(days: int = 7, country1: str = None, country2: str = None, 
                          output: str = None, use_cache: bool = True):
    """
    Scenario 2: View bilateral diplomatic events from Events database
    
    Args:
        days: Number of days to search back
        country1: First country code (e.g., USA)
        country2: Second country code (e.g., CHN)
        output: Output filename
        use_cache: Use cached results
    """
    print(f"\n{'='*60}")
    print("Scenario 2: View Bilateral Diplomatic Events")
    print(f"{'='*60}")
    print(f"Days to search: {days}")
    print(f"Countries: {country1} <-> {country2}")
    print(f"Use cache: {use_cache}")
    print(f"{'='*60}\n")
    
    # Validate countries
    if not country1 or not country2:
        print("Error: --country1 and --country2 are required for bilateral-events scenario")
        print("Example: --country1 USA --country2 CHN")
        return None
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format for Events V2
    start_str = start_date.strftime("%Y-%m-%d-00-00-00")
    end_str = end_date.strftime("%Y-%m-%d-00-00-00")
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Query Events database
    print("\nQuerying Events database...")
    events = EventV2(
        start_date=start_str,
        end_date=end_str,
        table="events",
        use_cache=use_cache,
        use_async=True
    )
    results = events.query()
    
    if len(results) == 0:
        print("No data found for the specified date range.")
        return None
    
    # Filter bilateral events
    print(f"\nFiltering bilateral events between {country1} and {country2}...")
    bilateral_events = results[
        (
            ((results['Actor1CountryCode'] == country1) & (results['Actor2CountryCode'] == country2)) |
            ((results['Actor1CountryCode'] == country2) & (results['Actor2CountryCode'] == country1))
        )
    ]
    
    print(f"Found {len(bilateral_events)} bilateral events")
    
    # Show statistics
    print(f"\nStatistics:")
    print(f"  Total events: {len(results)}")
    print(f"  Bilateral events: {len(bilateral_events)}")
    
    # Event type distribution
    if len(bilateral_events) > 0 and 'EventCode' in bilateral_events.columns:
        event_types = bilateral_events['EventCode'].value_counts().head(10)
        print(f"\n  Top 10 event types:")
        for event_code, count in event_types.items():
            print(f"    {event_code}: {count}")
    
    # Tone analysis
    if len(bilateral_events) > 0 and 'AvgTone' in bilateral_events.columns:
        avg_tone = bilateral_events['AvgTone'].mean()
        print(f"\n  Average tone: {avg_tone:.2f}")
        print(f"  Positive events (tone > 0): {len(bilateral_events[bilateral_events['AvgTone'] > 0])}")
        print(f"  Negative events (tone < 0): {len(bilateral_events[bilateral_events['AvgTone'] < 0])}")
    
    # Show sample results
    print(f"\nSample results (first 5):")
    sample_cols = ['SQLDATE', 'Actor1Name', 'Actor2Name', 'EventCode', 'AvgTone', 'NumArticles']
    available_cols = [col for col in sample_cols if col in bilateral_events.columns]
    print(bilateral_events[available_cols].head(5).to_string(index=False))
    
    # Export results
    if output is None:
        output = f"{country1}_{country2}_events_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    
    print(f"\nSaving to: {output}")
    bilateral_events.to_csv(output, index=False)
    print(f"Results saved: {os.path.abspath(output)}")
    
    return bilateral_events


def view_event_mentions(event_id: str = None, days: int = 7, output: str = None, use_cache: bool = True):
    """
    Scenario 3: View event media mentions from Mentions database
    
    Args:
        event_id: Specific event ID to query (if None, finds most mentioned events)
        days: Number of days to search back
        output: Output filename
        use_cache: Use cached results
    """
    print(f"\n{'='*60}")
    print("Scenario 3: View Event Media Mentions")
    print(f"{'='*60}")
    print(f"Event ID: {event_id if event_id else 'None (will analyze top events)'}")
    print(f"Days to search: {days}")
    print(f"Use cache: {use_cache}")
    print(f"{'='*60}\n")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format for Events V2 (Mentions)
    start_str = start_date.strftime("%Y-%m-%d-00-00-00")
    end_str = end_date.strftime("%Y-%m-%d-00-00-00")
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Query Mentions database
    print("\nQuerying Mentions database...")
    mentions = EventV2(
        start_date=start_str,
        end_date=end_str,
        table="mentions",
        use_cache=use_cache,
        use_async=True
    )
    results = mentions.query()
    
    if len(results) == 0:
        print("No data found for the specified date range.")
        return None
    
    # Filter by event ID if provided
    if event_id:
        print(f"\nFiltering by event ID: {event_id}")
        filtered_results = results[results['GLOBALEVENTID'] == event_id]
        print(f"Found {len(filtered_results)} mentions for this event")
    else:
        print(f"\nFinding most mentioned events...")
        # Find top mentioned events
        top_events = results['GLOBALEVENTID'].value_counts().head(10)
        print(f"Top 10 most mentioned events:")
        for i, (evt_id, count) in enumerate(top_events.items(), 1):
            print(f"  {i}. Event ID {evt_id}: {count} mentions")
        
        # Filter for top event
        top_event_id = top_events.index[0]
        filtered_results = results[results['GLOBALEVENTID'] == top_event_id]
        print(f"\nAnalyzing mentions for top event: {top_event_id}")
    
    # Show statistics
    print(f"\nStatistics:")
    print(f"  Total mentions: {len(filtered_results)}")
    
    # Media source analysis
    if 'MentionSourceName' in filtered_results.columns:
        top_sources = filtered_results['MentionSourceName'].value_counts().head(10)
        print(f"\n  Top 10 media sources:")
        for i, (source, count) in enumerate(top_sources.items(), 1):
            print(f"    {i}. {source}: {count} mentions")
    
    # Mention type analysis
    if 'MentionType' in filtered_results.columns:
        mention_types = filtered_results['MentionType'].value_counts()
        print(f"\n  Mention types:")
        for mention_type, count in mention_types.items():
            print(f"    {mention_type}: {count}")
    
    # Confidence analysis
    if 'Confidence' in filtered_results.columns:
        avg_confidence = filtered_results['Confidence'].mean()
        high_confidence = len(filtered_results[filtered_results['Confidence'] > 80])
        print(f"\n  Average confidence: {avg_confidence:.2f}")
        print(f"  High confidence (>80): {high_confidence} ({high_confidence/len(filtered_results)*100:.1f}%)")
    
    # Show sample results
    print(f"\nSample results (first 5):")
    sample_cols = ['MentionTimeDate', 'MentionSourceName', 'GLOBALEVENTID', 'Confidence', 'MentionDocTone']
    available_cols = [col for col in sample_cols if col in filtered_results.columns]
    print(filtered_results[available_cols].head(5).to_string(index=False))
    
    # Export results
    if output is None:
        output = f"event_mentions_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    
    print(f"\nSaving to: {output}")
    filtered_results.to_csv(output, index=False)
    print(f"Results saved: {os.path.abspath(output)}")
    
    return filtered_results


def main():
    parser = argparse.ArgumentParser(
        description="Newsfeed Scripts - Common use cases for GDELT databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scenario 1: Search by keywords
  python skills/script.py --scenario search-topics --days 1 --keywords "AI,GPT,LLM"
  python skills/script.py --scenario search-topics --days 7 --keywords "ECON,POL,China"
  python skills/script.py --scenario search-topics --days 1 --keywords "AI" --output results.csv

  # Scenario 2: View bilateral events
  python skills/script.py --scenario bilateral-events --days 7 --country1 USA --country2 CHN
  python skills/script.py --scenario bilateral-events --days 30 --country1 USA --country2 RUS
  python skills/script.py --scenario bilateral-events --days 7 --country1 FRA --country2 DEU --output results.csv

  # Scenario 3: View event mentions
  python skills/script.py --scenario event-mentions --days 7
  python skills/script.py --scenario event-mentions --event-id 12345 --days 7
        """
    )
    
    # Scenario selection
    parser.add_argument(
        "--scenario",
        type=str,
        choices=["search-topics", "bilateral-events", "event-mentions"],
        required=True,
        help="Scenario to run: search-topics, bilateral-events, event-mentions"
    )
    
    # Common parameters
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to search back (default: 1)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Output filename (default: auto-generated)"
    )
    
    parser.add_argument(
        "--use-cache",
        action="store_true",
        default=True,
        help="Use cached query results (default: True)"
    )
    
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache (force fresh download)"
    )
    
    # Scenario 1: Search topics specific parameters
    parser.add_argument(
        "--keywords",
        type=str,
        help="Comma-separated keywords to search (e.g., 'AI,GPT,LLM')"
    )
    
    # Scenario 2: Bilateral events specific parameters
    parser.add_argument(
        "--country1",
        type=str,
        help="First country code (e.g., USA, CHN, FRA)"
    )
    
    parser.add_argument(
        "--country2",
        type=str,
        help="Second country code (e.g., USA, CHN, RUS)"
    )
    
    # Scenario 3: Event mentions specific parameters
    parser.add_argument(
        "--event-id",
        type=str,
        help="Specific event ID to query (for event-mentions scenario)"
    )
    
    args = parser.parse_args()
    
    # Handle cache parameter
    use_cache = args.use_cache and not args.no_cache
    
    # Run selected scenario
    if args.scenario == "search-topics":
        keywords = args.keywords.split(',') if args.keywords else None
        search_topics(
            days=args.days,
            keywords=keywords,
            output=args.output,
            use_cache=use_cache
        )
    elif args.scenario == "bilateral-events":
        view_bilateral_events(
            days=args.days,
            country1=args.country1,
            country2=args.country2,
            output=args.output,
            use_cache=use_cache
        )
    elif args.scenario == "event-mentions":
        view_event_mentions(
            event_id=args.event_id,
            days=args.days,
            output=args.output,
            use_cache=use_cache
        )


if __name__ == "__main__":
    main()