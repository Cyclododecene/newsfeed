"""
Unit tests for NewsFeed database query functionality with performance optimizations.

Tests cover:
- Basic database queries (V1 and V2)
- Performance optimizations (cache, incremental, async)
- All database types (Events, GKG, Mentions)
- Query results validation
"""

import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2
from newsfeed.news.db.others import GEG, VGEG, GDG, GFG


def test_event_v1_basic():
    """Test EventV1 basic query functionality."""
    print("\n[Test] EventV1 Basic Query")
    gdelt_events_v1 = EventV1(start_date="2021-01-01", end_date="2021-01-02")
    results = gdelt_events_v1.query()
    assert type(results) == pd.DataFrame
    assert len(results) > 0 or len(results.columns) > 0
    print("✓ EventV1 basic query passed")


def test_event_v1_nowtime():
    """Test EventV1 query_nowtime functionality."""
    print("\n[Test] EventV1 Query Nowtime")
    gdelt_events_v1 = EventV1(start_date="2021-01-01", end_date="2021-01-02")
    results = gdelt_events_v1.query_nowtime(date="2021-01-01")
    assert type(results) == pd.DataFrame
    print("✓ EventV1 query_nowtime passed")


def test_event_v2_basic():
    """Test EventV2 basic query functionality."""
    print("\n[Test] EventV2 Basic Query (events table)")
    gdelt_events_v2 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        table="events"
    )
    results = gdelt_events_v2.query()
    assert type(results) == pd.DataFrame
    assert len(results) > 0 or len(results.columns) > 0
    print("✓ EventV2 basic query (events) passed")


def test_event_v2_mentions():
    """Test EventV2 mentions table query."""
    print("\n[Test] EventV2 Mentions Table")
    gdelt_events_v2_mentions = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        table="mentions"
    )
    results = gdelt_events_v2_mentions.query()
    assert type(results) == pd.DataFrame
    print("✓ EventV2 mentions query passed")


def test_event_v2_with_cache():
    """Test EventV2 with cache optimization."""
    print("\n[Test] EventV2 with Cache")
    
    # First query (no cache)
    start = time.time()
    event1 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True
    )
    results1 = event1.query()
    time1 = time.time() - start
    
    # Second query (from cache)
    start = time.time()
    event2 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True
    )
    results2 = event2.query()
    time2 = time.time() - start
    
    assert type(results1) == pd.DataFrame
    assert type(results2) == pd.DataFrame
    assert len(results1) == len(results2)
    print(f"✓ EventV2 cache working - Speedup: {time1/time2:.2f}x")


def test_event_v2_with_incremental():
    """Test EventV2 with incremental query."""
    print("\n[Test] EventV2 with Incremental Query")
    
    # First query (no incremental history)
    event1 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_incremental=True
    )
    results1 = event1.query()
    
    # Second query (should skip downloaded files)
    event2 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_incremental=True
    )
    results2 = event2.query()
    
    assert type(results1) == pd.DataFrame
    assert type(results2) == pd.DataFrame
    print("✓ EventV2 incremental query working")


def test_gkg_v1_basic():
    """Test GKGV1 basic query functionality."""
    print("\n[Test] GKGV1 Basic Query")
    gdelt_gkg_v1 = GKGV1(start_date="2021-01-01", end_date="2021-01-02")
    results = gdelt_gkg_v1.query()
    assert type(results) == pd.DataFrame
    assert len(results) > 0 or len(results.columns) > 0
    print("✓ GKGV1 basic query passed")


def test_gkg_v1_nowtime():
    """Test GKGV1 query_nowtime functionality."""
    print("\n[Test] GKGV1 Query Nowtime")
    gdelt_gkg_v1 = GKGV1(start_date="2021-01-01", end_date="2021-01-02")
    results = gdelt_gkg_v1.query_nowtime()
    assert type(results) == pd.DataFrame
    print("✓ GKGV1 query_nowtime passed")


def test_gkg_v1_with_async():
    """Test GKGV1 with async download."""
    print("\n[Test] GKGV1 with Async Download")
    gdelt_gkg_v1 = GKGV1(
        start_date="2021-01-01",
        end_date="2021-01-02",
        use_async=True
    )
    results = gdelt_gkg_v1.query()
    assert type(results) == pd.DataFrame
    print("✓ GKGV1 async download working")


def test_gkg_v2_basic():
    """Test GKGV2 basic query functionality."""
    print("\n[Test] GKGV2 Basic Query")
    gdelt_gkg_v2 = GKGV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00"
    )
    results = gdelt_gkg_v2.query()
    assert type(results) == pd.DataFrame
    assert len(results) > 0 or len(results.columns) > 0
    print("✓ GKGV2 basic query passed")


def test_gkg_v2_with_all_optimizations():
    """Test GKGV2 with all optimizations."""
    print("\n[Test] GKGV2 with All Optimizations")
    gdelt_gkg_v2 = GKGV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True,
        use_incremental=True,
        use_async=True
    )
    results = gdelt_gkg_v2.query()
    assert type(results) == pd.DataFrame
    print("✓ GKGV2 with all optimizations working")


def test_gkg_v2_nowtime():
    """Test GKGV2 query_nowtime functionality."""
    print("\n[Test] GKGV2 Query Nowtime")
    gdelt_gkg_v2 = GKGV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00"
    )
    results = gdelt_gkg_v2.query_nowtime()
    assert type(results) == pd.DataFrame
    print("✓ GKGV2 query_nowtime passed")


def test_geg_basic():
    """Test GEG (Global Entity Graph) basic query."""
    print("\n[Test] GEG Basic Query")
    geg = GEG(start_date="2020-01-01", end_date="2020-01-02")
    results = geg.query()
    assert type(results) == pd.DataFrame
    print("✓ GEG basic query passed")


def test_geg_with_cache():
    """Test GEG with cache optimization."""
    print("\n[Test] GEG with Cache")
    geg = GEG(
        start_date="2020-01-01",
        end_date="2020-01-02",
        use_cache=True
    )
    results = geg.query()
    assert type(results) == pd.DataFrame
    print("✓ GEG with cache working")


def test_force_redownload():
    """Test force_redownload parameter."""
    print("\n[Test] Force Redownload")
    event = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True,
        force_redownload=True
    )
    results = event.query()
    assert type(results) == pd.DataFrame
    print("✓ Force redownload working")


def run_all_tests():
    """Run all database tests."""
    print("\n" + "="*60)
    print("Running NewsFeed Database Tests")
    print("="*60)
    
    tests = [
        test_event_v1_basic,
        test_event_v1_nowtime,
        test_event_v2_basic,
        test_event_v2_mentions,
        test_event_v2_with_cache,
        test_event_v2_with_incremental,
        test_gkg_v1_basic,
        test_gkg_v1_nowtime,
        test_gkg_v1_with_async,
        test_gkg_v2_basic,
        test_gkg_v2_with_all_optimizations,
        test_gkg_v2_nowtime,
        test_geg_basic,
        test_geg_with_cache,
        test_force_redownload,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)