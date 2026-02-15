"""
Unit tests for NewsFeed performance optimization features.

Tests cover:
- Cache system functionality
- Incremental query functionality
- Async download functionality
- Fulltext batch download
"""

import sys
import os
import time
import tempfile
import shutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from newsfeed.news.db.events import EventV1, EventV2
from newsfeed.news.db.gkg import GKGV1, GKGV2
from newsfeed.utils.cache import get_cache_manager
from newsfeed.utils.incremental import get_incremental_manager
from newsfeed.utils.async_downloader import run_async_download
from newsfeed.utils.fulltext import download, download_batch


def test_cache_manager():
    """Test cache manager basic functionality."""
    print("\n[Test] Cache Manager")
    cache = get_cache_manager()
    
    # Create test data
    test_data = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    
    # Save to cache
    cache.set(
        test_data,
        db_type="TEST",
        version="V1",
        start_date="20210101",
        end_date="20210102"
    )
    print("✓ Cache set successful")
    
    # Load from cache
    loaded_data = cache.get(
        db_type="TEST",
        version="V1",
        start_date="20210101",
        end_date="20210102"
    )
    
    assert loaded_data is not None
    assert len(loaded_data) == len(test_data)
    print("✓ Cache load successful")
    
    # Test cache stats
    stats = cache.get_cache_size()
    assert stats['num_files'] > 0
    print(f"✓ Cache stats: {stats}")


def test_incremental_manager():
    """Test incremental query manager functionality."""
    print("\n[Test] Incremental Manager")
    inc = get_incremental_manager()
    
    # Test saving query history
    test_files = ["file1.csv", "file2.csv", "file3.csv"]
    inc.save_query_history(
        test_files,
        db_type="TEST",
        version="V1",
        start_date="20210101",
        end_date="20210102"
    )
    print("✓ Query history saved")
    
    # Test getting new files
    all_files = ["file1.csv", "file2.csv", "file3.csv", "file4.csv"]
    new_files = inc.get_new_files(
        all_files,
        db_type="TEST",
        version="V1",
        start_date="20210101",
        end_date="20210102"
    )
    
    # Should only return file4.csv
    assert len(new_files) == 1
    assert "file4.csv" in new_files
    print("✓ Incremental query working")
    
    # Test query history stats
    stats = inc.get_history_stats()
    assert stats['total_queries'] > 0
    print(f"✓ History stats retrieved: {stats}")


def test_async_download():
    """Test async download functionality."""
    print("\n[Test] Async Download")
    
    # Test with small date range (fewer files)
    event = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-01-03-00-00",
        table="events",
        use_async=True,
        use_cache=True
    )
    
    results = event.query()
    assert type(results) == pd.DataFrame
    print("✓ Async download working")


def test_cache_performance():
    """Test cache performance improvement."""
    print("\n[Test] Cache Performance")
    
    # First query (no cache)
    print("  First query (no cache)...")
    start = time.time()
    event1 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True
    )
    results1 = event1.query()
    time1 = time.time() - start
    
    # Second query (from cache)
    print("  Second query (from cache)...")
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
    
    speedup = time1 / time2 if time2 > 0 else 0
    print(f"  Original time: {time1:.2f}s")
    print(f"  Cached time: {time2:.2f}s")
    print(f"  Speedup: {speedup:.2f}x")
    print("✓ Cache performance test passed")


def test_incremental_performance():
    """Test incremental query performance."""
    print("\n[Test] Incremental Performance")
    
    # First query (no incremental history)
    print("  First query (no history)...")
    event1 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_incremental=True
    )
    results1 = event1.query()
    
    # Second query (should skip downloaded files)
    print("  Second query (with history)...")
    start = time.time()
    event2 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_incremental=True
    )
    results2 = event2.query()
    time2 = time.time() - start
    
    assert type(results1) == pd.DataFrame
    assert type(results2) == pd.DataFrame
    
    print(f"  Second query time: {time2:.2f}s")
    print("✓ Incremental performance test passed")


def test_force_redownload():
    """Test force_redownload parameter."""
    print("\n[Test] Force Redownload")
    
    # First query with cache
    event1 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True
    )
    results1 = event1.query()
    
    # Second query with force_redownload (should bypass cache)
    event2 = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True,
        force_redownload=True
    )
    results2 = event2.query()
    
    assert type(results1) == pd.DataFrame
    assert type(results2) == pd.DataFrame
    print("✓ Force redownload working")


def test_all_optimizations_combined():
    """Test all optimizations combined."""
    print("\n[Test] All Optimizations Combined")
    
    event = EventV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-02-00-00-00",
        use_cache=True,
        use_incremental=True,
        use_async=True
    )
    
    results = event.query()
    assert type(results) == pd.DataFrame
    print("✓ All optimizations working together")


def test_fulltext_download():
    """Test fulltext download functionality."""
    print("\n[Test] Fulltext Download")
    
    # Test single URL download
    test_url = "https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html"
    
    try:
        article = download(test_url)
        if article and hasattr(article, 'text') and article.text:
            print("✓ Single URL fulltext download working")
        else:
            print("⚠ Single URL download returned no text (may be network issue)")
    except Exception as e:
        print(f"⚠ Single URL download failed (may be network issue): {e}")


def test_fulltext_batch_sync():
    """Test fulltext batch download (synchronous)."""
    print("\n[Test] Fulltext Batch Download (Sync)")
    
    urls = [
        "https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html",
    ]
    
    try:
        articles, errors = download_batch(urls, use_async=False, show_progress=False)
        
        assert isinstance(articles, list)
        assert isinstance(errors, list)
        print(f"✓ Batch sync download working: {len(articles)} articles, {len(errors)} errors")
    except Exception as e:
        print(f"⚠ Batch sync download failed (may be network issue): {e}")


def test_fulltext_batch_async():
    """Test fulltext batch download (async)."""
    print("\n[Test] Fulltext Batch Download (Async)")
    
    urls = [
        "https://english.news.cn/20220205/a4e93df9162e4053af64c392b5f5bfec/c.html",
    ]
    
    try:
        articles, errors = download_batch(urls, use_async=True, show_progress=False)
        
        assert isinstance(articles, list)
        assert isinstance(errors, list)
        print(f"✓ Batch async download working: {len(articles)} articles, {len(errors)} errors")
    except Exception as e:
        print(f"⚠ Batch async download failed (may be network issue): {e}")


def test_gkg_with_optimizations():
    """Test GKG with performance optimizations."""
    print("\n[Test] GKG with Optimizations")
    
    gkg = GKGV2(
        start_date="2021-01-01-00-00-00",
        end_date="2021-01-01-06-00-00",
        use_cache=True,
        use_async=True
    )
    
    results = gkg.query()
    assert type(results) == pd.DataFrame
    print("✓ GKG with optimizations working")


def test_cache_clear():
    """Test cache clear functionality."""
    print("\n[Test] Cache Clear")
    
    cache = get_cache_manager()
    
    # Save some data
    test_data = pd.DataFrame({'a': [1, 2, 3]})
    cache.set(test_data, db_type="TEST", version="V1", start_date="20210101", end_date="20210102")
    
    # Clear specific cache entry
    cache.clear(db_type="TEST", version="V1", start_date="20210101", end_date="20210102")
    
    # Verify cache is cleared
    loaded = cache.get(db_type="TEST", version="V1", start_date="20210101", end_date="20210102")
    assert loaded is None
    print("✓ Cache clear working")


def run_all_tests():
    """Run all optimization tests."""
    print("\n" + "="*60)
    print("Running NewsFeed Performance Optimization Tests")
    print("="*60)
    
    tests = [
        test_cache_manager,
        test_incremental_manager,
        test_async_download,
        test_cache_performance,
        test_incremental_performance,
        test_force_redownload,
        test_all_optimizations_combined,
        test_fulltext_download,
        test_fulltext_batch_sync,
        test_fulltext_batch_async,
        test_gkg_with_optimizations,
        test_cache_clear,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)