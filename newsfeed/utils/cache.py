"""
Cache management for GDELT query results
author: Terence Junjie LIU
date: 2026
"""
import os
import hashlib
import joblib
import pandas as pd
from pathlib import Path
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)


class CacheManager:
    """Simple file-based cache manager for query results"""
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize cache manager
        
        Args:
            cache_dir: Custom cache directory path. If None, uses ~/.cache/newsfeed/
        """
        if cache_dir is None:
            home = Path.home()
            cache_dir = home / ".cache" / "newsfeed"
        else:
            cache_dir = Path(cache_dir)
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def _generate_cache_key(self, **kwargs) -> str:
        """
        Generate cache key from query parameters
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            Cache key string
        """
        # Sort kwargs to ensure consistent key generation
        sorted_items = sorted(kwargs.items())
        key_string = str(sorted_items)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        Retrieve cached data if exists
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            Cached DataFrame or None if not found
        """
        cache_key = self._generate_cache_key(**kwargs)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        if cache_file.exists():
            try:
                logger.info(f"Loading from cache: {cache_file}")
                data = joblib.load(cache_file)
                return data
            except Exception as e:
                logger.warning(f"Failed to load cache {cache_file}: {e}")
                return None
        
        return None
    
    def set(self, data: pd.DataFrame, **kwargs) -> None:
        """
        Store data in cache
        
        Args:
            data: DataFrame to cache
            **kwargs: Query parameters
        """
        cache_key = self._generate_cache_key(**kwargs)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            joblib.dump(data, cache_file)
            logger.info(f"Saved to cache: {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save cache {cache_file}: {e}")
    
    def clear(self, **kwargs) -> None:
        """
        Clear specific cache entry
        
        Args:
            **kwargs: Query parameters to identify cache entry
        """
        cache_key = self._generate_cache_key(**kwargs)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        if cache_file.exists():
            try:
                cache_file.unlink()
                logger.info(f"Deleted cache: {cache_file}")
            except Exception as e:
                logger.warning(f"Failed to delete cache {cache_file}: {e}")
    
    def clear_all(self) -> int:
        """
        Clear all cache entries
        
        Returns:
            Number of cache files deleted
        """
        count = 0
        for cache_file in self.cache_dir.glob("*.pkl"):
            try:
                cache_file.unlink()
                count += 1
            except Exception as e:
                logger.warning(f"Failed to delete {cache_file}: {e}")
        
        logger.info(f"Cleared {count} cache files")
        return count
    
    def get_cache_size(self) -> dict:
        """
        Get cache statistics
        
        Returns:
            Dictionary with cache statistics
        """
        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            "cache_dir": str(self.cache_dir),
            "num_files": len(cache_files),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_size_bytes": total_size
        }
    
    def prune_old_files(self, days: int = 7) -> int:
        """
        Remove cache files older than specified days
        
        Args:
            days: Number of days to keep
            
        Returns:
            Number of files removed
        """
        import time
        
        current_time = time.time()
        cutoff_time = current_time - (days * 24 * 60 * 60)
        count = 0
        
        for cache_file in self.cache_dir.glob("*.pkl"):
            if cache_file.stat().st_mtime < cutoff_time:
                try:
                    cache_file.unlink()
                    count += 1
                    logger.info(f"Removed old cache file: {cache_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove {cache_file}: {e}")
        
        logger.info(f"Pruned {count} cache files older than {days} days")
        return count


# Global cache manager instance
_global_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """Get global cache manager instance"""
    global _global_cache_manager
    if _global_cache_manager is None:
        _global_cache_manager = CacheManager()
    return _global_cache_manager