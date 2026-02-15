"""
Incremental query management for GDELT
author: Terence Junjie LIU
date: 2026
"""
import sqlite3
import json
from pathlib import Path
from typing import Optional, List, Set
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class IncrementalManager:
    """Manage incremental query history"""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize incremental manager
        
        Args:
            db_path: Path to SQLite database. If None, uses ~/.cache/newsfeed/query_history.db
        """
        if db_path is None:
            home = Path.home()
            db_path = home / ".cache" / "newsfeed" / "query_history.db"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create query history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_key TEXT UNIQUE NOT NULL,
                db_type TEXT NOT NULL,
                version TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                downloaded_files TEXT NOT NULL,
                query_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                table_type TEXT,
                translation INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def _generate_query_key(self, **kwargs) -> str:
        """
        Generate unique key for query
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            Query key string
        """
        key_parts = []
        for key in sorted(kwargs.keys()):
            value = kwargs[key]
            if value is not None:
                key_parts.append(f"{key}={value}")
        return "|".join(key_parts)
    
    def get_downloaded_files(self, **kwargs) -> Set[str]:
        """
        Get list of previously downloaded files for a query
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            Set of downloaded file names
        """
        query_key = self._generate_query_key(**kwargs)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT downloaded_files FROM query_history
            WHERE query_key = ?
            ORDER BY query_time DESC
            LIMIT 1
        ''', (query_key,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            try:
                files_json = result[0]
                return set(json.loads(files_json))
            except Exception as e:
                logger.warning(f"Failed to parse downloaded files: {e}")
                return set()
        
        return set()
    
    def save_query_history(self, downloaded_files: List[str], **kwargs) -> None:
        """
        Save query history with downloaded files
        
        Args:
            downloaded_files: List of downloaded file names
            **kwargs: Query parameters
        """
        query_key = self._generate_query_key(**kwargs)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Use OR REPLACE to update existing entry
        cursor.execute('''
            INSERT OR REPLACE INTO query_history
            (query_key, db_type, version, start_date, end_date, downloaded_files, table_type, translation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            query_key,
            kwargs.get('db_type'),
            kwargs.get('version'),
            kwargs.get('start_date'),
            kwargs.get('end_date'),
            json.dumps(downloaded_files),
            kwargs.get('table_type', ''),
            int(kwargs.get('translation', False))
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved query history for {query_key}: {len(downloaded_files)} files")
    
    def get_new_files(self, all_files: List[str], **kwargs) -> List[str]:
        """
        Get list of files that haven't been downloaded yet
        
        Args:
            all_files: Complete list of files to download
            **kwargs: Query parameters
            
        Returns:
            List of new files to download
        """
        downloaded_files = self.get_downloaded_files(**kwargs)
        new_files = [f for f in all_files if f not in downloaded_files]
        
        logger.info(f"Total files: {len(all_files)}, Already downloaded: {len(downloaded_files)}, New files: {len(new_files)}")
        
        return new_files
    
    def clear_history(self, **kwargs) -> int:
        """
        Clear query history for specific query
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            Number of records deleted
        """
        query_key = self._generate_query_key(**kwargs)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM query_history WHERE query_key = ?', (query_key,))
        count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"Cleared history for {query_key}: {count} record(s) deleted")
        return count
    
    def clear_all_history(self) -> int:
        """
        Clear all query history
        
        Returns:
            Number of records deleted
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM query_history')
        count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"Cleared all history: {count} record(s) deleted")
        return count
    
    def get_history_stats(self) -> dict:
        """
        Get statistics about query history
        
        Returns:
            Dictionary with history statistics
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total queries
        cursor.execute('SELECT COUNT(*) FROM query_history')
        total_queries = cursor.fetchone()[0]
        
        # Queries by database type
        cursor.execute('''
            SELECT db_type, COUNT(*) as count 
            FROM query_history 
            GROUP BY db_type
        ''')
        by_db_type = dict(cursor.fetchall())
        
        # Most recent query
        cursor.execute('''
            SELECT db_type, version, start_date, end_date, query_time
            FROM query_history
            ORDER BY query_time DESC
            LIMIT 1
        ''')
        most_recent = cursor.fetchone()
        
        conn.close()
        
        return {
            "db_path": str(self.db_path),
            "total_queries": total_queries,
            "by_db_type": by_db_type,
            "most_recent": {
                "db_type": most_recent[0] if most_recent else None,
                "version": most_recent[1] if most_recent else None,
                "start_date": most_recent[2] if most_recent else None,
                "end_date": most_recent[3] if most_recent else None,
                "query_time": most_recent[4] if most_recent else None
            }
        }


# Global incremental manager instance
_global_incremental_manager: Optional[IncrementalManager] = None


def get_incremental_manager() -> IncrementalManager:
    """Get global incremental manager instance"""
    global _global_incremental_manager
    if _global_incremental_manager is None:
        _global_incremental_manager = IncrementalManager()
    return _global_incremental_manager