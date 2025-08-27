"""Watermark manager for tracking incremental data source updates."""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path

from ..db import db_manager

logger = logging.getLogger(__name__)


class WatermarkManager:
    """Manages watermarks for incremental data source updates."""
    
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = Path(storage_path) if storage_path else Path("watermarks.json")
        self._watermarks = self._load_watermarks()
    
    def _load_watermarks(self) -> Dict[str, Any]:
        """Load watermarks from storage."""
        
        if self.storage_path.exists():
            try:
                with open(self.storage_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load watermarks: {e}")
        
        return {}
    
    def _save_watermarks(self):
        """Save watermarks to storage."""
        
        try:
            # Ensure directory exists
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.storage_path, 'w') as f:
                json.dump(self._watermarks, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save watermarks: {e}")
    
    def get_watermark(self, source_key: str) -> Optional[datetime]:
        """Get last watermark timestamp for a data source."""
        
        watermark_data = self._watermarks.get(source_key)
        if not watermark_data:
            return None
        
        try:
            timestamp_str = watermark_data.get("last_update")
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid watermark format for {source_key}: {e}")
        
        return None
    
    def set_watermark(self, source_key: str, timestamp: datetime, 
                     metadata: Dict[str, Any] = None):
        """Set watermark timestamp for a data source."""
        
        self._watermarks[source_key] = {
            "last_update": timestamp.isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
        
        self._save_watermarks()
        logger.info(f"Updated watermark for {source_key}: {timestamp}")
    
    def get_incremental_window(self, source_key: str, 
                              default_days_back: int = 7) -> datetime:
        """Get the start timestamp for incremental updates."""
        
        watermark = self.get_watermark(source_key)
        
        if watermark:
            # Start from last watermark minus a small overlap for safety
            return watermark - timedelta(hours=1)
        else:
            # First run - start from default days back
            return datetime.utcnow() - timedelta(days=default_days_back)
    
    def update_fetch_stats(self, source_key: str, records_fetched: int, 
                          fetch_duration_seconds: float):
        """Update fetch statistics for monitoring."""
        
        if source_key not in self._watermarks:
            self._watermarks[source_key] = {
                "last_update": None,
                "updated_at": datetime.utcnow().isoformat(),
                "metadata": {}
            }
        
        metadata = self._watermarks[source_key].get("metadata", {})
        
        # Update stats
        metadata.update({
            "last_fetch_records": records_fetched,
            "last_fetch_duration": fetch_duration_seconds,
            "last_fetch_timestamp": datetime.utcnow().isoformat(),
            "total_fetches": metadata.get("total_fetches", 0) + 1,
            "total_records": metadata.get("total_records", 0) + records_fetched
        })
        
        self._watermarks[source_key]["metadata"] = metadata
        self._save_watermarks()
    
    def get_all_watermarks(self) -> Dict[str, Any]:
        """Get all watermarks for monitoring."""
        return self._watermarks.copy()
    
    def reset_watermark(self, source_key: str):
        """Reset watermark for a data source (forces full refresh)."""
        
        if source_key in self._watermarks:
            del self._watermarks[source_key]
            self._save_watermarks()
            logger.info(f"Reset watermark for {source_key}")
    
    def cleanup_old_watermarks(self, days_old: int = 90):
        """Remove watermarks older than specified days."""
        
        cutoff = datetime.utcnow() - timedelta(days=days_old)
        to_remove = []
        
        for source_key, data in self._watermarks.items():
            try:
                updated_at = datetime.fromisoformat(data.get("updated_at", "1900-01-01"))
                if updated_at < cutoff:
                    to_remove.append(source_key)
            except ValueError:
                to_remove.append(source_key)  # Invalid format
        
        for source_key in to_remove:
            del self._watermarks[source_key]
            logger.info(f"Cleaned up old watermark: {source_key}")
        
        if to_remove:
            self._save_watermarks()
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get summary of watermark status for monitoring."""
        
        now = datetime.utcnow()
        summary = {
            "total_sources": len(self._watermarks),
            "recent_updates": 0,
            "stale_sources": 0,
            "sources": {}
        }
        
        for source_key, data in self._watermarks.items():
            try:
                last_update = None
                if data.get("last_update"):
                    last_update = datetime.fromisoformat(data["last_update"])
                
                updated_at = datetime.fromisoformat(data.get("updated_at", "1900-01-01"))
                
                hours_since_update = (now - updated_at).total_seconds() / 3600
                
                if hours_since_update < 24:
                    summary["recent_updates"] += 1
                elif hours_since_update > 168:  # 1 week
                    summary["stale_sources"] += 1
                
                metadata = data.get("metadata", {})
                summary["sources"][source_key] = {
                    "last_watermark": last_update.isoformat() if last_update else None,
                    "updated_at": updated_at.isoformat(),
                    "hours_since_update": round(hours_since_update, 1),
                    "total_records": metadata.get("total_records", 0),
                    "total_fetches": metadata.get("total_fetches", 0),
                    "last_fetch_records": metadata.get("last_fetch_records", 0)
                }
                
            except Exception as e:
                logger.warning(f"Error processing watermark {source_key}: {e}")
                summary["sources"][source_key] = {"error": str(e)}
        
        return summary
