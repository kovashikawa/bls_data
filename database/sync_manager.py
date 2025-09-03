# bls_data/database/sync_manager.py

from datetime import datetime, timedelta
from typing import List, Dict, Set
import asyncio

class BLSSyncManager:
    def __init__(self, repository: BLSDataRepository, client):
        self.repository = repository
        self.client = client
    
    def get_sync_strategy(self, series_ids: List[str]) -> Dict[str, str]:
        """Determine sync strategy for each series:
        - 'skip': Data is fresh
        - 'incremental': Only fetch recent data
        - 'full': Full refresh needed
        """
        
    def sync_series_data(self, series_ids: List[str], 
                        force_refresh: bool = False) -> Dict:
        """Smart sync with minimal API calls"""
        
    def get_data_with_smart_sync(self, series_ids: List[str]) -> pd.DataFrame:
        """Get complete dataset with intelligent syncing"""
