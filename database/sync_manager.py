# bls_data/database/sync_manager.py

import asyncio
from datetime import datetime, timedelta

import pandas as pd

from .repository import BLSDataRepository


class BLSSyncManager:
    def __init__(self, repository: BLSDataRepository, client):
        self.repository = repository
        self.client = client

    def get_sync_strategy(self, series_ids: list[str]) -> dict[str, str]:
        """Determine sync strategy for each series:
        - 'skip': Data is fresh
        - 'incremental': Only fetch recent data
        - 'full': Full refresh needed
        """

    def sync_series_data(
        self, series_ids: list[str], force_refresh: bool = False
    ) -> dict:
        """Smart sync with minimal API calls"""

    def get_data_with_smart_sync(self, series_ids: list[str]) -> pd.DataFrame:
        """Get complete dataset with intelligent syncing"""
