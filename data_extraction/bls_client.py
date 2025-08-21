# bls_client.py
"""
This module provides a client for interacting with the Bureau of Labor Statistics (BLS) API.
It includes functionality for making requests, handling retries, and managing API keys.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from api_key import get_random_bls_key

BLS_V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

log = logging.getLogger("bls")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)


@dataclass
class BLSClient:
    """
    A client for the BLS API, responsible for making requests and handling responses.
    """

    api_key: Optional[str] = field(default_factory=get_random_bls_key)
    url: str = BLS_V2_URL
    session: requests.Session = field(default_factory=requests.Session)

    series_limit: int = 50  # per request (v2)
    years_limit: int = 20  # per request (v2)

    def __post_init__(self) -> None:
        """
        Initializes the client and sets up retry logic for requests.
        """
        if self.api_key is None:
            self.api_key = get_random_bls_key()
        self._configure_retries()

    def _configure_retries(self) -> None:
        """
        Configures retry logic for HTTP requests to handle transient errors.
        """
        try:
            retry = Retry(
                total=5,
                backoff_factor=1.2,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET", "POST"}),
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except Exception:
            log.warning("Could not configure retries for requests.")

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a POST request to the BLS API with the given payload.
        """
        headers = {"Content-Type": "application/json"}
        try:
            resp = self.session.post(self.url, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"BLS API HTTP error: {e} — body: {resp.text[:500]}") from e

        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f'BLS API returned status={data.get("status")}: {data.get("message")}')
        return data

    def _year_chunks(self, start: int, end: int) -> List[Tuple[int, int]]:
        """
        Splits a date range into smaller chunks to respect API limits.
        """
        if start > end:
            start, end = end, start
        years = end - start + 1
        if years <= self.years_limit:
            return [(start, end)]

        chunks = []
        s = start
        while s <= end:
            e = min(s + self.years_limit - 1, end)
            chunks.append((s, e))
            s = e + 1
        return chunks

    def fetch(
        self,
        series_ids: Iterable[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches data from the BLS API, automatically handling series and year limits.
        """
        sids = list(series_ids)
        if not sids:
            raise ValueError("No series IDs provided.")

        merged: Dict[str, Any] = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}, "message": None}

        series_chunks = [sids[i : i + self.series_limit] for i in range(0, len(sids), self.series_limit)]
        year_chunks = self._year_chunks(start_year, end_year) if (start_year and end_year) else [(start_year, end_year)]

        for sc in series_chunks:
            for ys, ye in year_chunks:
                payload: Dict[str, Any] = {"seriesid": sc}
                if self.api_key:
                    payload["registrationkey"] = self.api_key
                if ys is not None:
                    payload["startyear"] = int(ys)
                if ye is not None:
                    payload["endyear"] = int(ye)
                if catalog:
                    payload["catalog"] = True
                if calculations:
                    payload["calculations"] = True
                if annualaverage:
                    payload["annualaverage"] = True
                if aspects:
                    payload["aspects"] = True

                data = self._post(payload)
                chunk_series = data.get("Results", {}).get("series", [])
                merged["Results"]["series"].extend(chunk_series)
        return merged
    