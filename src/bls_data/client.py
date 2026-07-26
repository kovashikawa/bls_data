"""BLS API v2 client — series fetching with retries and automatic chunking."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .api_key import get_random_bls_key

BLS_V2_URL = "https://api.bls.gov/publicAPI/v2"
log = logging.getLogger(__name__)


@dataclass
class BLSClient:
    """BLS API v2 client with retries, chunking, and key rotation.

    Handles:
    - Automatic series/year chunking (50 series, 20 years per request)
    - Exponential backoff retry on 429/5xx
    - Random API key rotation from env
    """

    api_key: Optional[str] = field(default_factory=get_random_bls_key)
    url: str = BLS_V2_URL
    session: requests.Session = field(default_factory=requests.Session)
    series_limit: int = 50
    years_limit: int = 20

    def __post_init__(self) -> None:
        if self.api_key is None:
            self.api_key = get_random_bls_key()
        self._configure_retries()

    def _configure_retries(self) -> None:
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

    def fetch(
        self,
        series_ids: list[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
    ) -> dict[str, Any]:
        """Fetch BLS time-series data, auto-chunking series and years."""
        sids = list(series_ids)
        if not sids:
            raise ValueError("No series IDs provided.")

        merged: dict[str, Any] = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": []},
        }

        series_chunks = [
            sids[i : i + self.series_limit]
            for i in range(0, len(sids), self.series_limit)
        ]
        year_chunks = (
            self._year_chunks(start_year, end_year)
            if (start_year is not None and end_year is not None)
            else [(start_year, end_year)]
        )

        for sc in series_chunks:
            for ys, ye in year_chunks:
                data = self._request(
                    sc, ys, ye, catalog, calculations, annualaverage, aspects
                )
                merged["Results"]["series"].extend(
                    data.get("Results", {}).get("series", [])
                )
        return merged

    def _request(
        self, series_ids, start_year, end_year, catalog, calculations, annualaverage, aspects
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"seriesid": series_ids}
        if self.api_key:
            payload["registrationkey"] = self.api_key
        if start_year is not None:
            payload["startyear"] = int(start_year)
        if end_year is not None:
            payload["endyear"] = int(end_year)
        if catalog:
            payload["catalog"] = True
        if calculations:
            payload["calculations"] = True
        if annualaverage:
            payload["annualaverage"] = True
        if aspects:
            payload["aspects"] = True

        headers = {"Content-Type": "application/json"}
        try:
            resp = self.session.post(
                f"{self.url}/timeseries/data/", json=payload, headers=headers, timeout=60
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            body = resp.text[:500] if resp is not None else ""
            raise RuntimeError(f"BLS API HTTP error: {e} — {body}") from e

        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(
                f"BLS API returned {data.get('status')}: {data.get('message')}"
            )
        return data

    def _year_chunks(self, start: int, end: int) -> list[tuple[int, int]]:
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


    def list_surveys(self) -> list[dict[str, str]]:
        """Return all BLS surveys with abbreviations and names."""
        resp = self.session.get(f"{self.url}/surveys", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f"BLS surveys failed: {data.get('message')}")
        return data.get("Results", {}).get("survey", [])

    def get_survey_info(self, abbreviation: str) -> dict[str, Any]:
        """Return metadata and popular series for a specific survey."""
        resp = self.session.get(f"{self.url}/surveys/{abbreviation}", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f"Survey '{abbreviation}' not found")
        return data.get("Results", {}).get("survey", [{}])[0]

    def get_popular_series(self, survey: Optional[str] = None) -> list[dict[str, str]]:
        """Return popular BLS series, optionally filtered by survey abbreviation."""
        url = f"{self.url}/timeseries/popular"
        if survey:
            url += f"?survey={survey}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f"Popular series failed: {data.get('message')}")
        return data.get("Results", {}).get("series", [])


def fetch_bls_data(
    series_ids: list[str],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    *,
    client: Optional[BLSClient] = None,
    **kwargs,
) -> dict[str, Any]:
    """Convenience function: fetch BLS data with a default client."""
    client = client or BLSClient()
    return client.fetch(series_ids, start_year, end_year, **kwargs)