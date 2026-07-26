"""Tests for bls_data client and parser."""

import pytest
from bls_data.parser import parse_results_to_df
from bls_data.api_key import get_random_bls_key


class TestParser:
    def test_empty_response(self):
        data = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}
        df = parse_results_to_df(data)
        assert len(df) == 0
        assert "series_id" in df.columns

    def test_single_series(self):
        data = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "CUUR0000SA0",
                        "catalog": {"series_title": "CPI All Items", "seasonality": "U"},
                        "data": [
                            {"year": "2024", "period": "M01", "periodName": "January", "value": "308.5", "footnotes": []},
                            {"year": "2024", "period": "M02", "periodName": "February", "value": "310.2", "footnotes": []},
                        ],
                    }
                ]
            },
        }
        df = parse_results_to_df(data)
        assert len(df) == 2
        assert df["value"].iloc[0] == 308.5
        assert df["series_title"].iloc[0] == "CPI All Items"

    def test_multiple_series(self):
        data = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "S1",
                        "catalog": {},
                        "data": [{"year": "2024", "period": "M01", "periodName": "Jan", "value": "100", "footnotes": []}],
                    },
                    {
                        "seriesID": "S2",
                        "catalog": {},
                        "data": [{"year": "2024", "period": "M01", "periodName": "Jan", "value": "200", "footnotes": []}],
                    },
                ]
            },
        }
        df = parse_results_to_df(data)
        assert len(df) == 2
        assert set(df["series_id"]) == {"S1", "S2"}

    def test_null_value(self):
        data = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "X",
                        "catalog": {},
                        "data": [{"year": "2024", "period": "M01", "periodName": "Jan", "value": "", "footnotes": []}],
                    }
                ]
            },
        }
        df = parse_results_to_df(data)
        assert df["value"].iloc[0] is None


class TestAPIKey:
    def test_no_keys_raises(self, monkeypatch):
        monkeypatch.delenv("BLS_API_KEY_0", raising=False)
        monkeypatch.delenv("BLS_API_KEY_1", raising=False)
        # Clear all BLS_API_KEY_ variables
        import os
        for k in list(os.environ):
            if k.startswith("BLS_API_KEY_"):
                monkeypatch.delenv(k, raising=False)
        with pytest.raises(ValueError, match="No BLS API keys"):
            get_random_bls_key()

    def test_returns_key(self, monkeypatch):
        import os
        # Clear all real keys loaded at import time, then set test key
        for k in list(os.environ):
            if k.startswith("BLS_API_KEY_"):
                monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("BLS_API_KEY_0", "test-key-abc")
        key = get_random_bls_key()
        assert key == "test-key-abc"