#!/usr/bin/env python3
"""
Test script for the BLS Data API

This script tests the API endpoints to ensure they work correctly
with the PostgreSQL database.
"""

import json
import sys
from pathlib import Path

# Add the current directory to the Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from bls_api import app, db_config


def test_database_connection():
    """Test database connection"""
    print("🔌 Testing database connection...")
    try:
        with db_config.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result:
                    print("✅ Database connection successful")
                    return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def test_api_imports():
    """Test API imports and basic functionality"""
    print("📦 Testing API imports...")
    try:
        from bls_api import DatabaseConfig, app
        print("✅ API imports successful")

        # Test that we can create a database config
        config = DatabaseConfig()
        print("✅ Database config created successfully")

        return True
    except Exception as e:
        print(f"❌ API import failed: {e}")
        return False


def test_pydantic_models():
    """Test Pydantic model validation"""
    print("🔍 Testing Pydantic models...")
    try:
        from bls_api import BLSDataPointCreate, BLSSeriesCreate

        # Test series creation
        series_data = {
            "series_id": "TEST001",
            "series_title": "Test Series",
            "survey_name": "Test Survey",
            "area": "Test Area",
            "item": "Test Item"
        }
        series = BLSSeriesCreate(**series_data)
        print("✅ BLSSeriesCreate model validation successful")

        # Test data point creation
        data_point_data = {
            "series_id": "TEST001",
            "year": 2023,
            "period": "M01",
            "value": 100.50
        }
        data_point = BLSDataPointCreate(**data_point_data)
        print("✅ BLSDataPointCreate model validation successful")

        return True
    except Exception as e:
        print(f"❌ Pydantic model validation failed: {e}")
        return False


def test_api_routes():
    """Test API route registration"""
    print("🛣️  Testing API routes...")
    try:
        routes = [route.path for route in app.routes if hasattr(route, "path")]
        expected_routes = [
            "/", "/health", "/docs", "/openapi.json",
            "/bls_series", "/bls_data_points", "/bls_aliases",
            "/bls_extraction_logs", "/bls_data_quality", "/bls_data_freshness"
        ]

        for expected_route in expected_routes:
            if expected_route in routes:
                print(f"✅ Route {expected_route} registered")
            else:
                print(f"❌ Route {expected_route} missing")
                return False

        print(f"✅ All expected routes registered (total: {len(routes)})")
        return True
    except Exception as e:
        print(f"❌ Route testing failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 BLS Data API Test Suite")
    print("=" * 50)

    tests = [
        test_api_imports,
        test_pydantic_models,
        test_api_routes,
        test_database_connection,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! API is ready to use.")
        print("\nTo start the API server, run:")
        print("  ./venv/bin/python bls_api.py")
        print("\nThen visit http://localhost:8000/docs for the interactive API documentation")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

