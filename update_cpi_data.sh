#!/bin/bash
# Update BLS CPI Database with Latest Data
# This script fetches the latest CPI data and updates the database

set -e  # Exit on any error

echo "🔄 Updating BLS CPI Database with Latest Data"
echo "=============================================="

# Check if we're in the correct directory
if [ ! -f "bls_api.py" ]; then
    echo "❌ Error: Please run this script from the bls_data directory"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Error: Virtual environment not found. Please run 'make setup' first."
    exit 1
fi

# Check database connection
echo "🔌 Checking database connection..."
if ! ./venv/bin/python -c "from database.config import DatabaseConfig; config = DatabaseConfig(); print('✅ Database connected' if config.check_connection() else '❌ Database connection failed')" 2>/dev/null; then
    echo "❌ Database connection failed. Please check your database configuration."
    exit 1
fi

echo "✅ Database connection successful"

# Get current year for end date
CURRENT_YEAR=$(date +%Y)
START_YEAR=$((CURRENT_YEAR - 2))  # Get last 2 years of data

echo "📅 Fetching CPI data from $START_YEAR to $CURRENT_YEAR"

# Method 1: Update all CPI US City Average series (comprehensive)
echo ""
echo "🎯 Method 1: Updating all CPI US City Average series..."
./venv/bin/python scripts/extract_all_cpi_us_city_avg.py --use-database --force-refresh --log INFO

# Method 2: Update specific key CPI series with latest data
echo ""
echo "🎯 Method 2: Updating key CPI series with latest data..."
./venv/bin/python -m data_extraction.main \
    cpi_all_items \
    --start $START_YEAR \
    --end $CURRENT_YEAR \
    --use-database \
    --force-refresh \
    --catalog \
    --log INFO

# Method 3: Update comprehensive CPI data (if you want historical data)
echo ""
echo "🎯 Method 3: Updating comprehensive CPI data..."
./venv/bin/python scripts/extract_cpi_comprehensive.py \
    --start-year $START_YEAR \
    --end-year $CURRENT_YEAR \
    --strategy recent \
    --max-series 100

# Check database stats after update
echo ""
echo "📊 Database Statistics After Update:"
./venv/bin/python -c "
from database.utils import get_database_stats
stats = get_database_stats()
print(f'✅ Total series: {stats.get(\"total_series\", 0):,}')
print(f'✅ Total data points: {stats.get(\"total_data_points\", 0):,}')
print(f'✅ Last updated: {stats.get(\"last_updated\", \"Unknown\")}')
"

echo ""
echo "🎉 CPI database update completed successfully!"
echo ""
echo "💡 To view the updated data:"
echo "   • API: http://localhost:8000/docs"
echo "   • Database: Check bls_series and bls_data_points tables"
echo "   • Test: ./venv/bin/python scripts/test_cpi_extraction.py"
