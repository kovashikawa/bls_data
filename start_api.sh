#!/bin/bash
# BLS Data API Startup Script

echo "🚀 Starting BLS Data API Server..."
echo "=================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run 'make setup' first."
    exit 1
fi

# Check if database is accessible
echo "🔌 Checking database connection..."
if ! ./venv/bin/python -c "from bls_api import db_config; conn = db_config.get_connection(); conn.close(); print('Database connection OK')" 2>/dev/null; then
    echo "❌ Database connection failed. Please check your database configuration."
    echo "   Make sure PostgreSQL is running and your .env file is configured correctly."
    exit 1
fi

echo "✅ Database connection successful"

# Start the API server
echo "🌐 Starting API server on http://localhost:8000"
echo "📚 API documentation available at http://localhost:8000/docs"
echo "🔍 Health check available at http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

./venv/bin/python bls_api.py

