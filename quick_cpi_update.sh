#!/bin/bash
# Quick CPI Update - One-liner command
# Updates the database with the latest CPI data for the last 2 years

cd /Users/rafaelkovashikawa/Downloads/projects/bls_food/bls_data && \
./venv/bin/python -m data_extraction.main cpi_all_items \
  --start $(python3 -c "import datetime; print(datetime.datetime.now().year - 2)") \
  --end $(python3 -c "import datetime; print(datetime.datetime.now().year)") \
  --use-database \
  --force-refresh \
  --catalog \
  --log INFO
