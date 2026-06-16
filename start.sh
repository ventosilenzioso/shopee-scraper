#!/bin/bash
set -e

# Pakai python dari venv Railway
PYTHON=/app/.venv/bin/python

# Install ke venv yang sama
$PYTHON -m pip install fastapi uvicorn playwright

# Install Chromium
$PYTHON -m playwright install chromium
$PYTHON -m playwright install-deps chromium

# Start server
$PYTHON -m uvicorn shopee_scraper:app --host 0.0.0.0 --port ${PORT:-8000}
