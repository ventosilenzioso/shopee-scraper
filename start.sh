#!/bin/bash
playwright install chromium
playwright install-deps chromium
python3 -m uvicorn shopee_scraper:app --host 0.0.0.0 --port ${PORT:-8000}
