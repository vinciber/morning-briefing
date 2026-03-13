import os
import requests
import json
from datetime import datetime, timezone, timedelta

FRED_API_KEY = '65b70d77c55fd6ff154fd9db0ff6f568'
BASE = 'https://api.stlouisfed.org/fred/series/observations'

series_to_check = {
    'PAYEMS': 'Non-Farm Payrolls',
    'CPIAUCSL': 'CPI YoY USA',
    'UNRATE': 'Unemployment Rate'
}

for series_id, label in series_to_check.items():
    print(f"\n--- Checking {label} ({series_id}) ---")
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'sort_order': 'desc',
        'limit': 5
    }
    r = requests.get(BASE, params=params)
    obs = r.json().get('observations', [])
    for o in obs:
        print(f"Date: {o['date']}, Value: {o['value']}")
