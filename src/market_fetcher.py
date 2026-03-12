#!/usr/bin/env python3
import json, logging, os, requests
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / 'data' / 'market_data.json'
ALPHA_VANTAGE_KEY = os.environ.get('ALPHA_VANTAGE_KEY', '')

def get_forex(base='EUR', target='USD'):
    try:
        r = requests.get(f'https://api.frankfurter.app/latest?from={base}&to={target}', timeout=10)
        rate = r.json()['rates'][target]
        return f'{rate:.4f}', 'N/A'
    except Exception as e:
        logger.error(f'Forex error: {e}')
        return 'N/A', 'N/A'

def get_alpha_vantage(symbol):
    if not ALPHA_VANTAGE_KEY:
        return 'N/A', 'N/A'
    try:
        url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}'
        r = requests.get(url, timeout=15)
        quote = r.json().get('Global Quote', {})
        return quote.get('05. price', 'N/A'), quote.get('10. change percent', 'N/A')
    except Exception as e:
        logger.error(f'Alpha Vantage error {symbol}: {e}')
        return 'N/A', 'N/A'

def run():
    logger.info('📈 Recupero dati di mercato...')
    results = {}

    val, chg = get_forex('EUR', 'USD')
    results['eur_usd'] = {'value': val, 'change': chg}
    logger.info(f'EUR/USD: {val}')

    av_symbols = {
        'sp500':     'SPY',
        'vix':       'VIXY',
        'gold':      'GLD',
        'oil_brent': 'BNO',
    }
    for key, symbol in av_symbols.items():
        val, chg = get_alpha_vantage(symbol)
        results[key] = {'value': val, 'change': chg}
        logger.info(f'{key}: {val}')

    for key in ['btp_10y', 'stoxx_600', 'nikkei', 'shanghai', 'us_10y']:
        results[key] = {'value': 'N/A', 'change': 'N/A'}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ Dati mercato salvati: {OUTPUT_PATH}')
    return results

if __name__ == '__main__':
    run()
