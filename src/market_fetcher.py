#!/usr/bin/env python3
"""
market_fetcher.py — Dati mercato da API gratuite affidabili da GitHub Actions
- Forex: frankfurter.app (no key)
- Indici/commodities: coinbase per gold proxy + yfinance con fallback multipli
"""
import json, logging, os, requests
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / 'data' / 'market_data.json'

def get_forex(base, target):
    """frankfurter.app — sempre funzionante, no API key."""
    try:
        r = requests.get(
            f'https://api.frankfurter.app/latest?from={base}&to={target}',
            timeout=10
        )
        rate = r.json()['rates'][target]
        return f'{rate:.4f}', None
    except Exception as e:
        logger.error(f'Forex {base}/{target}: {e}')
        return 'N/A', None

def get_stooq(symbol):
    """
    Stooq.com — API CSV gratuita, no key, funziona da GitHub Actions.
    Supporta indici globali: ^SPX, ^NDX, ^FTW5 (STOXX), ^NKX (Nikkei), ecc.
    """
    try:
        url = f'https://stooq.com/q/d/l/?s={symbol}&i=d'
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        lines = r.text.strip().split('\n')
        if len(lines) < 2:
            return 'N/A', 'N/A'
        # Formato CSV: Date,Open,High,Low,Close,Volume
        last = lines[-1].split(',')
        prev = lines[-2].split(',') if len(lines) >= 3 else last
        close = float(last[4])
        prev_close = float(prev[4])
        change_pct = ((close - prev_close) / prev_close) * 100
        # Formattazione
        if close > 1000:
            val = f'{close:,.0f}'
        elif close > 10:
            val = f'{close:.2f}'
        else:
            val = f'{close:.4f}'
        return val, f'{change_pct:+.2f}%'
    except Exception as e:
        logger.error(f'Stooq {symbol}: {e}')
        return 'N/A', 'N/A'

def run():
    logger.info('📈 Recupero dati di mercato...')
    results = {}

    # EUR/USD — frankfurter
    val, _ = get_forex('EUR', 'USD')
    results['eur_usd'] = {'value': val, 'change': 'N/A'}
    logger.info(f'EUR/USD: {val}')

    # S&P 500
    val, chg = get_stooq('^spx')
    results['sp500'] = {'value': val, 'change': chg}
    logger.info(f'S&P 500: {val}')

    # VIX — simbolo corretto stooq
    val, chg = get_stooq('^vix')
    results['vix'] = {'value': val, 'change': chg}
    logger.info(f'VIX: {val}')

    # Gold — stooq simbolo diretto
    val, chg = get_stooq('xauusd')
    results['gold'] = {'value': f'${val}/oz' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Gold: {val}')

    # Brent — contratto generico stooq
    val, chg = get_stooq('cb.f')
    results['oil_brent'] = {'value': f'${val}' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Brent: {val}')

    # STOXX 600 — simbolo corretto
    val, chg = get_stooq('^ftw5')
    results['stoxx_600'] = {'value': val, 'change': chg}
    logger.info(f'STOXX 600: {val}')

    # Nikkei
    val, chg = get_stooq('^nkx')
    results['nikkei'] = {'value': val, 'change': chg}
    logger.info(f'Nikkei: {val}')

    # Shanghai
    val, chg = get_stooq('^shc')
    results['shanghai'] = {'value': val, 'change': chg}
    logger.info(f'Shanghai: {val}')

    # BTP 10Y
    val, chg = get_stooq('10yity.b')
    results['btp_10y'] = {'value': f'{val}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'BTP 10Y: {val}')

    # US 10Y
    val, chg = get_stooq('10yusy.b')
    results['us_10y'] = {'value': f'{val}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'US 10Y: {val}')

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info(f'✅ Dati mercato salvati: {OUTPUT_PATH}')
    return results

if __name__ == '__main__':
    run()
